/*
 * Zynaptic Stash - An asynchronous persistence framework for Java.
 * 
 * Copyright (c) 2009-2019, Zynaptic Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * Please visit www.zynaptic.com or contact reaction@zynaptic.com if you need
 * additional information or have any questions.
 */

package com.zynaptic.stash.core;

import java.io.IOException;
import java.io.PrintStream;
import java.io.Serializable;
import java.net.URI;
import java.util.LinkedList;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.zynaptic.reaction.Deferrable;
import com.zynaptic.reaction.Deferred;
import com.zynaptic.reaction.Reactor;
import com.zynaptic.stash.InvalidStashIdException;
import com.zynaptic.stash.StashObjectEntry;
import com.zynaptic.stash.StashService;
import com.zynaptic.stash.StashTimeSeriesEntry;

/**
 * Provides core implementation of the common stash service functionality. This
 * processes backend-specific operations via the appropriate implementation of
 * the {@link StashServiceBackend} interface.
 * 
 * @author Chris Holgate
 */
public final class StashServiceCore implements StashService {

  // Precompiled pattern used for stash identifier validation.
  private static final Pattern STASH_ID_VALIDATION_PATTERN = Pattern
      .compile("^([\\p{Alnum}_-]+)(\\.[\\p{Alnum}_-]+)*$");

  // Define state space for stash service operation.
  private static enum StashState {
    INACTIVE, STARTING_UP, ACTIVE, SHUTTING_DOWN
  };

  // Handle on the reactor service which is used for event management.
  private final Reactor reactor;

  // Handle on the implementation specific stash backend.
  private final StashServiceBackend stashServiceBackend;

  // Map of stash identifiers to their associated stash entry records.
  private Map<String, StashEntryRecord<? extends Serializable>> stashEntryMap;

  // Internal state variable which is used to determine whether the stash is
  // currently active.
  private StashState stashState;

  /**
   * Default constructor assigns the appropriate implementation specific backend
   * for use by the stash service.
   * 
   * @param reactor This is the reactor service which is used for event
   *   management.
   * @param stashServiceBackend This is the implementation specific backend which
   *   is to be used by the stash service.
   */
  StashServiceCore(Reactor reactor, StashServiceBackend stashServiceBackend) {
    this.reactor = reactor;
    this.stashServiceBackend = stashServiceBackend;
    this.stashEntryMap = null;
    this.stashState = StashState.INACTIVE;
  }

  /*
   * Perform stash service setup on activation.
   */
  synchronized Deferred<StashService> setup() {
    if (stashState != StashState.INACTIVE) {
      throw new IllegalStateException("Stash service must be inactive when initiating setup.");
    }
    stashState = StashState.STARTING_UP;
    return stashServiceBackend.setupBackend().addDeferrable(new SetupStashBackendHandler());
  }

  /*
   * Perform stash service teardown on deactivation.
   */
  synchronized Deferred<Boolean> teardown() {
    if (stashState != StashState.ACTIVE) {
      throw new IllegalStateException("Stash service must be active when initiating teardown.");
    }
    stashState = StashState.SHUTTING_DOWN;
    return stashServiceBackend.teardownBackend().addDeferrable(new TeardownStashBackendHandler());
  }

  /*
   * Implements StashService.getStashUri()
   */
  public URI getStashUri() {
    return stashServiceBackend.getStashUri();
  }

  /*
   * Implements StashService.registerStashObjectId(...)
   */
  public synchronized <T extends Serializable> Deferred<StashObjectEntry<T>> registerStashObjectId(String stashId,
      String stashName, T defaultData) {
    if (stashState != StashState.ACTIVE) {
      throw new IllegalStateException("Stash service is not active on stash object entry ID registration.");
    } else if (defaultData == null) {
      throw new NullPointerException("No default data value specified on stash object entry ID registration.");
    }

    // Perform stash ID sanity checking.
    if (!stashIdIsValid(stashId)) {
      return reactor.failDeferred(
          new InvalidStashIdException("Invalid stash ID specified on stash object entry ID registration."));
    } else if (stashEntryMap.containsKey(stashId)) {
      return reactor.failDeferred(
          new InvalidStashIdException("Duplicate stash ID specified on stash object entry ID registration."));
    }

    // Insert placeholder record to prevent race conditions with duplicate stash
    // IDs. Then perform implementation specific setup.
    stashEntryMap.put(stashId, new StashPlaceholderRecord());
    return stashServiceBackend.initStashObjectEntry(stashId, stashName, defaultData)
        .addDeferrable(new SetupStashObjectEntryHandler<T>());
  }

  /*
   * Implements StashService.registerStashTimeSeriesId(...)
   */
  public synchronized <T extends Serializable> Deferred<StashTimeSeriesEntry<T>> registerStashTimeSeriesId(
      String stashId, String stashName, T defaultData, int dataBlockPeriod, int dataBlockSize) {
    if (stashState != StashState.ACTIVE) {
      throw new IllegalStateException("Stash service is not active on stash time series entry ID registration.");
    } else if (defaultData == null) {
      throw new NullPointerException("No default data value specified on stash time series entry ID registration.");
    }

    // Perform stash ID sanity checking.
    if (!stashIdIsValid(stashId)) {
      return reactor.failDeferred(
          new InvalidStashIdException("Invalid stash ID specified on stash time series entry ID registration."));
    } else if (stashEntryMap.containsKey(stashId)) {
      return reactor.failDeferred(
          new InvalidStashIdException("Duplicate stash ID specified on stash time series entry ID registration."));
    }

    // Insert placeholder record to prevent race conditions with duplicate stash
    // IDs. Then perform implementation specific setup.
    stashEntryMap.put(stashId, new StashPlaceholderRecord());
    return stashServiceBackend.initStashTimeSeriesEntry(stashId, stashName, defaultData, dataBlockPeriod, dataBlockSize)
        .addDeferrable(new SetupStashTimeSeriesEntryHandler<T>());
  }

  /*
   * Implements StashService.hasStashId(...)
   */
  public synchronized boolean hasStashId(String stashId) {
    return stashEntryMap.containsKey(stashId);
  }

  /*
   * Implements StashService.invalidateStashId(...)
   */
  public synchronized Deferred<Boolean> invalidateStashId(String stashId) {
    if (stashState != StashState.ACTIVE) {
      throw new IllegalStateException("Stash service is not active on requesting stash ID invalidation.");
    } else if (!stashIdIsValid(stashId)) {
      return reactor
          .failDeferred(new InvalidStashIdException("Invalid stash ID specified on stash entry invalidation."));
    }

    // Attempt to remove the stash entry, returning a status value of 'false' if
    // it is not present.
    StashEntryRecord<?> stashEntryRecord = stashEntryMap.remove(stashId);
    if (stashEntryRecord != null) {
      return stashEntryRecord.invalidate();
    } else {
      return reactor.callDeferred(false);
    }
  }

  /*
   * Implements StashService.getStashObjectEntry(...)
   */
  public <T extends Serializable> StashObjectEntry<T> getStashObjectEntry(String stashId, Class<T> dataClass)
      throws InvalidStashIdException, ClassNotFoundException {
    return getStashObjectEntry(stashId, dataClass, dataClass.getClassLoader());
  }

  /*
   * Implements getStashObjectEntry(...)
   */
  public synchronized <T extends Serializable> StashObjectEntry<T> getStashObjectEntry(String stashId,
      Class<T> dataClass, ClassLoader dataClassLoader) throws InvalidStashIdException, ClassNotFoundException {
    if (stashState != StashState.ACTIVE) {
      throw new IllegalStateException("Stash service is not active on requesting stash object entry.");
    } else if (!stashIdIsValid(stashId)) {
      throw new InvalidStashIdException("Invalid stash ID format detected on requesting stash object entry.");
    }

    // Process requests for missing or inconsistent stash entries.
    StashEntryRecord<?> stashEntryRecord = stashEntryMap.get(stashId);
    if (stashEntryRecord == null) {
      return null;
    } else if ((stashEntryRecord.getMetadata().getRecordFormat() != StashObjectMetadata.RecordFormat.RAW_OBJECT)) {
      throw new InvalidStashIdException("Stash ID '" + stashId + "' does not reference a valid stash object entry.");
    }

    // Performs runtime setup of data class loader, which is then used for
    // automatically decoding the default data value held in the stash entry
    // metadata. The default data is then checked for consistency against the
    // expected data class definition.
    stashEntryRecord.setDataClassLoader(dataClassLoader);
    if (!dataClass.isInstance(stashEntryRecord.getDefaultData())) {
      throw new ClassNotFoundException("Invalid stash object type requested for '"
          + stashEntryRecord.getMetadata().getStashId() + "' (was " + dataClass.getCanonicalName() + ", expected "
          + stashEntryRecord.getDefaultData().getClass().getCanonicalName() + ")");
    }
    return convertToObjectEntry(stashEntryRecord);
  }

  /*
   * Implements StashService.getStashTimeSeriesEntry(...)
   */
  public <T extends Serializable> StashTimeSeriesEntry<T> getStashTimeSeriesEntry(String stashId, Class<T> dataClass)
      throws InvalidStashIdException, ClassNotFoundException {
    return getStashTimeSeriesEntry(stashId, dataClass, dataClass.getClassLoader());
  }

  /*
   * Implements StashService.getStashTimeSeriesEntry(...)
   */
  public synchronized <T extends Serializable> StashTimeSeriesEntry<T> getStashTimeSeriesEntry(String stashId,
      Class<T> dataClass, ClassLoader dataClassLoader) throws InvalidStashIdException, ClassNotFoundException {
    if (stashState != StashState.ACTIVE) {
      throw new IllegalStateException("Stash service is not active on requesting stash time series entry.");
    } else if (!stashIdIsValid(stashId)) {
      throw new InvalidStashIdException("Invalid stash ID format detected on requesting stash time series entry.");
    }

    // Process requests for missing or inconsistent stash entries.
    StashEntryRecord<?> stashEntryRecord = stashEntryMap.get(stashId);
    if (stashEntryRecord == null) {
      return null;
    } else if ((stashEntryRecord.getMetadata().getRecordFormat() != StashObjectMetadata.RecordFormat.TIME_SERIES)) {
      throw new InvalidStashIdException(
          "Stash ID '" + stashId + "' does not reference a valid stash time series entry.");
    }

    // Performs runtime setup of data class loader, which is then used for
    // automatically decoding the default data value held in the stash entry
    // metadata. The default data is then checked for consistency against the
    // expected data class definition.
    stashEntryRecord.setDataClassLoader(dataClassLoader);
    if (!dataClass.isInstance(stashEntryRecord.getDefaultData())) {
      throw new ClassNotFoundException("Invalid stash time series type requested for '"
          + stashEntryRecord.getMetadata().getStashId() + "' (was " + dataClass.getCanonicalName() + ", expected "
          + stashEntryRecord.getDefaultData().getClass().getCanonicalName() + ")");
    }
    return convertToTimeSeriesEntry(stashEntryRecord);
  }

  /*
   * Implements StashService.reportStashContents(...)
   */
  public void reportStashContents(PrintStream printStream) throws IOException {
    LinkedList<StashEntryRecord<? extends Serializable>> stashRecordList;

    // The contents of the table are copied into an independent linked list
    // while holding the synchronization lock, creating a snapshot of the table.
    synchronized (this) {
      if (stashState != StashState.ACTIVE) {
        throw new IllegalStateException("Stash service is not active on requesting stash contents report.");
      }
      stashRecordList = new LinkedList<StashEntryRecord<? extends Serializable>>(stashEntryMap.values());
    }

    // The individual stash record contents are reported outside the
    // synchronization lock to avoid holding up subsequent operations while
    // blocking I/O is in progress.
    printStream.println("Current state of stash " + stashServiceBackend.getStashUri().toString());
    printStream.println("Data cache timeout : " + getCacheTimeout() + "ms");
    for (StashEntryRecord<?> stashEntryRecord : stashRecordList) {
      stashEntryRecord.reportRecordContents(printStream);
    }
  }

  /*
   * Implements StashService.setCacheTimeout(...)
   */
  public StashService setCacheTimeout(int cacheTimeout) {
    stashServiceBackend.setCacheTimeout(cacheTimeout);
    return this;
  }

  /*
   * Implements StashService.getCacheTimeout()
   */
  public int getCacheTimeout() {
    return stashServiceBackend.getCacheTimeout();
  }

  /*
   * Callback handler on completing setup of implementation specific stash
   * backend.
   */
  private final class SetupStashBackendHandler
      implements Deferrable<Map<String, StashEntryRecord<? extends Serializable>>, StashService> {
    public StashService onCallback(Deferred<Map<String, StashEntryRecord<? extends Serializable>>> deferred,
        Map<String, StashEntryRecord<? extends Serializable>> updatedStashEntryMap) {
      synchronized (StashServiceCore.this) {
        stashEntryMap = updatedStashEntryMap;
        stashState = StashState.ACTIVE;
      }
      return StashServiceCore.this;
    }

    public StashService onErrback(Deferred<Map<String, StashEntryRecord<? extends Serializable>>> deferred,
        Exception error) throws Exception {
      synchronized (StashServiceCore.this) {
        stashState = StashState.INACTIVE;
      }
      throw error;
    }
  }

  /*
   * Callback handler on completing teardown of implementation specific stash
   * backend.
   */
  private final class TeardownStashBackendHandler implements Deferrable<Boolean, Boolean> {
    public Boolean onCallback(Deferred<Boolean> deferred, Boolean status) {
      synchronized (StashServiceCore.this) {
        stashEntryMap = null;
        stashState = StashState.INACTIVE;
      }
      return status;
    }

    public Boolean onErrback(Deferred<Boolean> deferred, Exception error) throws Exception {
      synchronized (StashServiceCore.this) {
        stashEntryMap = null;
        stashState = StashState.INACTIVE;
      }
      throw error;
    }
  }

  /*
   * Callback handler on creating a new stash object record. Updates the local
   * stash entry map.
   */
  private final class SetupStashObjectEntryHandler<T extends Serializable>
      implements Deferrable<StashObjectEntry<T>, StashObjectEntry<T>> {
    public StashObjectEntry<T> onCallback(Deferred<StashObjectEntry<T>> deferred,
        StashObjectEntry<T> stashObjectEntry) {
      synchronized (StashServiceCore.this) {
        StashEntryRecord<?> stashEntryRecord = (StashEntryRecord<?>) stashObjectEntry;
        stashEntryMap.put(stashEntryRecord.getMetadata().getStashId(), stashEntryRecord);
      }
      return stashObjectEntry;
    }

    public StashObjectEntry<T> onErrback(Deferred<StashObjectEntry<T>> deferred, Exception error) throws Exception {
      throw error;
    }
  }

  /*
   * Callback handler on creating a new stash time series record. Updates the
   * local stash entry map.
   */
  private final class SetupStashTimeSeriesEntryHandler<T extends Serializable>
      implements Deferrable<StashTimeSeriesEntry<T>, StashTimeSeriesEntry<T>> {
    public StashTimeSeriesEntry<T> onCallback(Deferred<StashTimeSeriesEntry<T>> deferred,
        StashTimeSeriesEntry<T> stashTimeSeriesEntry) {
      synchronized (StashServiceCore.this) {
        StashEntryRecord<?> stashEntryRecord = (StashEntryRecord<?>) stashTimeSeriesEntry;
        stashEntryMap.put(stashEntryRecord.getMetadata().getStashId(), stashEntryRecord);
      }
      return stashTimeSeriesEntry;
    }

    public StashTimeSeriesEntry<T> onErrback(Deferred<StashTimeSeriesEntry<T>> deferred, Exception error)
        throws Exception {
      throw error;
    }
  }

  /*
   * Validate a supplied stash identifier to ensure that it meets the required
   * format, which is a set of dot separated groups of alphanumeric characters.
   */
  private static boolean stashIdIsValid(String stashId) {
    Matcher matcher = STASH_ID_VALIDATION_PATTERN.matcher(stashId);
    return matcher.matches();
  }

  /*
   * Perform unchecked casting from an internal stash entry record interface to a
   * public stash object entry interface.
   */
  @SuppressWarnings("unchecked")
  private static <T extends Serializable> StashObjectEntry<T> convertToObjectEntry(
      StashEntryRecord<?> stashEntryRecord) {
    return (StashObjectEntry<T>) stashEntryRecord;
  }

  /*
   * Perform unchecked casting from an internal stash entry record interface to a
   * public stash time series entry interface.
   */
  @SuppressWarnings("unchecked")
  private static <T extends Serializable> StashTimeSeriesEntry<T> convertToTimeSeriesEntry(
      StashEntryRecord<?> stashEntryRecord) {
    return (StashTimeSeriesEntry<T>) stashEntryRecord;
  }
}
