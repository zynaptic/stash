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

package com.zynaptic.stash.core.file;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import com.zynaptic.reaction.Deferred;
import com.zynaptic.reaction.Reactor;
import com.zynaptic.reaction.Threadable;
import com.zynaptic.stash.StashObjectEntry;
import com.zynaptic.stash.StashTimeSeriesEntry;
import com.zynaptic.stash.core.StashEntryRecord;
import com.zynaptic.stash.core.StashObjectMetadata;
import com.zynaptic.stash.core.StashServiceBackend;
import com.zynaptic.stash.core.StashTimeSeriesMetadata;

/**
 * Implements the stash backend functionality for stash services that use the
 * local filesystem as the persistent store.
 * 
 * @author Chris Holgate
 */
public final class StashFileServiceBackend implements StashServiceBackend {

  // Define the default read cache timeout period to be used.
  private static final int DEFAULT_CACHE_TIMEOUT_PERIOD = 60 * 1000;

  // Handle on the reactor component for event management.
  private final Reactor reactor;

  // This is the URI which defines the location of the persistent store on the
  // local filesystem.
  private final URI stashUri;

  // File object used for accessing the stash index directory.
  private File stashIndexDir = null;

  // File object used for accessing the stash data directory.
  private File stashDataDir = null;

  // The cache timeout period after which unused data blocks are discarded from
  // local memory.
  private int cacheTimeout = DEFAULT_CACHE_TIMEOUT_PERIOD;

  /**
   * Default constructor specifies the configuration options for filesystem backed
   * persistent store.
   * 
   * @param reactor This is a handle on the reactor component which is used for
   *   event management.
   * @param stashUri This is the URI which specifies the location of the
   *   persistent store on the local filesystem.
   */
  public StashFileServiceBackend(Reactor reactor, URI stashUri) {
    this.reactor = reactor;
    this.stashUri = stashUri;
  }

  /*
   * Implements StashServiceBackend.getStashUri()
   */
  public URI getStashUri() {
    return stashUri;
  }

  /*
   * Implements StashServiceBackend.setupBackend()
   */
  public synchronized Deferred<Map<String, StashEntryRecord<? extends Serializable>>> setupBackend() {

    // Derive the stash directory name from the URI.
    String stashDirName = stashUri.getPath();

    // Identify the various subdirectories within the root stash directory.
    stashIndexDir = new File(stashDirName, "index");
    stashDataDir = new File(stashDirName, "data");

    // Attempt to get the canonical form of the various directories.
    try {
      stashIndexDir = stashIndexDir.getCanonicalFile();
      stashDataDir = stashDataDir.getCanonicalFile();
    } catch (Exception error) {
      return reactor.failDeferred(error);
    }

    // Determine whether the specified stash directory is a new or existing data
    // stash. This is done by checking for the existence of the 'index'
    // subdirectory.
    if (stashIndexDir.exists()) {
      return reactor.runThread(new WarmStartupTask(), null);
    } else {
      return reactor.runThread(new ColdStartupTask(), null);
    }
  }

  /*
   * Implements StashServiceBackend.teardownBackend()
   */
  public synchronized Deferred<Boolean> teardownBackend() {
    stashIndexDir = null;
    stashDataDir = null;
    return reactor.callDeferred(true);
  }

  /*
   * Implements StashServiceBackend.initStashObjectEntry(...)
   */
  public <T extends Serializable> Deferred<StashObjectEntry<T>> initStashObjectEntry(String stashId, String stashName,
      T initValue) {

    // Create new stash object record and delegate file system setup to it.
    StashObjectMetadata<T> stashEntryMetadata = new StashObjectMetadata<T>(StashObjectMetadata.RecordFormat.RAW_OBJECT,
        stashId, stashName, initValue);
    return StashFileObjectRecord.initRecord(reactor, this, stashEntryMetadata, initValue);
  }

  /*
   * Implements StashServiceBackend.initStashTimeSeriesEntry(...)
   */
  public <T extends Serializable> Deferred<StashTimeSeriesEntry<T>> initStashTimeSeriesEntry(String stashId,
      String stashName, T initValue, int dataBlockPeriod, int dataBlockSize) {

    // Create new stash time series record and delegate file system setup to it.
    StashTimeSeriesMetadata<T> stashEntryMetadata = new StashTimeSeriesMetadata<T>(
        StashObjectMetadata.RecordFormat.TIME_SERIES, stashId, stashName, initValue, dataBlockPeriod, dataBlockSize);
    return StashFileTimeSeriesRecord.initRecord(reactor, this, stashEntryMetadata, initValue);
  }

  /*
   * Implements StashServiceBackend.setCacheTimeout(...)
   */
  public synchronized void setCacheTimeout(int cacheTimeout) {
    if (cacheTimeout < 0) {
      throw new IllegalArgumentException("Invalid data cache timeout period.");
    }
    this.cacheTimeout = cacheTimeout;
  }

  /*
   * Implements StashServiceBackend.getCacheTimeout()
   */
  public synchronized int getCacheTimeout() {
    return cacheTimeout;
  }

  /*
   * Get a handle on the root directory which is used to store the stash index
   * information.
   */
  synchronized File getStashIndexDir() {
    return stashIndexDir;
  }

  /*
   * Get a handle on the root directory which is used to store the stash data.
   */
  synchronized File getStashDataDir() {
    return stashDataDir;
  }

  /*
   * Threadable cold startup task. This threadable startup task runs when the
   * stash is initially set up, since the setup process makes use of blocking I/O.
   */
  private final class ColdStartupTask
      implements Threadable<Boolean, Map<String, StashEntryRecord<? extends Serializable>>> {
    public Map<String, StashEntryRecord<? extends Serializable>> run(Boolean status) throws IOException {

      // Start by creating the empty stash index directory.
      if (!getStashIndexDir().mkdirs())
        throw new IOException("Unable to create index directory '" + getStashIndexDir().toString() + "'.");

      // Create the empty stash data directory.
      if (!getStashDataDir().mkdirs())
        throw new IOException("Unable to create data directory '" + getStashDataDir().toString() + "'.");

      // Create empty collection of stash entry records.
      return new HashMap<String, StashEntryRecord<? extends Serializable>>();
    }
  }

  /*
   * Threadable warm startup task. This threadable startup task runs when the
   * stash service is being brought up using an existing persistent stash.
   */
  private final class WarmStartupTask
      implements Threadable<Boolean, Map<String, StashEntryRecord<? extends Serializable>>> {
    public Map<String, StashEntryRecord<? extends Serializable>> run(Boolean status)
        throws IOException, ClassNotFoundException {

      // Get a list of stash table entries. These are named using the unique
      // stash identifiers of the various stashed resources.
      HashSet<String> stashIdSet = new HashSet<String>();
      String[] stashIds = getStashIndexDir().list();
      for (int i = 0; i < stashIds.length; i++) {
        stashIdSet.add(stashIds[i]);
      }

      // Process the stash table entries, building the collection of valid stash
      // entry records.
      Map<String, StashEntryRecord<? extends Serializable>> stashEntryMap = new HashMap<String, StashEntryRecord<? extends Serializable>>();
      for (String stashId : stashIdSet) {
        File stashRecordFile = new File(getStashIndexDir(), stashId);

        // Clean out any stale invalidated table entries and their associated
        // data. Invalidated entries are identified using a leading ~ character
        // in the index filename.
        if (stashId.charAt(0) == '~') {
          String originalStashId = stashId.substring(1);
          if (stashIdSet.contains(originalStashId)) {
            stashRecordFile.delete();
          } else {
            cleanupInvalidatedEntry(originalStashId, stashRecordFile);
          }
        }

        // Attempt to recover stash entry metadata and insert it into the stash
        // entry map.
        else {
          ObjectInputStream stashRecordInputStream = null;
          try {
            stashRecordInputStream = new ObjectInputStream(
                new BufferedInputStream(new FileInputStream(stashRecordFile)));

            // Extract metadata to determine stash record format.
            Object rawObject = stashRecordInputStream.readObject();
            if (!StashObjectMetadata.class.isAssignableFrom(rawObject.getClass()))
              throw new ClassCastException("Invalid metadata type for stash entry '" + stashId + "' (was "
                  + rawObject.getClass().getSimpleName() + ").");
            StashObjectMetadata<?> stashEntryMetadata = (StashObjectMetadata<?>) rawObject;

            // Rebuild and insert the appropriate stash entry records into the
            // map.
            if (stashEntryMetadata.getRecordFormat() == StashObjectMetadata.RecordFormat.RAW_OBJECT) {
              stashEntryMap.put(stashId,
                  StashFileObjectRecord.rebuildRecord(reactor, StashFileServiceBackend.this, stashEntryMetadata));
            } else if (stashEntryMetadata.getRecordFormat() == StashObjectMetadata.RecordFormat.TIME_SERIES) {
              stashEntryMap.put(stashId, StashFileTimeSeriesRecord.rebuildRecord(reactor, StashFileServiceBackend.this,
                  (StashTimeSeriesMetadata<?>) stashEntryMetadata));
            }
          } finally {
            if (stashRecordInputStream != null)
              stashRecordInputStream.close();
          }
        }
      }
      return stashEntryMap;
    }
  }

  /*
   * Cleans up an invalidated stash ID, deleting all of the data directory
   * contents prior to deleting the invalidated index file.
   */
  void cleanupInvalidatedEntry(String stashId, File stashRecordFile) {
    StashFileUtils.cleanupDataFiles(stashDataDir, stashId);
    stashRecordFile.delete();
  }
}
