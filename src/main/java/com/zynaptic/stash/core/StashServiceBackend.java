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

import java.io.Serializable;
import java.net.URI;
import java.util.Map;

import com.zynaptic.reaction.Deferred;
import com.zynaptic.stash.StashObjectEntry;
import com.zynaptic.stash.StashTimeSeriesEntry;

/**
 * Provides a common interface to different stash backend implementations.
 * 
 * @author Chris Holgate
 */
public interface StashServiceBackend {

  /**
   * Accesses the URI which defines the location of the stash data associated with
   * the stash service.
   * 
   * @return Returns the URI which is associated with the stash service.
   */
  public URI getStashUri();

  /**
   * Initiates setup for an implementation specific stash backend. This will
   * attempt to initialise the persistence layer using either a cold start process
   * (for completely new stash services) or a warm start processes (which recovers
   * the existing stash records from the persistence layer). On completion it will
   * return the current set of stash entry records via a deferred callback.
   * 
   * @return Returns a deferred event object which will have its callbacks
   *   executed on completion. On successfully setting up the stash service
   *   backend, the current set of stash entries will be returned as the callback
   *   parameter.
   */
  public Deferred<Map<String, StashEntryRecord<? extends Serializable>>> setupBackend();

  /**
   * Initiates teardown for an implementation specific stash backend. This will
   * attempt to complete any outstanding persistence operations prior to
   * deactivating the persistence layer.
   * 
   * @return Returns a deferred event object which will have its callbacks
   *   executed on completion. On successfully tearing down the stash service
   *   backend, a boolean value which will normally be set to 'true' will be
   *   returned as the callback parameter.
   */
  public Deferred<Boolean> teardownBackend();

  /**
   * Initialises a stash object entry when registering a new stash object
   * identifier.
   * 
   * @param stashId This is the stash identifier to be registered with the stash.
   *   It takes the form of a number of dot separated alphanumeric fields, similar
   *   to the canonical Java package naming convention.
   * @param stashName This is a human readable string which is used to refer to
   *   the stash entry.
   * @param defaultValue This is the default value which is to be associated with
   *   the stash identifier. It is used to infer the type of data objects which
   *   may be subsequently stored and must implement the <code>Serializable</code>
   *   interface.
   * @return Returns a deferred event object which will have its callbacks
   *   executed on successful completion of the operation, passing a reference to
   *   the {@link StashObjectEntry} interface which was created.
   */
  public <T extends Serializable> Deferred<StashObjectEntry<T>> initStashObjectEntry(String stashId, String stashName,
      T defaultValue);

  /**
   * Initialises a stash time series entry when registering a new stash time
   * series identifier.
   * 
   * @param stashId This is the stash identifier to be registered with the stash.
   *   It takes the form of a number of dot separated alphanumeric fields, similar
   *   to the canonical Java package naming convention.
   * @param stashName This is a human readable string which is used to refer to
   *   the stash entry.
   * @param defaultValue This is the default value which is to be associated with
   *   the stash identifier. It is used to infer the type of data objects which
   *   may be subsequently stored and must implement the <code>Serializable</code>
   *   interface.
   * @param dataBlockPeriod This is the minimum data block period to be used for
   *   the given time series, expressed as an integer number of milliseconds. The
   *   standard data block period will be used if a value of zero is specified.
   * @param dataBlockSize This is the minimum data block size to be used for the
   *   given time series, expressed as an integer number of entries. The standard
   *   data block size will be used if a value of zero is specified.
   * @return Returns a deferred event object which will have its callbacks
   *   executed on successful completion of the operation, passing a reference to
   *   the {@link StashTimeSeriesEntry} interface which was created.
   */
  public <T extends Serializable> Deferred<StashTimeSeriesEntry<T>> initStashTimeSeriesEntry(String stashId,
      String stashName, T defaultValue, int dataBlockPeriod, int dataBlockSize);

  /**
   * Specifies the approximate timeout period for which copies of the time series
   * data blocks will be retained in local memory after use. Data blocks will
   * automatically be discarded from local memory after they have been unused for
   * the specified period. The default cache timeout period depends on the stash
   * service backend being used.
   * 
   * @param cacheTimeout This is the timeout period after which unused data blocks
   *   will be discarded from local memory, expressed as an integer number of
   *   milliseconds. May be set to zero or a positive integer value;
   */
  public void setCacheTimeout(int cacheTimeout);

  /**
   * Accesses the current cache timeout period which is in use by the stash time
   * series entries.
   * 
   * @return Returns the current cache timeout period, expressed as an integer
   *   number of milliseconds.
   */
  public int getCacheTimeout();

}
