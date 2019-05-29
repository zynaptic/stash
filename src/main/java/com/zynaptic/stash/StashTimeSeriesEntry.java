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

package com.zynaptic.stash;

import java.io.Serializable;
import java.util.NavigableMap;

import com.zynaptic.reaction.Deferred;

/**
 * Provides the interface for manipulating time series data in the persistent
 * stash storage.
 * 
 * @param <T> This is the generic type specifier which determines the class of
 *   objects which may be stored in the stash using this stash entry handle.
 * 
 * @author Chris Holgate
 */
public interface StashTimeSeriesEntry<T extends Serializable> {

  /**
   * Accesses the unique stash identifier which is associated with the stash time
   * series entry.
   * 
   * @return Returns the unique stash identifier for the stash time series entry.
   */
  public String getStashId();

  /**
   * Accesses the human readable stash entry name which is associated with the
   * stash time series entry.
   * 
   * @return Returns the human readable stash entry name for the stash time series
   *   entry.
   */
  public String getStashName();

  /**
   * Accesses the default value for the stash time series entry. This is the
   * default value which was specified when the stash entry was created and is
   * used for run time type checking.
   * 
   * @return Returns the default value for the stash time series entry.
   */
  public T getDefaultValue();

  /**
   * Accesses the minimum data block period which is used by the stash time series
   * entry.
   * 
   * @return Returns the minimum data block period which is used by the stash time
   *   series entry, expressed as an integer number of milliseconds.
   */
  public int getDataBlockPeriod();

  /**
   * Accesses the minimum data block size which is used by the stash time series
   * entry.
   * 
   * @return Returns the minimum number of block entries which is used by the
   *   stash time series entry, expressed as an integer number of entries.
   */
  public int getDataBlockSize();

  /**
   * Accesses the data point in the time series which immediately precedes the
   * current system time.
   * 
   * @return Returns a deferred event object which will have its callbacks
   *   executed on completion. It will pass the current value for the time series
   *   data as the callback parameter.
   */
  public Deferred<T> getCurrentValue();

  /**
   * Gets the time series data point associated with a given timestamp. If a data
   * point does not exist for the specified timestamp, the data point immediately
   * preceding the specified time will be used. If no such data point exists, the
   * default value will be used instead.
   * 
   * @param timestamp This is the timestamp for which the corresponding (or
   *   immediately preceding) data point will be accessed. This includes the
   *   default time series value which is stored with a timestamp of zero.
   * @return Returns a deferred event object which will have its callbacks
   *   executed on completion. It will pass the value of the appropriate time
   *   series data point as the callback parameter.
   */
  public Deferred<T> getDataPointValue(long timestamp);

  /**
   * Gets the full set of time series data points for this time series entry.
   * These are collected into a navigable map which indexes data point values by
   * their associated timestamps. This includes the default time series value
   * which is included in the map with a timestamp index value of zero.
   * 
   * @return Returns a deferred event object which will have its callbacks
   *   executed on completion. It will pass the appropriate set of time series
   *   data points as the callback parameter.
   */
  public Deferred<NavigableMap<Long, T>> getDataPointSet();

  /**
   * Gets a set of time series data points over a given timestamp range. These are
   * collected into a navigable map which indexes data point values by their
   * associated timestamps.
   * 
   * @param startTimestamp This specifies the start of the timestamp range for
   *   which time series data points are being retrieved. The range is inclusive
   *   of this timestamp value. A starting timestamp value of zero will include
   *   the default time series value at the start of the sequence. Timestamp
   *   values less than zero are not supported.
   * @param endTimestamp This specifies the end of the timestamp range for which
   *   time series data points are being retrieved. The range is inclusive of this
   *   timestamp value. The ending timestamp value must be greater than or equal
   *   to the starting timestamp value.
   * @return Returns a deferred event object which will have its callbacks
   *   executed on completion. It will pass the appropriate set of time series
   *   data points as the callback parameter.
   */
  public Deferred<NavigableMap<Long, T>> getDataPointSet(long startTimestamp, long endTimestamp);

  /**
   * Adds a new data point value to the time series, using the current system time
   * to generate the associated timestamp.
   * 
   * @param newValue This is the new data point value which is to be committed to
   *   the stash.
   * @return Returns a deferred event object which will have its callbacks
   *   executed on completion, passing a boolean value which will normally be set
   *   to true.
   */
  public Deferred<Boolean> setCurrentValue(T newValue);

  /**
   * Adds a new data point value to the time series. This will commit the
   * specified data point to the persistent store.
   * 
   * @param timestamp This is the timestamp associated with the new data point
   *   value. It specifies the time at which the data point value was generated as
   *   the integer number of milliseconds since the Unix epoch.
   * @param newValue This is the new data point value which is to be committed to
   *   the stash.
   * @return Returns a deferred event object which will have its callbacks
   *   executed on completion, passing a boolean value which will normally be set
   *   to true.
   */
  public Deferred<Boolean> setDataPointValue(long timestamp, T newValue);

}
