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

/**
 * Encapsulates the serializable metadata associated with a given time series
 * stash entry. This can be written to the persistent store to allow the stash
 * index to be rebuilt on a warm start.
 * 
 * @param <T> This defines the serializable data type which may be stored for
 *   this stash entry.
 * 
 * @author Chris Holgate
 */
public final class StashTimeSeriesMetadata<T extends Serializable> extends StashObjectMetadata<T> {
  private static final long serialVersionUID = 8379004606915215219L;

  // Define the default data block period to be used.
  private static final int DEFAULT_DATA_BLOCK_PERIOD = 24 * 60 * 60 * 1000;

  // Define the default data block size to be used.
  private static final int DEFAULT_DATA_BLOCK_SIZE = 256;

  // Define additional time series stash entry metadata fields.
  private final int dataBlockPeriod;
  private final int dataBlockSize;

  /**
   * Default constructor for building the time series stash entry metadata objects
   * with user specified data block periods.
   * 
   * @param recordFormat This specifies the record format which is used to store
   *   the stash entry data.
   * @param stashId This is the unique identifier which is associated with the
   *   stash entry.
   * @param stashName This is the human readable name which is associated with the
   *   stash entry.
   * @param defaultData This is the default data value which is associated with
   *   the stash identifier.
   * @param dataBlockPeriod This is the minimum data block period to be used for
   *   the given time series, expressed as an integer number of milliseconds. The
   *   standard data block period will be used if a value of zero is specified.
   * @param dataBlockSize This is the minumum data block size to be used for the
   *   given time series, expressed as an integer number of entries. The standard
   *   data block size will be used if a value of zero is specified.
   */
  public StashTimeSeriesMetadata(StashObjectMetadata.RecordFormat recordFormat, String stashId, String stashName,
      T defaultData, int dataBlockPeriod, int dataBlockSize) {
    super(recordFormat, stashId, stashName, defaultData);
    this.dataBlockPeriod = (dataBlockPeriod > 0) ? dataBlockPeriod : DEFAULT_DATA_BLOCK_PERIOD;
    this.dataBlockSize = (dataBlockSize > 0) ? dataBlockSize : DEFAULT_DATA_BLOCK_SIZE;
  }

  /**
   * Gets the minimum data block period to be used for this time series data.
   * 
   * @return Returns the minimum data block period to be used for partitioning the
   *   time series, expressed as an integer number of milliseconds.
   */
  public int getDataBlockPeriod() {
    return dataBlockPeriod;
  }

  /**
   * Gets the minimum data block size to be used for this time series data.
   * 
   * @return Returns the minimum data block size to be used for partitioning the
   *   time series, expressed as an integer number of entries.
   */
  public int getDataBlockSize() {
    return dataBlockSize;
  }
}
