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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * Encapsulates the serializable metadata associated with a given stash entry.
 * This can be written to the persistent store to allow the stash index to be
 * rebuilt on a warm start.
 * 
 * @param <T> This defines the serializable data type which may be stored for
 *   this stash entry.
 * 
 * @author Chris Holgate
 */
public class StashObjectMetadata<T extends Serializable> implements Serializable {
  private static final long serialVersionUID = -2352251054615619510L;

  // Define supported stash record formats.
  public static enum RecordFormat {
    PLACEHOLDER, RAW_OBJECT, TIME_SERIES
  };

  // Define stash entry metadata fields.
  private final RecordFormat recordFormat;
  private final String stashId;
  private final String stashName;
  private final byte[] encodedDefaultData;

  /**
   * Default constructor for building the stash entry metadata objects.
   * 
   * @param recordFormat This specifies the record format which is used to store
   *   the stash entry data.
   * @param stashId This is the unique identifier which is associated with the
   *   stash entry.
   * @param stashName This is the human readable name which is associated with the
   *   stash entry.
   * @param defaultData This is the default data value which is associated with
   *   the stash identifier.
   */
  public StashObjectMetadata(RecordFormat recordFormat, String stashId, String stashName, T defaultData) {
    this.recordFormat = recordFormat;
    this.stashId = stashId;
    this.stashName = stashName;

    // The default data value is serialised into a byte array so that the
    // metadata object can be processed without prior knowledge of its
    // associated data class loader.
    try {
      ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
      ObjectOutputStream objectStream = new ObjectOutputStream(byteStream);
      objectStream.writeObject(defaultData);
      objectStream.close();
      encodedDefaultData = byteStream.toByteArray();
    }

    // IO exceptions should never occur when writing to a local byte array, so
    // they are promoted to runtime exceptions here.
    catch (IOException error) {
      throw new RuntimeException("Unexpected IO exception when writing to a local byte array.", error);
    }
  }

  /**
   * Gets the record format associated with the stash entry record.
   * 
   * @return Returns the record format which was used to store the stash entry
   *   data.
   */
  public final RecordFormat getRecordFormat() {
    return recordFormat;
  }

  /**
   * Gets unique identifier associated with the stash entry record.
   * 
   * @return Returns a string containing the unique identifier for the associated
   *   stash entry.
   */
  public final String getStashId() {
    return stashId;
  }

  /**
   * Gets human readable name associated with the stash entry record.
   * 
   * @return Returns a string containing the human readable name for the stash
   *   entry record.
   */
  public final String getStashName() {
    return stashName;
  }

  /**
   * Gets the default data value which is encoded in the stash entry metadata.
   * 
   * @return Returns a byte array containing the encoded default data value which
   *   was originally supplied when creating the stash entry metadata object.
   */
  public final byte[] getDefaultData() {
    return encodedDefaultData;
  }
}
