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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.io.PrintStream;
import java.io.Serializable;

import com.zynaptic.reaction.Deferred;

/**
 * Provides a common stash record functionality for different stash entry
 * backend implementations.
 * 
 * @author Chris Holgate
 */
public abstract class StashEntryRecord<T extends Serializable> {

  // Encapsulates the stash entry metadata which is associated with this record.
  private final StashObjectMetadata<T> metadata;

  // The class loader to be used when serializing and deserializing data.
  private ClassLoader dataClassLoader;

  // Decoded default data value.
  private T defaultData;

  /**
   * Provides default constructor which is used to set up the stash entry record
   * using the supplied stash entry metadata.
   * 
   * @param metadata This is the stash entry metadata which is used to set up the
   *   stash entry record.
   */
  protected StashEntryRecord(StashObjectMetadata<T> metadata) {
    this.metadata = metadata;
    this.dataClassLoader = null;
    this.defaultData = null;
  }

  /**
   * Gets the metadata object associated with this stash entry.
   * 
   * @return Returns the metadata object which is associated with this stash
   *   entry.
   */
  public final StashObjectMetadata<T> getMetadata() {
    return metadata;
  }

  /**
   * Assigns the class loader to be used when serializing and deserializing data
   * for this stash entry. Automatically parses the default data value when
   * assigned in order to check that the class loader is valid for the serialized
   * data.
   * 
   * @param newClassLoader This is the new class loader which is to be used for
   *   serializing and deserializing the stash entry data.
   * @throws ClassNotFoundException This exception will be thrown if the supplied
   *   data class loader does not support the encoded default data classes.
   */
  protected final synchronized void setDataClassLoader(ClassLoader newClassLoader) throws ClassNotFoundException {
    if (dataClassLoader == null) {
      dataClassLoader = newClassLoader;
      defaultData = decodeDefaultData(metadata.getDefaultData());
    }

    // Implements a identity check on the class loader. This should always match
    // the previously assigned class loader object.
    else if (dataClassLoader != newClassLoader) {
      throw new IllegalArgumentException("Class loader does not match previously assigned instance.");
    }
  }

  /**
   * Accesses the data class loader which should be used when serializing and
   * deserializing data for this stash entry.
   * 
   * @return Returns the data class loader for this stash entry, or a null
   *   reference if one has not yet been assigned.
   */
  public final synchronized ClassLoader getDataClassLoader() {
    return dataClassLoader;
  }

  /**
   * Gets the default data value which is associated with the stash entry.
   * 
   * @return Returns the default data value which is associated with the stash
   *   entry.
   */
  public final synchronized T getDefaultData() {
    return defaultData;
  }

  /**
   * Invalidates the stash entry. This will cause the persisted state associated
   * with the stash entry to be deleted when no longer in use.
   * 
   * @return Returns a deferred event object which will have its callbacks
   *   executed on completion. A boolean value which will normally be set to
   *   'true' will be passed as the callback parameter.
   */
  protected abstract Deferred<Boolean> invalidate();

  /**
   * Reports the contents of the stash entry record for informational purposes.
   * This method generates a text report of the current state of the stash entry
   * record for debug and monitoring purposes.
   * 
   * @param printStream This is the output print stream to which the stash
   *   contents report will be sent.
   * @throws IOException This exception will be thrown on failure to write to the
   *   specified print stream.
   */
  protected abstract void reportRecordContents(PrintStream printStream) throws IOException;

  /*
   * Decodes the default data value which is encoded in the stash entry metadata.
   * This uses a supplied data class loader to interpret the encoded default data.
   */
  private final T decodeDefaultData(byte[] defaultDataArray) throws ClassNotFoundException {
    try {
      ByteArrayInputStream byteStream = new ByteArrayInputStream(defaultDataArray);
      ObjectInputStream objectStream = new ResolvedObjectInputStream(byteStream, dataClassLoader);
      Object rawObject = objectStream.readObject();
      objectStream.close();

      @SuppressWarnings("unchecked")
      T defaultData = (T) rawObject;
      return defaultData;
    }

    // IO exceptions should never occur when reading from a well formed local
    // byte array, so they are promoted to runtime exceptions here.
    catch (IOException error) {
      throw new RuntimeException("Unexpected IO exception when reading from a local byte array.", error);
    }
  }

  /*
   * Subclass the input object data stream to force it to use a specific class
   * loader. This allows hidden classes in other OSGi bundles to be correctly
   * handled by overriding the class name resolution to use a specific data class
   * loader.
   */
  private final class ResolvedObjectInputStream extends ObjectInputStream {
    private ResolvedObjectInputStream(InputStream in, ClassLoader dataClassLoader) throws IOException {
      super(in);
    }

    @Override
    protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
      return Class.forName(desc.getName(), false, dataClassLoader);
    }
  }
}
