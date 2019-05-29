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

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;

/**
 * Subclass the input object data stream to force it to use a specific class
 * loader. This allows hidden classes in other OSGi bundles to be correctly
 * handled by the stash service.
 * 
 * @author Chris Holgate
 */
public final class ResolvedObjectInputStream extends ObjectInputStream {
  private final ClassLoader dataClassLoader;

  /**
   * Provides the default constructor for object input streams with specific data
   * class loader object resolution.
   * 
   * @param in This is the input stream to read from.
   * @param dataClassLoader This is the class loader which should be used to
   *   resolve the data classes in the input stream.
   * @throws IOException This exception is thrown if an I/O error occurs while
   *   reading the wrapped input stream.
   */
  public ResolvedObjectInputStream(InputStream in, ClassLoader dataClassLoader) throws IOException {
    super(in);
    this.dataClassLoader = dataClassLoader;
  }

  /*
   * Overrides the default class resolution to use the supplied data class loader.
   */
  @Override
  protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
    return Class.forName(desc.getName(), false, dataClassLoader);
  }
}
