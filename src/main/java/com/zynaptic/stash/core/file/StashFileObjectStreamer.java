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
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * Provides generic stash object writer functionality for arbitrary serializable
 * objects. This is subclassed for the various native type wrappers in order to
 * use the most space efficient serialized representaion for time series data.
 * 
 * @author Chris Holgate
 * 
 * @param <T> This identifies the type of serializable objects which may be
 *   written using this stash object writer.
 */
abstract class StashFileObjectStreamer<T extends Serializable> {

  /**
   * Provides a factory method which returns the appropriate object stream writer
   * for a given class.
   */
  @SuppressWarnings("unchecked")
  final static <U extends Serializable> StashFileObjectStreamer<U> create(U templateData) {
    Class<?> dataType = templateData.getClass();

    // Selecting integer encoding...
    if (dataType.equals(Integer.class)) {
      return (StashFileObjectStreamer<U>) new IntegerStreamer();
    }

    // Selecting long integer encoding...
    else if (dataType.equals(Long.class)) {
      return (StashFileObjectStreamer<U>) new LongStreamer();
    }

    // Selecting short integer encoding...
    else if (dataType.equals(Long.class)) {
      return (StashFileObjectStreamer<U>) new ShortStreamer();
    }

    // Selecting floating point encoding...
    else if (dataType.equals(Float.class)) {
      return (StashFileObjectStreamer<U>) new FloatStreamer();
    }

    // Selecting double precision encoding...
    else if (dataType.equals(Double.class)) {
      return (StashFileObjectStreamer<U>) new DoubleStreamer();
    }

    // Selecting boolean encoding...
    else if (dataType.equals(Boolean.class)) {
      return (StashFileObjectStreamer<U>) new BooleanStreamer();
    }

    // Selecting byte encoding...
    else if (dataType.equals(Byte.class)) {
      return (StashFileObjectStreamer<U>) new ByteStreamer();
    }

    // Selecting character encoding...
    else if (dataType.equals(Character.class)) {
      return (StashFileObjectStreamer<U>) new CharStreamer();
    }

    // Selecting raw data array encoding...
    else if (dataType.equals(byte[].class)) {
      return (StashFileObjectStreamer<U>) new RawDataStreamer();
    }

    // Fall back to generic object encoding...
    else {
      return new ObjectStreamer<U>();
    }
  }

  /**
   * Provides the main stash object writer function for arbitrary serializable
   * objects.
   * 
   * @param outputStream This is the object output stream to which the object data
   *   is to be serialized.
   * @param object This is the serializable object which is to be written to the
   *   object output stream.
   * @throws IOException
   */
  abstract void write(ObjectOutputStream outputStream, T object) throws IOException;

  /**
   * Provides the main stash object reader function for arbitrary serializable
   * objects.
   * 
   * @param inputStream This is the object input stream from which the object data
   *   is to be deserialized.
   * @return Returns the deserialized object which has been read from the input
   *   stream.
   * @throws IOException
   * @throws ClassNotFoundException
   */
  abstract T read(ObjectInputStream inputStream) throws IOException, ClassNotFoundException;

  /**
   * Overrides the generic stash object streamer functions to provide standard
   * object data storage.
   */
  static final class ObjectStreamer<U extends Serializable> extends StashFileObjectStreamer<U> {
    @Override
    void write(ObjectOutputStream outputStream, U object) throws IOException {
      outputStream.writeObject(object);
    }

    @Override
    @SuppressWarnings("unchecked")
    U read(ObjectInputStream inputStream) throws IOException, ClassNotFoundException {
      return (U) inputStream.readObject();
    }
  }

  /**
   * Overrides the generic stash object streamer functions to provide efficient
   * integer data storage.
   */
  static final class IntegerStreamer extends StashFileObjectStreamer<Integer> {
    @Override
    public void write(ObjectOutputStream outputStream, Integer data) throws IOException {
      outputStream.writeInt(data);
    }

    @Override
    public Integer read(ObjectInputStream inputStream) throws IOException {
      return inputStream.readInt();
    }
  }

  /**
   * Overrides the generic stash object streamer functions to provide efficient
   * long integer data storage.
   */
  static final class LongStreamer extends StashFileObjectStreamer<Long> {
    @Override
    void write(ObjectOutputStream outputStream, Long data) throws IOException {
      outputStream.writeLong(data);
    }

    @Override
    Long read(ObjectInputStream inputStream) throws IOException {
      return inputStream.readLong();
    }
  }

  /**
   * Overrides the generic stash object streamer functions to provide efficient
   * short integer data storage.
   */
  static final class ShortStreamer extends StashFileObjectStreamer<Short> {
    @Override
    void write(ObjectOutputStream outputStream, Short data) throws IOException {
      outputStream.writeShort(data);
    }

    @Override
    Short read(ObjectInputStream inputStream) throws IOException {
      return inputStream.readShort();
    }
  }

  /**
   * Overrides the generic stash object streamer functions to provide efficient
   * floating point data storage.
   */
  static final class FloatStreamer extends StashFileObjectStreamer<Float> {
    @Override
    void write(ObjectOutputStream outputStream, Float data) throws IOException {
      outputStream.writeFloat(data);
    }

    @Override
    Float read(ObjectInputStream inputStream) throws IOException {
      return inputStream.readFloat();
    }
  }

  /**
   * Overrides the generic stash object streamer function to provide efficient
   * double precision floating point data storage.
   */
  static final class DoubleStreamer extends StashFileObjectStreamer<Double> {
    @Override
    void write(ObjectOutputStream outputStream, Double data) throws IOException {
      outputStream.writeDouble(data);
    }

    @Override
    Double read(ObjectInputStream inputStream) throws IOException {
      return inputStream.readDouble();
    }
  }

  /**
   * Overrides the generic stash object streamer function to provide efficient
   * boolean data storage.
   */
  static final class BooleanStreamer extends StashFileObjectStreamer<Boolean> {
    @Override
    void write(ObjectOutputStream outputStream, Boolean data) throws IOException {
      outputStream.writeBoolean(data);
    }

    @Override
    Boolean read(ObjectInputStream inputStream) throws IOException {
      return inputStream.readBoolean();
    }
  }

  /**
   * Overrides the generic stash object streamer function to provide efficient
   * byte data storage.
   */
  static final class ByteStreamer extends StashFileObjectStreamer<Byte> {
    @Override
    void write(ObjectOutputStream outputStream, Byte data) throws IOException {
      outputStream.writeByte(data);
    }

    @Override
    Byte read(ObjectInputStream inputStream) throws IOException {
      return inputStream.readByte();
    }
  }

  /**
   * Overrides the generic stash object streamer function to provide efficient
   * character data storage.
   */
  static final class CharStreamer extends StashFileObjectStreamer<Character> {
    @Override
    void write(ObjectOutputStream outputStream, Character data) throws IOException {
      outputStream.writeChar(data);
    }

    @Override
    Character read(ObjectInputStream inputStream) throws IOException {
      return inputStream.readChar();
    }
  }

  /**
   * Overrides the generic stash object streamer function to provide efficient raw
   * data array storage.
   */
  static final class RawDataStreamer extends StashFileObjectStreamer<byte[]> {
    @Override
    void write(ObjectOutputStream outputStream, byte[] data) throws IOException {
      outputStream.writeInt(data.length);
      outputStream.write(data);
    }

    @Override
    byte[] read(ObjectInputStream inputStream) throws IOException {
      int dataLength = inputStream.readInt();
      byte[] data = new byte[dataLength];
      if (inputStream.read(data) != dataLength) {
        throw new IOException("Invalid raw data array format.");
      }
      return data;
    }
  }
}
