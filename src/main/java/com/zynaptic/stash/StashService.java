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

import java.io.IOException;
import java.io.PrintStream;
import java.io.Serializable;
import java.net.URI;

import com.zynaptic.reaction.Deferred;

/**
 * This interface provides access to a single persistent stash storage service.
 * It provides the common stash management API as well as providing access to
 * the type specific stash entry access objects.
 * 
 * @author Chris Holgate
 */
public interface StashService {

  /**
   * Accesses the URI of the stash resource which is accessed via the stash
   * service object.
   * 
   * @return Returns the URI of the stash resource which is associated with the
   *   stash service object.
   */
  public URI getStashUri();

  /**
   * Registers a stash identifier which will be used to access a specific
   * persistent object.
   * 
   * @param <T> This is the generic type specifier which determines the class of
   *   objects which may be stored in the stash using the given stash identifier.
   * @param stashId This is the stash identifier to be registered with the stash.
   *   It takes the form of a number of dot separated alphanumeric fields, similar
   *   to the canonical Java package naming convention.
   * @param stashName This is a human readable string which is used to refer to
   *   the stash entry.
   * @param defaultValue This is the default data value which is to be associated
   *   with the stash identifier. It is used to infer the type and structure of
   *   data objects which may subsequently be stored and must implement the
   *   <code>Serializable</code> interface.
   * @return Returns a deferred event object which will have its callbacks
   *   executed on successful completion of the operation, passing a reference to
   *   the {@link StashObjectEntry} interface which was created. On failure an
   *   exception of type <code>InvalidStashIdException</code> will be passed back
   *   via the errback chain if the specified stash identifier is already in use
   *   or the specified stash identifier is malformed.
   */
  public <T extends Serializable> Deferred<StashObjectEntry<T>> registerStashObjectId(String stashId, String stashName,
      T defaultValue);

  /**
   * Registers a stash identifier which will be used to access a set of time
   * series data, using a configurable data block size and period.
   * 
   * @param <T> This is the generic type specifier which determines the class of
   *   objects which may be stored in the stash using the given stash identifier.
   * @param stashId This is the stash identifier to be registered with the stash.
   *   It takes the form of a number of dot separated alphanumeric fields, similar
   *   to the canonical Java package naming convention.
   * @param stashName This is a human readable string which is used to refer to
   *   the stash entry.
   * @param defaultValue This is the default data value which is to be associated
   *   with the stash identifier. It is automatically inserted into the time
   *   series data with a timestamp value of 0. The default value is used to infer
   *   the type and structure of data objects which may be subsequently stored and
   *   must implement the <code>Serializable</code> interface.
   * @param dataBlockPeriod This is the minimum data block period to be used for
   *   the given time series, expressed as an integer number of milliseconds. The
   *   default URI type data block period will be used if a value of zero is
   *   specified.
   * @param dataBlockSize This is the minimum data block size to be used for the
   *   given time series, expressed as an integer number of entries. The default
   *   URI type data block size will be used if a value of zero is specified.
   * @return Returns a deferred event object which will have its callbacks
   *   executed on successful completion of the operation, passing a reference to
   *   the {@link StashTimeSeriesEntry} interface which was created. On failure an
   *   exception of type <code>InvalidStashIdException</code> will be passed back
   *   via the errback chain if the specified stash identifier is already in use
   *   or the specified stash identifier is malformed.
   */
  public <T extends Serializable> Deferred<StashTimeSeriesEntry<T>> registerStashTimeSeriesId(String stashId,
      String stashName, T defaultValue, int dataBlockPeriod, int dataBlockSize);

  /**
   * Determines whether a given stash ID has been registered with the stash.
   * 
   * @param stashId This is the stash identifier being tested for current stash
   *   registration.
   * @return Returns a boolean flag which will be set to 'true' only if the
   *   specified stash ID has been registered with the stash and has not been
   *   invalidated.
   */
  public boolean hasStashId(String stashId);

  /**
   * Invalidates a stash identifier which is no longer to be used for accessing
   * its associated stash entry. Once invalidated, the associated persistent
   * resource will be deleted once it is not longer in use.
   * 
   * @param stashId This is the stash identifier to be invalidated in the stash.
   *   It takes the form of a number of dot separated alphanumeric fields, similar
   *   to the canonical Java package naming convention.
   * @return Returns a deferred event object which will have its callbacks
   *   executed on successful completion of the operation, passing a boolean
   *   parameter which will be set to 'true' if a matching entry was found and
   *   removed and 'false' if no matching entry could be found. On failure an
   *   exception of type <code>InvalidStashIdException</code> will be passed back
   *   via the errback chain if the specified stash identifier is malformed.
   */
  public Deferred<Boolean> invalidateStashId(String stashId);

  /**
   * Accesses the public interface for manipulating an individual raw Java object
   * in the persistent stash storage. This method uses implicit class loader
   * selection for the specified object data type.
   * 
   * @param <T> This is the generic type specifier which determines the class of
   *   objects which may be stored in the stash using the given stash identifier.
   * @param stashId This is the stash identifier associated with the raw Java
   *   object. It takes the form of a number of dot separated alphanumeric fields,
   *   similar to the canonical Java package naming convention.
   * @param dataClass This is the runtime type which is used to check that the
   *   object stored in the stash is consistent with the expected data type. It is
   *   also used to infer the class loader to be used when deserializing the stash
   *   object data.
   * @return Returns a reference to the {@link StashObjectEntry} interface which
   *   is associated with the specified stash identifier, or a null reference if
   *   there is no corresponding stash entry.
   * @throws InvalidStashIdException This exception will be thrown if the
   *   specified stash identifier does not refer to a raw Java object entry or the
   *   specified stash identifier is malformed.
   * @throws ClassNotFoundException This exception will be thrown if there is a
   *   mismatch between the specified data class and the default data value held
   *   by the stash entry.
   */
  public <T extends Serializable> StashObjectEntry<T> getStashObjectEntry(String stashId, Class<T> dataClass)
      throws InvalidStashIdException, ClassNotFoundException;

  /**
   * Accesses the public interface for manipulating an individual raw Java object
   * in the persistent stash storage. This method uses explicit class loader
   * selection.
   * 
   * @param <T> This is the generic type specifier which determines the class of
   *   objects which may be stored in the stash using the given stash identifier.
   * @param stashId This is the stash identifier associated with the raw Java
   *   object. It takes the form of a number of dot separated alphanumeric fields,
   *   similar to the canonical Java package naming convention.
   * @param dataClass This is the runtime type which is used to check that the
   *   object stored in the stash is consistent with the expected data type.
   * @param classLoader This is the class loader which will be used when
   *   deserializing the stash object data.
   * @return Returns a reference to the {@link StashObjectEntry} interface which
   *   is associated with the specified stash identifier, or a null reference if
   *   there is no corresponding stash entry.
   * @throws InvalidStashIdException This exception will be thrown if the
   *   specified stash identifier does not refer to a raw Java object entry or the
   *   specified stash identifier is malformed.
   * @throws ClassNotFoundException This exception will be thrown if there is a
   *   mismatch between the specified data class and the default data value held
   *   by the stash entry.
   */
  public <T extends Serializable> StashObjectEntry<T> getStashObjectEntry(String stashId, Class<T> dataClass,
      ClassLoader classLoader) throws InvalidStashIdException, ClassNotFoundException;

  /**
   * Accesses the public interface for for manipulating time series data in the
   * persistent stash storage. This method uses implicit class loader selection.
   * 
   * @param <T> This is the generic type specifier which determines the class of
   *   objects which may be stored in the stash using the given stash identifier.
   * @param stashId This is the stash identifier associated with the time series
   *   data. It takes the form of a number of dot separated alphanumeric fields,
   *   similar to the canonical Java package naming convention.
   * @param dataClass This is the runtime type which is used to check that the
   *   time series data stored in the stash is consistent with the expected data
   *   type. It is also used to infer the class loader to be used when
   *   deserializing the time series data.
   * @return Returns a reference to the {@link StashTimeSeriesEntry} interface
   *   which is associated with the specified stash identifier, or a null
   *   reference if there is no corresponding stash entry.
   * @throws InvalidStashIdException This exception will be thrown if the
   *   specified stash identifier does not refer to a time series entry or the
   *   specified stash identifier is malformed.
   * @throws ClassNotFoundException This exception will be thrown if there is a
   *   mismatch between the specified data class and the default data value held
   *   by the stash entry.
   */
  public <T extends Serializable> StashTimeSeriesEntry<T> getStashTimeSeriesEntry(String stashId, Class<T> dataClass)
      throws InvalidStashIdException, ClassNotFoundException;

  /**
   * Accesses the public interface for for manipulating time series data in the
   * persistent stash storage. This method uses explicit class loader selection.
   * 
   * @param <T> This is the generic type specifier which determines the class of
   *   objects which may be stored in the stash using the given stash identifier.
   * @param stashId This is the stash identifier associated with the time series
   *   data. It takes the form of a number of dot separated alphanumeric fields,
   *   similar to the canonical Java package naming convention.
   * @param dataClass This is the runtime type which is used to check that the
   *   time series data stored in the stash is consistent with the expected data
   *   type.
   * @param classLoader This is the class loader which will be used when
   *   deserializing the time series data.
   * @return Returns a reference to the {@link StashTimeSeriesEntry} interface
   *   which is associated with the specified stash identifier, or a null
   *   reference if there is no corresponding stash entry.
   * @throws InvalidStashIdException This exception will be thrown if the
   *   specified stash identifier does not refer to a time series entry or the
   *   specified stash identifier is malformed.
   * @throws ClassNotFoundException This exception will be thrown if there is a
   *   mismatch between the specified data class and the default data value held
   *   by the stash entry.
   */
  public <T extends Serializable> StashTimeSeriesEntry<T> getStashTimeSeriesEntry(String stashId, Class<T> dataClass,
      ClassLoader classLoader) throws InvalidStashIdException, ClassNotFoundException;

  /**
   * Reports the contents of the stash for informational purposes. This method
   * generates a text report of the current state of the stash for debug and
   * monitoring purposes.
   * 
   * @param printStream This is the output print stream to which the stash
   *   contents report will be sent.
   * @throws IOException This exception will be thrown on failure to write to the
   *   specified print stream.
   */
  public void reportStashContents(PrintStream printStream) throws IOException;

  /**
   * Specifies the approximate timeout period for which copies of the time series
   * data blocks will be retained in local memory after use. Data blocks will
   * automatically be discarded from local memory after they have been unused for
   * the specified period. The default cache timeout period depends on the stash
   * service URI type being used.
   * 
   * @param cacheTimeout This is the timeout period after which unused data blocks
   *   will be discarded from local memory, expressed as an integer number of
   *   milliseconds. May be set to zero or a positive integer value;
   * @return Returns a reference to the stash service object, enabling fluent
   *   parameter assignment to be used.
   */
  public StashService setCacheTimeout(int cacheTimeout);

  /**
   * Accesses the current cache timeout period which is in use by the stash time
   * series entries.
   * 
   * @return Returns the current cache timeout period, expressed as an integer
   *   number of milliseconds.
   */
  public int getCacheTimeout();

}
