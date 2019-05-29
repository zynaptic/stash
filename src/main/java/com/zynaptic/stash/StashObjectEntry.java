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

import com.zynaptic.reaction.Deferred;

/**
 * Provides the interface for manipulating an individual raw Java object in the
 * persistent stash storage.
 * 
 * @param <T> This is the generic type specifier which determines the class of
 *   objects which may be stored in the stash using the stash entry handle.
 * 
 * @author Chris Holgate
 */
public interface StashObjectEntry<T extends Serializable> {

  /**
   * Accesses the unique stash identifier which is associated with the stash
   * object entry.
   * 
   * @return Returns the unique stash identifier for the stash object entry.
   */
  public String getStashId();

  /**
   * Accesses the human readable stash entry name which is associated with the
   * stash object entry.
   * 
   * @return Returns the human readable stash entry name for the stash object
   *   entry.
   */
  public String getStashName();

  /**
   * Accesses the default value for the stash object entry. This is the default
   * value which was specified when the stash entry was created and is used for
   * run time type checking.
   * 
   * @return Returns the default value for the stash object entry.
   */
  public T getDefaultValue();

  /**
   * Accesses the current stored value for the stash object entry. This is the
   * most recent value which was committed to the persistent store.
   * 
   * @return Returns a deferred event object which will have its callbacks
   *   executed on completion. It will pass the current value for the stash object
   *   entry as the callback parameter.
   */
  public Deferred<T> getCurrentValue();

  /**
   * Assigns a new value to the stash object entry. This will commit the specified
   * data to the persistent store.
   * 
   * @param newValue This is the new value for the object entry which is to be
   *   committed to the stash.
   * @return Returns a deferred event object which will have its callbacks
   *   executed once the data has been committed to the persistent store. It will
   *   pass a boolean value which will normally be set to 'true' as the callback
   *   parameter.
   */
  public Deferred<Boolean> setCurrentValue(T newValue);

}
