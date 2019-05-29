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

import java.net.URISyntaxException;

import com.zynaptic.reaction.Deferred;

/**
 * Defines the factory interface used to create new stash service objects. This
 * factory interface should be used to create a new persistent stash resource or
 * connect to existing stash resources.
 * 
 * @author Chris Holgate
 */
public interface StashFactory {

  /**
   * Initialises a stash service for the stash resource located at a given URI.
   * Every stash resource must be initialised in this manner before it can be
   * accessed via a stash service object. This creates a new stash resource at the
   * specified location if one does not already exist. Only a subset of the URI
   * schemes are currently supported for stash resource creation, as follows:
   * <ul>
   * <li><code>file</code> This scheme initialises a stash resource on the local
   * file system which uses standard Java object serialization.</li>
   * </ul>
   * 
   * @param stashUri This is the URI of the new stash resource to be created. The
   *   specified URI scheme must be one of the schemes supported for stash
   *   resource creation.
   * @return Returns a deferred event object which will have its callbacks
   *   executed on completion. If the stash resource already exists or was
   *   successfully initialised, a handle on the corresponding stash service
   *   object of type {@link StashService} will be passed back as the deferred
   *   callback parameter. If the specified stash resource does not already exist
   *   or cannot be created, an errback condition will be generated which will
   *   pass back an exception object indicating the reason for failure. This
   *   includes situations where a <code>URISyntaxException</code> is generated
   *   because a malformed or unsupported URI has been provided.
   */
  public Deferred<StashService> initStashService(String stashUri);

  /**
   * Halts the currently active stash service associated with the stash resource
   * located at a given URI. This performs an orderly shutdown of the stash
   * service, after which it will no longer be accessible via calls to the
   * {@link #getStashService} method.
   * 
   * @param stashUri This is the URI of the stash resource for which the
   *   associated stash service it to be halted. The specified URI scheme must be
   *   one of the schemes supported for stash resource creation.
   * @return Returns a deferred event object which will have its callbacks
   *   executed on completion. A boolean value will be passed as the callback
   *   parameter which will be set to 'true' if the specified stash resource URI
   *   corresponded to an active stash service which was halted and 'false' if no
   *   active stash service was found for the specified resource URI. On failure,
   *   an errback condition will be generated which will pass back an exception
   *   object indicating the reason for failure. This includes situations where a
   *   <code>URISyntaxException</code> is generated because a malformed URI has
   *   been provided.
   */
  public Deferred<Boolean> haltStashService(String stashUri);

  /**
   * Gets the currently active stash service interface for the stash resource
   * located at the specified URI.
   * 
   * @param stashUri This is the URI for the stash resource which is to be
   *   accessed by the stash service object.
   * @return Returns a stash service object which may be used to access the stash
   *   resource at the specified URI, or a null reference if the specified stash
   *   resource has not been initialised.
   * @throws URISyntaxException This exception will be thrown if the supplied URI
   *   was malformed or if it specified an unsupported URI scheme.
   */
  public StashService getStashService(String stashUri) throws URISyntaxException;

}
