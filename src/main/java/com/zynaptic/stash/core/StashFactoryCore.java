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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;

import com.zynaptic.reaction.Deferrable;
import com.zynaptic.reaction.Deferred;
import com.zynaptic.reaction.DeferredConcentrator;
import com.zynaptic.reaction.DeferredSplitter;
import com.zynaptic.reaction.Logger;
import com.zynaptic.reaction.Reactor;
import com.zynaptic.stash.StashFactory;
import com.zynaptic.stash.StashService;
import com.zynaptic.stash.core.file.StashFileServiceBackend;

/**
 * Provides core implementation of the stash service factory. This delegates
 * creation of the stash resources themselves to protocol specific handlers,
 * based on the supplied URI scheme.
 * 
 * @author Chris Holgate
 */
public final class StashFactoryCore implements StashFactory {

  // Local handle on the reactor service.
  private Reactor reactor;

  // Local handle on the reactor logging service.
  private Logger logger;

  // Map used to track initialised stash resources.
  private final HashMap<String, StashServiceMapEntry> stashMap = new HashMap<String, StashServiceMapEntry>();

  /**
   * Sets up the stash factory service on bundle initialisation.
   * 
   * @param reactor This is the reactor service object which will be used for
   *   event management.
   * @return Returns a deferred event object which will have its callbacks
   *   executed on completion.
   */
  public synchronized Deferred<Boolean> setup(Reactor reactor) {
    Deferred<Boolean> deferredSetup = reactor.newDeferred();
    this.reactor = reactor;
    this.logger = reactor.getLogger("com.zynaptic.stash");
    logger.log(Level.INFO, "\n  Stash Asynchronous Persistence Framework (c)2009-2019 Zynaptic Ltd.\n"
        + "  For license terms see http://www.apache.org/licenses/LICENSE-2.0\n");
    deferredSetup.callback(true);
    return deferredSetup.makeRestricted();
  }

  /**
   * Tears down the stash factory service on bundle shutdown. This causes all the
   * stash service objects to be torn down prior to exit.
   * 
   * @return Returns a deferred event object which will have its callbacks
   *   executed on completion.
   */
  public synchronized Deferred<Boolean> teardown() {
    DeferredConcentrator<Boolean> deferredConcentrator = reactor.newDeferredConcentrator();
    Iterator<String> keyIter = stashMap.keySet().iterator();
    while (keyIter.hasNext()) {
      String key = keyIter.next();
      StashServiceMapEntry mapEntry = stashMap.remove(key);
      if (mapEntry != null) {
        deferredConcentrator.addInputDeferred(mapEntry.teardownStashService());
      }
    }
    return deferredConcentrator.getOutputDeferred().addDeferrable(new StashTeardownCallbackHandler(), false);
  }

  /*
   * Implements StashFactory.initStashResource(...)
   */
  public synchronized Deferred<StashService> initStashService(String stashUri) {
    try {

      // Normalize the URI prior to processing.
      URI uri = new URI(stashUri).normalize();
      String uriString = uri.toString();

      // Check to see if the specified stash resource has already been
      // initialised, or is in the process of being initialised.
      StashServiceMapEntry mapEntry = stashMap.get(uriString);
      if (mapEntry == null) {
        mapEntry = new StashServiceMapEntry();
        stashMap.put(uriString, mapEntry);
      }
      return mapEntry.initStashService(uri);
    }

    // Any error conditions are passed back via deferred errback.
    catch (Exception error) {
      return reactor.failDeferred(error);
    }
  }

  /*
   * Implements StashFactory.haltStashService(...)
   */
  public synchronized Deferred<Boolean> haltStashService(String stashUri) {
    try {

      // Normalize the URI prior to processing.
      URI uri = new URI(stashUri).normalize();
      String uriString = uri.toString();

      // Check to see if the specified stash resource is in active use.
      StashServiceMapEntry mapEntry = stashMap.remove(uriString);
      if (mapEntry != null) {
        return mapEntry.teardownStashService();
      } else {
        return reactor.callDeferred(false);
      }
    }

    // Any error conditions are passed back via deferred errback.
    catch (Exception error) {
      return reactor.failDeferred(error);
    }
  }

  /*
   * Implements StashFactory.getStashService(...)
   */
  public synchronized StashService getStashService(String stashUri) throws URISyntaxException {
    URI uri = new URI(stashUri).normalize();
    StashServiceMapEntry mapEntry = stashMap.get(uri.toString());
    if (mapEntry != null) {
      return mapEntry.getStashService();
    } else {
      return null;
    }
  }

  /*
   * Callback handler for combining the output of the deferred concentrator used
   * during teardown to a single boolean value.
   */
  private final class StashTeardownCallbackHandler implements Deferrable<List<Boolean>, Boolean> {
    public Boolean onCallback(Deferred<List<Boolean>> deferred, List<Boolean> data) {
      return true;
    }

    public Boolean onErrback(Deferred<List<Boolean>> deferred, Exception error) throws Exception {
      throw error;
    }
  }

  /*
   * Stash service map entry and callback handler.
   */
  private final class StashServiceMapEntry implements Deferrable<StashService, StashService> {
    private DeferredSplitter<StashService> deferredSplitter = null;
    private StashServiceCore stashService = null;

    /*
     * Only return stash service references once the service has been set up.
     */
    private synchronized StashService getStashService() {
      return stashService;
    }

    /*
     * Initialise the stash service. Multiple attempts to initialise the stash
     * service are handled by using a deferred splitter.
     */
    private synchronized Deferred<StashService> initStashService(URI uri) {

      // Stash service already initialised - return handle to current service
      // object.
      if (stashService != null) {
        return reactor.callDeferred((StashService) stashService);
      }

      // Stash service initialisation in progress - return handle to the output
      // of the deferred splitter.
      else if (deferredSplitter != null) {
        return deferredSplitter.getOutputDeferred();
      }

      // Initialise the stash service using scheme specific initialisation
      // routines.
      else {
        try {

          // Implements simple filesystem backed stash service.
          if (uri.getScheme().equals("file")) {
            StashServiceCore stashServiceCore = new StashServiceCore(reactor,
                new StashFileServiceBackend(reactor, uri));
            deferredSplitter = reactor.newDeferredSplitter();

            // The callbacks on the deferred splitter will only be made after
            // the update callback to this stash service map entry.
            deferredSplitter.addInputDeferred(stashServiceCore.setup().addDeferrable(this));
            return deferredSplitter.getOutputDeferred();
          }

          // All other URI types are currently unsupported.
          else {
            throw (new URISyntaxException(uri.toString(), "Unsupported stash scheme."));
          }
        }

        // Any error conditions are passed back via deferred errback.
        catch (Exception error) {
          return reactor.failDeferred(error);
        }
      }
    }

    /*
     * Teardown the associated stash service on shutdown.
     */
    private synchronized Deferred<Boolean> teardownStashService() {
      Deferred<Boolean> deferredTeardown = stashService.teardown();
      stashService = null;
      return deferredTeardown;
    }

    /*
     * Update the local stash service reference on setup completion.
     */
    public synchronized StashService onCallback(Deferred<StashService> deferred, StashService stashService) {
      this.stashService = (StashServiceCore) stashService;
      return stashService;
    }

    /*
     * Re-throw error conditions so that they will appear in the logs.
     */
    public synchronized StashService onErrback(Deferred<StashService> deferred, Exception error) throws Exception {
      throw error;
    }
  }
}
