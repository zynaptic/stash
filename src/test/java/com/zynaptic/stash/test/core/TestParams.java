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

package com.zynaptic.stash.test.core;

import com.zynaptic.reaction.Deferred;
import com.zynaptic.reaction.Reactor;
import com.zynaptic.stash.StashFactory;
import com.zynaptic.stash.StashService;

/**
 * Defines the set of parameters to be passed into the test cases.
 * 
 * @author Chris Holgate
 */
public final class TestParams {

  // Test parameter values.
  private final Reactor reactor;
  private final Deferred<Boolean> deferredResult;
  private final StashFactory stashFactory;
  private final StashService stashService;

  public TestParams(Reactor reactor, Deferred<Boolean> deferredResult, StashFactory stashFactory,
      StashService stashService) {
    this.reactor = reactor;
    this.deferredResult = deferredResult;
    this.stashFactory = stashFactory;
    this.stashService = stashService;
  }

  public Reactor getReactor() {
    return reactor;
  }

  public Deferred<Boolean> getDeferredResult() {
    return deferredResult;
  }

  public StashFactory getStashFactory() {
    return stashFactory;
  }

  public StashService getStashService() {
    return stashService;
  }
}
