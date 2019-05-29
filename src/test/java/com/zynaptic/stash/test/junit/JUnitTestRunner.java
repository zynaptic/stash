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

package com.zynaptic.stash.test.junit;

import junit.framework.Assert;

import com.zynaptic.reaction.Deferrable;
import com.zynaptic.reaction.Deferred;
import com.zynaptic.reaction.Reactor;
import com.zynaptic.reaction.Timeable;
import com.zynaptic.reaction.core.ReactorControl;
import com.zynaptic.reaction.core.ReactorCore;
import com.zynaptic.reaction.util.FixedUpMonotonicClock;
import com.zynaptic.reaction.util.MonotonicClockSource;
import com.zynaptic.reaction.util.ReactorLogSystemOut;
import com.zynaptic.reaction.util.ReactorLogTarget;
import com.zynaptic.stash.StashService;
import com.zynaptic.stash.core.StashFactoryCore;
import com.zynaptic.stash.test.core.TestParams;

/**
 * Wrapper used to set up common tests in the JUnit framework. Includes reactor
 * control and the Deferrable interface which is used for notifying the JUnit
 * framework of successful or failed text completions.
 * 
 * @author Chris Holgate
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public class JUnitTestRunner implements Deferrable {

  // Define test fixture state space.
  private static final int TEST_STATE_IDLE = 0x00;
  private static final int TEST_STATE_STARTING_FACTORY = 0x01;
  private static final int TEST_STATE_STARTING_STASH = 0x02;
  private static final int TEST_STATE_RUNNING = 0x03;
  private static final int TEST_STATE_STOPPING_STASH = 0x04;

  // Use fixed up wallclock as reactor timebase.
  private final MonotonicClockSource reactorClock = new FixedUpMonotonicClock();

  // Use logging to System.out.
  private final ReactorLogTarget logService = new ReactorLogSystemOut();

  // Local handle on the reactor control interface.
  private final ReactorControl reactorControl = ReactorCore.getReactorControl();

  // Local handle on the reactor user interface.
  private final Reactor reactor = ReactorCore.getReactor();

  // Handle on the stash factory.
  private StashFactoryCore stashFactory;

  // Handle on the stash component.
  private StashService stashService;

  // Local handle on the test case object.
  private Timeable testCase;

  // Track the current test case state.
  private int testState = TEST_STATE_IDLE;

  /**
   * Run the specified common test code.
   */
  public void runTest(Timeable test) {
    testCase = test;
    reactorControl.start(reactorClock, logService);

    // Create the stash service factory and start it up.
    testState = TEST_STATE_STARTING_FACTORY;
    stashFactory = new StashFactoryCore();
    stashFactory.setup(reactor).addDeferrable(this, true);

    // Wait for test completion.
    try {
      reactorControl.join();
    } catch (Exception error) {
      error.printStackTrace();
      Assert.fail(error.getMessage());
    }
  }

  /**
   * Deferred callback implies successful completion. This method just stops the
   * reactor.
   */
  public Object onCallback(Deferred deferred, Object data) throws Exception {
    switch (testState) {

    // Callback on completion of factory setup process.
    case TEST_STATE_STARTING_FACTORY:
      System.out.println("Started stash resource factory.");
      testState = TEST_STATE_STARTING_STASH;
      String currentDir = System.getProperty("user.dir");
      currentDir = currentDir.replace('\\', '/');
      stashFactory.initStashService("file:///" + currentDir + "/stashtest").addDeferrable(this, true);
      break;

    // Callback on completion of setup process - run the test case. Note that
    // the callback parameter from the stash resource initialisation is the
    // corresponding stash service object.
    case TEST_STATE_STARTING_STASH:
      System.out.println("Created file system stash.");
      stashService = (StashService) data;
      Deferred deferredDone = reactor.newDeferred();
      deferredDone.addDeferrable(this, true);
      TestParams params = new TestParams(reactor, deferredDone, stashFactory, stashService);
      testState = TEST_STATE_RUNNING;
      reactor.runTimerOneShot(testCase, 0, params);
      break;

    // Callback on test completion. Initiate teardown of stash service.
    case TEST_STATE_RUNNING:
      System.out.println("RESULT  : TEST PASSED.");
      testState = TEST_STATE_STOPPING_STASH;
      stashFactory.teardown().addDeferrable(this, true);
      break;

    // Callback on completion of stash service teardown. Stop the reactor.
    case TEST_STATE_STOPPING_STASH:
      testState = TEST_STATE_IDLE;
      reactorControl.stop();
      break;
    }
    return null;
  }

  /**
   * Errback implies failed completion. This method asserts then stops the
   * reactor.
   */
  public Object onErrback(Deferred deferred, Exception error) throws Exception {
    System.out.println("RESULT  : TEST FAILED.");
    error.printStackTrace();
    Assert.fail(error.toString());
    return null;
  }
}
