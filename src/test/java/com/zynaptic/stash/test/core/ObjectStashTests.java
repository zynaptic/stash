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

import java.io.Serializable;
import java.net.URI;
import java.util.List;

import com.zynaptic.reaction.Deferrable;
import com.zynaptic.reaction.Deferred;
import com.zynaptic.reaction.DeferredConcentrator;
import com.zynaptic.reaction.Reactor;
import com.zynaptic.reaction.Timeable;
import com.zynaptic.stash.StashFactory;
import com.zynaptic.stash.StashService;

/**
 * This container class encapsulates the tests required to verify correct
 * functionality of the object stash service.
 * 
 * @author Chris Holgate
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public class ObjectStashTests {

  /**
   * This test case exercises the basic object stash functionality using native
   * strings.
   */
  public static class BasicStringStash implements Timeable, Deferrable {

    // List of stash IDs to use during the test.
    private final String[] stashIds = { "string.test.foo", "string.test.bar.nested", "string.test.foo.nested",
        "string.test.bar" };
    private final String[] stashNames = { "Foo string test.", "Bar nested string test.", "Foo nested string test.",
        "Bar string test." };

    // Local test case state.
    private Reactor reactor;
    private Deferred deferredResult;
    private StashService stashService;
    private int callbackCount;

    /*
     * The timeable tick method is the entry point for the test case.
     */
    public void onTick(Object data) {
      TestParams params = (TestParams) data;
      reactor = params.getReactor();
      deferredResult = params.getDeferredResult();
      stashService = params.getStashService();

      // Start by adding the specified stash identifiers to the persistent set.
      try {
        stashService.reportStashContents(System.out);
        DeferredConcentrator deferredConcentrator = reactor.newDeferredConcentrator();
        for (int i = 0; i < stashIds.length; i++) {
          deferredConcentrator.addInputDeferred(stashService.registerStashObjectId(stashIds[i], stashNames[i], "Init"));
        }
        deferredConcentrator.getOutputDeferred().addDeferrable(this, true);
        callbackCount = 1;
      } catch (Exception error) {
        deferredResult.errback(error);
        deferredResult = null;
      }
    }

    /*
     * Callback handler is used to progress the test.
     */
    public Object onCallback(Deferred deferred, Object data) {
      DeferredConcentrator deferredConcentrator;
      try {
        switch (callbackCount) {

        // Read back the initialised object data.
        case 1:
          stashService.reportStashContents(System.out);
          deferredConcentrator = reactor.newDeferredConcentrator();
          for (int i = 0; i < stashIds.length; i++) {
            deferredConcentrator
                .addInputDeferred(stashService.getStashObjectEntry(stashIds[i], String.class).getCurrentValue());
          }
          deferredConcentrator.getOutputDeferred().addDeferrable(this, true);
          callbackCount++;
          break;

        // Check the initialised object data - should all be set to 'null'.
        case 2:
          Object[] initVals = ((List) data).toArray();
          boolean initValsOk = (initVals.length == stashIds.length);
          for (int i = 0; i < initVals.length; i++) {
            if (!((String) initVals[i]).equals("Init"))
              initValsOk = false;
          }
          if (initValsOk == false) {
            deferredResult.errback(new Exception("Initialised object stash values invalid."));
            deferredResult = null;
          }

          // Update contents of stashed object - use the stash entry name
          // strings.
          deferredConcentrator = reactor.newDeferredConcentrator();
          for (int i = 0; i < stashIds.length; i++) {
            deferredConcentrator.addInputDeferred(
                stashService.getStashObjectEntry(stashIds[i], String.class).setCurrentValue(stashNames[i]));
          }
          deferredConcentrator.getOutputDeferred().addDeferrable(this, true);
          callbackCount++;
          break;

        // Read back updated object data.
        case 3:
          stashService.reportStashContents(System.out);
          deferredConcentrator = reactor.newDeferredConcentrator();
          for (int i = 0; i < stashIds.length; i++) {
            deferredConcentrator
                .addInputDeferred(stashService.getStashObjectEntry(stashIds[i], String.class).getCurrentValue());
          }
          deferredConcentrator.getOutputDeferred().addDeferrable(this, true);
          callbackCount++;
          break;

        // Check that the retrieved object data matches the stash entry name
        // strings, then initiate object invalidation.
        case 4:
          Object[] updatedVals = ((List) data).toArray();
          boolean updatedValsOk = (updatedVals.length == stashIds.length);
          for (int i = 0; i < updatedVals.length; i++) {
            if (!stashNames[i].equals(updatedVals[i]))
              updatedValsOk = false;
          }
          if (updatedValsOk == false) {
            deferredResult.errback(new Exception("Updated object stash values invalid."));
            deferredResult = null;
          }

          // Invalidate the stashed objects.
          deferredConcentrator = reactor.newDeferredConcentrator();
          for (int i = 0; i < stashIds.length; i++) {
            deferredConcentrator.addInputDeferred(stashService.invalidateStashId(stashIds[i]));
          }
          deferredConcentrator.getOutputDeferred().addDeferrable(this, true);
          callbackCount++;
          break;

        // Callback on test complete.
        case 5:
          stashService.reportStashContents(System.out);
          if (deferredResult != null) {
            deferredResult.callback(null);
            deferredResult = null;
          }
          break;
        }
      } catch (Exception error) {
        if (deferredResult != null) {
          deferredResult.errback(error);
          deferredResult = null;
        }
      }
      return null;
    }

    /*
     * Errback handler forwards the error condition back to the test framework.
     */
    public Object onErrback(Deferred deferred, Exception error) {
      if (deferredResult != null) {
        deferredResult.errback(error);
        deferredResult = null;
      }
      return null;
    }
  }

  /**
   * This test case exercises the basic object stash functionality using arbitrary
   * objects.
   */
  public static class BasicObjectStash implements Timeable, Deferrable {

    // List of stash IDs to use during the test.
    private final String[] stashIds = { "object.test.foo", "object.test.bar.nested", "object.test.foo.nested",
        "object.test.bar" };
    private final String[] stashNames = { "Foo object test.", "Bar nested object test.", "Foo nested object test.",
        "Bar object test." };

    // Local test case state.
    private Reactor reactor;
    private Deferred deferredResult;
    private StashService stashService;
    private int callbackCount;

    /*
     * The timeable tick method is the entry point for the test case.
     */
    public void onTick(Object data) {
      TestParams params = (TestParams) data;
      reactor = params.getReactor();
      deferredResult = params.getDeferredResult();
      stashService = params.getStashService();

      // Start by adding the specified stash identifiers to the persistent set.
      try {
        stashService.reportStashContents(System.out);
        DeferredConcentrator deferredConcentrator = reactor.newDeferredConcentrator();
        TestObject initData = new TestObject(0, "Init");
        // String initData = "WTF?";
        for (int i = 0; i < stashIds.length; i++) {
          deferredConcentrator
              .addInputDeferred(stashService.registerStashObjectId(stashIds[i], stashNames[i], initData));
        }
        deferredConcentrator.getOutputDeferred().addDeferrable(this, true);
        callbackCount = 1;
      } catch (Exception error) {
        deferredResult.errback(error);
        deferredResult = null;
      }
    }

    /*
     * Callback handler is used to progress the test.
     */
    public Object onCallback(Deferred deferred, Object data) {
      DeferredConcentrator deferredConcentrator;
      try {
        switch (callbackCount) {

        // Read back the initialised object data.
        case 1:
          stashService.reportStashContents(System.out);
          deferredConcentrator = reactor.newDeferredConcentrator();
          for (int i = 0; i < stashIds.length; i++) {
            deferredConcentrator
                .addInputDeferred(stashService.getStashObjectEntry(stashIds[i], TestObject.class).getCurrentValue());
          }
          deferredConcentrator.getOutputDeferred().addDeferrable(this, true);
          callbackCount++;
          break;

        // Check the initialised object data - should all be set to 'null'.
        case 2:
          Object[] initVals = ((List) data).toArray();
          boolean initValsOk = (initVals.length == stashIds.length);
          for (int i = 0; i < initVals.length; i++) {
            TestObject testObject = (TestObject) initVals[i];
            if (!((testObject.getObjectString().equals("Init")) && (testObject.getObjectNumber() == 0)))
              initValsOk = false;
          }
          if (initValsOk == false) {
            deferredResult.errback(new Exception("Initialised object stash values invalid."));
            deferredResult = null;
          }

          // Update contents of stashed object - use the stash entry name
          // strings.
          deferredConcentrator = reactor.newDeferredConcentrator();
          for (int i = 0; i < stashIds.length; i++) {
            deferredConcentrator.addInputDeferred(stashService.getStashObjectEntry(stashIds[i], TestObject.class)
                .setCurrentValue(new TestObject(i + 1, stashNames[i])));
          }
          deferredConcentrator.getOutputDeferred().addDeferrable(this, true);
          callbackCount++;
          break;

        // Read back updated object data.
        case 3:
          stashService.reportStashContents(System.out);
          deferredConcentrator = reactor.newDeferredConcentrator();
          for (int i = 0; i < stashIds.length; i++) {
            deferredConcentrator
                .addInputDeferred(stashService.getStashObjectEntry(stashIds[i], TestObject.class).getCurrentValue());
          }
          deferredConcentrator.getOutputDeferred().addDeferrable(this, true);
          callbackCount++;
          break;

        // Check that the retrieved object data matches the stash entry name
        // strings, then initiate object invalidation.
        case 4:
          Object[] updatedVals = ((List) data).toArray();
          boolean updatedValsOk = (updatedVals.length == stashIds.length);
          for (int i = 0; i < updatedVals.length; i++) {
            TestObject testObject = (TestObject) updatedVals[i];
            if (!((stashNames[i].equals(testObject.getObjectString())) && (testObject.getObjectNumber() == i + 1)))
              updatedValsOk = false;
          }
          if (updatedValsOk == false) {
            deferredResult.errback(new Exception("Updated object stash values invalid."));
            deferredResult = null;
          }

          // Invalidate the stashed objects.
          deferredConcentrator = reactor.newDeferredConcentrator();
          for (int i = 0; i < stashIds.length; i++) {
            deferredConcentrator.addInputDeferred(stashService.invalidateStashId(stashIds[i]));
          }
          deferredConcentrator.getOutputDeferred().addDeferrable(this, true);
          callbackCount++;
          break;

        // Callback on test complete.
        case 5:
          stashService.reportStashContents(System.out);
          if (deferredResult != null) {
            deferredResult.callback(null);
            deferredResult = null;
          }
          break;
        }
      } catch (Exception error) {
        if (deferredResult != null) {
          deferredResult.errback(error);
          deferredResult = null;
        }
      }
      return null;
    }

    /*
     * Errback handler forwards the error condition back to the test framework.
     */
    public Object onErrback(Deferred deferred, Exception error) {
      if (deferredResult != null) {
        deferredResult.errback(error);
        deferredResult = null;
      }
      return null;
    }
  }

  /**
   * This test case exercises the basic object stash functionality using arbitrary
   * objects and restarted stash.
   */
  public static class BasicObjectStashRestart implements Timeable, Deferrable {

    // List of stash IDs to use during the test.
    private final String[] stashIds = { "object.test.foo", "object.test.bar.nested", "object.test.foo.nested",
        "object.test.bar" };
    private final String[] stashNames = { "Foo object test.", "Bar nested object test.", "Foo nested object test.",
        "Bar object test." };

    // Local test case state.
    private Reactor reactor;
    private Deferred deferredResult;
    private StashFactory stashFactory;
    private StashService stashService;
    private URI stashUri;
    private int callbackCount;

    /*
     * The timeable tick method is the entry point for the test case.
     */
    public void onTick(Object data) {
      TestParams params = (TestParams) data;
      reactor = params.getReactor();
      deferredResult = params.getDeferredResult();
      stashFactory = params.getStashFactory();
      stashService = params.getStashService();

      // Start by adding the specified stash identifiers to the persistent set.
      try {
        stashService.reportStashContents(System.out);
        DeferredConcentrator deferredConcentrator = reactor.newDeferredConcentrator();
        TestObject initData = new TestObject(0, "Init");
        // String initData = "WTF?";
        for (int i = 0; i < stashIds.length; i++) {
          deferredConcentrator
              .addInputDeferred(stashService.registerStashObjectId(stashIds[i], stashNames[i], initData));
        }
        deferredConcentrator.getOutputDeferred().addDeferrable(this, true);
        callbackCount = 1;
      } catch (Exception error) {
        deferredResult.errback(error);
        deferredResult = null;
      }
    }

    /*
     * Callback handler is used to progress the test.
     */
    public Object onCallback(Deferred deferred, Object data) {
      DeferredConcentrator deferredConcentrator;
      try {
        switch (callbackCount) {

        // Read back the initialised object data.
        case 1:
          stashService.reportStashContents(System.out);
          deferredConcentrator = reactor.newDeferredConcentrator();
          for (int i = 0; i < stashIds.length; i++) {
            deferredConcentrator
                .addInputDeferred(stashService.getStashObjectEntry(stashIds[i], TestObject.class).getCurrentValue());
          }
          deferredConcentrator.getOutputDeferred().addDeferrable(this, true);
          callbackCount++;
          break;

        // Check the initialised object data - should all be set to 'null'.
        case 2:
          Object[] initVals = ((List) data).toArray();
          boolean initValsOk = (initVals.length == stashIds.length);
          for (int i = 0; i < initVals.length; i++) {
            TestObject testObject = (TestObject) initVals[i];
            if (!((testObject.getObjectString().equals("Init")) && (testObject.getObjectNumber() == 0)))
              initValsOk = false;
          }
          if (initValsOk == false) {
            deferredResult.errback(new Exception("Initialised object stash values invalid."));
            deferredResult = null;
          }

          // Update contents of stashed object - use the stash entry name
          // strings.
          deferredConcentrator = reactor.newDeferredConcentrator();
          for (int i = 0; i < stashIds.length; i++) {
            deferredConcentrator.addInputDeferred(stashService.getStashObjectEntry(stashIds[i], TestObject.class)
                .setCurrentValue(new TestObject(i + 1, stashNames[i])));
          }
          deferredConcentrator.getOutputDeferred().addDeferrable(this, true);
          callbackCount++;
          break;

        // Halt the stash service.
        case 3:
          stashUri = stashService.getStashUri();
          stashFactory.haltStashService(stashUri.toString()).addDeferrable(this, true);
          callbackCount++;
          break;

        // Restart the stash service.
        case 4:
          stashFactory.initStashService(stashUri.toString()).addDeferrable(this, true);
          callbackCount++;
          break;

        // Read back updated object data.
        case 5:
          stashService = (StashService) data;
          stashService.reportStashContents(System.out);
          deferredConcentrator = reactor.newDeferredConcentrator();
          for (int i = 0; i < stashIds.length; i++) {
            deferredConcentrator
                .addInputDeferred(stashService.getStashObjectEntry(stashIds[i], TestObject.class).getCurrentValue());
          }
          deferredConcentrator.getOutputDeferred().addDeferrable(this, true);
          callbackCount++;
          break;

        // Check that the retrieved object data matches the stash entry name
        // strings, then initiate object invalidation.
        case 6:
          stashService.reportStashContents(System.out);
          Object[] updatedVals = ((List) data).toArray();
          boolean updatedValsOk = (updatedVals.length == stashIds.length);
          for (int i = 0; i < updatedVals.length; i++) {
            TestObject testObject = (TestObject) updatedVals[i];
            if (!((stashNames[i].equals(testObject.getObjectString())) && (testObject.getObjectNumber() == i + 1)))
              updatedValsOk = false;
          }
          if (updatedValsOk == false) {
            deferredResult.errback(new Exception("Updated object stash values invalid."));
            deferredResult = null;
          }

          // Invalidate the stashed objects.
          deferredConcentrator = reactor.newDeferredConcentrator();
          for (int i = 0; i < stashIds.length; i++) {
            deferredConcentrator.addInputDeferred(stashService.invalidateStashId(stashIds[i]));
          }
          deferredConcentrator.getOutputDeferred().addDeferrable(this, true);
          callbackCount++;
          break;

        // Callback on test complete.
        case 7:
          stashService.reportStashContents(System.out);
          if (deferredResult != null) {
            deferredResult.callback(null);
            deferredResult = null;
          }
          break;
        }
      } catch (Exception error) {
        if (deferredResult != null) {
          deferredResult.errback(error);
          deferredResult = null;
        }
      }
      return null;
    }

    /*
     * Errback handler forwards the error condition back to the test framework.
     */
    public Object onErrback(Deferred deferred, Exception error) {
      if (deferredResult != null) {
        deferredResult.errback(error);
        deferredResult = null;
      }
      return null;
    }
  }

  /*
   * The test object to be stored in the stash.
   */
  public static final class TestObject implements Serializable {
    private static final long serialVersionUID = -7235102330944103386L;
    private final int objectNumber;
    private final String objectString;

    private TestObject(int objectNumber, String objectString) {
      this.objectNumber = objectNumber;
      this.objectString = objectString;
    }

    private int getObjectNumber() {
      return objectNumber;
    }

    private String getObjectString() {
      return objectString;
    }
  }
}
