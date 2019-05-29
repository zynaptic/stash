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

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.Date;
import java.util.List;
import java.util.NavigableMap;

import com.zynaptic.reaction.Deferrable;
import com.zynaptic.reaction.Deferred;
import com.zynaptic.reaction.DeferredConcentrator;
import com.zynaptic.reaction.Reactor;
import com.zynaptic.reaction.Timeable;
import com.zynaptic.stash.StashFactory;
import com.zynaptic.stash.StashService;
import com.zynaptic.stash.StashTimeSeriesEntry;

/**
 * This container class encapsulates the tests required to verify correct
 * functionality of the time series stash service.
 * 
 * @author Chris Holgate
 */
public class TimeSeriesStashTests {

  // Set this to true to prevent the stash from being automatically cleaned by
  // the tests.
  private static final boolean SKIP_STASH_CLEANUP = false;

  // Set the data block period.
  private static final int DATA_BLOCK_PERIOD = 5000;

  /**
   * This test case exercises the basic integer value stash functionality.
   */
  public static class TimeSeriesIntegerStash implements Timeable<TestParams> {

    // Specify maximum number of datapoints.
    private static final int MAX_DATAPOINT_COUNT = 25;

    // List of stash IDs to use during the test.
    private final String[] stashIds = { "time_series.integer.foo", "time_series.integer.bar.nested",
        "time_series.integer.foo.nested", "time_series.integer.bar" };
    private final String[] stashNames = { "Foo integer test.", "Bar nested integer test.", "Foo nested integer test.",
        "Bar integer test." };

    // Local test case state.
    private Reactor reactor;
    private Deferred<Boolean> deferredResult;
    private StashService stashService;

    // Data point counter for writes.
    private int dataPointCount;

    /*
     * Provides main entry point for the test case.
     */
    public void onTick(TestParams testParams) {
      reactor = testParams.getReactor();
      deferredResult = testParams.getDeferredResult();
      stashService = testParams.getStashService();

      // Start by adding the specified stash identifiers to the persistent set.
      try {
        stashService.reportStashContents(System.out);
        DeferredConcentrator<StashTimeSeriesEntry<Integer>> deferredConcentrator = reactor.newDeferredConcentrator();
        for (int i = 0; i < stashIds.length; i++) {
          deferredConcentrator.addInputDeferred(stashService.registerStashTimeSeriesId(stashIds[i], stashNames[i],
              Integer.valueOf(-1), DATA_BLOCK_PERIOD, 1));
        }
        deferredConcentrator.getOutputDeferred().addDeferrable(new StashRegistrationCompleteHandler(), true);
      } catch (Exception error) {
        deferredResult.errback(error);
      }
    }

    /*
     * Implements callback handler on stash registration completion. Reads back the
     * initialised object data.
     */
    private final class StashRegistrationCompleteHandler
        implements Deferrable<List<StashTimeSeriesEntry<Integer>>, Integer> {
      public Integer onCallback(Deferred<List<StashTimeSeriesEntry<Integer>>> deferred,
          List<StashTimeSeriesEntry<Integer>> stashEntryList) throws IOException {
        stashService.reportStashContents(System.out);
        DeferredConcentrator<Integer> deferredConcentrator = reactor.newDeferredConcentrator();
        for (int i = 0; i < stashIds.length; i++) {
          try {
            deferredConcentrator
                .addInputDeferred(stashService.getStashTimeSeriesEntry(stashIds[i], Integer.class).getCurrentValue());
          } catch (Exception error) {
            deferredResult.errback(error);
            return null;
          }
        }
        deferredConcentrator.getOutputDeferred().addDeferrable(new InitialReadbackHandler(), true);
        return null;
      }

      public Integer onErrback(Deferred<List<StashTimeSeriesEntry<Integer>>> deferred, Exception error) {
        deferredResult.errback(error);
        return null;
      }
    }

    /*
     * Implements callback handler on initial stash contents readback. Attempts to
     * write some new data.
     */
    private final class InitialReadbackHandler implements Deferrable<List<Integer>, Integer> {
      public Integer onCallback(Deferred<List<Integer>> deferred, List<Integer> readDataList) throws Exception {
        for (Integer readData : readDataList) {
          if (readData != -1) {
            deferredResult.errback(new Exception("Invalid default data in time series."));
            return null;
          }
        }
        dataPointCount = 1;
        reactor.runTimerOneShot(new DataPointWriter(), 0, null);
        return null;
      }

      public Integer onErrback(Deferred<List<Integer>> deferred, Exception error) {
        deferredResult.errback(error);
        return null;
      }
    }

    /*
     * Timed callback handler for initiating a data point write to the stash.
     */
    private final class DataPointWriter implements Timeable<Integer> {
      public void onTick(Integer data) {

        // Initiate readback if all data points have been written.
        if (dataPointCount == MAX_DATAPOINT_COUNT) {
          DeferredConcentrator<NavigableMap<Long, Integer>> deferredConcentrator = reactor.newDeferredConcentrator();
          for (int i = 0; i < stashIds.length; i++) {
            try {
              deferredConcentrator
                  .addInputDeferred(stashService.getStashTimeSeriesEntry(stashIds[i], Integer.class).getDataPointSet());
            } catch (Exception error) {
              deferredResult.errback(error);
              return;
            }
          }
          deferredConcentrator.getOutputDeferred().addDeferrable(new DataPointSetReadbackHandler(), true);
        }

        // Perform data point write.
        else {
          DeferredConcentrator<Boolean> deferredConcentrator = reactor.newDeferredConcentrator();
          for (int i = 0; i < stashIds.length; i++) {
            try {
              deferredConcentrator.addInputDeferred(stashService.getStashTimeSeriesEntry(stashIds[i], Integer.class)
                  .setCurrentValue(Integer.valueOf(dataPointCount)));
            } catch (Exception error) {
              deferredResult.errback(error);
              return;
            }
          }
          deferredConcentrator.getOutputDeferred().addDeferrable(new DataPointWriteHandler(), true);
        }
      }
    }

    /*
     * Implements callback handler for data point writes. Initiates next write cycle
     * after a short delay.
     */
    private final class DataPointWriteHandler implements Deferrable<List<Boolean>, Boolean> {
      public Boolean onCallback(Deferred<List<Boolean>> deferred, List<Boolean> data) {
        dataPointCount += 1;
        reactor.runTimerOneShot(new DataPointWriter(), 1000, null);
        return null;
      }

      public Boolean onErrback(Deferred<List<Boolean>> deferred, Exception error) {
        deferredResult.errback(error);
        return null;
      }
    }

    /*
     * Implements callback handler for data point set readback. Initiates stash
     * invalidation.
     */
    private class DataPointSetReadbackHandler implements Deferrable<List<NavigableMap<Long, Integer>>, Integer> {
      public Integer onCallback(Deferred<List<NavigableMap<Long, Integer>>> deferred,
          List<NavigableMap<Long, Integer>> dataPointSetList) throws IOException {
        for (NavigableMap<Long, Integer> dataPointSet : dataPointSetList) {
          for (Long timestamp : dataPointSet.navigableKeySet()) {
            Integer dataPoint = dataPointSet.get(timestamp);
            System.out.println(new Date(timestamp).toString() + " : Seq " + dataPoint);
          }
        }

        stashService.reportStashContents(System.out);
        if (SKIP_STASH_CLEANUP) {
          deferredResult.callback(true);
          return null;
        }

        DeferredConcentrator<Boolean> deferredConcentrator = reactor.newDeferredConcentrator();
        for (int i = 0; i < stashIds.length; i++) {
          deferredConcentrator.addInputDeferred(stashService.invalidateStashId(stashIds[i]));
        }
        deferredConcentrator.getOutputDeferred().addDeferrable(new StashInvalidationCompleteHandler(), true);
        return null;
      }

      public Integer onErrback(Deferred<List<NavigableMap<Long, Integer>>> deferred, Exception error) {
        deferredResult.errback(error);
        return null;
      }
    }

    /*
     * Implements callback handler on stash invalidation complete.
     */
    private final class StashInvalidationCompleteHandler implements Deferrable<List<Boolean>, Boolean> {
      public Boolean onCallback(Deferred<List<Boolean>> deferred, List<Boolean> data) {
        deferredResult.callback(true);
        return null;
      }

      public Boolean onErrback(Deferred<List<Boolean>> deferred, Exception error) {
        deferredResult.errback(error);
        return null;
      }
    }
  }

  /**
   * This test case exercises the basic double precision data value stash
   * functionality.
   */
  public static class TimeSeriesDoubleStash implements Timeable<TestParams> {

    // Specify maximum number of datapoints.
    private static final int MAX_DATAPOINT_COUNT = 25;

    // List of stash IDs to use during the test.
    private final String[] stashIds = { "time_series.double.foo", "time_series.double.bar.nested",
        "time_series.double.foo.nested", "time_series.double.bar" };
    private final String[] stashNames = { "Foo double precision test.", "Bar nested double precision test.",
        "Foo nested double precision test.", "Bar double precision test." };

    // Local test case state.
    private Reactor reactor;
    private Deferred<Boolean> deferredResult;
    private StashService stashService;

    // Data point counter for writes.
    private int dataPointCount;

    /*
     * Provides main entry point for the test case.
     */
    public void onTick(TestParams testParams) {
      reactor = testParams.getReactor();
      deferredResult = testParams.getDeferredResult();
      stashService = testParams.getStashService();

      // Start by adding the specified stash identifiers to the persistent set.
      try {
        stashService.reportStashContents(System.out);
        DeferredConcentrator<StashTimeSeriesEntry<Double>> deferredConcentrator = reactor.newDeferredConcentrator();
        for (int i = 0; i < stashIds.length; i++) {
          deferredConcentrator.addInputDeferred(
              stashService.registerStashTimeSeriesId(stashIds[i], stashNames[i], Double.valueOf(-1.1), 0, 4));
        }
        deferredConcentrator.getOutputDeferred().addDeferrable(new StashRegistrationCompleteHandler(), true);
      } catch (Exception error) {
        deferredResult.errback(error);
      }
    }

    /*
     * Implements callback handler on stash registration completion. Reads back the
     * initialised object data.
     */
    private final class StashRegistrationCompleteHandler
        implements Deferrable<List<StashTimeSeriesEntry<Double>>, Integer> {
      public Integer onCallback(Deferred<List<StashTimeSeriesEntry<Double>>> deferred,
          List<StashTimeSeriesEntry<Double>> stashEntryList) throws IOException {
        stashService.reportStashContents(System.out);
        DeferredConcentrator<Double> deferredConcentrator = reactor.newDeferredConcentrator();
        for (StashTimeSeriesEntry<Double> stashEntry : stashEntryList) {
          try {
            deferredConcentrator.addInputDeferred(stashEntry.getCurrentValue());
          } catch (Exception error) {
            deferredResult.errback(error);
            return null;
          }
        }
        deferredConcentrator.getOutputDeferred().addDeferrable(new InitialReadbackHandler(), true);
        return null;
      }

      public Integer onErrback(Deferred<List<StashTimeSeriesEntry<Double>>> deferred, Exception error) {
        deferredResult.errback(error);
        return null;
      }
    }

    /*
     * Implements callback handler on initial stash contents readback. Attempts to
     * write some new data.
     */
    private final class InitialReadbackHandler implements Deferrable<List<Double>, Integer> {
      public Integer onCallback(Deferred<List<Double>> deferred, List<Double> readDataList) throws Exception {
        for (Double readData : readDataList) {
          if (readData != -1.1) {
            deferredResult.errback(new Exception("Invalid default data in time series."));
            return null;
          }
        }
        dataPointCount = 1;
        reactor.runTimerOneShot(new DataPointWriter(), 0, null);
        return null;
      }

      public Integer onErrback(Deferred<List<Double>> deferred, Exception error) {
        deferredResult.errback(error);
        return null;
      }
    }

    /*
     * Timed callback handler for initiating a data point write to the stash.
     */
    private final class DataPointWriter implements Timeable<Integer> {
      public void onTick(Integer data) {

        // Initiate readback if all data points have been written.
        if (dataPointCount == MAX_DATAPOINT_COUNT) {
          DeferredConcentrator<NavigableMap<Long, Double>> deferredConcentrator = reactor.newDeferredConcentrator();
          for (int i = 0; i < stashIds.length; i++) {
            try {
              deferredConcentrator
                  .addInputDeferred(stashService.getStashTimeSeriesEntry(stashIds[i], Double.class).getDataPointSet());
            } catch (Exception error) {
              deferredResult.errback(error);
              return;
            }
          }
          deferredConcentrator.getOutputDeferred().addDeferrable(new DataPointSetReadbackHandler(), true);
        }

        // Perform data point write.
        else {
          DeferredConcentrator<Boolean> deferredConcentrator = reactor.newDeferredConcentrator();
          for (int i = 0; i < stashIds.length; i++) {
            try {
              deferredConcentrator.addInputDeferred(stashService.getStashTimeSeriesEntry(stashIds[i], Double.class)
                  .setCurrentValue(Double.valueOf(dataPointCount * 1.1)));
            } catch (Exception error) {
              deferredResult.errback(error);
              return;
            }
          }
          deferredConcentrator.getOutputDeferred().addDeferrable(new DataPointWriteHandler(), true);
        }
      }
    }

    /*
     * Implements callback handler for data point writes. Initiates next write cycle
     * after a short delay.
     */
    private final class DataPointWriteHandler implements Deferrable<List<Boolean>, Boolean> {
      public Boolean onCallback(Deferred<List<Boolean>> deferred, List<Boolean> data) {
        dataPointCount += 1;
        reactor.runTimerOneShot(new DataPointWriter(), 1000, null);
        return null;
      }

      public Boolean onErrback(Deferred<List<Boolean>> deferred, Exception error) {
        deferredResult.errback(error);
        return null;
      }
    }

    /*
     * Implements callback handler for data point set readback. Initiates stash
     * invalidation.
     */
    private class DataPointSetReadbackHandler implements Deferrable<List<NavigableMap<Long, Double>>, Integer> {
      public Integer onCallback(Deferred<List<NavigableMap<Long, Double>>> deferred,
          List<NavigableMap<Long, Double>> dataPointSetList) throws IOException {
        for (NavigableMap<Long, Double> dataPointSet : dataPointSetList) {
          for (Long timestamp : dataPointSet.navigableKeySet()) {
            Double dataPoint = dataPointSet.get(timestamp);
            System.out.println(new Date(timestamp).toString() + " : Seq " + dataPoint);
          }
        }

        stashService.reportStashContents(System.out);
        if (SKIP_STASH_CLEANUP) {
          deferredResult.callback(true);
          return null;
        }

        DeferredConcentrator<Boolean> deferredConcentrator = reactor.newDeferredConcentrator();
        for (int i = 0; i < stashIds.length; i++) {
          deferredConcentrator.addInputDeferred(stashService.invalidateStashId(stashIds[i]));
        }
        deferredConcentrator.getOutputDeferred().addDeferrable(new StashInvalidationCompleteHandler(), true);
        return null;
      }

      public Integer onErrback(Deferred<List<NavigableMap<Long, Double>>> deferred, Exception error) {
        deferredResult.errback(error);
        return null;
      }
    }

    /*
     * Implements callback handler on stash invalidation complete.
     */
    private final class StashInvalidationCompleteHandler implements Deferrable<List<Boolean>, Boolean> {
      public Boolean onCallback(Deferred<List<Boolean>> deferred, List<Boolean> data) {
        deferredResult.callback(true);
        return null;
      }

      public Boolean onErrback(Deferred<List<Boolean>> deferred, Exception error) {
        deferredResult.errback(error);
        return null;
      }
    }
  }

  /**
   * This test case exercises the array value stash functionality using a two
   * dimensional array of floating point values.
   */
  public static class TimeSeriesFloatArrayStash implements Timeable<TestParams> {

    // Specify maximum number of datapoints.
    private static final int MAX_DATAPOINT_COUNT = 25;

    // List of stash IDs to use during the test.
    private final String[] stashIds = { "time_series.float_array.foo", "time_series.float_array.bar.nested",
        "time_series.float_array.foo.nested", "time_series.float_array.bar" };
    private final String[] stashNames = { "Foo float array test.", "Bar nested float array test.",
        "Foo nested float array test.", "Bar float array test." };

    // Local test case state.
    private Reactor reactor;
    private Deferred<Boolean> deferredResult;
    private StashService stashService;

    // Data point counter for writes.
    private int dataPointCount;

    /*
     * Provides main entry point for the test case.
     */
    public void onTick(TestParams testParams) {
      reactor = testParams.getReactor();
      deferredResult = testParams.getDeferredResult();
      stashService = testParams.getStashService();

      // Start by adding the specified stash identifiers to the persistent set.
      try {
        stashService.reportStashContents(System.out);
        DeferredConcentrator<StashTimeSeriesEntry<float[][]>> deferredConcentrator = reactor.newDeferredConcentrator();
        for (int i = 0; i < stashIds.length; i++) {
          deferredConcentrator.addInputDeferred(stashService.registerStashTimeSeriesId(stashIds[i], stashNames[i],
              new float[][] { { -1, 0 }, { 1, 2 }, { 3, 4 } }, -1, 1));
        }
        deferredConcentrator.getOutputDeferred().addDeferrable(new StashRegistrationCompleteHandler(), true);
      } catch (Exception error) {
        deferredResult.errback(error);
      }
    }

    /*
     * Implements callback handler on stash registration completion. Reads back the
     * initialised object data.
     */
    private final class StashRegistrationCompleteHandler
        implements Deferrable<List<StashTimeSeriesEntry<float[][]>>, Integer> {
      public Integer onCallback(Deferred<List<StashTimeSeriesEntry<float[][]>>> deferred,
          List<StashTimeSeriesEntry<float[][]>> stashEntryList) throws IOException {
        stashService.reportStashContents(System.out);
        DeferredConcentrator<float[][]> deferredConcentrator = reactor.newDeferredConcentrator();
        for (int i = 0; i < stashIds.length; i++) {
          try {
            deferredConcentrator
                .addInputDeferred(stashService.getStashTimeSeriesEntry(stashIds[i], float[][].class).getCurrentValue());
          } catch (Exception error) {
            deferredResult.errback(error);
            return null;
          }
        }
        deferredConcentrator.getOutputDeferred().addDeferrable(new InitialReadbackHandler(), true);
        return null;
      }

      public Integer onErrback(Deferred<List<StashTimeSeriesEntry<float[][]>>> deferred, Exception error) {
        deferredResult.errback(error);
        return null;
      }
    }

    /*
     * Implements callback handler on initial stash contents readback. Attempts to
     * write some new data.
     */
    private final class InitialReadbackHandler implements Deferrable<List<float[][]>, Integer> {
      public Integer onCallback(Deferred<List<float[][]>> deferred, List<float[][]> readDataList) throws Exception {
        for (float[][] readData : readDataList) {
          if (readData[0][0] != -1) {
            deferredResult.errback(new Exception("Invalid default data in time series."));
            return null;
          }
        }
        dataPointCount = 1;
        reactor.runTimerOneShot(new DataPointWriter(), 0, null);
        return null;
      }

      public Integer onErrback(Deferred<List<float[][]>> deferred, Exception error) {
        deferredResult.errback(error);
        return null;
      }
    }

    /*
     * Timed callback handler for initiating a data point write to the stash.
     */
    private final class DataPointWriter implements Timeable<Integer> {
      public void onTick(Integer data) {

        // Initiate readback if all data points have been written.
        if (dataPointCount == MAX_DATAPOINT_COUNT) {
          DeferredConcentrator<NavigableMap<Long, float[][]>> deferredConcentrator = reactor.newDeferredConcentrator();
          for (int i = 0; i < stashIds.length; i++) {
            try {
              deferredConcentrator.addInputDeferred(
                  stashService.getStashTimeSeriesEntry(stashIds[i], float[][].class).getDataPointSet());
            } catch (Exception error) {
              deferredResult.errback(error);
              return;
            }
          }
          deferredConcentrator.getOutputDeferred().addDeferrable(new DataPointSetReadbackHandler(), true);
        }

        // Perform data point write.
        else {
          DeferredConcentrator<Boolean> deferredConcentrator = reactor.newDeferredConcentrator();
          for (int i = 0; i < stashIds.length; i++) {
            try {
              deferredConcentrator.addInputDeferred(stashService.getStashTimeSeriesEntry(stashIds[i], float[][].class)
                  .setCurrentValue(new float[][] { { dataPointCount, dataPointCount * 1.1f },
                      { dataPointCount * 1.2f, dataPointCount * 1.3f },
                      { dataPointCount * 1.4f, dataPointCount * 1.5f } }));
            } catch (Exception error) {
              deferredResult.errback(error);
              return;
            }
          }
          deferredConcentrator.getOutputDeferred().addDeferrable(new DataPointWriteHandler(), true);
        }
      }
    }

    /*
     * Implements callback handler for data point writes. Initiates next write cycle
     * after a short delay.
     */
    private final class DataPointWriteHandler implements Deferrable<List<Boolean>, Boolean> {
      public Boolean onCallback(Deferred<List<Boolean>> deferred, List<Boolean> data) {
        dataPointCount += 1;
        reactor.runTimerOneShot(new DataPointWriter(), 1000, null);
        return null;
      }

      public Boolean onErrback(Deferred<List<Boolean>> deferred, Exception error) {
        deferredResult.errback(error);
        return null;
      }
    }

    /*
     * Implements callback handler for data point set readback. Initiates stash
     * invalidation.
     */
    private class DataPointSetReadbackHandler implements Deferrable<List<NavigableMap<Long, float[][]>>, Integer> {
      public Integer onCallback(Deferred<List<NavigableMap<Long, float[][]>>> deferred,
          List<NavigableMap<Long, float[][]>> dataPointSetList) throws IOException {
        for (NavigableMap<Long, float[][]> dataPointSet : dataPointSetList) {
          for (Long timestamp : dataPointSet.navigableKeySet()) {
            float[][] dataPoint = dataPointSet.get(timestamp);
            System.out.println(new Date(timestamp).toString() + " : Seq " + dataPoint[2][1]);
          }
        }

        stashService.reportStashContents(System.out);
        if (SKIP_STASH_CLEANUP) {
          deferredResult.callback(true);
          return null;
        }

        DeferredConcentrator<Boolean> deferredConcentrator = reactor.newDeferredConcentrator();
        for (int i = 0; i < stashIds.length; i++) {
          deferredConcentrator.addInputDeferred(stashService.invalidateStashId(stashIds[i]));
        }
        deferredConcentrator.getOutputDeferred().addDeferrable(new StashInvalidationCompleteHandler(), true);
        return null;
      }

      public Integer onErrback(Deferred<List<NavigableMap<Long, float[][]>>> deferred, Exception error) {
        deferredResult.errback(error);
        return null;
      }
    }

    /*
     * Implements callback handler on stash invalidation complete.
     */
    private final class StashInvalidationCompleteHandler implements Deferrable<List<Boolean>, Boolean> {
      public Boolean onCallback(Deferred<List<Boolean>> deferred, List<Boolean> data) {
        deferredResult.callback(true);
        return null;
      }

      public Boolean onErrback(Deferred<List<Boolean>> deferred, Exception error) {
        deferredResult.errback(error);
        return null;
      }

    }
  }

  /**
   * This test case exercises the basic object stash functionality.
   */
  public static class TimeSeriesObjectStash implements Timeable<TestParams> {

    // Specify maximum number of datapoints.
    private static final int MAX_DATAPOINT_COUNT = 25;

    // List of stash IDs to use during the test.
    private final String[] stashIds = { "time_series.object.foo", "time_series.object.bar.nested",
        "time_series.object.foo.nested", "time_series.object.bar" };
    private final String[] stashNames = { "Foo object test.", "Bar nested object test.", "Foo nested object test.",
        "Bar object test." };

    // Local test case state.
    private Reactor reactor;
    private Deferred<Boolean> deferredResult;
    private StashService stashService;

    // Data point counter for writes.
    private int dataPointCount;

    /*
     * Provides main entry point for the test case.
     */
    public void onTick(TestParams testParams) {
      reactor = testParams.getReactor();
      deferredResult = testParams.getDeferredResult();
      stashService = testParams.getStashService().setCacheTimeout(250);

      // Start by adding the specified stash identifiers to the persistent set.
      try {
        stashService.reportStashContents(System.out);
        DeferredConcentrator<StashTimeSeriesEntry<ObjectWrapper>> deferredConcentrator = reactor
            .newDeferredConcentrator();
        for (int i = 0; i < stashIds.length; i++) {
          deferredConcentrator.addInputDeferred(
              stashService.registerStashTimeSeriesId(stashIds[i], stashNames[i], new ObjectWrapper("Init", 0), 1, 1));
        }
        deferredConcentrator.getOutputDeferred().addDeferrable(new StashRegistrationCompleteHandler(), true);
      } catch (Exception error) {
        deferredResult.errback(error);
      }
    }

    /*
     * Implements callback handler on stash registration completion. Reads back the
     * initialised object data.
     */
    private final class StashRegistrationCompleteHandler
        implements Deferrable<List<StashTimeSeriesEntry<ObjectWrapper>>, Integer> {
      public Integer onCallback(Deferred<List<StashTimeSeriesEntry<ObjectWrapper>>> deferred,
          List<StashTimeSeriesEntry<ObjectWrapper>> stashEntryList) throws IOException {
        stashService.reportStashContents(System.out);
        DeferredConcentrator<ObjectWrapper> deferredConcentrator = reactor.newDeferredConcentrator();
        for (int i = 0; i < stashIds.length; i++) {
          try {
            deferredConcentrator.addInputDeferred(
                stashService.getStashTimeSeriesEntry(stashIds[i], ObjectWrapper.class).getCurrentValue());
          } catch (Exception error) {
            deferredResult.errback(error);
            return null;
          }
        }
        deferredConcentrator.getOutputDeferred().addDeferrable(new InitialReadbackHandler(), true);
        return null;
      }

      public Integer onErrback(Deferred<List<StashTimeSeriesEntry<ObjectWrapper>>> deferred, Exception error) {
        deferredResult.errback(error);
        return null;
      }
    }

    /*
     * Implements callback handler on initial stash contents readback. Attempts to
     * write some new data.
     */
    private final class InitialReadbackHandler implements Deferrable<List<ObjectWrapper>, Integer> {
      public Integer onCallback(Deferred<List<ObjectWrapper>> deferred, List<ObjectWrapper> readDataList)
          throws Exception {
        for (ObjectWrapper readData : readDataList) {
          if ((!readData.getData().equals("Init")) && (readData.getCount() != 0)) {
            deferredResult.errback(new Exception("Invalid default data in time series."));
            return null;
          }
        }
        dataPointCount = 1;
        reactor.runTimerOneShot(new DataPointWriter(), 0, null);
        return null;
      }

      public Integer onErrback(Deferred<List<ObjectWrapper>> deferred, Exception error) {
        deferredResult.errback(error);
        return null;
      }
    }

    /*
     * Timed callback handler for initiating a data point write to the stash.
     */
    private final class DataPointWriter implements Timeable<Integer> {
      public void onTick(Integer data) {

        // Initiate readback if all data points have been written.
        if (dataPointCount == MAX_DATAPOINT_COUNT) {
          DeferredConcentrator<NavigableMap<Long, ObjectWrapper>> deferredConcentrator = reactor
              .newDeferredConcentrator();
          for (int i = 0; i < stashIds.length; i++) {
            try {
              deferredConcentrator.addInputDeferred(
                  stashService.getStashTimeSeriesEntry(stashIds[i], ObjectWrapper.class).getDataPointSet());
            } catch (Exception error) {
              deferredResult.errback(error);
              return;
            }
          }
          deferredConcentrator.getOutputDeferred().addDeferrable(new DataPointSetReadbackHandler(), true);
        }

        // Perform data point write.
        else {
          DeferredConcentrator<Boolean> deferredConcentrator = reactor.newDeferredConcentrator();
          for (int i = 0; i < stashIds.length; i++) {
            try {
              deferredConcentrator
                  .addInputDeferred(stashService.getStashTimeSeriesEntry(stashIds[i], ObjectWrapper.class)
                      .setCurrentValue(new ObjectWrapper(stashIds[i], dataPointCount)));
            } catch (Exception error) {
              deferredResult.errback(error);
              return;
            }
          }
          deferredConcentrator.getOutputDeferred().addDeferrable(new DataPointWriteHandler(), true);
        }
      }
    }

    /*
     * Implements callback handler for data point writes. Initiates next write cycle
     * after a short delay.
     */
    private final class DataPointWriteHandler implements Deferrable<List<Boolean>, Boolean> {
      public Boolean onCallback(Deferred<List<Boolean>> deferred, List<Boolean> data) {
        dataPointCount += 1;
        reactor.runTimerOneShot(new DataPointWriter(), 1000, null);
        return null;
      }

      public Boolean onErrback(Deferred<List<Boolean>> deferred, Exception error) {
        deferredResult.errback(error);
        return null;
      }
    }

    /*
     * Implements callback handler for data point set readback. Initiates stash
     * invalidation.
     */
    private class DataPointSetReadbackHandler implements Deferrable<List<NavigableMap<Long, ObjectWrapper>>, Integer> {
      public Integer onCallback(Deferred<List<NavigableMap<Long, ObjectWrapper>>> deferred,
          List<NavigableMap<Long, ObjectWrapper>> dataPointSetList) throws IOException {
        for (NavigableMap<Long, ObjectWrapper> dataPointSet : dataPointSetList) {
          for (Long timestamp : dataPointSet.navigableKeySet()) {
            ObjectWrapper dataPoint = dataPointSet.get(timestamp);
            System.out.println(new Date(timestamp).toString() + " : Seq " + dataPoint.count + " = " + dataPoint.data);
          }
        }

        stashService.reportStashContents(System.out);
        if (SKIP_STASH_CLEANUP) {
          deferredResult.callback(true);
          return null;
        }

        DeferredConcentrator<Boolean> deferredConcentrator = reactor.newDeferredConcentrator();
        for (int i = 0; i < stashIds.length; i++) {
          deferredConcentrator.addInputDeferred(stashService.invalidateStashId(stashIds[i]));
        }
        deferredConcentrator.getOutputDeferred().addDeferrable(new StashInvalidationCompleteHandler(), true);
        return null;
      }

      public Integer onErrback(Deferred<List<NavigableMap<Long, ObjectWrapper>>> deferred, Exception error) {
        deferredResult.errback(error);
        return null;
      }
    }

    /*
     * Implements callback handler on stash invalidation complete.
     */
    private final class StashInvalidationCompleteHandler implements Deferrable<List<Boolean>, Boolean> {
      public Boolean onCallback(Deferred<List<Boolean>> deferred, List<Boolean> data) {
        deferredResult.callback(true);
        return null;
      }

      public Boolean onErrback(Deferred<List<Boolean>> deferred, Exception error) {
        deferredResult.errback(error);
        return null;
      }
    }
  }

  /**
   * This test case exercises the time series object stash functionality with
   * stash service restart.
   */
  public static class TimeSeriesObjectStashRestart implements Timeable<TestParams> {

    // Specify maximum number of datapoints.
    private static final int MAX_DATAPOINT_COUNT = 25;

    // List of stash IDs to use during the test.
    private final String[] stashIds = { "time_series.object.foo", "time_series.object.bar.nested",
        "time_series.object.foo.nested", "time_series.object.bar" };
    private final String[] stashNames = { "Foo object test.", "Bar nested object test.", "Foo nested object test.",
        "Bar object test." };

    // Local test case state.
    private Reactor reactor;
    private Deferred<Boolean> deferredResult;
    private StashFactory stashFactory;
    private StashService stashService;
    private URI stashUri;

    // Data point counter for writes.
    private int dataPointCount = 0;

    // Flag which indicates whether the teardown cycle has completed.
    private boolean restarted = false;

    /*
     * Provides main entry point for the test case.
     */
    public void onTick(TestParams testParams) {
      reactor = testParams.getReactor();
      deferredResult = testParams.getDeferredResult();
      stashFactory = testParams.getStashFactory();
      stashService = testParams.getStashService().setCacheTimeout(250);

      // Start by adding the specified stash identifiers to the persistent set.
      try {
        stashService.reportStashContents(System.out);
        DeferredConcentrator<StashTimeSeriesEntry<ObjectWrapper>> deferredConcentrator = reactor
            .newDeferredConcentrator();
        for (int i = 0; i < stashIds.length; i++) {
          deferredConcentrator.addInputDeferred(
              stashService.registerStashTimeSeriesId(stashIds[i], stashNames[i], new ObjectWrapper("Init", 0), 10, 10));
        }
        deferredConcentrator.getOutputDeferred().addDeferrable(new StashRegistrationCompleteHandler(), true);
      } catch (Exception error) {
        deferredResult.errback(error);
      }
    }

    /*
     * Implements callback handler on stash registration completion. Reads back the
     * initialised object data.
     */
    private final class StashRegistrationCompleteHandler
        implements Deferrable<List<StashTimeSeriesEntry<ObjectWrapper>>, Integer> {
      public Integer onCallback(Deferred<List<StashTimeSeriesEntry<ObjectWrapper>>> deferred,
          List<StashTimeSeriesEntry<ObjectWrapper>> stashEntryList) throws IOException {
        stashService.reportStashContents(System.out);
        DeferredConcentrator<ObjectWrapper> deferredConcentrator = reactor.newDeferredConcentrator();
        for (int i = 0; i < stashIds.length; i++) {
          try {
            deferredConcentrator.addInputDeferred(
                stashService.getStashTimeSeriesEntry(stashIds[i], ObjectWrapper.class).getCurrentValue());
          } catch (Exception error) {
            deferredResult.errback(error);
            return null;
          }
        }
        deferredConcentrator.getOutputDeferred().addDeferrable(new InitialReadbackHandler(), true);
        return null;
      }

      public Integer onErrback(Deferred<List<StashTimeSeriesEntry<ObjectWrapper>>> deferred, Exception error) {
        deferredResult.errback(error);
        return null;
      }
    }

    /*
     * Implements callback handler on initial stash contents readback. Attempts to
     * write some new data.
     */
    private final class InitialReadbackHandler implements Deferrable<List<ObjectWrapper>, Integer> {
      public Integer onCallback(Deferred<List<ObjectWrapper>> deferred, List<ObjectWrapper> readDataList)
          throws Exception {
        for (ObjectWrapper readData : readDataList) {
          if ((!readData.getData().equals("Init")) && (readData.getCount() != 0)) {
            deferredResult.errback(new Exception("Invalid default data in time series."));
            return null;
          }
        }
        dataPointCount = 1;
        reactor.runTimerOneShot(new DataPointWriter(), 0, null);
        return null;
      }

      public Integer onErrback(Deferred<List<ObjectWrapper>> deferred, Exception error) {
        deferredResult.errback(error);
        return null;
      }
    }

    /*
     * Timed callback handler for initiating a data point write to the stash.
     */
    private final class DataPointWriter implements Timeable<Integer> {
      public void onTick(Integer data) {

        // Initiate stash teardown if half of the points have been written.
        if (!restarted && (dataPointCount == MAX_DATAPOINT_COUNT / 2)) {
          restarted = true;
          stashUri = stashService.getStashUri();
          stashFactory.haltStashService(stashUri.toString()).addDeferrable(new StashTeardownCallbackHandler(), true);
        }

        // Initiate readback if all data points have been written.
        else if (dataPointCount == MAX_DATAPOINT_COUNT) {
          DeferredConcentrator<NavigableMap<Long, ObjectWrapper>> deferredConcentrator = reactor
              .newDeferredConcentrator();
          for (int i = 0; i < stashIds.length; i++) {
            try {
              deferredConcentrator.addInputDeferred(
                  stashService.getStashTimeSeriesEntry(stashIds[i], ObjectWrapper.class).getDataPointSet());
            } catch (Exception error) {
              deferredResult.errback(error);
              return;
            }
          }
          deferredConcentrator.getOutputDeferred().addDeferrable(new DataPointSetReadbackHandler(), true);
        }

        // Perform data point write.
        else {
          DeferredConcentrator<Boolean> deferredConcentrator = reactor.newDeferredConcentrator();
          for (int i = 0; i < stashIds.length; i++) {
            try {
              StashTimeSeriesEntry<ObjectWrapper> stashEntry = stashService.getStashTimeSeriesEntry(stashIds[i],
                  ObjectWrapper.class);
              deferredConcentrator.addInputDeferred(stashEntry.setDataPointValue(System.currentTimeMillis(),
                  new ObjectWrapper(stashIds[i], 3 * dataPointCount - 2)));
              deferredConcentrator.addInputDeferred(stashEntry.setDataPointValue(System.currentTimeMillis() + 1,
                  new ObjectWrapper(stashIds[i], 3 * dataPointCount - 1)));
              deferredConcentrator.addInputDeferred(stashEntry.setDataPointValue(System.currentTimeMillis() + 2,
                  new ObjectWrapper(stashIds[i], 3 * dataPointCount)));
            } catch (Exception error) {
              deferredResult.errback(error);
              return;
            }
          }
          deferredConcentrator.getOutputDeferred().addDeferrable(new DataPointWriteHandler(), true);
        }
      }
    }

    /*
     * Implements callback handler for data point writes. Initiates next write cycle
     * after a short delay.
     */
    private final class DataPointWriteHandler implements Deferrable<List<Boolean>, Boolean> {
      public Boolean onCallback(Deferred<List<Boolean>> deferred, List<Boolean> data) {
        dataPointCount += 1;
        reactor.runTimerOneShot(new DataPointWriter(), 1000, null);
        return null;
      }

      public Boolean onErrback(Deferred<List<Boolean>> deferred, Exception error) {
        deferredResult.errback(error);
        return null;
      }
    }

    /*
     * Implements callback handler for data point set readback. Initiates stash
     * invalidation.
     */
    private class DataPointSetReadbackHandler implements Deferrable<List<NavigableMap<Long, ObjectWrapper>>, Integer> {
      public Integer onCallback(Deferred<List<NavigableMap<Long, ObjectWrapper>>> deferred,
          List<NavigableMap<Long, ObjectWrapper>> dataPointSetList) throws IOException {
        for (NavigableMap<Long, ObjectWrapper> dataPointSet : dataPointSetList) {
          int testCount = 0;
          for (Long timestamp : dataPointSet.navigableKeySet()) {
            ObjectWrapper dataPoint = dataPointSet.get(timestamp);
            System.out.println(new Date(timestamp).toString() + " : Seq " + dataPoint.count + " = " + dataPoint.data);
            if (dataPoint.count != testCount) {
              deferredResult.errback(new Exception("Read data sequence mismatch."));
              return null;
            }
            testCount += 1;
          }
        }

        stashService.reportStashContents(System.out);
        if (SKIP_STASH_CLEANUP) {
          deferredResult.callback(true);
          return null;
        }

        DeferredConcentrator<Boolean> deferredConcentrator = reactor.newDeferredConcentrator();
        for (int i = 0; i < stashIds.length; i++) {
          deferredConcentrator.addInputDeferred(stashService.invalidateStashId(stashIds[i]));
        }
        deferredConcentrator.getOutputDeferred().addDeferrable(new StashInvalidationCompleteHandler(), true);
        return null;
      }

      public Integer onErrback(Deferred<List<NavigableMap<Long, ObjectWrapper>>> deferred, Exception error) {
        deferredResult.errback(error);
        return null;
      }
    }

    /*
     * Implements callback handler on stash invalidation complete.
     */
    private final class StashInvalidationCompleteHandler implements Deferrable<List<Boolean>, Boolean> {
      public Boolean onCallback(Deferred<List<Boolean>> deferred, List<Boolean> data) {
        deferredResult.callback(true);
        return null;
      }

      public Boolean onErrback(Deferred<List<Boolean>> deferred, Exception error) {
        deferredResult.errback(error);
        return null;
      }
    }

    /*
     * Callback on completion of stash teardown.
     */
    private final class StashTeardownCallbackHandler implements Deferrable<Boolean, Boolean> {
      public Boolean onCallback(Deferred<Boolean> deferred, Boolean data) {
        System.out.println("Stash service torn down...");
        reactor.runTimerOneShot(new StashRestartInitiationHandler(), 5000, null);
        return null;
      }

      public Boolean onErrback(Deferred<Boolean> deferred, Exception error) {
        deferredResult.errback(error);
        return null;
      }
    }

    /*
     * Timed callback for initiating stash restart.
     */
    private final class StashRestartInitiationHandler implements Timeable<Boolean> {
      public void onTick(Boolean data) {
        stashFactory.initStashService(stashUri.toString()).addDeferrable(new StashRestartCallbackHandler(), true);
      }
    }

    /*
     * Callback on completion of stash restart.
     */
    private final class StashRestartCallbackHandler implements Deferrable<StashService, Boolean> {
      public Boolean onCallback(Deferred<StashService> deferred, StashService restartedStashService)
          throws IOException {
        System.out.println("Stash service restarted...");
        stashService = restartedStashService;
        stashService.reportStashContents(System.out);
        reactor.runTimerOneShot(new DataPointWriter(), 0, null);
        return null;
      }

      public Boolean onErrback(Deferred<StashService> deferred, Exception error) {
        deferredResult.errback(error);
        return null;
      }
    }
  }

  /*
   * Serializable object used for test data points.
   */
  public static final class ObjectWrapper implements Serializable {
    private static final long serialVersionUID = -9154668118029421074L;

    private final String data;
    private final int count;

    public ObjectWrapper(String data, int count) {
      this.data = data;
      this.count = count;
    }

    public String getData() {
      return data;
    }

    public int getCount() {
      return count;
    }
  }
}
