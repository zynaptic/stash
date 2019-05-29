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

import junit.framework.TestCase;

import com.zynaptic.stash.test.core.TimeSeriesStashTests;

/**
 * Test case for checking functionality of object stash.
 * 
 * @author Chris Holgate
 */
public class TimeSeriesStashTest extends TestCase {

  /**
   * Test basic time series stash functionality for simple integer objects.
   */
  public void testTimeSeriesIntegerStash() throws InterruptedException {
    JUnitTestRunner testRunner = new JUnitTestRunner();
    testRunner.runTest(new TimeSeriesStashTests.TimeSeriesIntegerStash());
  }

  /**
   * Test basic time series stash functionality for simple double precision
   * objects.
   */
  public void testTimeSeriesDoubleStash() throws InterruptedException {
    JUnitTestRunner testRunner = new JUnitTestRunner();
    testRunner.runTest(new TimeSeriesStashTests.TimeSeriesDoubleStash());
  }

  /**
   * Test basic time series stash functionality for floating point array objects.
   */
  public void testTimeSeriesFloatArrayStash() throws InterruptedException {
    JUnitTestRunner testRunner = new JUnitTestRunner();
    testRunner.runTest(new TimeSeriesStashTests.TimeSeriesFloatArrayStash());
  }

  /**
   * Test basic time series stash functionality for serializable objects.
   */
  public void testTimeSeriesObjectStash() throws InterruptedException {
    JUnitTestRunner testRunner = new JUnitTestRunner();
    testRunner.runTest(new TimeSeriesStashTests.TimeSeriesObjectStash());
  }

  /**
   * Test restarted time series stash functionality for serializable objects.
   */
  public void testTimeSeriesObjectStashRestart() throws InterruptedException {
    JUnitTestRunner testRunner = new JUnitTestRunner();
    testRunner.runTest(new TimeSeriesStashTests.TimeSeriesObjectStashRestart());
  }
}
