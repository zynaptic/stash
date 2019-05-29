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

import com.zynaptic.stash.test.core.ObjectStashTests;

/**
 * Test case for checking functionality of object stash.
 * 
 * @author Chris Holgate
 */
public class ObjectStashTest extends TestCase {

  /**
   * Test basic string stash functionality.
   */
  public void testBasicStringStash() throws InterruptedException {
    JUnitTestRunner testRunner = new JUnitTestRunner();
    testRunner.runTest(new ObjectStashTests.BasicStringStash());
  }

  /**
   * Test basic object stash functionality.
   */
  public void testBasicObjectStash() throws InterruptedException {
    JUnitTestRunner testRunner = new JUnitTestRunner();
    testRunner.runTest(new ObjectStashTests.BasicObjectStash());
  }

  /**
   * Test basic object stash functionality with stash restart.
   */
  public void testBasicObjectStashRestart() throws InterruptedException {
    JUnitTestRunner testRunner = new JUnitTestRunner();
    testRunner.runTest(new ObjectStashTests.BasicObjectStashRestart());
  }
}
