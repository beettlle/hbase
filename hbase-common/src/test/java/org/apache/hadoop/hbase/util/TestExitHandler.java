/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MiscTests.class, SmallTests.class })
public class TestExitHandler {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestExitHandler.class);

  @Test
  public void testExitHandlerSingleton() {
    ExitHandler handler1 = ExitHandler.getInstance();
    ExitHandler handler2 = ExitHandler.getInstance();
    assertEquals("ExitHandler should be a singleton", handler1, handler2);
  }

  @Test
  public void testDefaultState() {
    ExitHandler handler = ExitHandler.getInstance();
    assertFalse("Exit should be prevented by default", handler.isExitAllowed());
  }

  @Test
  public void testAllowExit() {
    ExitHandler handler = ExitHandler.getInstance();
    handler.allowExit();
    assertTrue("Exit should be allowed after allowExit()", handler.isExitAllowed());
  }

  @Test
  public void testPreventExit() {
    ExitHandler handler = ExitHandler.getInstance();
    handler.allowExit();
    handler.preventExit();
    assertFalse("Exit should be prevented after preventExit()", handler.isExitAllowed());
  }

  @Test(expected = SecurityException.class)
  public void testExitThrowsExceptionWhenPrevented() {
    ExitHandler handler = ExitHandler.getInstance();
    handler.preventExit();
    handler.exit(1);
  }

  @Test
  public void testExitMessage() {
    ExitHandler handler = ExitHandler.getInstance();
    handler.preventExit();
    try {
      handler.exit(42);
    } catch (SecurityException e) {
      assertEquals("System.exit(42) intercepted", e.getMessage());
    }
  }
} 