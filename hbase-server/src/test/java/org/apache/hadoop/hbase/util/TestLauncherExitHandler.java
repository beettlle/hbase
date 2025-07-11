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

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@Category({ MiscTests.class, SmallTests.class })
public class TestLauncherExitHandler {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestLauncherExitHandler.class);

  @Test
  public void testLauncherExitHandlerCapturesExitCode() {
    LauncherExitHandler exitHandler = new LauncherExitHandler();
    assertEquals("Initial exit code should be 0", 0, exitHandler.getExitCode());
    
    try {
      exitHandler.install();
      ExitHandler.getInstance().exit(42);
      fail("Should have thrown SecurityException");
    } catch (SecurityException e) {
      assertEquals("Exit code should be captured", 42, exitHandler.getExitCode());
    } finally {
      exitHandler.restore();
    }
  }

  @Test
  public void testLauncherExitHandlerReset() {
    LauncherExitHandler exitHandler = new LauncherExitHandler();
    
    try {
      exitHandler.install();
      ExitHandler.getInstance().exit(42);
    } catch (SecurityException e) {
      assertEquals("Exit code should be captured", 42, exitHandler.getExitCode());
    } finally {
      exitHandler.restore();
    }
    
    exitHandler.reset();
    assertEquals("Exit code should be reset to 0", 0, exitHandler.getExitCode());
  }

  @Test
  public void testLauncherExitHandlerMultipleExits() {
    LauncherExitHandler exitHandler = new LauncherExitHandler();
    
    try {
      exitHandler.install();
      try {
        ExitHandler.getInstance().exit(10);
      } catch (SecurityException e) {
        assertEquals("First exit code should be captured", 10, exitHandler.getExitCode());
      }
      
      try {
        ExitHandler.getInstance().exit(20);
      } catch (SecurityException e) {
        assertEquals("Second exit code should overwrite first", 20, exitHandler.getExitCode());
      }
    } finally {
      exitHandler.restore();
    }
  }
} 