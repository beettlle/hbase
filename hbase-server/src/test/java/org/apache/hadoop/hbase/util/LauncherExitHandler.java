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

/**
 * Utility class for testing main methods that call System.exit.
 * This replaces the deprecated LauncherSecurityManager approach.
 * 
 * Usage:
 * LauncherExitHandler exitHandler = new LauncherExitHandler();
 * try {
 *   SomeTool.main(args);
 *   fail("should be exception");
 * } catch (SecurityException e) {
 *   assertEquals(expectedExitCode, exitHandler.getExitCode());
 * }
 */
public class LauncherExitHandler {
  private int exitCode;
  private final ExitHandler originalExitHandler;

  public LauncherExitHandler() {
    reset();
    // Store the original exit handler state
    originalExitHandler = ExitHandler.getInstance();
  }

  /**
   * Get the exit code from the last intercepted System.exit call.
   * @return the exit code
   */
  public int getExitCode() {
    return exitCode;
  }

  /**
   * Reset the exit handler state.
   */
  public void reset() {
    exitCode = 0;
  }

  /**
   * Install this exit handler to intercept System.exit calls.
   * This replaces the original ExitHandler temporarily.
   */
  public void install() {
    // Create a custom ExitHandler that captures the exit code
    ExitHandler customHandler = new ExitHandler() {
      @Override
      public void exit(int status) throws SecurityException {
        exitCode = status;
        throw new SecurityException("Intercepted System.exit(" + status + ")");
      }
    };
    
    // Replace the singleton instance temporarily
    ExitHandler.setInstance(customHandler);
  }

  /**
   * Restore the original exit handler.
   */
  public void restore() {
    ExitHandler.setInstance(originalExitHandler);
  }
} 