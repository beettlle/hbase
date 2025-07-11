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

import org.apache.yetus.audience.InterfaceAudience;

/**
 * A utility class to handle System.exit calls in a controlled manner.
 * This allows tests to intercept System.exit calls without actually terminating the JVM.
 * Instead of calling System.exit directly, use ExitHandler.getInstance().exit(status).
 */
@InterfaceAudience.Private
public class ExitHandler {
  private static volatile ExitHandler INSTANCE = new ExitHandler();
  private volatile boolean exitAllowed = false;

  // Package-private constructor for testing
  ExitHandler() {
    // Package-private constructor to allow testing
  }

  /**
   * Get the singleton instance of ExitHandler.
   * @return the ExitHandler instance
   */
  public static ExitHandler getInstance() {
    return INSTANCE;
  }

  /**
   * Set a custom ExitHandler instance for testing purposes.
   * This method should only be used in test code.
   * @param handler the custom ExitHandler instance
   */
  static void setInstance(ExitHandler handler) {
    INSTANCE = handler;
  }

  /**
   * Exit the application with the given status code.
   * In test environments, this throws a SecurityException instead of actually exiting.
   * In production, this calls System.exit(status).
   * 
   * @param status the exit status code
   * @throws SecurityException if exit is not allowed (typically in test environments)
   */
  public void exit(int status) throws SecurityException {
    if (exitAllowed) {
      System.exit(status);
    } else {
      throw new SecurityException("System.exit(" + status + ") intercepted");
    }
  }

  /**
   * Allow System.exit calls to proceed normally.
   * This should be called in production code or when exit behavior is desired.
   */
  public void allowExit() {
    this.exitAllowed = true;
  }

  /**
   * Prevent System.exit calls from proceeding (default behavior).
   * This should be called in test environments.
   */
  public void preventExit() {
    this.exitAllowed = false;
  }

  /**
   * Check if exit is currently allowed.
   * @return true if exit is allowed, false otherwise
   */
  public boolean isExitAllowed() {
    return exitAllowed;
  }
} 