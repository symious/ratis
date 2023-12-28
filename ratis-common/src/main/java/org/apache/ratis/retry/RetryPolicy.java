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
package org.apache.ratis.retry;

import org.apache.ratis.util.TimeDuration;

import java.util.concurrent.TimeUnit;

/**
 * Policy abstract for retrying.
 */
public interface RetryPolicy {
  TimeDuration ZERO_MILLIS = TimeDuration.valueOf(0, TimeUnit.MILLISECONDS);

  /**
   * Determines whether it is supposed to retry the connection if the operation
   * fails for some reason.
   *
   * @param attemptCount The number of times attempted so far
   * @return true if it has to make another attempt, otherwise, false
   */
  boolean shouldRetry(int attemptCount);

  /**
   * Returns the time duration for sleep in between the retries.
   */
  default TimeDuration getSleepTime() {
    return ZERO_MILLIS;
  }
}
