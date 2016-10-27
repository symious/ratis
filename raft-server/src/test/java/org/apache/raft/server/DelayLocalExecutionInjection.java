/**
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
package org.apache.raft.server;

import org.apache.raft.RaftTestUtil;
import org.apache.raft.util.CodeInjectionForTesting;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/** Inject code to delay particular servers. */
public class DelayLocalExecutionInjection implements CodeInjectionForTesting.Code {
  private final Map<String, AtomicInteger> delays = new ConcurrentHashMap<>();

  public DelayLocalExecutionInjection(String method) {
    CodeInjectionForTesting.put(method, this);
  }

  public void clear() {
    delays.clear();
  }

  public void setDelayMs(String id, int delayMs) {
    AtomicInteger d = delays.get(id);
    if (d == null) {
      delays.put(id, d = new AtomicInteger());
    }
    d.set(delayMs);
  }

  public void removeDelay(String id) {
    delays.remove(id);
  }

  @Override
  public boolean execute(String localId, String remoteId, Object... args) {
    final AtomicInteger d = delays.get(localId);
    if (d == null) {
      return false;
    }
    LOG.info("{} delay {} ms, args={}", localId, d.get(),
        Arrays.toString(args));
    try {
      RaftTestUtil.delay(d::get);
    } catch (InterruptedException e) {
      LOG.debug("Interrupted while delaying " + localId);
    }
    return true;
  }
}
