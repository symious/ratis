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

import org.apache.ratis.BaseTest;
import org.apache.ratis.client.ClientRetryEvent;
import org.apache.ratis.client.retry.RequestTypeDependentRetryPolicy;
import org.apache.ratis.proto.RaftProtos.RaftClientRequestProto;
import org.apache.ratis.proto.RaftProtos.ReplicationLevel;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.LeaderNotReadyException;
import org.apache.ratis.protocol.NotLeaderException;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.TimeoutIOException;
import org.apache.ratis.protocol.exceptions.ResourceUnavailableException;
import org.apache.ratis.util.TimeDuration;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** Test {@link RetryPolicy}. */
public class TestRetryPolicy extends BaseTest {
  @Override
  public int getGlobalTimeoutSeconds() {
    return 1;
  }

  @Test
  public void testRetryMultipleTimesWithFixedSleep() {
    final int n = 4;
    final TimeDuration sleepTime = HUNDRED_MILLIS;
    final RetryPolicy policy = RetryPolicies.retryUpToMaximumCountWithFixedSleep(n, sleepTime);
    for(int i = 1; i < 2*n; i++) {
      final int attempt = i;
      final RetryPolicy.Event event = () -> attempt;
      final RetryPolicy.Action action = policy.handleAttemptFailure(event);

      final boolean expected = i < n;
      Assert.assertEquals(expected, action.shouldRetry());
      if (expected) {
        Assert.assertEquals(sleepTime, action.getSleepTime());
      } else {
        Assert.assertEquals(0L, action.getSleepTime().getDuration());
      }
    }
  }

  @Test
  public void testRequestTypeDependentRetry() {
    final RequestTypeDependentRetryPolicy.Builder b = RequestTypeDependentRetryPolicy.newBuilder();
    final int n = 4;
    final TimeDuration writeSleep = HUNDRED_MILLIS;
    final RetryPolicies.RetryLimited writePolicy = RetryPolicies.retryUpToMaximumCountWithFixedSleep(n, writeSleep);
    b.set(RaftClientRequestProto.TypeCase.WRITE, writePolicy);
    b.set(RaftClientRequestProto.TypeCase.WATCH, RetryPolicies.noRetry());
    final RetryPolicy policy = b.build();
    LOG.info("policy = {}", policy);

    final RaftClientRequest staleReadRequest = newRaftClientRequest(RaftClientRequest.staleReadRequestType(1));
    final RaftClientRequest readRequest = newRaftClientRequest(RaftClientRequest.readRequestType());
    final RaftClientRequest writeRequest = newRaftClientRequest(RaftClientRequest.writeRequestType());
    final RaftClientRequest watchRequest = newRaftClientRequest(
        RaftClientRequest.watchRequestType(1, ReplicationLevel.MAJORITY));
    for(int i = 1; i < 2*n; i++) {
      { //write
        final ClientRetryEvent event = new ClientRetryEvent(i, writeRequest);
        final RetryPolicy.Action action = policy.handleAttemptFailure(event);

        final boolean expected = i < n;
        Assert.assertEquals(expected, action.shouldRetry());
        if (expected) {
          Assert.assertEquals(writeSleep, action.getSleepTime());
        } else {
          Assert.assertEquals(0L, action.getSleepTime().getDuration());
        }
      }

      { //read and stale read are using default
        final ClientRetryEvent event = new ClientRetryEvent(i, readRequest);
        final RetryPolicy.Action action = policy.handleAttemptFailure(event);
        Assert.assertTrue(action.shouldRetry());
        Assert.assertEquals(0L, action.getSleepTime().getDuration());
      }

      {
        final ClientRetryEvent event = new ClientRetryEvent(i, staleReadRequest);
        final RetryPolicy.Action action = policy.handleAttemptFailure(event);
        Assert.assertTrue(action.shouldRetry());
        Assert.assertEquals(0L, action.getSleepTime().getDuration());
      }

      { //watch has no retry
        final ClientRetryEvent event = new ClientRetryEvent(i, watchRequest);
        final RetryPolicy.Action action = policy.handleAttemptFailure(event);
        Assert.assertFalse(action.shouldRetry());
        Assert.assertEquals(0L, action.getSleepTime().getDuration());
      }
    }

  }

  @Test
  public void testRequestTypeDependentRetryWithExceptionDependentPolicy() throws Exception {
    final RequestTypeDependentRetryPolicy.Builder retryPolicy =
        RequestTypeDependentRetryPolicy.newBuilder();
    Map<Class<? extends Throwable>, Pair> exceptionPolicyMap = new HashMap<>();
    exceptionPolicyMap.put(NotLeaderException.class, new Pair(10, 1));
    exceptionPolicyMap.put(LeaderNotReadyException.class, new Pair(10, 1));
    exceptionPolicyMap.put(TimeoutIOException.class, new Pair(5, 5));
    exceptionPolicyMap.put(ResourceUnavailableException.class, new Pair(5, 5));
    Pair defaultPolicy = new Pair(10, 2);

    retryPolicy.set(RaftClientRequestProto.TypeCase.WRITE, buildExceptionBasedRetry(exceptionPolicyMap, defaultPolicy));
    retryPolicy.set(RaftClientRequestProto.TypeCase.WATCH, buildExceptionBasedRetry(exceptionPolicyMap, defaultPolicy));

    final RetryPolicy policy = retryPolicy.build();
    LOG.info("policy = {}", policy);

    final RaftClientRequest writeRequest = newRaftClientRequest(RaftClientRequest.writeRequestType());
    final RaftClientRequest watchRequest = newRaftClientRequest(
        RaftClientRequest.watchRequestType(1, ReplicationLevel.MAJORITY));

    List<RaftClientRequest> requests = new ArrayList<>();
    requests.add(writeRequest);
    requests.add(watchRequest);

    for (RaftClientRequest raftClientRequest : requests) {
      for (Map.Entry< Class< ? extends Throwable >, Pair > exceptionPolicy :
          exceptionPolicyMap.entrySet()) {
        Throwable exception = createException(exceptionPolicy.getKey());
        for (int j = 1; j < exceptionPolicy.getValue().retries * 2; j++) {
          checkEvent(j, policy, raftClientRequest, exception,
              exceptionPolicy.getValue());
        }
      }
    }
    // Now try with an exception that is not defined in retry exception map.
    for (RaftClientRequest raftClientRequest : requests) {
      Throwable exception = createException(IOException.class);
      for (int j = 1; j < defaultPolicy.retries * 2; j++) {
        checkEvent(j, policy, raftClientRequest, exception, defaultPolicy);
      }
    }
  }

  /**
   * Check Event with the policy defined.
   * @param exceptionAttemptCount
   * @param retryPolicy
   * @param raftClientRequest
   * @param exception
   * @param exceptionPolicyPair
   */
  private void checkEvent(int exceptionAttemptCount, RetryPolicy retryPolicy, RaftClientRequest raftClientRequest,
      Throwable exception, Pair exceptionPolicyPair) {
    final ClientRetryEvent event = new ClientRetryEvent(exceptionAttemptCount, raftClientRequest, exception);
    final RetryPolicy.Action action = retryPolicy.handleAttemptFailure(event);

    final boolean expected = exceptionAttemptCount < exceptionPolicyPair.retries;
    Assert.assertEquals(expected, action.shouldRetry());
    if (expected) {
      Assert.assertEquals(exceptionPolicyPair.sleepTime, action.getSleepTime().getDuration());
    } else {
      Assert.assertEquals(0L, action.getSleepTime().getDuration());
    }
  }

  /**
   * Create exception object based on the exception class,
   * @param exception
   * @return exception object.
   */
  private Throwable createException(Class<? extends Throwable> exception) {
    Throwable ex;
    if (exception.getName().equals(LeaderNotReadyException.class.getName())) {
      ex =
          new LeaderNotReadyException(RaftGroupMemberId.valueOf(RaftPeerId.valueOf("node1"), RaftGroupId.randomId()));
    } else if (exception.getName().equals(NotLeaderException.class.getName())) {
      ex = new NotLeaderException(null, null, null);
    } else if (exception.getName().equals(TimeoutIOException.class.getName())) {
      ex = new TimeoutIOException("time out");
    } else if (exception.getName().equals(ResourceUnavailableException.class.getName())){
      ex = new ResourceUnavailableException("resource unavailable");
    } else {
      ex = new IOException("io exception");
    }
    return ex;
  }

  /**
   * Build {@link ExceptionDependentRetry} object.
   * @param exceptionPolicyMap
   * @param defaultPolicy
   * @return ExceptionDependentRetry
   */
  private ExceptionDependentRetry buildExceptionBasedRetry(Map<Class<?
      extends Throwable>, Pair> exceptionPolicyMap, Pair defaultPolicy) {
    final ExceptionDependentRetry.Builder policy =
        ExceptionDependentRetry.newBuilder();
    exceptionPolicyMap.forEach((k, v) -> {
      policy.setExceptionToPolicy(k,
          RetryPolicies.retryUpToMaximumCountWithFixedSleep(v.retries,
              TimeDuration.valueOf(v.sleepTime, TimeUnit.SECONDS)));
    });

    policy.setDefaultPolicy(RetryPolicies.retryUpToMaximumCountWithFixedSleep(defaultPolicy.retries,
        TimeDuration.valueOf(defaultPolicy.sleepTime, TimeUnit.SECONDS)));

    return policy.build();
  }


  private static RaftClientRequest newRaftClientRequest(RaftClientRequest.Type type) {
    return new RaftClientRequest(ClientId.randomId(), RaftPeerId.valueOf("s0"), RaftGroupId.randomId(), 1L, type);
  }

  /**
   * Pair class to hold retries and sleep time.
   */
  static class Pair {
    private int retries;
    private long sleepTime;

    Pair(int retries, long sleepTime) {
      this.retries = retries;
      this.sleepTime = sleepTime;
    }
  }
}