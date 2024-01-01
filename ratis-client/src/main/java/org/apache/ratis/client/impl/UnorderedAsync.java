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
package org.apache.ratis.client.impl;

import org.apache.ratis.client.impl.RaftClientImpl.PendingClientRequest;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.GroupMismatchException;
import org.apache.ratis.protocol.NotLeaderException;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.retry.RetryPolicy;
import org.apache.ratis.util.JavaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;

/** Send unordered asynchronous requests to a raft service. */
public interface UnorderedAsync {
  Logger LOG = LoggerFactory.getLogger(UnorderedAsync.class);

  class PendingUnorderedRequest extends PendingClientRequest {
    private final Supplier<RaftClientRequest> requestConstructor;

    PendingUnorderedRequest(Supplier<RaftClientRequest> requestConstructor) {
      this.requestConstructor = requestConstructor;
    }

    @Override
    RaftClientRequest newRequestImpl() {
      return requestConstructor.get();
    }
  }

  static CompletableFuture<RaftClientReply> send(RaftClientRequest.Type type, RaftClientImpl client) {
    final long callId = RaftClientImpl.nextCallId();
    final PendingClientRequest pending = new PendingUnorderedRequest(
        () -> client.newRaftClientRequest(null, callId, null, type, null));
    sendRequestWithRetry(pending, client);
    return pending.getReplyFuture()
        .thenApply(reply -> RaftClientImpl.handleStateMachineException(reply, CompletionException::new));
  }

  static void sendRequestWithRetry(PendingClientRequest pending, RaftClientImpl client) {
    final CompletableFuture<RaftClientReply> f = pending.getReplyFuture();
    if (f.isDone()) {
      return;
    }

    final RaftClientRequest request = pending.newRequest();
    final int attemptCount = pending.getAttemptCount();

    final ClientId clientId = client.getId();
    LOG.debug("{}: attempt #{} send~ {}", clientId, attemptCount, request);
    client.getClientRpc().sendRequestAsyncUnordered(request).whenCompleteAsync((reply, e) -> {
      try {
        LOG.debug("{}: attempt #{} receive~ {}", clientId, attemptCount, reply);
        reply = client.handleNotLeaderException(request, reply, false);
        if (reply != null) {
          f.complete(reply);
          return;
        }
        final RetryPolicy retryPolicy = client.getRetryPolicy();
        if (!retryPolicy.shouldRetry(attemptCount)) {
          f.completeExceptionally(RaftClientImpl.newRaftRetryFailureException(request, attemptCount, retryPolicy));
          return;
        }

        if (e != null) {
          if (LOG.isTraceEnabled()) {
            LOG.trace(clientId + ": attempt #" + attemptCount + " failed~ " + request, e);
          } else {
            LOG.debug("{}: attempt #{} failed {} with {}", clientId, attemptCount, request, e);
          }
          e = JavaUtils.unwrapCompletionException(e);

          if (e instanceof IOException) {
            if (e instanceof NotLeaderException) {
              client.handleNotLeaderException(request, (NotLeaderException) e, false);
            } else if (!(e instanceof GroupMismatchException)) {
              client.handleIOException(request, (IOException) e, null, false);
            }
          } else {
            if (!client.getClientRpc().handleException(request.getServerId(), e, false)) {
              f.completeExceptionally(e);
              return;
            }
          }
        }

        LOG.debug("schedule retry for attempt #{}, policy={}, request={}", attemptCount, retryPolicy, request);
        client.getScheduler().onTimeout(retryPolicy.getSleepTime(), () -> sendRequestWithRetry(pending, client),
            LOG, () -> clientId + ": Failed~ to retry " + request);
      } catch (Throwable t) {
        LOG.error(clientId + ": XXX Failed " + request, t);
        f.completeExceptionally(t);
      }
    });
  }
}
