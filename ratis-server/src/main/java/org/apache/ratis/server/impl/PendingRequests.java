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
package org.apache.ratis.server.impl;

import org.apache.ratis.protocol.*;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.util.RaftUtils;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

class PendingRequests {
  private static final Logger LOG = RaftServerImpl.LOG;

  private PendingRequest pendingSetConf;
  private final RaftServerImpl server;
  private final ConcurrentMap<Long, PendingRequest> pendingRequests = new ConcurrentHashMap<>();
  private PendingRequest last = null;

  PendingRequests(RaftServerImpl server) {
    this.server = server;
  }

  PendingRequest addPendingRequest(long index, RaftClientRequest request,
      TransactionContext entry) {
    // externally synced for now
    RaftUtils.assertTrue(!request.isReadOnly());
    RaftUtils.assertTrue(last == null || index == last.getIndex() + 1);
    return add(index, request, entry);
  }

  private PendingRequest add(long index, RaftClientRequest request,
      TransactionContext entry) {
    final PendingRequest pending = new PendingRequest(index, request, entry);
    pendingRequests.put(index, pending);
    last = pending;
    return pending;
  }

  PendingRequest addConfRequest(SetConfigurationRequest request) {
    RaftUtils.assertTrue(pendingSetConf == null);
    pendingSetConf = new PendingRequest(request);
    return pendingSetConf;
  }

  void replySetConfiguration() {
    // we allow the pendingRequest to be null in case that the new leader
    // commits the new configuration while it has not received the retry
    // request from the client
    if (pendingSetConf != null) {
      // for setConfiguration we do not need to wait for statemachine. send back
      // reply after it's committed.
      pendingSetConf.setSuccessReply(null);
      pendingSetConf = null;
    }
  }

  void failSetConfiguration(RaftException e) {
    RaftUtils.assertTrue(pendingSetConf != null);
    pendingSetConf.setException(e);
    pendingSetConf = null;
  }

  TransactionContext getTransactionContext(long index) {
    PendingRequest pendingRequest = pendingRequests.get(index);
    // it is possible that the pendingRequest is null if this peer just becomes
    // the new leader and commits transactions received by the previous leader
    return pendingRequest != null ? pendingRequest.getEntry() : null;
  }

  void replyPendingRequest(long index,
      CompletableFuture<Message> stateMachineFuture) {
    final PendingRequest pending = pendingRequests.get(index);
    if (pending != null) {
      RaftUtils.assertTrue(pending.getIndex() == index);

      stateMachineFuture.whenComplete((reply, exception) -> {
        if (exception == null) {
          pending.setSuccessReply(reply);
        } else {
          // the exception is coming from the state machine. wrap it into the
          // reply as a StateMachineException
          final StateMachineException e = new StateMachineException(
              server.getId().toString(), exception);
          pending.setReply(new RaftClientReply(pending.getRequest(), e));
        }
      });
    }
  }

  /**
   * The leader state is stopped. Send NotLeaderException to all the pending
   * requests since they have not got applied to the state machine yet.
   */
  void sendNotLeaderResponses() throws IOException {
    LOG.info("{} sends responses before shutting down PendingRequestsHandler",
        server.getId());

    Collection<TransactionContext> pendingEntries = pendingRequests.values().stream()
        .map(PendingRequest::getEntry).collect(Collectors.toList());
    // notify the state machine about stepping down
    server.getStateMachine().notifyNotLeader(pendingEntries);
    pendingRequests.values().forEach(this::setNotLeaderException);
    if (pendingSetConf != null) {
      setNotLeaderException(pendingSetConf);
    }
  }

  private void setNotLeaderException(PendingRequest pending) {
    RaftClientReply reply = new RaftClientReply(pending.getRequest(),
        server.generateNotLeaderException());
    pending.setReply(reply);
  }
}
