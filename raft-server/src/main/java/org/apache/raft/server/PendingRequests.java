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

import com.google.common.base.Preconditions;
import org.apache.raft.protocol.RaftClientReply;
import org.apache.raft.protocol.RaftClientRequest;
import org.apache.raft.protocol.RaftException;
import org.apache.raft.protocol.SetConfigurationRequest;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.Map;

class PendingRequests {
  private static final Logger LOG = RaftServer.LOG;

  private PendingRequest pendingSetConf;
  private final RaftServer server;
  private final Map<Long, PendingRequest> pendingRequests = new HashMap<>();

  PendingRequests(RaftServer server) {
    this.server = server;
  }

  PendingRequest addPendingRequest(long index, RaftClientRequest request) {
    final PendingRequest pending = new PendingRequest(index, request);
    Preconditions.checkState(pendingRequests.put(index, pending) == null);
    return pending;
  }

  PendingRequest addConfRequest(SetConfigurationRequest request) {
    Preconditions.checkState(pendingSetConf == null);
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
      setSuccessReply(pendingSetConf);
      pendingSetConf = null;
    }
  }

  void failSetConfiguration(RaftException e) {
    Preconditions.checkState(pendingSetConf != null);
    pendingSetConf.setException(e);
    pendingSetConf = null;
  }

  void replyPendingRequest(long index, Exception e) {
    PendingRequest pending = pendingRequests.remove(index);
    if (pending != null) {
      if (e == null) {
        setSuccessReply(pending);
      } else {
        pending.setException(e);
      }
    }
  }

  void sendResponses(final long lastCommitted) {
    LOG.info("{} sends responses before shutting down PendingRequestsHandler",
        server.getId());

    pendingRequests.entrySet().forEach(entry -> {
      if (entry.getKey() <= lastCommitted) {
        setSuccessReply(entry.getValue());
      } else {
        setNotLeaderException(entry.getValue());
      }
    });
    if (pendingSetConf != null) {
      setNotLeaderException(pendingSetConf);
    }
  }

  private void setSuccessReply(PendingRequest pending) {
    pending.setReply(new RaftClientReply(pending.getRequest(), true, null));
  }

  private void setNotLeaderException(PendingRequest pending) {
    RaftClientReply reply = new RaftClientReply(pending.getRequest(), false,
        server.generateNotLeaderException());
    pending.setReply(reply);
  }
}
