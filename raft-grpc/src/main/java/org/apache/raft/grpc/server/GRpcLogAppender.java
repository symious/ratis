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
package org.apache.raft.grpc.server;

import com.google.common.base.Preconditions;
import io.grpc.stub.StreamObserver;
import org.apache.hadoop.util.Time;
import org.apache.raft.grpc.RaftGrpcConfigKeys;
import org.apache.raft.proto.RaftProtos.AppendEntriesReplyProto;
import org.apache.raft.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.raft.server.FollowerInfo;
import org.apache.raft.server.LeaderState;
import org.apache.raft.server.LogAppender;
import org.apache.raft.server.RaftServer;

import java.util.LinkedList;
import java.util.Queue;

/**
 * A new log appender implementation using grpc bi-directional stream API.
 */
public class GRpcLogAppender extends LogAppender {
  private final RaftServerProtocolClient client;
  private final Queue<AppendEntriesRequestProto> pendingRequests;
  private final int maxPendingRequestsNum;
  private volatile boolean firstResponseReceived = false;

  public GRpcLogAppender(RaftServer server, LeaderState leaderState,
      FollowerInfo f) {
    super(server, leaderState, f);

    RaftGRpcService rpcService = (RaftGRpcService) server.getServerRpc();
    client = rpcService.getRpcClient(f.getPeer());
    maxPendingRequestsNum = server.getProperties().getInt(
        RaftGrpcConfigKeys.RAFT_GRPC_MAX_OUTSTANDING_APPENDS_KEY,
        RaftGrpcConfigKeys.RAFT_GRPC_MAX_OUTSTANDING_APPENDS_DEFAULT);
    pendingRequests = new LinkedList<>();
  }

  @Override
  public void run() {
    final StreamObserver<AppendEntriesRequestProto> requestObserver =
        client.appendEntries(new ResponseHandler());
    while (isAppenderRunning()) {
      if (shouldSendRequest()) {
        // keep appending log entries or sending heartbeats
        appendLog(requestObserver);
        // update the last rpc time
        follower.updateLastRpcTime(Time.monotonicNow());
      }

      if (isAppenderRunning()) { // TODO also check if there is more log to tail
        try {
          synchronized (this) {
            wait(getHeartbeatRemainingTime(follower.getLastRpcTime()));
          }
        } catch (InterruptedException e) {
          LOG.info(this + " was interrupted: " + e);
        }
      }
    }
  }

  private void appendLog(
      StreamObserver<AppendEntriesRequestProto> requestObserver) {
    try {
      AppendEntriesRequestProto pending = null;
      // if the queue's size >= maxSize, wait
      synchronized (this) {
        while (isAppenderRunning() &&
            (pendingRequests.size() >= maxPendingRequestsNum ||
                shouldWaitForFirstResponse())) {
          try {
            this.wait();
          } catch (InterruptedException ignored) {
          }
        }

        if (isAppenderRunning() && shouldSendRequest()) {
          // prepare and enqueue the append request. note changes on follower's
          // nextIndex and ops on pendingRequests should always be associated
          // together and protected by the lock
          pending = createRequest();
          if (pending != null) {
            Preconditions.checkState(pendingRequests.offer(pending));
          }
        }
      }

      if (pending != null && isAppenderRunning()) {
        requestObserver.onNext(pending);
      }
    } catch (RuntimeException e) {
      // TODO we can cancel the original RPC and restart it. In this way we can
      // cancel all the pending requests in the channel
      LOG.info(this + "got exception when appending log to " + follower, e);
    }
  }

  /**
   * if this is the first append, wait for the response of the first append so
   * that we can get the correct next index.
   */
  private boolean shouldWaitForFirstResponse() {
    return pendingRequests.size() > 0 && !firstResponseReceived;
  }

  /**
   * StreamObserver for handling responses from the follower
   */
  private class ResponseHandler implements StreamObserver<AppendEntriesReplyProto> {
    /**
     * After receiving a appendEntries reply, do the following:
     * 1. If the reply is success, update the follower's match index and submit
     *    an event to leaderState
     * 2. If the reply is NOT_LEADER, step down
     * 3. If the reply is INCONSISTENCY, decrease the follower's next index
     *    based on the response
     */
    @Override
    public void onNext(AppendEntriesReplyProto reply) {
      if (!firstResponseReceived) {
        firstResponseReceived = true;
      }
      switch (reply.getResult()) {
        case SUCCESS:
          onSuccess(reply);
          break;
        case NOT_LEADER:
          onNotLeader(reply);
          break;
        case INCONSISTENCY:
          onInconsistency(reply);
          break;
        default:
          break;
      }
    }

    /**
     * for now we simply retry the first pending request
     */
    @Override
    public void onError(Throwable t) {
      // clear the pending requests queue and reset the next index of follower
      // TODO reuse the requests
      AppendEntriesRequestProto request = pendingRequests.peek();
      if (request != null) {
        final long nextIndex = request.hasPreviousLog() ?
            request.getPreviousLog().getIndex() + 1 : raftLog.getStartIndex();
        clearPendingRequests(nextIndex);
      }
    }

    @Override
    public void onCompleted() {
      LOG.info("{} stops appending log entries to follower {}", server.getId(),
          follower);
    }
  }

  private synchronized void clearPendingRequests(long newNextIndex) {
    pendingRequests.clear();
    follower.decreaseNextIndex(newNextIndex);
  }

  private void onSuccess(AppendEntriesReplyProto reply) {
    AppendEntriesRequestProto request = pendingRequests.poll();
    final long replyNextIndex = reply.getNextIndex();
    Preconditions.checkNotNull(request,
        "Got reply with next index %s but the pending queue is empty",
        replyNextIndex);

    if (request.getEntriesCount() == 0) {
      Preconditions.checkState(!request.hasPreviousLog() ||
              replyNextIndex - 1 == request.getPreviousLog().getIndex(),
          "reply's next index is %s, request's previous is %s",
          replyNextIndex, request.getPreviousLog());
    } else {
      // check if the reply and the pending request is consistent
      final long lastEntryIndex = request
          .getEntries(request.getEntriesCount() - 1).getIndex();
      Preconditions.checkState(replyNextIndex == lastEntryIndex + 1,
          "reply's next index is %s, request's last entry index is %s",
          replyNextIndex, lastEntryIndex);
      follower.updateMatchIndex(lastEntryIndex);
      submitEventOnSuccessAppend();
    }
  }

  private void onNotLeader(AppendEntriesReplyProto reply) {
    checkResponseTerm(reply.getTerm());
  }

  private void onInconsistency(AppendEntriesReplyProto reply) {
    AppendEntriesRequestProto request = pendingRequests.peek();
    Preconditions.checkState(request.hasPreviousLog());
    if (request.getPreviousLog().getIndex() >= reply.getNextIndex()) {
      clearPendingRequests(reply.getNextIndex());
    }
    // TODO cancel the rpc call and restart it, so as not to send in-q requests
  }
}
