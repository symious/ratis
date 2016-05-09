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
package org.apache.hadoop.raft.server;

import com.google.common.base.Preconditions;
import org.apache.hadoop.raft.server.protocol.AppendEntriesRequest;
import org.apache.hadoop.raft.server.protocol.Entry;
import org.apache.hadoop.raft.server.protocol.RaftPeer;
import org.apache.hadoop.raft.server.protocol.RaftServerResponse;
import org.apache.hadoop.raft.server.protocol.TermIndex;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * States for leader only
 */
class LeaderState {
  private static final Logger LOG = RaftServer.LOG;

  private final RaftServer server;
  private final RaftConfiguration conf;
  private final RaftLog raftLog;
  private final List<RpcSender> senders;

  LeaderState(RaftServer server) {
    this.server = server;
    this.conf = server.getRaftConf();
    this.raftLog = server.getState().getLog();

    Collection<RaftPeer> others = conf.getOtherPeers(server.getState().getSelfId());
    final long t = Time.monotonicNow() - RaftConstants.RPC_TIMEOUT_MAX_MS;
    final long nextIndex = raftLog.getNextIndex();
    senders = new ArrayList<>(others.size());
    for (RaftPeer p : others) {
      FollowerInfo f = new FollowerInfo(p, t, nextIndex);
      senders.add(new RpcSender(f));
    }
  }

  void start() {
    for (RpcSender sender : senders) {
      sender.start();
    }
  }

  void stop() {
    for (RpcSender sender : senders) {
      sender.stopRunning();
      sender.interrupt();
      // we do not call sender.join but we make sure senders will not change
      // anything after being stopped.
      // TODO we should have a single coordinator thread assigning tasks to
      // senders, and updating committedIndex or change server's state based on
      // response.
    }
  }

  void notifySenders() {
    for (RpcSender sender : senders) {
      synchronized (sender) {
        sender.notify();
      }
    }
  }

  // TODO this should be called by the coordinator and should hold the server's lock
  private void updateLastCommitted() {
    final String selfId = server.getState().getSelfId();
    List<List<FollowerInfo>> followerLists = divideFollowers();
    long majorityInNewConf = updateLastCommitted(followerLists.get(0),
        conf.containsInConf(selfId));
    if (!conf.inTransitionState()) {
      raftLog.updateLastCommitted(majorityInNewConf,
          server.getState().getCurrentTerm());
    } else {
      long majorityInOldConf = updateLastCommitted(followerLists.get(1),
          conf.containsInOldConf(selfId));
      raftLog.updateLastCommitted(Math.min(majorityInNewConf, majorityInOldConf),
          server.getState().getCurrentTerm());
    }
  }

  private long updateLastCommitted(List<FollowerInfo> followers,
      boolean includeSelf) {
    final int length = includeSelf ? followers.size() + 1 : followers.size();
    final long[] indices = new long[length];
    for (int i = 0; i < followers.size(); i++) {
      indices[i] = followers.get(i).matchIndex.get();
    }
    if (includeSelf) {
      indices[length - 1] = raftLog.getNextIndex() - 1;
    }

    Arrays.sort(indices);
    return indices[(indices.length - 1) / 2];
  }

  private List<List<FollowerInfo>> divideFollowers() {
    List<List<FollowerInfo>> lists = new ArrayList<>(2);
    List<FollowerInfo> listForNew = new ArrayList<>();
    for (RpcSender sender : senders) {
      if (conf.containsInConf(sender.follower.peer.getId())) {
        listForNew.add(sender.follower);
      }
    }
    lists.add(listForNew);
    if (conf.inTransitionState()) {
      List<FollowerInfo> listForOld = new ArrayList<>();
      for (RpcSender sender : senders) {
        if (conf.containsInOldConf(sender.follower.peer.getId())) {
          listForOld.add(sender.follower);
        }
      }
      lists.add(listForOld);
    }
    return lists;
  }

  /**
   * @return the time in milliseconds that the leader should send a heartbeat.
   */
  private static long getHeartbeatRemainingTime(long lastTime) {
    return lastTime + RaftConstants.RPC_TIMEOUT_MIN_MS / 2 - Time.monotonicNow();
  }

  private class FollowerInfo {
    private final RaftPeer peer;
    private final AtomicLong lastRpcTime;
    private long nextIndex;
    private final AtomicLong matchIndex = new AtomicLong();

    FollowerInfo(RaftPeer peer, long lastRpcTime, long nextIndex) {
      this.peer = peer;
      this.lastRpcTime = new AtomicLong(lastRpcTime);
      this.nextIndex = nextIndex;
    }

    void updateMatchIndex(final long matchIndex) {
      this.matchIndex.set(matchIndex);
      updateLastCommitted();
    }

    /** Should the leader send appendEntries RPC to this follower? */
    boolean shouldSend() {
      return raftLog.get(nextIndex) != null ||
          getHeartbeatRemainingTime(lastRpcTime.get()) <= 0;
    }

    void updateNextIndex(long i) {
      nextIndex = i;
    }

    void decreaseNextIndex() {
      nextIndex--;
    }
  }

  class RpcSender extends Daemon {
    private volatile boolean running = true;
    private final FollowerInfo follower;

    public RpcSender(FollowerInfo f) {
      this.follower = f;
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "(" + server.getState().getSelfId()
          + " -> " + follower.peer.getId() + ")";
    }

    @Override
    public void run() {
      try {
        checkAndSendAppendEntries();
      } catch (InterruptedException | InterruptedIOException e) {
        LOG.info(this + " was interrupted: " + e);
        LOG.trace("TRACE", e);
      }
    }

    void stopRunning() {
      this.running = false;
    }

    /** Send an appendEntries RPC; retry indefinitely. */
    private RaftServerResponse sendAppendEntriesWithRetries()
        throws InterruptedException, InterruptedIOException {
      Entry[] entries = null;
      int retry = 0;
      while (running) {
        try {
          if (entries == null) {
            entries = raftLog.getEntries(follower.nextIndex);
          }
          final TermIndex previous = raftLog.get(follower.nextIndex - 1);
          AppendEntriesRequest request = server.createAppendEntriesRequest(
              follower.peer.getId(), previous, entries);
          final RaftServerResponse r = server.sendAppendEntries(request);
          if (running && r.isSuccess()) {
            if (entries != null && entries.length > 0) {
              final long mi = entries[entries.length - 1].getIndex();
              follower.updateMatchIndex(mi);
              follower.updateNextIndex(mi + 1);
            }
          }
          return r;
        } catch (InterruptedIOException iioe) {
          throw iioe;
        } catch (IOException ioe) {
          LOG.warn(this + ": failed to send appendEntries; retry " + retry++,
              ioe);
        }
        if (running) {
          Thread.sleep(RaftConstants.RPC_SLEEP_TIME_MS);
        }
      }
      return null;
    }

    /** Check and send appendEntries RPC */
    private void checkAndSendAppendEntries() throws InterruptedException,
        InterruptedIOException {
      while (running) {
        if (follower.shouldSend()) {
          final RaftServerResponse r = sendAppendEntriesWithRetries();
          if (r == null) {
            Preconditions.checkState(!server.isLeader());
            break;
          }
          follower.lastRpcTime.set(Time.monotonicNow());

          // check if should step down
          checkResponseTerm(r.getTerm());
          if (!running) {
            return;
          }

          if (!r.isSuccess()) {
            // may implements the optimization in Section 5.3
            follower.decreaseNextIndex();
          }
        }
        synchronized (this) {
          wait(getHeartbeatRemainingTime(follower.lastRpcTime.get()));
        }
      }
    }

    private void checkResponseTerm(long responseTerm) {
      synchronized (server) {
        if (running && responseTerm > server.getState().getCurrentTerm()) {
          server.changeToFollower();
        }
      }
    }
  }
}
