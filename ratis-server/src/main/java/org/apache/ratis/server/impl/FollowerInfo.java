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
package org.apache.ratis.server.impl;

import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.raftlog.RaftLogIndex;
import org.apache.ratis.util.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class FollowerInfo {
  public static final Logger LOG = LoggerFactory.getLogger(FollowerInfo.class);

  private final String name;
  private final Consumer<Object> infoIndexChange;
  private final Consumer<Object> debugIndexChange;

  private final RaftPeer peer;
  private final AtomicReference<Timestamp> lastRpcResponseTime;
  private final AtomicReference<Timestamp> lastRpcSendTime;
  private final RaftLogIndex nextIndex;
  private final RaftLogIndex matchIndex = new RaftLogIndex("matchIndex", 0L);
  private final RaftLogIndex commitIndex = new RaftLogIndex("commitIndex", RaftLog.INVALID_LOG_INDEX);
  private final RaftLogIndex snapshotIndex = new RaftLogIndex("snapshotIndex", 0L);
  private volatile boolean attendVote;
  private final int rpcSlownessTimeoutMs;

  FollowerInfo(RaftGroupMemberId id, RaftPeer peer, Timestamp lastRpcTime, long nextIndex,
      boolean attendVote, int rpcSlownessTimeoutMs) {
    this.name = id + "->" + peer.getId();
    this.infoIndexChange = s -> LOG.info("{}: {}", name, s);
    this.debugIndexChange = s -> LOG.debug("{}: {}", name, s);

    this.peer = peer;
    this.lastRpcResponseTime = new AtomicReference<>(lastRpcTime);
    this.lastRpcSendTime = new AtomicReference<>(lastRpcTime);
    this.nextIndex = new RaftLogIndex("nextIndex", nextIndex);
    this.attendVote = attendVote;
    this.rpcSlownessTimeoutMs = rpcSlownessTimeoutMs;
  }

  public long getMatchIndex() {
    return matchIndex.get();
  }

  public boolean updateMatchIndex(long newMatchIndex) {
    return matchIndex.updateToMax(newMatchIndex, debugIndexChange);
  }

  /** @return the commit index acked by the follower. */
  long getCommitIndex() {
    return commitIndex.get();
  }

  boolean updateCommitIndex(long newCommitIndex) {
    return commitIndex.updateToMax(newCommitIndex, debugIndexChange);
  }

  long getSnapshotIndex() {
    return snapshotIndex.get();
  }

  public long getNextIndex() {
    return nextIndex.get();
  }

  public void increaseNextIndex(long newNextIndex) {
    nextIndex.updateIncreasingly(newNextIndex, debugIndexChange);
  }

  public void decreaseNextIndex(long newNextIndex) {
    nextIndex.updateUnconditionally(old -> old <= 0L? old: Math.min(old - 1, newNextIndex), infoIndexChange);
  }

  public void updateNextIndex(long newNextIndex) {
    nextIndex.updateUnconditionally(old -> newNextIndex >= 0 ? newNextIndex : old, infoIndexChange);
  }

  public void updateNextIndexToMax(long newNextIndex) {
    nextIndex.updateToMax(newNextIndex, infoIndexChange);
  }

  public void setSnapshotIndex(long newSnapshotIndex) {
    snapshotIndex.setUnconditionally(newSnapshotIndex, infoIndexChange);
    matchIndex.setUnconditionally(newSnapshotIndex, infoIndexChange);
    nextIndex.setUnconditionally(newSnapshotIndex + 1, infoIndexChange);
  }

  public String getName() {
    return name;
  }

  @Override
  public String toString() {
    return name + "(c" + getCommitIndex() + ",m" + getMatchIndex() + ",n" + getNextIndex()
        + ", attendVote=" + attendVote +
        ", lastRpcSendTime=" + lastRpcSendTime.get().elapsedTimeMs() +
        ", lastRpcResponseTime=" + lastRpcResponseTime.get().elapsedTimeMs() + ")";
  }

  void startAttendVote() {
    attendVote = true;
  }

  boolean isAttendingVote() {
    return attendVote;
  }

  public RaftPeer getPeer() {
    return peer;
  }

  /** Update lastRpcResponseTime to the current time. */
  public void updateLastRpcResponseTime() {
    lastRpcResponseTime.set(Timestamp.currentTime());
  }

  Timestamp getLastRpcResponseTime() {
    return lastRpcResponseTime.get();
  }

  /** Update lastRpcSendTime to the current time. */
  public void updateLastRpcSendTime() {
    lastRpcSendTime.set(Timestamp.currentTime());
  }

  public Timestamp getLastRpcTime() {
    return Timestamp.latest(lastRpcResponseTime.get(), lastRpcSendTime.get());
  }

  boolean isSlow() {
    return lastRpcResponseTime.get().elapsedTimeMs() > rpcSlownessTimeoutMs;
  }
}
