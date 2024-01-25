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
package org.apache.ratis.server.raftlog.memory;

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.impl.ServerProtoUtils;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.util.AutoCloseableLock;
import org.apache.ratis.util.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A simple RaftLog implementation in memory. Used only for testing.
 */
public class MemoryRaftLog extends RaftLog {
  static class EntryList {
    private final List<LogEntryProto> entries = new ArrayList<>();

    LogEntryProto get(int i) {
      return i >= 0 && i < entries.size() ? entries.get(i) : null;
    }

    TermIndex getTermIndex(int i) {
      return ServerProtoUtils.toTermIndex(get(i));
    }

    int size() {
      return entries.size();
    }

    void truncate(int index) {
      if (entries.size() > index) {
        entries.subList(index, entries.size()).clear();
      }
    }

    void purge(int index) {
      if (entries.size() > index) {
        entries.subList(0, index).clear();
      }
    }

    void add(LogEntryProto entry) {
      entries.add(entry);
    }
  }

  private final EntryList entries = new EntryList();
  private final AtomicReference<Metadata> metadata = new AtomicReference<>(new Metadata(null, 0));

  public MemoryRaftLog(RaftGroupMemberId memberId, long commitIndex, RaftProperties properties) {
    super(memberId, commitIndex, properties);
  }

  @Override
  public LogEntryProto get(long index) {
    checkLogState();
    try(AutoCloseableLock readLock = readLock()) {
      return entries.get(Math.toIntExact(index));
    }
  }

  @Override
  public EntryWithData getEntryWithData(long index) {
    return new EntryWithData(get(index), null);
  }

  @Override
  public TermIndex getTermIndex(long index) {
    checkLogState();
    try(AutoCloseableLock readLock = readLock()) {
      return entries.getTermIndex(Math.toIntExact(index));
    }
  }

  @Override
  public TermIndex[] getEntries(long startIndex, long endIndex) {
    checkLogState();
    try(AutoCloseableLock readLock = readLock()) {
      if (startIndex >= entries.size()) {
        return null;
      }
      final int from = Math.toIntExact(startIndex);
      final int to = Math.toIntExact(Math.min(entries.size(), endIndex));
      TermIndex[] ti = new TermIndex[to - from];
      for (int i = 0; i < ti.length; i++) {
        ti[i] = entries.getTermIndex(i);
      }
      return ti;
    }
  }

  @Override
  protected CompletableFuture<Long> truncateImpl(long index) {
    checkLogState();
    try(AutoCloseableLock writeLock = writeLock()) {
      Preconditions.assertTrue(index >= 0);
      entries.truncate(Math.toIntExact(index));
    }
    return CompletableFuture.completedFuture(index);
  }


  @Override
  protected CompletableFuture<Long> purgeImpl(long index) {
    try (AutoCloseableLock writeLock = writeLock()) {
      Preconditions.assertTrue(index >= 0);
      entries.purge(Math.toIntExact(index));
    }
    return CompletableFuture.completedFuture(index);
  }

  @Override
  public TermIndex getLastEntryTermIndex() {
    checkLogState();
    try(AutoCloseableLock readLock = readLock()) {
      return entries.getTermIndex(entries.size() - 1);
    }
  }

  @Override
  protected CompletableFuture<Long> appendEntryImpl(LogEntryProto entry) {
    checkLogState();
    try(AutoCloseableLock writeLock = writeLock()) {
      validateLogEntry(entry);
      entries.add(entry);
    }
    return CompletableFuture.completedFuture(entry.getIndex());
  }

  @Override
  public long getStartIndex() {
    return entries.size() == 0? INVALID_LOG_INDEX: entries.getTermIndex(0).getIndex();
  }

  @Override
  public List<CompletableFuture<Long>> appendImpl(LogEntryProto... logEntryProtos) {
    checkLogState();
    if (logEntryProtos == null || logEntryProtos.length == 0) {
      return Collections.emptyList();
    }
    try(AutoCloseableLock writeLock = writeLock()) {
      // Before truncating the entries, we first need to check if some
      // entries are duplicated. If the leader sends entry 6, entry 7, then
      // entry 6 again, without this check the follower may truncate entry 7
      // when receiving entry 6 again. Then before the leader detects this
      // truncation in the next appendEntries RPC, leader may think entry 7 has
      // been committed but in the system the entry has not been committed to
      // the quorum of peers' disks.
      // TODO add a unit test for this
      boolean toTruncate = false;
      int truncateIndex = (int) logEntryProtos[0].getIndex();
      int index = 0;
      for (; truncateIndex < getNextIndex() && index < logEntryProtos.length;
           index++, truncateIndex++) {
        if (this.entries.get(truncateIndex).getTerm() !=
            logEntryProtos[index].getTerm()) {
          toTruncate = true;
          break;
        }
      }
      final List<CompletableFuture<Long>> futures;
      if (toTruncate) {
        futures = new ArrayList<>(logEntryProtos.length - index + 1);
        futures.add(truncate(truncateIndex));
      } else {
        futures = new ArrayList<>(logEntryProtos.length - index);
      }
      for (int i = index; i < logEntryProtos.length; i++) {
        this.entries.add(logEntryProtos[i]);
        futures.add(CompletableFuture.completedFuture(logEntryProtos[i].getIndex()));
      }
      return futures;
    }
  }

  public String getEntryString() {
    return "entries=" + entries;
  }

  @Override
  public long getFlushIndex() {
    return getNextIndex() - 1;
  }

  @Override
  public void writeMetadata(long term, RaftPeerId votedFor) {
    metadata.set(new Metadata(votedFor, term));
  }

  @Override
  public Metadata loadMetadata() {
    return metadata.get();
  }

  @Override
  public void syncWithSnapshot(long lastSnapshotIndex) {
    // do nothing
  }

  @Override
  public boolean isConfigEntry(TermIndex ti) {
    return get(ti.getIndex()).hasConfigurationEntry();
  }
}
