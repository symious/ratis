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
package org.apache.hadoop.raft.server.storage;

import com.google.common.base.Preconditions;
import org.apache.hadoop.raft.proto.RaftProtos.LogEntryProto;
import org.apache.hadoop.raft.protocol.Message;
import org.apache.hadoop.raft.server.RaftConfiguration;
import org.apache.hadoop.raft.util.RaftUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A simple RaftLog implementation in memory. Used only for testing.
 */
public class MemoryRaftLog extends RaftLog {
  private final List<LogEntryProto> entries = new ArrayList<>();

  public MemoryRaftLog(String selfId) {
    super(selfId);
  }

  public MemoryRaftLog(String selfId, List<LogEntryProto> newEntries) {
    super(selfId);
    entries.addAll(newEntries);
  }

  @Override
  public synchronized LogEntryProto get(long index) {
    final int i = (int) index;
    return i >= 0 && i < entries.size()? entries.get(i): null;
  }

  @Override
  public synchronized LogEntryProto[] getEntries(long startIndex) {
    final int i = (int) startIndex;
    final int size = entries.size();
    return i < size ?
        entries.subList(i, size).toArray(EMPTY_LOGENTRY_ARRAY) : null;
  }

  @Override
  void truncate(long index) {
    Preconditions.checkArgument(index >= 0);
    final int truncateIndex = (int) index;
    for(int i = entries.size() - 1; i >= truncateIndex; i--) {
      entries.remove(i);
    }
  }

  @Override
  public synchronized LogEntryProto getLastEntry() {
    final int size = entries.size();
    return size == 0? null: entries.get(size - 1);
  }

  @Override
  public synchronized long append(long term, Message message) {
    final long nextIndex = getNextIndex();
    final LogEntryProto e = RaftUtils.convertRequestToLogEntryProto(message,
        term, nextIndex);
    entries.add(e);
    return nextIndex;
  }

  @Override
  public synchronized long append(long term, RaftConfiguration newConf) {
    final long nextIndex = getNextIndex();
    final LogEntryProto e = RaftUtils.convertConfToLogEntryProto(newConf, term,
        nextIndex);
    entries.add(e);
    return nextIndex;
  }

  @Override
  public synchronized void append(LogEntryProto... entries) {
    if (entries != null && entries.length > 0) {
      truncate(entries[0].getIndex());
      Collections.addAll(this.entries, entries);
    }
  }

  @Override
  public synchronized String toString() {
    return "last=" + getLastEntry() + ", committed=" + getLastCommitted();
  }

  public synchronized String getEntryString() {
    return "entries=" + entries;
  }

  @Override
  public void logSync() {
  }
}
