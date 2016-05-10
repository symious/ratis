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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.common.base.Preconditions;
import org.apache.hadoop.raft.server.protocol.ConfigurationEntry;
import org.apache.hadoop.raft.server.protocol.Entry;
import org.apache.hadoop.raft.protocol.Message;
import org.apache.hadoop.raft.server.protocol.TermIndex;

public class RaftLog {
  static final Entry DUMMY_ENTRY = new Entry(-1, 0, null);

  private final List<Entry> entries = new ArrayList<>();
  private long lastCommitted = 0;

  {
    //add a dummy entry so that the first log index is 1.
    entries.add(DUMMY_ENTRY);
  }

  synchronized TermIndex getLastCommitted() {
    return get(lastCommitted);
  }

  synchronized void updateLastCommitted(long majority, long currentTerm) {
    if (lastCommitted < majority) {
      // Only update last committed index for current term.
      final TermIndex ti = get(majority);
      if (ti != null && ti.getTerm() == currentTerm) {
        this.lastCommitted = majority;
      }
    }
  }

  private int findIndex(long index) {
    return (int)index;
  }

  synchronized TermIndex get(long index) {
    final int i = findIndex(index);
    return i >= 0 && i < entries.size()? entries.get(i): null;
  }

  synchronized Entry[] getEntries(long startIndex) {
    final int i = findIndex(startIndex);
    final int size = entries.size();
    return i < size? entries.subList(i, size).toArray(Entry.EMPTY_ARRAY): null;
  }

  private RaftConfiguration truncate(long index) {
    final int truncateIndex = findIndex(index);
    RaftConfiguration oldConf = null;
    for(int i = entries.size() - 1; i >= truncateIndex; i--) {
      Entry removed = entries.remove(i);
      if (removed.isConfigurationEntry()) {
        oldConf = ((ConfigurationEntry) removed).getPrev();
      }
    }
    return oldConf;
  }

  /** Does the contain the given term and index? */
  boolean contains(TermIndex ti) {
    return ti != null && ti.equals(get(ti.getIndex()));
  }

  synchronized Entry getLastEntry() {
    final int size = entries.size();
    return size == 0? null: entries.get(size - 1);
  }

  long getNextIndex() {
    final Entry last = getLastEntry();
    return last == null? 1: last.getIndex() + 1;
  }

  synchronized long apply(long term, Message message) {
    final long nextIndex = getNextIndex();
    final Entry e = new Entry(term, nextIndex, message);
    Preconditions.checkState(entries.add(e));
    return nextIndex;
  }

  /**
   * If an existing entry conflicts with a new one (same index but different
   * terms), delete the existing entry and all that follow it (§5.3)
   *
   * This method, {@link #apply(long, Message)}, and {@link #truncate(long)}
   * do not guarantee the changes are persisted. Need to call {@link #logSync()}
   * to persist the changes.
   */
  synchronized RaftConfiguration apply(Entry... entries) {
    if (entries == null || entries.length == 0) {
      return null;
    }
    RaftConfiguration conf = truncate(entries[0].getIndex());
    Preconditions.checkState(this.entries.addAll(Arrays.asList(entries)));
    for (Entry entry : entries) {
      this.entries.add(entry);
      if (entry.isConfigurationEntry()) {
        conf = ((ConfigurationEntry) entry).getCurrent();
      }
    }
    return conf;
  }

  @Override
  public String toString() {
    synchronized (this) {
      return "last=" + getLastEntry() + ", committed=" + getLastCommitted();
    }
  }

  /**
   * TODO persist the log
   * TODO also need to persist leaderId/currentTerm in ServerState when logSync
   * is triggered by AppendEntries RPC request from the leader
   */
  public void logSync() {
  }
}
