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
package org.apache.ratis.server.storage;

import org.apache.log4j.Level;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.util.AutoCloseableLock;
import org.apache.ratis.util.LogUtils;

import java.util.function.Consumer;

public interface RaftStorageTestUtils {
  static void setRaftLogWorkerLogLevel(Level level) {
    LogUtils.setLogLevel(RaftLogWorker.LOG, level);
  }

  static void printLog(RaftLog log, Consumer<String> println) {
    if (log == null) {
      println.accept("log == null");
      return;
    }

    final TermIndex last;
    final long flushed, committed;
    try(AutoCloseableLock readlock = log.readLock()) {
      last = log.getLastEntryTermIndex();
      flushed = log.getLatestFlushedIndex();
      committed = log.getLastCommittedIndex();
    }
    final StringBuilder b = new StringBuilder();
    for(long i = 0; i <= last.getIndex(); i++) {
      b.setLength(0);
      b.append(i == flushed? 'f': ' ');
      b.append(i == committed? 'c': ' ');
      b.append(String.format("%3d: ", i));
      try {
        final RaftProtos.LogEntryProto entry = log.get(i);
        b.append(entry != null? entry.getLogEntryBodyCase(): null);
      } catch (RaftLogIOException e) {
        b.append(e);
      }
      println.accept(b.toString());
    }
  }
}
