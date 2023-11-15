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
package org.apache.raft.server.storage;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.apache.raft.MiniRaftCluster;
import org.apache.raft.RaftTestUtil;
import org.apache.raft.RaftTestUtil.SimpleMessage;
import org.apache.raft.conf.RaftProperties;
import org.apache.raft.proto.RaftProtos.LogEntryProto;
import org.apache.raft.util.ProtoUtils;
import org.apache.raft.server.ConfigurationManager;
import org.apache.raft.server.RaftServerConstants;
import org.apache.raft.server.RaftServerConfigKeys;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TestSegmentedRaftLog {
  static {
    GenericTestUtils.setLogLevel(RaftLogWorker.LOG, Level.DEBUG);
  }

  private static final String peerId = "s0";

  private static class SegmentRange {
    final long start;
    final long end;
    final long term;
    final boolean isOpen;

    SegmentRange(long s, long e, long term, boolean isOpen) {
      this.start = s;
      this.end = e;
      this.term = term;
      this.isOpen = isOpen;
    }
  }

  private File storageDir;
  private final RaftProperties properties = new RaftProperties();
  private RaftStorage storage;
  private final ConfigurationManager cm =
      new ConfigurationManager(MiniRaftCluster.initConfiguration(3));

  @Before
  public void setup() throws Exception {
    storageDir = RaftTestUtil.getTestDir(TestSegmentedRaftLog.class);
    properties.set(RaftServerConfigKeys.RAFT_SERVER_STORAGE_DIR_KEY,
        storageDir.getCanonicalPath());
    storage = new RaftStorage(properties, RaftServerConstants.StartupOption.REGULAR);
  }

  @After
  public void tearDown() throws Exception {
    if (storageDir != null) {
      FileUtil.fullyDelete(storageDir.getParentFile());
    }
  }

  private LogEntryProto[] prepareLog(List<SegmentRange> list) throws IOException {
    List<LogEntryProto> entryList = new ArrayList<>();
    for (SegmentRange range : list) {
      File file = range.isOpen ?
          storage.getStorageDir().getOpenLogFile(range.start) :
          storage.getStorageDir().getClosedLogFile(range.start, range.end);

      final int size = (int) (range.end - range.start + 1);
      LogEntryProto[] entries = new LogEntryProto[size];
      try (LogOutputStream out = new LogOutputStream(file, false)) {
        for (int i = 0; i < size; i++) {
          SimpleMessage m = new SimpleMessage("m" + (i + range.start));
          entries[i] = ProtoUtils.toLogEntryProto(m, range.term,
              i + range.start);
          out.write(entries[i]);
        }
      }
      Collections.addAll(entryList, entries);
    }
    return entryList.toArray(new LogEntryProto[entryList.size()]);
  }

  private List<SegmentRange> prepareRanges(int number, int segmentSize,
      long startIndex) {
    List<SegmentRange> list = new ArrayList<>(number);
    for (int i = 0; i < number; i++) {
      list.add(new SegmentRange(startIndex, startIndex + segmentSize - 1, i,
          i == number - 1));
      startIndex += segmentSize;
    }
    return list;
  }

  @Test
  public void testLoadLogSegments() throws Exception {
    // first generate log files
    List<SegmentRange> ranges = prepareRanges(5, 100, 0);
    LogEntryProto[] entries = prepareLog(ranges);

    // create RaftLog object and load log file
    try (SegmentedRaftLog raftLog = new SegmentedRaftLog(peerId, null, storage, -1)) {
      raftLog.open(cm, RaftServerConstants.INVALID_LOG_INDEX);
      // check if log entries are loaded correctly
      for (LogEntryProto e : entries) {
        LogEntryProto entry = raftLog.get(e.getIndex());
        Assert.assertEquals(e, entry);
      }

      Assert.assertArrayEquals(entries, raftLog.getEntries(0, 500));
      Assert.assertEquals(entries[entries.length - 1], raftLog.getLastEntry());
    }
  }

  List<LogEntryProto> prepareLogEntries(List<SegmentRange> slist) {
    List<LogEntryProto> eList = new ArrayList<>();
    for (SegmentRange range : slist) {
      for (long index = range.start; index <= range.end; index++) {
        SimpleMessage m = new SimpleMessage("m" + index);
        eList.add(ProtoUtils.toLogEntryProto(m, range.term, index));
      }
    }
    return eList;
  }

  /**
   * Append entry one by one and check if log state is correct.
   */
  @Test
  public void testAppendEntry() throws Exception {
    List<SegmentRange> ranges = prepareRanges(5, 200, 0);
    List<LogEntryProto> entries = prepareLogEntries(ranges);

    try (SegmentedRaftLog raftLog = new SegmentedRaftLog(peerId, null, storage, -1)) {
      raftLog.open(cm, RaftServerConstants.INVALID_LOG_INDEX);
      // append entries to the raftlog
      entries.forEach(raftLog::appendEntry);
      raftLog.logSync();
    }

    try (SegmentedRaftLog raftLog = new SegmentedRaftLog(peerId, null, storage, -1)) {
      raftLog.open(cm, RaftServerConstants.INVALID_LOG_INDEX);
      // check if the raft log is correct
      checkEntries(raftLog, entries, 0, entries.size());
    }
  }

  @Test
  public void testTruncate() throws Exception {
    // prepare the log for truncation
    List<SegmentRange> ranges = prepareRanges(5, 200, 0);
    List<LogEntryProto> entries = prepareLogEntries(ranges);

    try (SegmentedRaftLog raftLog = new SegmentedRaftLog(peerId, null, storage, -1)) {
      raftLog.open(cm, RaftServerConstants.INVALID_LOG_INDEX);
      // append entries to the raftlog
      entries.forEach(raftLog::appendEntry);
      raftLog.logSync();
    }

    for (long fromIndex = 900; fromIndex >= 0; fromIndex -= 150) {
      testTruncate(entries, fromIndex);
    }
  }

  private void testTruncate(List<LogEntryProto> entries, long fromIndex)
      throws Exception {
    try (SegmentedRaftLog raftLog = new SegmentedRaftLog(peerId, null, storage, -1)) {
      raftLog.open(cm, RaftServerConstants.INVALID_LOG_INDEX);
      // truncate the log
      raftLog.truncate(fromIndex);
      raftLog.logSync();

      checkEntries(raftLog, entries, 0, (int) fromIndex);
    }

    try (SegmentedRaftLog raftLog = new SegmentedRaftLog(peerId, null, storage, -1)) {
      raftLog.open(cm, RaftServerConstants.INVALID_LOG_INDEX);
      // check if the raft log is correct
      if (fromIndex > 0) {
        Assert.assertEquals(entries.get((int) (fromIndex - 1)),
            raftLog.getLastEntry());
      } else {
        Assert.assertNull(raftLog.getLastEntry());
      }
      checkEntries(raftLog, entries, 0, (int) fromIndex);
    }
  }

  private void checkEntries(RaftLog raftLog, List<LogEntryProto> expected,
      int offset, int size) {
    if (size > 0) {
      for (int i = offset; i < size + offset; i++) {
        LogEntryProto entry = raftLog.get(expected.get(i).getIndex());
        Assert.assertEquals(expected.get(i), entry);
      }
      LogEntryProto[] entriesFromLog = raftLog.getEntries(
          expected.get(offset).getIndex(),
          expected.get(offset + size - 1).getIndex() + 1);
      LogEntryProto[] expectedArray = expected.subList(offset, offset + size)
          .toArray(SegmentedRaftLog.EMPTY_LOGENTRY_ARRAY);
      Assert.assertArrayEquals(expectedArray, entriesFromLog);
    }
  }

  /**
   * Test append with inconsistent entries
   */
  @Test
  public void testAppendEntriesWithInconsistency() throws Exception {
    // prepare the log for truncation
    List<SegmentRange> ranges = prepareRanges(5, 200, 0);
    List<LogEntryProto> entries = prepareLogEntries(ranges);

    try (SegmentedRaftLog raftLog = new SegmentedRaftLog(peerId, null, storage, -1)) {
      raftLog.open(cm, RaftServerConstants.INVALID_LOG_INDEX);
      // append entries to the raftlog
      entries.forEach(raftLog::appendEntry);
      raftLog.logSync();
    }

    // append entries whose first 100 entries are the same with existing log,
    // and the next 100 are with different term
    SegmentRange r1 = new SegmentRange(550, 599, 2, false);
    SegmentRange r2 = new SegmentRange(600, 649, 3, false);
    SegmentRange r3 = new SegmentRange(650, 749, 10, false);
    List<LogEntryProto> newEntries = prepareLogEntries(Arrays.asList(r1, r2, r3));

    try (SegmentedRaftLog raftLog = new SegmentedRaftLog(peerId, null, storage, -1)) {
      raftLog.open(cm, RaftServerConstants.INVALID_LOG_INDEX);
      raftLog.append(newEntries.toArray(new LogEntryProto[newEntries.size()]));
      raftLog.logSync();

      checkEntries(raftLog, entries, 0, 650);
      checkEntries(raftLog, newEntries, 100, 100);
      Assert.assertEquals(newEntries.get(newEntries.size() - 1),
          raftLog.getLastEntry());
      Assert.assertEquals(newEntries.get(newEntries.size() - 1).getIndex(),
          raftLog.getLatestFlushedIndex());
    }

    // load the raftlog again and check
    try (SegmentedRaftLog raftLog = new SegmentedRaftLog(peerId, null, storage, -1)) {
      raftLog.open(cm, RaftServerConstants.INVALID_LOG_INDEX);
      checkEntries(raftLog, entries, 0, 650);
      checkEntries(raftLog, newEntries, 100, 100);
      Assert.assertEquals(newEntries.get(newEntries.size() - 1),
          raftLog.getLastEntry());
      Assert.assertEquals(newEntries.get(newEntries.size() - 1).getIndex(),
          raftLog.getLatestFlushedIndex());

      RaftLogCache cache = raftLog.getRaftLogCache();
      Assert.assertEquals(5, cache.getNumOfSegments());
    }
  }
}
