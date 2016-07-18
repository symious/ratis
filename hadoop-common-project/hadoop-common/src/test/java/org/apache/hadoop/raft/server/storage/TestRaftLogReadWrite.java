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

import com.google.protobuf.CodedOutputStream;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.raft.RaftTestUtil;
import org.apache.hadoop.raft.RaftTestUtil.SimpleMessage;
import org.apache.hadoop.raft.conf.RaftProperties;
import org.apache.hadoop.raft.proto.RaftProtos.LogEntryProto;
import org.apache.hadoop.raft.server.RaftConfKeys;
import org.apache.hadoop.raft.server.RaftConstants;
import org.apache.hadoop.raft.server.RaftConstants.StartupOption;
import org.apache.hadoop.raft.util.RaftUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Test basic functionality of LogReader, LogInputStream, and LogOutputStream.
 */
public class TestRaftLogReadWrite {
  private File storageDir;
  private final RaftProperties properties = new RaftProperties();

  @Before
  public void setup() throws Exception {
    storageDir = RaftTestUtil.getTestDir(TestRaftLogReadWrite.class);
    properties.set(RaftConfKeys.RAFT_SERVER_STORAGE_DIR_KEY,
        storageDir.getCanonicalPath());
  }

  @After
  public void tearDown() throws Exception {
    if (storageDir != null) {
      FileUtil.fullyDelete(storageDir.getParentFile());
    }
  }

  private LogEntryProto[] readLog(File file, long startIndex, long endIndex,
      boolean isOpen) throws IOException {
    List<LogEntryProto> list = new ArrayList<>();
    try (LogInputStream in =
             new LogInputStream(file, startIndex, endIndex, isOpen)) {
      LogEntryProto entry;
      while ((entry = in.nextEntry()) != null) {
        list.add(entry);
      }
    }
    return list.toArray(new LogEntryProto[list.size()]);
  }

  /**
   * Test basic functionality: write several log entries, then read
   */
  @Test
  public void testReadWriteLog() throws IOException {
    RaftStorage storage = new RaftStorage(properties, StartupOption.REGULAR);
    File openSegment = storage.getStorageDir().getOpenLogFile(0);
    long size = SegmentedRaftLog.HEADER.length;
    LogEntryProto[] entries = new LogEntryProto[100];
    try (LogOutputStream out = new LogOutputStream(openSegment, false)) {
      for (int i = 0; i < 100; i++) {
        SimpleMessage m = new SimpleMessage("m" + i);
        entries[i] = RaftUtils.convertRequestToLogEntryProto(m, 0, i);
        final int s = entries[i].getSerializedSize();
        size += CodedOutputStream.computeRawVarint32Size(s) + s + 4;
        out.write(entries[i]);
      }
    } finally {
      storage.close();
    }

    Assert.assertEquals(size, openSegment.length());

    LogEntryProto[] readEntries = readLog(openSegment, 0,
        RaftConstants.INVALID_LOG_INDEX, true);
    Assert.assertArrayEquals(entries, readEntries);
  }

  @Test
  public void testAppendLog() throws IOException {
    RaftStorage storage = new RaftStorage(properties, StartupOption.REGULAR);
    File openSegment = storage.getStorageDir().getOpenLogFile(0);
    LogEntryProto[] entries = new LogEntryProto[200];
    try (LogOutputStream out = new LogOutputStream(openSegment, false)) {
      for (int i = 0; i < 100; i++) {
        SimpleMessage m = new SimpleMessage("m" + i);
        entries[i] = RaftUtils.convertRequestToLogEntryProto(m, 0, i);
        out.write(entries[i]);
      }
    }

    try (LogOutputStream out = new LogOutputStream(openSegment, true)) {
      for (int i = 100; i < 200; i++) {
        SimpleMessage m = new SimpleMessage("m" + i);
        entries[i] = RaftUtils.convertRequestToLogEntryProto(m, 0, i);
        out.write(entries[i]);
      }
    }

    LogEntryProto[] readEntries = readLog(openSegment, 0,
        RaftConstants.INVALID_LOG_INDEX, true);
    Assert.assertArrayEquals(entries, readEntries);

    storage.close();
  }

  /**
   * Simulate the scenario that the peer is shutdown without truncating
   * log segment file padding. Make sure the reader can correctly handle this.
   */
  @Test
  public void testReadWithPadding() throws IOException {
    RaftStorage storage = new RaftStorage(properties, StartupOption.REGULAR);
    File openSegment = storage.getStorageDir().getOpenLogFile(0);
    long size = SegmentedRaftLog.HEADER.length;

    LogEntryProto[] entries = new LogEntryProto[100];
    LogOutputStream out = new LogOutputStream(openSegment, false);
    for (int i = 0; i < 100; i++) {
      SimpleMessage m = new SimpleMessage("m" + i);
      entries[i] = RaftUtils.convertRequestToLogEntryProto(m, 0, i);
      final int s = entries[i].getSerializedSize();
      size += CodedOutputStream.computeRawVarint32Size(s) + s + 4;
      out.write(entries[i]);
    }
    out.flush();

    // make sure the file contains padding
    Assert.assertEquals(RaftConstants.LOG_SEGMENT_MAX_SIZE, openSegment.length());

    // check if the reader can correctly read the log file
    LogEntryProto[] readEntries = readLog(openSegment, 0,
        RaftConstants.INVALID_LOG_INDEX, true);
    Assert.assertArrayEquals(entries, readEntries);

    out.close();
    Assert.assertEquals(size, openSegment.length());
  }

  /**
   * corrupt the padding by inserting non-zero bytes. Make sure the reader
   * throws exception.
   */
  @Test
  public void testReadWithCorruptPadding() throws IOException {
    RaftStorage storage = new RaftStorage(properties, StartupOption.REGULAR);
    File openSegment = storage.getStorageDir().getOpenLogFile(0);

    LogEntryProto[] entries = new LogEntryProto[10];
    LogOutputStream out = new LogOutputStream(openSegment, false);
    for (int i = 0; i < 10; i++) {
      SimpleMessage m = new SimpleMessage("m" + i);
      entries[i] = RaftUtils.convertRequestToLogEntryProto(m, 0, i);
      out.write(entries[i]);
    }
    out.flush();

    // make sure the file contains padding
    Assert.assertEquals(RaftConstants.LOG_SEGMENT_MAX_SIZE,
        openSegment.length());

    try (FileOutputStream fout = new FileOutputStream(openSegment, true)) {
      ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[]{-1, 1});
      fout.getChannel()
          .write(byteBuffer, RaftConstants.LOG_SEGMENT_MAX_SIZE - 10);
    }

    List<LogEntryProto> list = new ArrayList<>();
    try (LogInputStream in = new LogInputStream(openSegment, 0,
        RaftConstants.INVALID_LOG_INDEX, true)) {
      LogEntryProto entry;
      while ((entry = in.nextEntry()) != null) {
        list.add(entry);
      }
      Assert.fail("should fail since we corrupt the padding");
    } catch (IOException e) {
      boolean findVerifyTerminator = false;
      for (StackTraceElement s : e.getStackTrace()) {
        if (s.getMethodName().equals("verifyTerminator")) {
          findVerifyTerminator = true;
          break;
        }
      }
      Assert.assertTrue(findVerifyTerminator);
    }
    Assert.assertArrayEquals(entries,
        list.toArray(new LogEntryProto[list.size()]));
  }
}
