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
package org.apache.ratis.logservice.server;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.apache.ratis.logservice.metrics.LogServiceMetricsRegistry;
import org.apache.ratis.logservice.proto.LogServiceProtos.AppendLogEntryRequestProto;
import org.apache.ratis.logservice.proto.LogServiceProtos.CloseLogReplyProto;
import org.apache.ratis.logservice.proto.LogServiceProtos.CloseLogRequestProto;
import org.apache.ratis.logservice.proto.LogServiceProtos.GetLogLengthRequestProto;
import org.apache.ratis.logservice.proto.LogServiceProtos.GetLogSizeRequestProto;
import org.apache.ratis.logservice.proto.LogServiceProtos.GetStateRequestProto;
import org.apache.ratis.logservice.proto.LogServiceProtos.LogServiceRequestProto;
import org.apache.ratis.logservice.proto.LogServiceProtos.ReadLogRequestProto;
import org.apache.ratis.logservice.util.LogServiceProtoUtil;
import org.apache.ratis.metrics.impl.RatisMetricRegistry;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.impl.RaftServerConstants;
import org.apache.ratis.server.impl.RaftServerProxy;
import org.apache.ratis.server.impl.ServerState;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.apache.ratis.thirdparty.com.google.protobuf.TextFormat;
import org.apache.ratis.util.AutoCloseableLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogStateMachine extends BaseStateMachine {
  public static final Logger LOG = LoggerFactory.getLogger(LogStateMachine.class);
  private RatisMetricRegistry metricRegistry;
  private Timer sizeRequestTimer;
  private Timer readNextQueryTimer;
  private Timer getStateTimer;
  private Timer lastIndexQueryTimer;
  private Timer lengthQueryTimer;
  private Timer startIndexTimer;
  private Timer appendRequestTimer;
  private Timer syncRequesTimer;
  private Timer getCloseLogTimer;

  public static enum State {
    OPEN, CLOSED
  }

  /*
   *  State is a log's length, size, and state (closed/open);
   */
  private long length;

  /**
   * The size (number of bytes) of the log records. Does not include Ratis storage overhead
   */
  private long dataRecordsSize;

  private State state = State.OPEN;

  private final SimpleStateMachineStorage storage = new SimpleStateMachineStorage();

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

  private RaftLog log;

  private RaftGroupId groupId;

  private RaftServerProxy proxy ;

  private AutoCloseableLock readLock() {
    return AutoCloseableLock.acquire(lock.readLock());
  }

  private AutoCloseableLock writeLock() {
    return AutoCloseableLock.acquire(lock.writeLock());
  }

  /**
   * Reset state machine
   */
  void reset() {
    this.length = 0;
    this.dataRecordsSize = 0;
    setLastAppliedTermIndex(null);
  }

  @Override
  public void initialize(RaftServer server, RaftGroupId groupId,
      RaftStorage raftStorage) throws IOException {
    super.initialize(server, groupId, raftStorage);
    this.storage.init(raftStorage);
    this.proxy = (RaftServerProxy) server;
    this.groupId = groupId;
    //TODO: using groupId for metric now but better to tag it with LogName
    this.metricRegistry =
        LogServiceMetricsRegistry.createMetricRegistryForLogService(groupId.toString());
    this.readNextQueryTimer = metricRegistry.timer("readNextQueryTime");
    this.startIndexTimer= metricRegistry.timer("startIndexTime");
    this.sizeRequestTimer = metricRegistry.timer("sizeRequestTime");
    this.getStateTimer = metricRegistry.timer("getStateTime");
    this.lastIndexQueryTimer = metricRegistry.timer("lastIndexQueryTime");
    this.lengthQueryTimer = metricRegistry.timer("lengthQueryTime");
    this.syncRequesTimer = metricRegistry.timer("syncRequesTime");
    this.appendRequestTimer = metricRegistry.timer("appendRequestTime");
    this.getCloseLogTimer = metricRegistry.timer("getCloseLogTime");
    loadSnapshot(storage.getLatestSnapshot());
  }

  private void checkInitialization() throws IOException {
    if (this.log == null) {
      ServerState state = proxy.getImpl(groupId).getState();
      this.log = state.getLog();
    }
  }

  @Override
  public void reinitialize() throws IOException {
    close();
    loadSnapshot(storage.getLatestSnapshot());
  }

  @Override
  public long takeSnapshot() {
    final TermIndex last;
    try(final AutoCloseableLock readLock = readLock()) {
      last = getLastAppliedTermIndex();
    }

    final File snapshotFile =  storage.getSnapshotFile(last.getTerm(), last.getIndex());
    LOG.info("Taking a snapshot to file {}", snapshotFile);

    try(final AutoCloseableLock readLock = readLock();
        final ObjectOutputStream out = new ObjectOutputStream(
        new BufferedOutputStream(new FileOutputStream(snapshotFile)))) {
      out.writeLong(length);
      out.writeLong(dataRecordsSize);
      out.writeObject(state);
    } catch(IOException ioe) {
      LOG.warn("Failed to write snapshot file \"" + snapshotFile
          + "\", last applied index=" + last);
    }

    return last.getIndex();
  }

  private long loadSnapshot(SingleFileSnapshotInfo snapshot) throws IOException {
    return load(snapshot, false);
  }

  private long load(SingleFileSnapshotInfo snapshot, boolean reload) throws IOException {
    if (snapshot == null) {
      LOG.warn("The snapshot info is null.");
      return RaftServerConstants.INVALID_LOG_INDEX;
    }
    final File snapshotFile = snapshot.getFile().getPath().toFile();
    if (!snapshotFile.exists()) {
      LOG.warn("The snapshot file {} does not exist for snapshot {}", snapshotFile, snapshot);
      return RaftServerConstants.INVALID_LOG_INDEX;
    }

    final TermIndex last = SimpleStateMachineStorage.getTermIndexFromSnapshotFile(snapshotFile);
    try(final AutoCloseableLock writeLock = writeLock();
        final ObjectInputStream in = new ObjectInputStream(
            new BufferedInputStream(new FileInputStream(snapshotFile)))) {
      if (reload) {
        reset();
      }
      setLastAppliedTermIndex(last);
      this.length = in.readLong();
      this.dataRecordsSize = in.readLong();
      this.state = (State) in.readObject();
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException(e);
    }
    return last.getIndex();
  }

  @Override
  public StateMachineStorage getStateMachineStorage() {
    return storage;
  }

  @Override
  public CompletableFuture<Message> query(Message request) {

    try {

      checkInitialization();
      LogServiceRequestProto logServiceRequestProto =
          LogServiceRequestProto.parseFrom(request.getContent());
      if (LOG.isTraceEnabled()) {
        LOG.trace("Processing LogService query: {}", TextFormat.shortDebugString(logServiceRequestProto));
      }

      switch (logServiceRequestProto.getRequestCase()) {

        case READNEXTQUERY:
          return recordTime(readNextQueryTimer, new Task(){
            @Override public CompletableFuture<Message> run() {
              return processReadRequest(logServiceRequestProto);
            }
          });
        case SIZEREQUEST:
          return recordTime(sizeRequestTimer, new Task(){
            @Override public CompletableFuture<Message> run() {
              return processGetSizeRequest(logServiceRequestProto);
            }
          });
        case STARTINDEXQUERY:
          return recordTime(startIndexTimer, new Task(){
            @Override public CompletableFuture<Message> run() {
              return processGetStartIndexRequest(logServiceRequestProto);
            }
          });
        case GETSTATE:
          return recordTime(getStateTimer, new Task(){
            @Override public CompletableFuture<Message> run() {
              return processGetStateRequest(logServiceRequestProto);
            }
          });
        case LASTINDEXQUERY:
          return recordTime(lastIndexQueryTimer, new Task(){
            @Override public CompletableFuture<Message> run() {
              return processGetLastCommittedIndexRequest(logServiceRequestProto);
            }
          });
        case LENGTHQUERY:
          return recordTime(lengthQueryTimer, new Task(){
            @Override public CompletableFuture<Message> run() {
              return processGetLengthRequest(logServiceRequestProto);
            }
          });
        default:
          // TODO
          throw new RuntimeException(
            "Wrong message type for query: " + logServiceRequestProto.getRequestCase());
      }

    } catch (IOException e) {
      // TODO exception handling
      throw new RuntimeException(e);
    }

  }

  /**
   * Process get start index request
   * @param proto message
   * @return reply message
   */
  private CompletableFuture<Message>
      processGetStartIndexRequest(LogServiceRequestProto proto)
  {

    Throwable t = verifyState(State.OPEN);
    long startIndex = log.getStartIndex();
    return CompletableFuture.completedFuture(Message
      .valueOf(LogServiceProtoUtil.toGetLogStartIndexReplyProto(startIndex, t).toByteString()));
  }

  /**
   * Process get last committed record index
   * @param proto message
   * @return reply message
   */
  private CompletableFuture<Message>
      processGetLastCommittedIndexRequest(LogServiceRequestProto proto)
  {

    Throwable t = verifyState(State.OPEN);
    long lastIndex = log.getLastCommittedIndex();
    return CompletableFuture.completedFuture(Message
      .valueOf(LogServiceProtoUtil.toGetLogLastIndexReplyProto(lastIndex, t).toByteString()));
  }

  /**
   * Process get length request
   * @param proto message
   * @return reply message
   */
  private CompletableFuture<Message> processGetSizeRequest(LogServiceRequestProto proto) {
    GetLogSizeRequestProto msgProto = proto.getSizeRequest();
    Throwable t = verifyState(State.OPEN);
    LOG.trace("Size query: {}, Result: {}", msgProto, this.dataRecordsSize);
    return CompletableFuture.completedFuture(Message
      .valueOf(LogServiceProtoUtil.toGetLogSizeReplyProto(this.dataRecordsSize, t).toByteString()));
  }

  private CompletableFuture<Message> processGetLengthRequest(LogServiceRequestProto proto) {
    GetLogLengthRequestProto msgProto = proto.getLengthQuery();
    Throwable t = verifyState(State.OPEN);
    LOG.trace("Length query: {}, Result: {}", msgProto, this.length);
    return CompletableFuture.completedFuture(Message
      .valueOf(LogServiceProtoUtil.toGetLogLengthReplyProto(this.length, t).toByteString()));
  }
  /**
   * Process read log entries request
   * @param proto message
   * @return reply message
   */
  private CompletableFuture<Message> processReadRequest(LogServiceRequestProto proto) {
    ReadLogRequestProto msgProto = proto.getReadNextQuery();
    // Get the recordId the user wants to start reading at
    long startRecordId = msgProto.getStartRecordId();
    // And the number of records they want to read
    int numRecordsToRead = msgProto.getNumRecords();
    Throwable t = verifyState(State.OPEN);
    List<byte[]> list = null;

    if (t == null) {
      LogServiceRaftLogReader reader = new LogServiceRaftLogReader(log);
      list = new ArrayList<byte[]>();
      try {
        reader.seek(startRecordId);
        for (int i = 0; i < numRecordsToRead; i++) {
          if (!reader.hasNext()) {
            break;
          }
          list.add(reader.next().toByteArray());
        }
      } catch (Exception e) {
        LOG.error("Failed to execute ReadNextQuery", e);
        t = e;
        list = null;
      }
    }
    return CompletableFuture.completedFuture(
      Message.valueOf(LogServiceProtoUtil.toReadLogReplyProto(list, t).toByteString()));
  }

  /**
   * Process sync request
   * @param trx transaction
   * @param logMessage message
   * @return reply message
   */
  private CompletableFuture<Message> processSyncRequest(TransactionContext trx,
      LogServiceRequestProto logMessage) {
     long index = trx.getLogEntry().getIndex();
    // TODO: Do we really need this call?
    return CompletableFuture.completedFuture(Message
      .valueOf(LogServiceProtoUtil.toSyncLogReplyProto(index, null).toByteString()));

  }

  private CompletableFuture<Message> processAppendRequest(TransactionContext trx,
      LogServiceRequestProto logProto) {

    final LogEntryProto entry = trx.getLogEntry();
    AppendLogEntryRequestProto proto = logProto.getAppendRequest();
    final long index = entry.getIndex();
    long newSize = 0;
    Throwable t = verifyState(State.OPEN);
    final List<Long> ids = new ArrayList<Long>();
    if (t == null) {
      try (final AutoCloseableLock writeLock = writeLock()) {
          List<byte[]> entries = LogServiceProtoUtil.toListByteArray(proto.getDataList());
          for (byte[] bb : entries) {
            ids.add(this.length);
            newSize += bb.length;
            this.length++;
          }
          this.dataRecordsSize += newSize;
          // TODO do we need this for other write request (close, sync)
          updateLastAppliedTermIndex(entry.getTerm(), index);
      }
    }
    final CompletableFuture<Message> f =
        CompletableFuture.completedFuture(
          Message.valueOf(LogServiceProtoUtil.toAppendLogReplyProto(ids, t).toByteString()));
    final RaftProtos.RaftPeerRole role = trx.getServerRole();
    if (LOG.isTraceEnabled()) {
      LOG.trace("{}:{}-{}: {} new length {}", role, getId(), index,
          TextFormat.shortDebugString(proto), dataRecordsSize);
    }
    return f;
  }

  @Override
  public void close() {
    reset();
  }

  @Override
  public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
    try {
      checkInitialization();
      final LogEntryProto entry = trx.getLogEntry();
      LogServiceRequestProto logServiceRequestProto =
          LogServiceRequestProto.parseFrom(entry.getStateMachineLogEntry().getLogData());
      switch (logServiceRequestProto.getRequestCase()) {
        case CLOSELOG:
          return recordTime(getCloseLogTimer, new Task(){
            @Override public CompletableFuture<Message> run() {
              return processCloseLog(logServiceRequestProto);
            }});
        case APPENDREQUEST:
          return recordTime(appendRequestTimer, new Task(){
              @Override public CompletableFuture<Message> run() {
                return processAppendRequest(trx, logServiceRequestProto);
              }});
        case SYNCREQUEST:
          return recordTime(syncRequesTimer, new Task(){
            @Override public CompletableFuture<Message> run() {
              return processSyncRequest(trx, logServiceRequestProto);
            }});
        default:
          //TODO
          return null;
      }
    } catch (IOException e) {
      // TODO exception handling
      throw new RuntimeException(e);
    }
  }



  private CompletableFuture<Message> processCloseLog(LogServiceRequestProto logServiceRequestProto) {
    CloseLogRequestProto closeLog = logServiceRequestProto.getCloseLog();
    // Need to check whether the file is opened if opened close it.
    // TODO need to handle exceptions while operating with files.
    return CompletableFuture.completedFuture(Message
      .valueOf(CloseLogReplyProto.newBuilder().build().toByteString()));
  }



  private CompletableFuture<Message> processGetStateRequest(
      LogServiceRequestProto logServiceRequestProto) {
    GetStateRequestProto getState = logServiceRequestProto.getGetState();
    return CompletableFuture.completedFuture(Message.valueOf(LogServiceProtoUtil
        .toGetStateReplyProto(state == State.OPEN).toByteString()));
  }

  private Throwable verifyState(State state) {
       if (this.state != state) {
          return new IOException("Wrong state: " + this.state);
        }
        return null;
   }


}
