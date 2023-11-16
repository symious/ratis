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
package org.apache.raft.examples.arithmatic;

import org.apache.raft.conf.RaftProperties;
import org.apache.raft.examples.arithmatic.expression.Expression;
import org.apache.raft.proto.RaftProtos.LogEntryProto;
import org.apache.raft.protocol.Message;
import org.apache.raft.server.BaseStateMachine;
import org.apache.raft.server.RaftConfiguration;
import org.apache.raft.server.RaftServerConstants;
import org.apache.raft.server.StateMachine;
import org.apache.raft.server.protocol.TermIndex;
import org.apache.raft.server.storage.RaftStorage;
import org.apache.raft.server.storage.RaftStorageDirectory;
import org.apache.raft.statemachine.SimpleStateMachineStorage;
import org.apache.raft.statemachine.SimpleStateMachineStorage.SingleFileSnapshotInfo;
import org.apache.raft.statemachine.SnapshotInfo;
import org.apache.raft.statemachine.StateMachineStorage;
import org.apache.raft.statemachine.TermIndexTracker;
import org.apache.raft.util.AutoCloseableLock;
import org.apache.raft.util.ProtoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ArithmeticStateMachine extends BaseStateMachine {
  static final Logger LOG = LoggerFactory.getLogger(ArithmeticStateMachine.class);

  private RaftConfiguration conf;
  private SimpleStateMachineStorage storage;
  private final Map<String, Double> variables = new ConcurrentHashMap<>();
  private TermIndexTracker termIndexTracker = new TermIndexTracker();

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

  private AutoCloseableLock readLock() {
    return AutoCloseableLock.acquire(lock.readLock());
  }

  private AutoCloseableLock writeLock() {
    return AutoCloseableLock.acquire(lock.writeLock());
  }

  public ArithmeticStateMachine() {
    this.storage  = new SimpleStateMachineStorage();
  }

  void reset() {
    variables.clear();
    termIndexTracker.reset();
  }

  @Override
  public void initialize(RaftProperties properties, RaftStorage raftStorage) throws IOException {
    super.initialize(properties, raftStorage);
    this.storage.init(raftStorage);
    SingleFileSnapshotInfo snapshot = this.storage.findLatestSnapshot();
    loadSnapshot(snapshot);
  }

  @Override
  public void pause() {
  }

  @Override
  public void reinitialize(RaftProperties properties, RaftStorage storage) throws IOException {
    close();
    this.initialize(properties, storage);
  }

  @Override
  public long takeSnapshot() throws IOException {
    final Map<String, Double> copy;
    final TermIndex last;
    try(final AutoCloseableLock readLock = readLock()) {
      copy = new HashMap<>(variables);
      last = termIndexTracker.getLatestTermIndex();
    }

    File snapshotFile =  new File(SimpleStateMachineStorage.getSnapshotFileName(
        last.getTerm(), last.getIndex()));

    try(final ObjectOutputStream out = new ObjectOutputStream(
        new BufferedOutputStream(new FileOutputStream(snapshotFile)))) {
      out.writeObject(copy);
    } catch(IOException ioe) {
      LOG.warn("Failed to write snapshot file \"" + snapshotFile
          + "\", last applied index=" + last);
    }

    return last.getIndex();
  }

  public long loadSnapshot(SingleFileSnapshotInfo snapshot) throws IOException {
    return load(snapshot, false);
  }

  public long reloadSnapshot(SingleFileSnapshotInfo snapshot) throws IOException {
    return load(snapshot, true);
  }

  private long load(SingleFileSnapshotInfo snapshot, boolean reload) throws IOException {
    if (snapshot == null || !snapshot.getFile().getPath().toFile().exists()) {
      LOG.warn("The snapshot file {} does not exist", snapshot);
      return RaftServerConstants.INVALID_LOG_INDEX;
    }

    File snapshotFile =snapshot.getFile().getPath().toFile();
    final TermIndex last = SimpleStateMachineStorage.getTermIndexFromSnapshotFile(snapshotFile);
    try(final AutoCloseableLock writeLock = writeLock();
        final ObjectInputStream in = new ObjectInputStream(
            new BufferedInputStream(new FileInputStream(snapshotFile)))) {
      if (reload) {
        reset();
      }
      termIndexTracker.init(last);
      variables.putAll((Map<String, Double>) in.readObject());
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException(e);
    }
    return last.getIndex();
  }

  @Override
  public void setRaftConfiguration(RaftConfiguration conf) {
    this.conf = conf;
  }

  @Override
  public RaftConfiguration getRaftConfiguration() {
    return conf;
  }

  @Override
  public StateMachineStorage getStateMachineStorage() {
    return storage;
  }

  @Override
  public void close() {
    reset();
  }

  @Override
  public Message applyLogEntry(LogEntryProto entry) {
    final Message message = ProtoUtils.toMessage(entry.getClientMessageEntry());
    final AssignmentMessage assignment = new AssignmentMessage(message);

    final long last = entry.getIndex();
    final Double result;
    try(final AutoCloseableLock writeLock = writeLock()) {
      result = assignment.evaluate(variables);
      termIndexTracker.update(entry.getTerm(), entry.getIndex());
    }
    final Expression r = Expression.Utils.double2Expression(result);
    LOG.debug("{}: {} = {}, variables={}", last, assignment, r, variables);
    return Expression.Utils.toMessage(r);
  }

  @Override
  public Message query(Message query) {
    final Expression q = Expression.Utils.bytes2Expression(query.getContent(), 0);
    final Double result;
    try(final AutoCloseableLock readLock = readLock()) {
      result = q.evaluate(variables);
    }
    final Expression r = Expression.Utils.double2Expression(result);
    LOG.debug("QUERY: {} = {}", q, r);
    return Expression.Utils.toMessage(r);
  }
}
