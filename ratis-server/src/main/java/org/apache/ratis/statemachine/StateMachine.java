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
package org.apache.ratis.statemachine;

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.RaftConfiguration;
import org.apache.ratis.server.storage.RaftLog;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.shaded.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.util.LifeCycle;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * StateMachine is the entry point for the custom implementation of replicated state as defined in
 * the "State Machine Approach" in the literature
 * (see https://en.wikipedia.org/wiki/State_machine_replication).
 */
public interface StateMachine extends Closeable {
  /**
   * Initializes the State Machine with the given properties and storage. The state machine is
   * responsible reading the latest snapshot from the file system (if any) and initialize itself
   * with the latest term and index there including all the edits.
   */
  void initialize(RaftPeerId id, RaftProperties properties, RaftStorage storage)
      throws IOException;

  /**
   * Returns the lifecycle state for this StateMachine.
   * @return the lifecycle state.
   */
  LifeCycle.State getLifeCycleState();

  /**
   * Pauses the state machine. On return, the state machine should have closed all open files so
   * that a new snapshot can be installed.
   */
  void pause();

  /**
   * Re-initializes the State Machine in PAUSED state with the given properties and storage. The
   * state machine is responsible reading the latest snapshot from the file system (if any) and
   * initialize itself with the latest term and index there including all the edits.
   */
  void reinitialize(RaftPeerId id, RaftProperties properties, RaftStorage storage)
      throws IOException;

  /**
   * Dump the in-memory state into a snapshot file in the RaftStorage. The
   * StateMachine implementation can decide 1) its own snapshot format, 2) when
   * a snapshot is taken, and 3) how the snapshot is taken (e.g., whether the
   * snapshot blocks the state machine, and whether to purge log entries after
   * a snapshot is done).
   *
   * In the meanwhile, when the size of raft log outside of the latest snapshot
   * exceeds certain threshold, the RaftServer may choose to trigger a snapshot
   * if {@link RaftServerConfigKeys.Snapshot#AUTO_TRIGGER_ENABLED_KEY} is enabled.
   *
   * The snapshot should include the latest raft configuration.
   *
   * @return the largest index of the log entry that has been applied to the
   *         state machine and also included in the snapshot. Note the log purge
   *         should be handled separately.
   */
  // TODO: refactor this
  long takeSnapshot() throws IOException;

  /**
   * Record the RaftConfiguration in the state machine. The RaftConfiguration
   * should also be stored in the snapshot.
   */
  void setRaftConfiguration(RaftConfiguration conf);

  /**
   * @return the latest raft configuration recorded in the state machine.
   */
  RaftConfiguration getRaftConfiguration();

  /**
   * @return StateMachineStorage to interact with the durability guarantees provided by the
   * state machine.
   */
  StateMachineStorage getStateMachineStorage();

  /**
   * Returns the information for the latest durable snapshot.
   */
  SnapshotInfo getLatestSnapshot();

  /**
   * Query the state machine. The request must be read-only.
   * TODO: extend RaftClientRequest to have a read-only request subclass.
   */
  CompletableFuture<RaftClientReply> query(RaftClientRequest request);

  /**
   * Validate/pre-process the incoming update request in the state machine.
   * @return the content to be written to the log entry. Null means the request
   * should be rejected.
   * @throws IOException thrown by the state machine while validation
   */
  TransactionContext startTransaction(RaftClientRequest request)
      throws IOException;

  /**
   * Write asynchronously the state machine data to this state machine.
   *
   * @return a future for the write task if the state machine data should be sync'ed;
   *         otherwise, return null.
   */
  default CompletableFuture<?> writeStateMachineData(LogEntryProto entry) {
    return null;
  }

  /**
   * This is called before the transaction passed from the StateMachine is appended to the raft log.
   * This method will be called from log append and having the same strict serial order that the
   * transactions will have in the RAFT log. Since this is called serially in the critical path of
   * log append, it is important to do only required operations here.
   * @return The Transaction context.
   */
  TransactionContext preAppendTransaction(TransactionContext trx) throws IOException;

  /**
   * Called to notify the state machine that the Transaction passed cannot be appended (or synced).
   * The exception field will indicate whether there was an exception or not.
   * @param trx the transaction to cancel
   * @return cancelled transaction
   */
  TransactionContext cancelTransaction(TransactionContext trx) throws IOException;

  /**
   * Called for transactions that have been committed to the RAFT log. This step is called
   * sequentially in strict serial order that the transactions have been committed in the log.
   * The SM is expected to do only necessary work, and leave the actual apply operation to the
   * applyTransaction calls that can happen concurrently.
   * @param trx the transaction state including the log entry that has been committed to a quorum
   *            of the raft peers
   * @return The Transaction context.
   */
  TransactionContext applyTransactionSerial(TransactionContext trx);

  /**
   * Apply a committed log entry to the state machine. This method can be called concurrently with
   * the other calls, and there is no guarantee that the calls will be ordered according to the
   * log commit order.
   * @param trx the transaction state including the log entry that has been committed to a quorum
   *            of the raft peers
   */
  // TODO: We do not need to return CompletableFuture
  CompletableFuture<Message> applyTransaction(TransactionContext trx);

  /**
   * Notify the state machine that the raft peer is no longer leader.
   */
  void notifyNotLeader(Collection<TransactionContext> pendingEntries) throws IOException;
}
