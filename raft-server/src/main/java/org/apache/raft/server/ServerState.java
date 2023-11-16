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
package org.apache.raft.server;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.raft.conf.RaftProperties;
import org.apache.raft.proto.RaftProtos.LogEntryProto;
import org.apache.raft.protocol.Message;
import org.apache.raft.server.protocol.InstallSnapshotRequest;
import org.apache.raft.server.protocol.ServerProtoUtils;
import org.apache.raft.server.protocol.TermIndex;
import org.apache.raft.server.storage.*;
import org.apache.raft.statemachine.SnapshotInfo;
import org.apache.raft.util.ProtoUtils;
import org.apache.raft.util.RaftUtils;

import java.io.Closeable;
import java.io.IOException;

import static org.apache.raft.server.RaftServerConfigKeys.RAFT_SERVER_STATEMACHINE_CLASS_DEFAULT;
import static org.apache.raft.server.RaftServerConfigKeys.RAFT_SERVER_STATEMACHINE_CLASS_KEY;
import static org.apache.raft.server.RaftServerConfigKeys.RAFT_SERVER_USE_MEMORY_LOG_DEFAULT;
import static org.apache.raft.server.RaftServerConfigKeys.RAFT_SERVER_USE_MEMORY_LOG_KEY;

/**
 * Common states of a raft peer. Protected by RaftServer's lock.
 */
public class ServerState implements Closeable {
  private final String selfId;
  private final RaftServer server;
  /** Raft log */
  private final RaftLog log;
  /** Raft configuration */
  private final ConfigurationManager configurationManager;
  /** The thread that applies committed log entries to the state machine */
  private final StateMachineUpdater stateMachineUpdater;
  /** local storage for log and snapshot */
  private final RaftStorage storage;
  private final SnapshotManager snapshotManager;

  /**
   * Latest term server has seen. initialized to 0 on first boot, increases
   * monotonically.
   */
  private long currentTerm;
  /**
   * The server ID of the leader for this term. Null means either there is
   * no leader for this term yet or this server does not know who it is yet.
   */
  private String leaderId;
  /**
   * Candidate that this peer granted vote for in current term (or null if none)
   */
  private String votedFor;

  /**
   * Latest installed snapshot for this server. This maybe different than StateMachine's latest
   * snapshot. Once we successfully install a snapshot, the SM may not pick it up immediately.
   * Further, this will not get updated when SM does snapshots itself.
   */
  private TermIndex latestInstalledSnapshot;

  ServerState(String id, RaftConfiguration conf, RaftProperties prop,
      RaftServer server) throws IOException {
    this.selfId = id;
    this.server = server;
    configurationManager = new ConfigurationManager(conf);
    storage = new RaftStorage(prop, RaftServerConstants.StartupOption.REGULAR);
    snapshotManager = new SnapshotManager(storage, id);

    StateMachine stateMachine = getStateMachine(prop);
    long lastApplied = initStatemachine(stateMachine, prop);

    leaderId = null;
    log = initLog(id, prop, server, lastApplied);
    RaftLog.Metadata metadata = log.loadMetadata();
    currentTerm = metadata.getTerm();
    votedFor = metadata.getVotedFor();

    stateMachineUpdater = new StateMachineUpdater(stateMachine, server, log,
        lastApplied, prop);
  }

  /**
   * Used by tests to set initial raft configuration with correct port bindings.
   */
  @VisibleForTesting
  public void setInitialConf(RaftConfiguration initialConf) {
    configurationManager.setInitialConf(initialConf);
  }

  private StateMachine getStateMachine(RaftProperties properties) {
    final Class<? extends StateMachine> smClass = properties.getClass(
        RAFT_SERVER_STATEMACHINE_CLASS_KEY,
        RAFT_SERVER_STATEMACHINE_CLASS_DEFAULT, StateMachine.class);
    return RaftUtils.newInstance(smClass);
  }

  private long initStatemachine(StateMachine sm, RaftProperties properties)
      throws IOException {
    sm.initialize(properties, storage);
    storage.setStateMachineStorage(sm.getStateMachineStorage());
    SnapshotInfo snapshot = sm.getLatestSnapshot();

    if (snapshot == null || snapshot.getTermIndex().getIndex() < 0) {
      return RaftServerConstants.INVALID_LOG_INDEX;
    }

    // get the raft configuration from the snapshot
    RaftConfiguration raftConf = sm.getRaftConfiguration();
    if (raftConf != null) {
      configurationManager.addConfiguration(raftConf.getLogEntryIndex(),
          raftConf);
    }
    return snapshot.getIndex();
  }

  void start() {
    stateMachineUpdater.start();
  }

  /**
   * note we do not apply log entries to the state machine here since we do not
   * know whether they have been committed.
   */
  private RaftLog initLog(String id, RaftProperties prop, RaftServer server,
      long lastIndexInSnapshot) throws IOException {
    if (prop.getBoolean(RAFT_SERVER_USE_MEMORY_LOG_KEY,
        RAFT_SERVER_USE_MEMORY_LOG_DEFAULT)) {
      return new MemoryRaftLog(id);
    } else {
      RaftLog log = new SegmentedRaftLog(id, server, this.storage,
          lastIndexInSnapshot, prop);
      log.open(configurationManager, lastIndexInSnapshot);
      return log;
    }
  }

  public RaftConfiguration getRaftConf() {
    return configurationManager.getCurrent();
  }

  @VisibleForTesting

  public String getSelfId() {
    return this.selfId;
  }

  public long getCurrentTerm() {
    return currentTerm;
  }

  void setCurrentTerm(long term) {
    currentTerm = term;
  }

  String getLeaderId() {
    return leaderId;
  }

  boolean hasLeader() {
    return leaderId != null;
  }

  /**
   * Become a candidate and start leader election
   */
  long initElection() {
    votedFor = selfId;
    leaderId = null;
    return ++currentTerm;
  }

  void persistMetadata() throws IOException {
    this.log.writeMetadata(currentTerm, votedFor);
  }

  void resetLeaderAndVotedFor() {
    votedFor = null;
    leaderId = null;
  }

  /**
   * Vote for a candidate and update the local state.
   */
  void grantVote(String candidateId) {
    votedFor = candidateId;
    leaderId = null;
  }

  void setLeader(String leaderId) {
    this.leaderId = leaderId;
  }

  void becomeLeader() {
    leaderId = selfId;
  }

  public RaftLog getLog() {
    return log;
  }

  long applyLog(Message message) {
    return log.append(currentTerm, message);
  }

  /**
   * Check if accept the leader selfId and term from the incoming AppendEntries rpc.
   * If accept, update the current state.
   * @return true if the check passes
   */
  boolean recognizeLeader(String leaderId, long leaderTerm) {
    if (leaderTerm < currentTerm) {
      return false;
    } else if (leaderTerm > currentTerm || this.leaderId == null) {
      // If the request indicates a term that is greater than the current term
      // or no leader has been set for the current term, make sure to update
      // leader and term later
      return true;
    }
    Preconditions.checkArgument(this.leaderId.equals(leaderId),
        "selfId:%s, this.leaderId:%s, received leaderId:%s",
        selfId, this.leaderId, leaderId);
    return true;
  }

  /**
   * Check if the candidate's term is acceptable
   */
  boolean recognizeCandidate(String candidateId,
      long candidateTerm) {
    if (candidateTerm > currentTerm) {
      return true;
    } else if (candidateTerm == currentTerm) {
      // has not voted yet or this is a retry
      return votedFor == null || votedFor.equals(candidateId);
    }
    return false;
  }

  boolean isLogUpToDate(TermIndex candidateLastEntry) {
    LogEntryProto lastEntry = log.getLastEntry();
    // need to take into account snapshot
    SnapshotInfo snapshot = getStateMachine().getLatestSnapshot();
     if (lastEntry == null && snapshot == null) {
      return true;
    } else if (candidateLastEntry == null) {
      return false;
    }
    TermIndex local = ServerProtoUtils.toTermIndex(lastEntry);
    if (local == null || (snapshot != null && snapshot.getIndex() > lastEntry.getIndex())) {
      local = snapshot.getTermIndex();
    }
    return local.compareTo(candidateLastEntry) <= 0;
  }

  @Override
  public String toString() {
    return selfId + ": term=" + currentTerm + ", leader=" + leaderId
        + ", voted=" + votedFor + ", raftlog: " + log + ", conf: "
        + getRaftConf();
  }

  boolean isConfCommitted() {
    return getLog().getLastCommittedIndex() >=
        getRaftConf().getLogEntryIndex();
  }

  public void setRaftConf(long logIndex, RaftConfiguration conf) {
    configurationManager.addConfiguration(logIndex, conf);
    RaftServer.LOG.info("{}: successfully update the configuration {}",
        getSelfId(), conf);
  }

  void updateConfiguration(LogEntryProto[] entries) {
    if (entries != null && entries.length > 0) {
      configurationManager.removeConfigurations(entries[0].getIndex());
      for (LogEntryProto entry : entries) {
        if (ProtoUtils.isConfigurationLogEntry(entry)) {
          final RaftConfiguration conf = ServerProtoUtils.toRaftConfiguration(
              entry.getIndex(), entry.getConfigurationEntry());
          configurationManager.addConfiguration(entry.getIndex(), conf);
          server.addPeersToRPC(conf.getPeers());
        }
      }
    }
  }

  void updateStatemachine(long majorityIndex, long currentTerm) {
    log.updateLastCommitted(majorityIndex, currentTerm);
    stateMachineUpdater.notifyUpdater();
  }

  void reloadStateMachine(long lastIndexInSnapshot, long currentTerm)
      throws IOException {
    log.updateLastCommitted(lastIndexInSnapshot, currentTerm);

    stateMachineUpdater.reloadStateMachine();
  }

  @Override
  public void close() throws IOException {
    stateMachineUpdater.stop();
    storage.close();
  }

  @VisibleForTesting
  public RaftStorage getStorage() {
    return storage;
  }

  public StateMachine getStateMachine() {
    return stateMachineUpdater.getStateMachine();
  }

  void installSnapshot(InstallSnapshotRequest request) throws IOException {
    // TODO: verify that we need to install the snapshot
    getStateMachine().pause(); // pause the SM to prepare for install snapshot
    snapshotManager.installSnapshot(getStateMachine(), request);
    log.syncWithSnapshot(request.getLastIncludedIndex());
    this.latestInstalledSnapshot = new TermIndex(request.getLastIncludedTerm(),
        request.getLastIncludedIndex());
  }

  SnapshotInfo getLatestSnapshot() {
    return getStateMachine().getStateMachineStorage().getLatestSnapshot();
  }

  public TermIndex getLatestInstalledSnapshot() {
    return latestInstalledSnapshot;
  }

  @VisibleForTesting
  public long getLastAppliedIndex() {
    return stateMachineUpdater.getLastAppliedIndex();
  }

  boolean isCurrentConfCommitted() {
    return getRaftConf().getLogEntryIndex() <= getLog().getLastCommittedIndex();
  }
}