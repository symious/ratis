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
package org.apache.ratis.server.impl;

import org.apache.ratis.proto.RaftProtos.RequestVoteReplyProto;
import org.apache.ratis.proto.RaftProtos.RequestVoteRequestProto;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.DivisionInfo;
import org.apache.ratis.server.RaftConfiguration;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.util.ServerStringUtils;
import org.apache.ratis.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.ratis.util.Daemon;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.LogUtils;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.ratis.util.LifeCycle.State.NEW;
import static org.apache.ratis.util.LifeCycle.State.RUNNING;
import static org.apache.ratis.util.LifeCycle.State.STARTING;

import com.codahale.metrics.Timer;

class LeaderElection implements Runnable {
  public static final Logger LOG = LoggerFactory.getLogger(LeaderElection.class);

  private ResultAndTerm logAndReturn(Result result,
      Map<RaftPeerId, RequestVoteReplyProto> responses,
      List<Exception> exceptions, long newTerm, boolean preVote) {
    LOG.info("{}: {} {} received {} response(s):{}",
        this,
        preVote ? "Pre-vote " : "Election ",
        result,
        responses.size(),
        responses.values().stream().map(ServerStringUtils::toRequestVoteReplyString).collect(Collectors.toList()));

    int i = 0;
    for(Exception e : exceptions) {
      final int j = i++;
      LogUtils.infoOrTrace(LOG, () -> "  Exception " + j, e);
    }
    return new ResultAndTerm(result, newTerm);
  }

  enum Result {PASSED, REJECTED, TIMEOUT, DISCOVERED_A_NEW_TERM, SHUTDOWN}

  private static class ResultAndTerm {
    private final Result result;
    private final long term;

    ResultAndTerm(Result result, long term) {
      this.result = result;
      this.term = term;
    }
  }

  static class Executor {
    private final ExecutorCompletionService<RequestVoteReplyProto> service;
    private final ExecutorService executor;

    private final AtomicInteger count = new AtomicInteger();

    Executor(Object name, int size) {
      Preconditions.assertTrue(size > 0);
      executor = Executors.newFixedThreadPool(size, r -> new Daemon(r, name + "-" + count.incrementAndGet()));
      service = new ExecutorCompletionService<>(executor);
    }

    void shutdown() {
      executor.shutdown();
    }

    void submit(Callable<RequestVoteReplyProto> task) {
      service.submit(task);
    }

    Future<RequestVoteReplyProto> poll(TimeDuration waitTime) throws InterruptedException {
      return service.poll(waitTime.getDuration(), waitTime.getUnit());
    }
  }

  private static final AtomicInteger COUNT = new AtomicInteger();

  private final String name;
  private final LifeCycle lifeCycle;
  private final Daemon daemon;

  private final RaftServerImpl server;
  private final boolean force;

  LeaderElection(RaftServerImpl server, boolean force) {
    this.name = server.getMemberId() + "-" + JavaUtils.getClassSimpleName(getClass()) + COUNT.incrementAndGet();
    this.lifeCycle = new LifeCycle(this);
    this.daemon = new Daemon(this);
    this.server = server;
    this.force = force;
  }

  void start() {
    startIfNew(daemon::start);
  }

  @VisibleForTesting
  void startInForeground() {
    startIfNew(this);
  }

  private void startIfNew(Runnable starter) {
    if (lifeCycle.compareAndTransition(NEW, STARTING)) {
      starter.run();
    } else {
      final LifeCycle.State state = lifeCycle.getCurrentState();
      LOG.info("{}: skip starting since this is already {}", this, state);
    }
  }

  void shutdown() {
    lifeCycle.checkStateAndClose();
  }

  @VisibleForTesting
  LifeCycle.State getCurrentState() {
    return lifeCycle.getCurrentState();
  }

  @Override
  public void run() {
    if (!lifeCycle.compareAndTransition(STARTING, RUNNING)) {
      final LifeCycle.State state = lifeCycle.getCurrentState();
      LOG.info("{}: skip running since this is already {}", this, state);
      return;
    }

    Timer.Context electionContext =
        server.getLeaderElectionMetrics().getLeaderElectionTimer().time();
    try {
      /**
       * See the thesis section 9.6: In the Pre-Vote algorithm, a candidate
       * only increments its term and start a real election if it first learns
       * from a majority of the cluster that they would be willing to grant
       * the candidate their votes (if the candidate’s log is sufficiently
       * up-to-date, and the voters have not received heartbeats from a valid
       * leader for at least a baseline election timeout).
       */
      boolean preVotePass = force ? true : askForPreVotes();

      if (preVotePass) {
        askForVotes();
      }
    } catch(Exception e) {
      final LifeCycle.State state = lifeCycle.getCurrentState();
      if (state.isClosingOrClosed()) {
        LOG.info("{}: {} is safely ignored since this is already {}",
            this, JavaUtils.getClassSimpleName(e.getClass()), state, e);
      } else {
        if (!server.getInfo().isAlive()) {
          LOG.info("{}: {} is safely ignored since the server is not alive: {}",
              this, JavaUtils.getClassSimpleName(e.getClass()), server, e);
        } else {
          LOG.error("{}: Failed, state={}", this, state, e);
        }
        shutdown();
      }
    } finally {
      // Update leader election completion metric(s).
      electionContext.stop();
      server.getLeaderElectionMetrics().onNewLeaderElectionCompletion();
      lifeCycle.checkStateAndClose(() -> {});
    }
  }

  private boolean shouldRun() {
    final DivisionInfo info = server.getInfo();
    return lifeCycle.getCurrentState().isRunning() && info.isCandidate() && info.isAlive();
  }

  private boolean shouldRun(long electionTerm) {
    return shouldRun() && server.getState().getCurrentTerm() == electionTerm;
  }

  private ResultAndTerm submitRequestAndWaitResult(
      final ServerState state, final RaftConfigurationImpl conf, final long electionTerm, boolean preVote)
      throws InterruptedException {
    final ResultAndTerm r;
    final Collection<RaftPeer> others = conf.getOtherPeers(server.getId());
    if (others.isEmpty()) {
      r = new ResultAndTerm(Result.PASSED, electionTerm);
    } else {
      TermIndex lastEntry = state.getLastEntry();
      final Executor voteExecutor = new Executor(this, others.size());
      try {
        final int submitted = submitRequests(electionTerm, lastEntry, others, voteExecutor, preVote);
        r = waitForResults(electionTerm, submitted, conf, voteExecutor, preVote);
      } finally {
        voteExecutor.shutdown();
      }
    }

    return r;
  }

  /**
   * After a peer changes its role to candidate, it invokes this method to
   * send out requestVote rpc to all other peers.
   */
  private boolean askForPreVotes() throws InterruptedException, IOException {
    final ServerState state = server.getState();
    if (shouldRun()) {
      // one round of request pre-votes
      final long electionTerm;
      final RaftConfigurationImpl conf;
      synchronized (server) {
        if (!shouldRun()) {
          return false;
        }
        state.setLeader(null, "initPreVote");
        conf = state.getRaftConf();
        electionTerm = state.getCurrentTerm();
      }

      LOG.info("{}: begin a pre-vote at term {} for {}", this, electionTerm, conf);
      final ResultAndTerm r = submitRequestAndWaitResult(state, conf, electionTerm, true);
      LOG.info("{} pre-vote result is {}.", this, r.result);

      synchronized (server) {
        if (!shouldRun(electionTerm)) {
          return false; // term already passed or this should not run anymore.
        }

        switch (r.result) {
          case PASSED:
            return true;
          case REJECTED:
          case TIMEOUT:
            server.changeToFollowerAndPersistMetadata(state.getCurrentTerm(), r.result);
            return false;
          case SHUTDOWN:
            LOG.info("{} received shutdown response when requesting pre-vote.", this);
            server.getRaftServer().close();
            return false;
          case DISCOVERED_A_NEW_TERM:
            LOG.error("{} should not happen {} when requesting pre-vote.", this, r.result);
            return false;
          default: throw new IllegalArgumentException("Unable to process result " + r.result);
        }
      }
    }

    return false;
  }

  /**
   * After a peer changes its role to candidate and pass pre-vote, it invokes this method to
   * send out requestVote rpc to all other peers.
   */
  private void askForVotes() throws InterruptedException, IOException {
    final ServerState state = server.getState();
    while (shouldRun()) {
      // one round of requestVotes
      final long electionTerm;
      final RaftConfigurationImpl conf;
      synchronized (server) {
        if (!shouldRun()) {
          break;
        }
        electionTerm = state.initElection();
        conf = state.getRaftConf();
        state.persistMetadata();
      }
      LOG.info("{}: begin an election at term {} for {}", this, electionTerm, conf);

      final ResultAndTerm r = submitRequestAndWaitResult(state, conf, electionTerm, false);

      synchronized (server) {
        if (!shouldRun(electionTerm)) {
          return; // term already passed or this should not run anymore.
        }

        switch (r.result) {
          case PASSED:
            server.changeToLeader();
            return;
          case SHUTDOWN:
            LOG.info("{} received shutdown response when requesting votes.", this);
            server.getRaftServer().close();
            return;
          case REJECTED:
          case DISCOVERED_A_NEW_TERM:
            final long term = Math.max(r.term, state.getCurrentTerm());
            server.changeToFollowerAndPersistMetadata(term, Result.DISCOVERED_A_NEW_TERM);
            return;
          case TIMEOUT: // should start another election
            continue;
          default: throw new IllegalArgumentException("Unable to process result " + r.result);
        }
      }
    }
  }

  private int submitRequests(final long electionTerm, final TermIndex lastEntry,
      Collection<RaftPeer> others, Executor voteExecutor, boolean preVote) {
    int submitted = 0;
    for (final RaftPeer peer : others) {
      final RequestVoteRequestProto r = ServerProtoUtils.toRequestVoteRequestProto(
          server.getMemberId(), peer.getId(), electionTerm, lastEntry, preVote);
      voteExecutor.submit(() -> server.getServerRpc().requestVote(r));
      submitted++;
    }
    return submitted;
  }

  private Set<RaftPeerId> getHigherPriorityPeers(RaftConfiguration conf) {
    Set<RaftPeerId> higherPriorityPeers = new HashSet<>();

    int currPriority = conf.getPeer(server.getId()).getPriority();
    final Collection<RaftPeer> peers = conf.getAllPeers();

    for (RaftPeer peer : peers) {
      if (peer.getPriority() > currPriority) {
        higherPriorityPeers.add(peer.getId());
      }
    }

    return higherPriorityPeers;
  }

  private ResultAndTerm waitForResults(final long electionTerm, final int submitted,
      RaftConfigurationImpl conf, Executor voteExecutor, boolean preVote) throws InterruptedException {
    final Timestamp timeout = Timestamp.currentTime().addTime(server.getRandomElectionTimeout());
    final Map<RaftPeerId, RequestVoteReplyProto> responses = new HashMap<>();
    final List<Exception> exceptions = new ArrayList<>();
    int waitForNum = submitted;
    Collection<RaftPeerId> votedPeers = new ArrayList<>();
    Collection<RaftPeerId> rejectedPeers = new ArrayList<>();
    Set<RaftPeerId> higherPriorityPeers = getHigherPriorityPeers(conf);

    while (waitForNum > 0 && shouldRun(electionTerm)) {
      final TimeDuration waitTime = timeout.elapsedTime().apply(n -> -n);
      if (waitTime.isNonPositive()) {
        if (conf.hasMajority(votedPeers, server.getId())) {
          // if some higher priority peer did not response when timeout, but candidate get majority,
          // candidate pass vote
          return logAndReturn(Result.PASSED, responses, exceptions, -1, preVote);
        } else {
          return logAndReturn(Result.TIMEOUT, responses, exceptions, -1, preVote);
        }
      }

      try {
        final Future<RequestVoteReplyProto> future = voteExecutor.poll(waitTime);
        if (future == null) {
          continue; // poll timeout, continue to return Result.TIMEOUT
        }

        final RequestVoteReplyProto r = future.get();
        final RaftPeerId replierId = RaftPeerId.valueOf(r.getServerReply().getReplyId());
        final RequestVoteReplyProto previous = responses.putIfAbsent(replierId, r);
        if (previous != null) {
          if (LOG.isWarnEnabled()) {
            LOG.warn("{} received duplicated replies from {}, the 2nd reply is ignored: 1st={}, 2nd={}",
                this, replierId,
                ServerStringUtils.toRequestVoteReplyString(previous),
                ServerStringUtils.toRequestVoteReplyString(r));
          }
          continue;
        }
        if (r.getShouldShutdown()) {
          return logAndReturn(Result.SHUTDOWN, responses, exceptions, -1, preVote);
        }
        if (!preVote && r.getTerm() > electionTerm) {
          return logAndReturn(Result.DISCOVERED_A_NEW_TERM, responses,
              exceptions, r.getTerm(), false);
        }

        // If any peer with higher priority rejects vote, candidate can not pass vote
        if (!r.getServerReply().getSuccess() && higherPriorityPeers.contains(replierId)) {
          return logAndReturn(Result.REJECTED, responses, exceptions, -1, preVote);
        }

        // remove higher priority peer, so that we check higherPriorityPeers empty to make sure
        // all higher priority peers have replied
        higherPriorityPeers.remove(replierId);

        if (r.getServerReply().getSuccess()) {
          votedPeers.add(replierId);
          // If majority and all peers with higher priority have voted, candidate pass vote
          if (higherPriorityPeers.size() == 0 && conf.hasMajority(votedPeers, server.getId())) {
            return logAndReturn(Result.PASSED, responses, exceptions, -1, preVote);
          }
        } else {
          rejectedPeers.add(replierId);
          if (conf.majorityRejectVotes(rejectedPeers)) {
            return logAndReturn(Result.REJECTED, responses, exceptions, -1, preVote);
          }
        }
      } catch(ExecutionException e) {
        LogUtils.infoOrTrace(LOG, () -> this + " got exception when requesting votes", e);
        exceptions.add(e);
      }
      waitForNum--;
    }
    // received all the responses
    if (conf.hasMajority(votedPeers, server.getId())) {
      return logAndReturn(Result.PASSED, responses, exceptions, -1, preVote);
    } else {
      return logAndReturn(Result.REJECTED, responses, exceptions, -1, preVote);
    }
  }

  @Override
  public String toString() {
    return name;
  }
}
