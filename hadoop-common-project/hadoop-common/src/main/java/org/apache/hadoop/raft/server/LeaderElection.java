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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.raft.server.protocol.RaftPeer;
import org.apache.hadoop.raft.server.protocol.RaftServerResponse;
import org.apache.hadoop.raft.server.protocol.RequestVoteRequest;
import org.apache.hadoop.raft.server.protocol.TermIndex;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.raft.server.LeaderElection.Result.ELECTED;
import static org.apache.hadoop.raft.server.LeaderElection.Result.NEWTERM;
import static org.apache.hadoop.raft.server.LeaderElection.Result.REJECTED;

class LeaderElection extends Daemon {
  public static final Logger LOG = RaftServer.LOG;

  static String string(List<RaftServerResponse> responses,
      List<Exception> exceptions) {
    return "received " + responses.size() + " response(s) and "
        + exceptions.size() + " exception(s); "
        + responses + "; " + exceptions;
  }

  enum Result {ELECTED, REJECTED, TIMEOUT, NEWTERM}

  private final RaftServer raftServer;
  private ExecutorCompletionService<RaftServerResponse> service;
  private ExecutorService executor;
  private final Collection<RaftPeer> others;
  private volatile boolean running;

  LeaderElection(RaftServer raftServer) {
    this.raftServer = raftServer;
    others = raftServer.getOtherPeers();
    this.running = true;
  }

  void stopRunning() {
    this.running = false;
  }

  private void initExecutor() {
    executor = Executors.newFixedThreadPool(others.size(),
        new ThreadFactoryBuilder().setDaemon(true).build());
    service = new ExecutorCompletionService<>(executor);
  }

  @Override
  public void run() {
    try {
      askForVotes();
    } catch (InterruptedException e) {
      // the leader election thread is interrupted. The peer may already step
      // down to a follower. The leader election should skip.
      LOG.info("The leader election thread of peer {} is interrupted. " +
          "Currently role: {}.", raftServer.getState().getSelfId(),
          raftServer.getRole());
    }
  }

  /**
   * After a peer changes its role to candidate, it invokes this method to
   * send out requestVote rpc to all other peers.
   */
  private void askForVotes() throws InterruptedException {
    final ServerState state = raftServer.getState();
    while (running && raftServer.isCandidate()) {
      // one round of requestVotes
      final long electionTerm = raftServer.initElection();
      final TermIndex lastEntry = state.getLog().getLastEntry();

      Result r = Result.REJECTED;
      try {
        initExecutor();
        int submitted = submitRequests(electionTerm, lastEntry);
        r = waitForResults(electionTerm, submitted);
      } finally {
        executor.shutdown();
      }

      synchronized (raftServer) {
        if (electionTerm != state.getCurrentTerm() || !running ||
            !raftServer.isCandidate()) {
          return; // term already passed or no longer a candidate.
        }

        switch (r) {
          case ELECTED:
            raftServer.changeToLeader();
            return;
          case REJECTED:
          case NEWTERM:
            raftServer.changeToFollower();
            return;
          case TIMEOUT:
            // should start another election
        }
      }
    }
  }

  private int submitRequests(final long electionTerm, final TermIndex lastEntry) {
    int submitted = 0;
    for (final RaftPeer peer : others) {
      final RequestVoteRequest r = raftServer.createRequestVoteRequest(
          peer.getId(), electionTerm, lastEntry);
      service.submit(new Callable<RaftServerResponse>() {
        @Override
        public RaftServerResponse call() throws RaftServerException {
          return raftServer.sendRequestVote(peer, r);
        }
      });
      submitted++;
    }
    return submitted;
  }

  private Result waitForResults(final long electionTerm, final int submitted)
      throws InterruptedException {
    final long startTime = Time.monotonicNow();
    final long timeout = startTime + RaftConstants.getRandomElectionWaitTime();
    final List<RaftServerResponse> responses = new ArrayList<>();
    final List<Exception> exceptions = new ArrayList<>();
    int granted = 1; // self
    int waitForNum = submitted;
    while (waitForNum > 0 && running && raftServer.isCandidate()) {
      final long waitTime = timeout - Time.monotonicNow();
      if (waitTime <= 0) {
        LOG.info("Election timeout: " + string(responses, exceptions));
        return Result.TIMEOUT;
      }

      try {
        RaftServerResponse r = service.poll(waitTime, TimeUnit.MILLISECONDS).get();
        if (r.getTerm() > electionTerm) {
          return NEWTERM;
        }
        responses.add(r);
        if (r.isSuccess()) {
          granted++;
          if (granted > others.size()/2) {
            LOG.info("Election passed: " + string(responses, exceptions));
            return ELECTED;
          }
        }
      } catch(ExecutionException e) {
        LOG.warn("", e);
        exceptions.add(e);
      }
      waitForNum--;
    }
    // received all the responses
    LOG.info("Election rejected: " + string(responses, exceptions));
    return REJECTED;
  }
}
