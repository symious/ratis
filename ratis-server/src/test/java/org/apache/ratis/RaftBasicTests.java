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
package org.apache.ratis;

import org.apache.log4j.Level;
import org.apache.ratis.RaftTestUtil.*;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.impl.BlockRequestHandlingInjection;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.server.storage.RaftLog;
import org.apache.ratis.shaded.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.LogUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.ratis.RaftTestUtil.*;
import static org.junit.Assert.assertTrue;

public abstract class RaftBasicTests extends BaseTest {
  {
    LogUtils.setLogLevel(RaftServerImpl.LOG, Level.DEBUG);
    LogUtils.setLogLevel(RaftClient.LOG, Level.DEBUG);
  }

  public static final int NUM_SERVERS = 5;

  protected static final RaftProperties properties = new RaftProperties();

  public abstract MiniRaftCluster getCluster();

  public RaftProperties getProperties() {
    return properties;
  }

  @Before
  public void setup() throws IOException {
    Assert.assertNull(getCluster().getLeader());
    getCluster().start();
  }

  @After
  public void tearDown() {
    final MiniRaftCluster cluster = getCluster();
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testBasicAppendEntries() throws Exception {
    runTestBasicAppendEntries(false, 10, getCluster(), LOG);
  }

  static void runTestBasicAppendEntries(
      boolean async, int numMessages, MiniRaftCluster cluster, Logger LOG) throws Exception {
    LOG.info("runTestBasicAppendEntries: async? " + async + ", numMessages=" + numMessages);
    RaftServerImpl leader = waitForLeader(cluster);
    final long term = leader.getState().getCurrentTerm();
    final RaftPeerId killed = cluster.getFollowers().get(0).getId();
    cluster.killServer(killed);
    LOG.info(cluster.printServers());

    final SimpleMessage[] messages = SimpleMessage.create(numMessages);

    try (final RaftClient client = cluster.createClient()) {
      final AtomicInteger asyncReplyCount = new AtomicInteger();
      final CompletableFuture<Void> f = new CompletableFuture<>();

      for (SimpleMessage message : messages) {
        if (async) {
          client.sendAsync(message).thenAcceptAsync(reply -> {
            if (!reply.isSuccess()) {
              f.completeExceptionally(
                  new AssertionError("Failed with reply " + reply));
            } else if (asyncReplyCount.incrementAndGet() == messages.length) {
              f.complete(null);
            }
          });
        } else {
          client.send(message);
        }
      }
      if (async) {
        f.join();
        Assert.assertEquals(messages.length, asyncReplyCount.get());
      }
    }

    Thread.sleep(cluster.getMaxTimeout() + 100);
    LOG.info(cluster.printAllLogs());

    cluster.getServerAliveStream().map(s -> s.getState().getLog())
        .forEach(log -> RaftTestUtil.assertLogEntries(log, async, term, messages));
  }

  @Test
  public void testOldLeaderCommit() throws Exception {
    LOG.info("Running testOldLeaderCommit");
    final MiniRaftCluster cluster = getCluster();
    final RaftServerImpl leader = waitForLeader(cluster);
    final RaftPeerId leaderId = leader.getId();
    final long term = leader.getState().getCurrentTerm();

    List<RaftServerImpl> followers = cluster.getFollowers();
    final RaftServerImpl followerToSendLog = followers.get(0);
    for (int i = 1; i < NUM_SERVERS - 1; i++) {
      RaftServerImpl follower = followers.get(i);
      cluster.killServer(follower.getId());
    }

    SimpleMessage[] messages = SimpleMessage.create(1);
    RaftTestUtil.sendMessageInNewThread(cluster, messages);

    Thread.sleep(cluster.getMaxTimeout() + 100);
    RaftLog followerLog = followerToSendLog.getState().getLog();
    assertTrue(logEntriesContains(followerLog, messages));

    LOG.info(String.format("killing old leader: %s", leaderId.toString()));
    cluster.killServer(leaderId);

    for (int i = 1; i < 3; i++) {
      RaftServerImpl follower = followers.get(i);
      LOG.info(String.format("restarting follower: %s", follower.getId().toString()));
      cluster.restartServer(follower.getId(), false );
    }

    Thread.sleep(cluster.getMaxTimeout() * 5);
    // confirm the server with log is elected as new leader.
    final RaftPeerId newLeaderId = waitForLeader(cluster).getId();
    Assert.assertEquals(followerToSendLog.getId(), newLeaderId);

    cluster.getServerAliveStream().map(s -> s.getState().getLog())
        .forEach(log -> RaftTestUtil.assertLogEntries(log, false, term, messages));
    LOG.info("terminating testOldLeaderCommit test");
  }

  @Test
  public void testOldLeaderNotCommit() throws Exception {
    LOG.info("Running testOldLeaderNotCommit");
    final MiniRaftCluster cluster = getCluster();
    final RaftPeerId leaderId = waitForLeader(cluster).getId();

    List<RaftServerImpl> followers = cluster.getFollowers();
    final RaftServerImpl followerToCommit = followers.get(0);
    for (int i = 1; i < NUM_SERVERS - 1; i++) {
      RaftServerImpl follower = followers.get(i);
      cluster.killServer(follower.getId());
    }

    SimpleMessage[] messages = SimpleMessage.create(1);
    sendMessageInNewThread(cluster, messages);

    Thread.sleep(cluster.getMaxTimeout() + 100);
    logEntriesContains(followerToCommit.getState().getLog(), messages);

    cluster.killServer(leaderId);
    cluster.killServer(followerToCommit.getId());

    for (int i = 1; i < NUM_SERVERS - 1; i++) {
      RaftServerImpl follower = followers.get(i);
      cluster.restartServer(follower.getId(), false );
    }
    waitForLeader(cluster);
    Thread.sleep(cluster.getMaxTimeout() + 100);

    final Predicate<LogEntryProto> predicate = l -> l.getTerm() != 1;
    cluster.getServerAliveStream()
            .map(s -> s.getState().getLog())
            .forEach(log -> RaftTestUtil.checkLogEntries(log, messages, predicate));
  }

  static class Client4TestWithLoad extends Thread {
    boolean useAsync;
    final int index;
    final SimpleMessage[] messages;

    final AtomicBoolean isRunning = new AtomicBoolean(true);
    final AtomicInteger step = new AtomicInteger();
    final AtomicReference<Throwable> exceptionInClientThread = new AtomicReference<>();

    final MiniRaftCluster cluster;
    final Logger LOG;

    Client4TestWithLoad(int index, int numMessages, boolean useAsync,
        MiniRaftCluster cluster, Logger LOG) {
      super("client-" + index);
      this.index = index;
      this.messages = SimpleMessage.create(numMessages, index + "-");
      this.useAsync = useAsync;
      this.cluster = cluster;
      this.LOG = LOG;
    }

    boolean isRunning() {
      return isRunning.get();
    }

    @Override
    public void run() {
      try (RaftClient client = cluster.createClient()) {
        final CompletableFuture f = new CompletableFuture();
        for (int i = 0; i < messages.length; i++) {
          if (!useAsync) {
            final RaftClientReply reply =
                client.send(messages[step.getAndIncrement()]);
            Assert.assertTrue(reply.isSuccess());
          } else {
            final CompletableFuture<RaftClientReply> replyFuture =
                client.sendAsync(messages[i]);
            replyFuture.thenAcceptAsync(r -> {
              if (!r.isSuccess()) {
                f.completeExceptionally(
                    new AssertionError("Failed with reply: " + r));
              }
              if (step.incrementAndGet() == messages.length) {
                f.complete(null);
              }
              Assert.assertTrue(r.isSuccess());
            });
          }
        }
        if (useAsync) {
          f.join();
          Assert.assertTrue(step.get() == messages.length);
        }
      } catch(Throwable t) {
        if (exceptionInClientThread.compareAndSet(null, t)) {
          LOG.error(this + " failed", t);
        } else {
          exceptionInClientThread.get().addSuppressed(t);
          LOG.error(this + " failed again!", t);
        }
      } finally {
        isRunning.set(false);
      }
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + index
          + "(step=" + step + "/" + messages.length
          + ", isRunning=" + isRunning
          + ", isAlive=" + isAlive()
          + ", exception=" + exceptionInClientThread
          + ")";
    }
  }

  @Test
  public void testWithLoad() throws Exception {
    testWithLoad(10, 500, false, getCluster(), LOG);
  }

  public static void testWithLoad(final int numClients, final int numMessages,
      boolean useAsync, MiniRaftCluster cluster, Logger LOG) throws Exception {
    LOG.info("Running testWithLoad: numClients=" + numClients
        + ", numMessages=" + numMessages + ", async=" + useAsync);

    waitForLeader(cluster);

    final List<Client4TestWithLoad> clients
        = Stream.iterate(0, i -> i+1).limit(numClients)
        .map(i -> new Client4TestWithLoad(i, numMessages, useAsync, cluster, LOG))
        .collect(Collectors.toList());
    final AtomicInteger lastStep = new AtomicInteger();

    final Timer timer = new Timer();
    timer.schedule(new TimerTask() {
      private int previousLastStep = lastStep.get();

      @Override
      public void run() {
        LOG.info(cluster.printServers());
        LOG.info(BlockRequestHandlingInjection.getInstance().toString());
        LOG.info(cluster.toString());
        clients.forEach(c -> LOG.info("  " + c));
        JavaUtils.dumpAllThreads(s -> LOG.info(s));

        final int last = lastStep.get();
        if (last != previousLastStep) {
          previousLastStep = last;
        } else {
          final RaftServerImpl leader = cluster.getLeader();
          LOG.info("NO PROGRESS at " + last + ", try to restart leader=" + leader);
          if (leader != null) {
            try {
              cluster.restartServer(leader.getId(), false);
              LOG.info("Restarted leader=" + leader);
            } catch (IOException e) {
              LOG.error("Failed to restart leader=" + leader);
            }
          }
        }
      }
    }, 5_000L, 10_000L);

    clients.forEach(Thread::start);

    int count = 0;
    for(;; ) {
      if (clients.stream().filter(Client4TestWithLoad::isRunning).count() == 0) {
        break;
      }

      final int n = clients.stream().mapToInt(c -> c.step.get()).sum();
      assertTrue(n >= lastStep.get());

      if (n - lastStep.get() < 50 * numClients) { // Change leader at least 50 steps.
        Thread.sleep(10);
        continue;
      }
      lastStep.set(n);
      count++;

      RaftServerImpl leader = cluster.getLeader();
      if (leader != null) {
        RaftTestUtil.changeLeader(cluster, leader.getId());
      }
    }
    LOG.info("Leader change count=" + count);
    timer.cancel();

    for(Client4TestWithLoad c : clients) {
      if (c.exceptionInClientThread.get() != null) {
        throw new AssertionError(c.exceptionInClientThread.get());
      }
      RaftTestUtil.assertLogEntries(cluster.getServers(), c.messages);
    }
  }
}
