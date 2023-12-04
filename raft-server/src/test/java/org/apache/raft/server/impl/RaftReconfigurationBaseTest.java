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
package org.apache.raft.server.impl;

import org.apache.log4j.Level;
import org.apache.raft.MiniRaftCluster;
import org.apache.raft.MiniRaftCluster.PeerChanges;
import org.apache.raft.RaftTestUtil;
import org.apache.raft.RaftTestUtil.SimpleMessage;
import org.apache.raft.client.RaftClient;
import org.apache.raft.client.RaftClientRequestSender;
import org.apache.raft.client.impl.RaftClientImpl;
import org.apache.raft.conf.RaftProperties;
import org.apache.raft.protocol.*;
import org.apache.raft.server.RaftServerConfigKeys;
import org.apache.raft.server.simulation.RequestHandler;
import org.apache.raft.server.storage.RaftLog;
import org.apache.raft.util.RaftUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Arrays.asList;
import static org.apache.raft.MiniRaftCluster.logSyncDelay;
import static org.apache.raft.server.impl.RaftServerConstants.DEFAULT_SEQNUM;
import static org.apache.raft.server.impl.RaftServerTestUtil.waitAndCheckNewConf;
import static org.apache.raft.shaded.proto.RaftProtos.LogEntryProto.LogEntryBodyCase.CONFIGURATIONENTRY;

public abstract class RaftReconfigurationBaseTest {
  static {
    RaftUtils.setLogLevel(RaftServerImpl.LOG, Level.DEBUG);
    RaftUtils.setLogLevel(RequestHandler.LOG, Level.DEBUG);
    RaftUtils.setLogLevel(RaftClient.LOG, Level.DEBUG);
  }
  static final Logger LOG = LoggerFactory.getLogger(RaftReconfigurationBaseTest.class);

  protected static final RaftProperties prop = new RaftProperties();

  @BeforeClass
  public static void setup() {
    // set a small gap for tests
    prop.setInt(RaftServerConfigKeys.RAFT_SERVER_STAGING_CATCHUP_GAP_KEY, 10);
  }

  public abstract MiniRaftCluster getCluster(int peerNum) throws IOException;

  private static int getStagingGap() {
    return prop.getInt(RaftServerConfigKeys.RAFT_SERVER_STAGING_CATCHUP_GAP_KEY,
        RaftServerConfigKeys.RAFT_SERVER_STAGING_CATCHUP_GAP_DEFAULT);
  }

  /**
   * add 2 new peers (3 peers -> 5 peers), no leader change
   */
  @Test
  public void testAddPeers() throws Exception {
    LOG.info("Start testAddPeers");
    MiniRaftCluster cluster = getCluster(3);
    cluster.start();
    try {
      RaftTestUtil.waitForLeader(cluster);

      // add new peers
      RaftPeer[] allPeers = cluster.addNewPeers(2, true).allPeersInNewConf;

      // trigger setConfiguration
      SetConfigurationRequest request = new SetConfigurationRequest("client",
          cluster.getLeader().getId(), DEFAULT_SEQNUM, allPeers);
      LOG.info("Start changing the configuration: {}", request);
      cluster.getLeader().setConfiguration(request);

      // wait for the new configuration to take effect
      waitAndCheckNewConf(cluster, allPeers, 0, null);
    } finally {
      cluster.shutdown();
    }
  }

  /**
   * remove 2 peers (5 peers -> 3 peers), no leader change
   */
  @Test
  public void testRemovePeers() throws Exception {
    LOG.info("Start testRemovePeers");
    MiniRaftCluster cluster = getCluster(5);
    cluster.start();
    try {
      RaftTestUtil.waitForLeader(cluster);

      // remove peers, leader still included in the new conf
      RaftPeer[] allPeers = cluster
          .removePeers(2, false, Collections.emptyList()).allPeersInNewConf;

      // trigger setConfiguration
      SetConfigurationRequest request = new SetConfigurationRequest("client",
          cluster.getLeader().getId(), DEFAULT_SEQNUM, allPeers);
      LOG.info("Start changing the configuration: {}", request);
      cluster.getLeader().setConfiguration(request);

      // wait for the new configuration to take effect
      waitAndCheckNewConf(cluster, allPeers, 2, null);
    } finally {
      cluster.shutdown();
    }
  }

  /**
   * 5 peers -> 5 peers, remove 2 old, add 2 new, no leader change
   */
  @Test
  public void testAddRemovePeers() throws Exception {
    LOG.info("Start testAddRemovePeers");
    testAddRemovePeers(false);
  }

  @Test
  public void testLeaderStepDown() throws Exception {
    LOG.info("Start testLeaderStepDown");
    testAddRemovePeers(true);
  }

  private void testAddRemovePeers(boolean leaderStepdown) throws Exception {
    MiniRaftCluster cluster = getCluster(5);
    cluster.start();
    try {
      RaftTestUtil.waitForLeader(cluster);

      PeerChanges change = cluster.addNewPeers(2, true);
      RaftPeer[] allPeers = cluster.removePeers(2, leaderStepdown,
          asList(change.newPeers)).allPeersInNewConf;

      // trigger setConfiguration
      SetConfigurationRequest request = new SetConfigurationRequest("client",
          cluster.getLeader().getId(), DEFAULT_SEQNUM, allPeers);
      LOG.info("Start changing the configuration: {}", request);
      cluster.getLeader().setConfiguration(request);

      // wait for the new configuration to take effect
      waitAndCheckNewConf(cluster, allPeers, 2, null);
    } finally {
      cluster.shutdown();
    }
  }

  @Test(timeout = 30000)
  public void testReconfTwice() throws Exception {
    LOG.info("Start testReconfTwice");
    final MiniRaftCluster cluster = getCluster(3);
    cluster.start();
    try {
      RaftTestUtil.waitForLeader(cluster);
      final String leaderId = cluster.getLeader().getId();
      final RaftClient client = cluster.createClient("client", leaderId);

      // submit some msgs before reconf
      for (int i = 0; i < getStagingGap() * 2; i++) {
        RaftClientReply reply = client.send(new SimpleMessage("m" + i));
        Assert.assertTrue(reply.isSuccess());
      }

      final AtomicBoolean reconf1 = new AtomicBoolean(false);
      final AtomicBoolean reconf2 = new AtomicBoolean(false);
      final AtomicReference<RaftPeer[]> finalPeers = new AtomicReference<>(null);
      final AtomicReference<RaftPeer[]> deadPeers = new AtomicReference<>(null);
      CountDownLatch latch = new CountDownLatch(1);
      Thread clientThread = new Thread(() -> {
        try {
          PeerChanges c1 = cluster.addNewPeers(2, true);
          LOG.info("Start changing the configuration: {}",
              asList(c1.allPeersInNewConf));

          RaftClientReply reply = client.setConfiguration(c1.allPeersInNewConf);
          reconf1.set(reply.isSuccess());

          PeerChanges c2 = cluster.removePeers(2, true, asList(c1.newPeers));
          finalPeers.set(c2.allPeersInNewConf);
          deadPeers.set(c2.removedPeers);

          LOG.info("Start changing the configuration again: {}",
              asList(c2.allPeersInNewConf));
          reply = client.setConfiguration(c2.allPeersInNewConf);
          reconf2.set(reply.isSuccess());

          latch.countDown();
          client.close();
        } catch (IOException ignored) {
        }
      });
      clientThread.start();

      latch.await();
      Assert.assertTrue(reconf1.get());
      Assert.assertTrue(reconf2.get());
      waitAndCheckNewConf(cluster, finalPeers.get(), 2, null);

      // check configuration manager's internal state
      // each reconf will generate two configurations: (old, new) and (new)
      cluster.getServers().stream().filter(RaftServerImpl::isAlive)
          .forEach(server -> {
        ConfigurationManager confManager =
            (ConfigurationManager) Whitebox.getInternalState(server.getState(),
                "configurationManager");
        // each reconf will generate two configurations: (old, new) and (new)
        Assert.assertEquals(5, confManager.numOfConf());
      });
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testReconfTimeout() throws Exception {
    LOG.info("Start testReconfTimeout");
    // originally 3 peers
    final MiniRaftCluster cluster = getCluster(3);
    cluster.start();
    try {
      RaftTestUtil.waitForLeader(cluster);
      final String leaderId = cluster.getLeader().getId();
      final RaftClient client = cluster.createClient("client", leaderId);

      PeerChanges c1 = cluster.addNewPeers(2, false);

      LOG.info("Start changing the configuration: {}",
          asList(c1.allPeersInNewConf));
      Assert.assertFalse(cluster.getLeader().getRaftConf().isTransitional());

      final RaftClientRequestSender sender = ((RaftClientImpl)client).getRequestSender();
      final SetConfigurationRequest request = new SetConfigurationRequest(
          "client", leaderId, DEFAULT_SEQNUM, c1.allPeersInNewConf);
      try {
        sender.sendRequest(request);
        Assert.fail("did not get expected exception");
      } catch (IOException e) {
        Assert.assertTrue("Got exception " + e,
            e instanceof ReconfigurationTimeoutException);
      }

      // the two new peers have not started yet, the bootstrapping must timeout
      LOG.info(cluster.printServers());

      // resend the same request, make sure the server has correctly reset its
      // state so that we still get timeout instead of in-progress exception
      try {
        sender.sendRequest(request);
        Assert.fail("did not get expected exception");
      } catch (IOException e) {
        Assert.assertTrue("Got exception " + e,
            e instanceof ReconfigurationTimeoutException);
      }

      // start the two new peers
      LOG.info("Start new peers");
      for (RaftPeer np : c1.newPeers) {
        cluster.startServer(np.getId());
      }
      Assert.assertTrue(client.setConfiguration(c1.allPeersInNewConf).isSuccess());
      client.close();
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testBootstrapReconf() throws Exception {
    LOG.info("Start testBootstrapReconf");
    // originally 3 peers
    final MiniRaftCluster cluster = getCluster(3);
    cluster.start();
    try {
      RaftTestUtil.waitForLeader(cluster);
      final String leaderId = cluster.getLeader().getId();
      final RaftClient client = cluster.createClient("client", leaderId);

      // submit some msgs before reconf
      for (int i = 0; i < getStagingGap() * 2; i++) {
        RaftClientReply reply = client.send(new SimpleMessage("m" + i));
        Assert.assertTrue(reply.isSuccess());
      }

      PeerChanges c1 = cluster.addNewPeers(2, true);
      LOG.info("Start changing the configuration: {}",
          asList(c1.allPeersInNewConf));
      final AtomicReference<Boolean> success = new AtomicReference<>();

      Thread clientThread = new Thread(() -> {
        try {
          RaftClientReply reply = client.setConfiguration(c1.allPeersInNewConf);
          success.set(reply.isSuccess());
          client.close();
        } catch (IOException ioe) {
          LOG.error("FAILED", ioe);
        }
      });
      clientThread.start();

      Thread.sleep(5000);
      LOG.info(cluster.printServers());
      assertSuccess(success);

      final RaftLog leaderLog = cluster.getLeader().getState().getLog();
      for (RaftPeer newPeer : c1.newPeers) {
        Assert.assertArrayEquals(leaderLog.getEntries(0, Long.MAX_VALUE),
            cluster.getServer(newPeer.getId()).getState().getLog()
                .getEntries(0, Long.MAX_VALUE));
      }
    } finally {
      cluster.shutdown();
    }
  }

  /**
   * kill the leader before reconfiguration finishes. Make sure the client keeps
   * retrying.
   */
  @Test
  public void testKillLeaderDuringReconf() throws Exception {
    LOG.info("Start testKillLeaderDuringReconf");
    // originally 3 peers
    final MiniRaftCluster cluster = getCluster(3);
    cluster.start();
    try {
      RaftTestUtil.waitForLeader(cluster);
      final String leaderId = cluster.getLeader().getId();
      final RaftClient client = cluster.createClient("client", leaderId);

      PeerChanges c1 = cluster.addNewPeers(2, false);
      PeerChanges c2 = cluster.removePeers(2, false, asList(c1.newPeers));

      LOG.info("Start changing the configuration: {}",
          asList(c2.allPeersInNewConf));
      final AtomicReference<Boolean> success = new AtomicReference<>();
      final AtomicBoolean clientRunning = new AtomicBoolean(true);
      Thread clientThread = new Thread(() -> {
        try {
          boolean r = false;
          while (clientRunning.get() && !r) {
            r = client.setConfiguration(c2.allPeersInNewConf).isSuccess();
          }
          success.set(r);
          client.close();
        } catch (IOException ignored) {
        }
      });
      clientThread.start();

      // the leader cannot generate the (old, new) conf, and it will keep
      // bootstrapping the 2 new peers since they have not started yet
      LOG.info(cluster.printServers());
      Assert.assertFalse(cluster.getLeader().getRaftConf().isTransitional());

      // only the first empty entry got committed
      final long committedIndex = cluster.getLeader().getState().getLog()
          .getLastCommittedIndex();
      Assert.assertTrue("committedIndex is " + committedIndex,
          committedIndex <= 1);

      LOG.info("kill the current leader");
      final String oldLeaderId = RaftTestUtil.waitAndKillLeader(cluster, true);
      LOG.info("start the two new peers: {}", Arrays.asList(c1.newPeers));
      for (RaftPeer np : c1.newPeers) {
        cluster.startServer(np.getId());
      }

      Thread.sleep(3000);
      // the client should get the NotLeaderException from the first leader, and
      // will retry the same setConfiguration request
      waitAndCheckNewConf(cluster, c2.allPeersInNewConf, 2,
          Collections.singletonList(oldLeaderId));
      clientRunning.set(false);
      //Assert.assertTrue(success.get());
    } finally {
      cluster.shutdown();
    }
  }

  static void assertSuccess(final AtomicReference<Boolean> success) {
    final String s = "success=" + success;
    Assert.assertNotNull(s, success.get());
    Assert.assertTrue(s, success.get());
  }

  /**
   * When a request's new configuration is the same with the current one, make
   * sure we return success immediately and no log entry is recorded.
   */
  @Test
  public void testNoChangeRequest() throws Exception {
    LOG.info("Start testNoChangeRequest");
    // originally 3 peers
    final MiniRaftCluster cluster = getCluster(3);
    try {
      cluster.start();
      RaftTestUtil.waitForLeader(cluster);

      final String leaderId = cluster.getLeader().getId();
      final RaftClient client = cluster.createClient("client", leaderId);
      client.send(new SimpleMessage("m"));

      final long committedIndex = cluster.getLeader().getState().getLog()
          .getLastCommittedIndex();
      final RaftConfiguration confBefore = cluster.getLeader().getRaftConf();

      // no real configuration change in the request
      RaftClientReply reply = client.setConfiguration(cluster.getPeers()
          .toArray(new RaftPeer[0]));
      Assert.assertTrue(reply.isSuccess());
      Assert.assertEquals(committedIndex, cluster.getLeader().getState()
          .getLog().getLastCommittedIndex());
      Assert.assertSame(confBefore, cluster.getLeader().getRaftConf());
      client.close();
    } finally {
      cluster.shutdown();
    }
  }

  /**
   * Make sure a setConfiguration request is rejected if a configuration change
   * is still in progress (i.e., has not been committed yet).
   */
  @Test
  public void testOverlappedSetConfRequests() throws Exception {
    LOG.info("Start testOverlappedSetConfRequests");
    // originally 3 peers
    final MiniRaftCluster cluster = getCluster(3);
    try {
      cluster.start();
      RaftTestUtil.waitForLeader(cluster);

      final String leaderId = cluster.getLeader().getId();

      RaftPeer[] newPeers = cluster.addNewPeers(2, true).allPeersInNewConf;

      // delay every peer's logSync so that the setConf request is delayed
      cluster.getPeers()
          .forEach(peer -> logSyncDelay.setDelayMs(peer.getId(), 1000));

      final CountDownLatch latch = new CountDownLatch(1);
      final RaftPeer[] peersInRequest2 = cluster.getPeers().toArray(new RaftPeer[0]);
      AtomicBoolean caughtException = new AtomicBoolean(false);
      new Thread(() -> {
        try(final RaftClient client2 = cluster.createClient("client2", leaderId)) {
          latch.await();
          LOG.info("client2 starts to change conf");
          final RaftClientRequestSender sender2 = ((RaftClientImpl)client2).getRequestSender();
          sender2.sendRequest(new SetConfigurationRequest(
              "client2", leaderId, DEFAULT_SEQNUM, peersInRequest2));
        } catch (ReconfigurationInProgressException e) {
          caughtException.set(true);
        } catch (Exception e) {
          LOG.warn("Got unexpected exception when client2 changes conf", e);
        }
      }).start();

      AtomicBoolean confChanged = new AtomicBoolean(false);
      new Thread(() -> {
        try(final RaftClient client1 = cluster.createClient("client1", leaderId)) {
          LOG.info("client1 starts to change conf");
          confChanged.set(client1.setConfiguration(newPeers).isSuccess());
        } catch (IOException e) {
          LOG.warn("Got unexpected exception when client1 changes conf", e);
        }
      }).start();
      Thread.sleep(100);
      latch.countDown();

      for (int i = 0; i < 10 && !confChanged.get(); i++) {
        Thread.sleep(1000);
      }
      Assert.assertTrue(confChanged.get());
      Assert.assertTrue(caughtException.get());
    } finally {
      logSyncDelay.clear();
      cluster.shutdown();
    }
  }

  /**
   * Test a scenario where the follower truncates its log entries which causes
   * configuration change.
   */
  @Test
  public void testRevertConfigurationChange() throws Exception {
    LOG.info("Start testRevertConfigurationChange");
    // originally 3 peers
    final MiniRaftCluster cluster = getCluster(5);
    try {
      cluster.start();
      RaftTestUtil.waitForLeader(cluster);

      final String leaderId = cluster.getLeader().getId();

      final RaftLog log = cluster.getServer(leaderId).getState().getLog();
      Thread.sleep(1000);
      Assert.assertEquals(0, log.getLatestFlushedIndex());

      // we block the incoming msg for the leader and block its requests to
      // followers, so that we force the leader change and the old leader will
      // not know
      LOG.info("start blocking the leader");
      BlockRequestHandlingInjection.getInstance().blockReplier(leaderId);
      cluster.setBlockRequestsFrom(leaderId, true);

      PeerChanges change = cluster.removePeers(1, false, new ArrayList<>());

      AtomicBoolean gotNotLeader = new AtomicBoolean(false);
      new Thread(() -> {
        try(final RaftClient client = cluster.createClient("client1", leaderId)) {
          LOG.info("client starts to change conf");
          final RaftClientRequestSender sender = ((RaftClientImpl)client).getRequestSender();
          RaftClientReply reply = sender.sendRequest(new SetConfigurationRequest(
              "client", leaderId, DEFAULT_SEQNUM, change.allPeersInNewConf));
          if (reply.isNotLeader()) {
            gotNotLeader.set(true);
          }
        } catch (IOException e) {
          LOG.warn("Got unexpected exception when client1 changes conf", e);
        }
      }).start();

      // wait till the old leader persist the new conf
      for (int i = 0; i < 10 && log.getLatestFlushedIndex() < 1; i++) {
        Thread.sleep(500);
      }
      Assert.assertEquals(1, log.getLatestFlushedIndex());
      Assert.assertEquals(CONFIGURATIONENTRY,
          log.getLastEntry().getLogEntryBodyCase());

      // unblock the old leader
      BlockRequestHandlingInjection.getInstance().unblockReplier(leaderId);
      cluster.setBlockRequestsFrom(leaderId, false);

      // the client should get NotLeaderException
      for (int i = 0; i < 10 && !gotNotLeader.get(); i++) {
        Thread.sleep(500);
      }
      Assert.assertTrue(gotNotLeader.get());

      // the old leader should have truncated the setConf from the log
      boolean newState = false;
      for (int i = 0; i < 10 && !newState; i++) {
        Thread.sleep(500);
        newState = log.getLastCommittedIndex() == 1 &&
            log.getLastEntry().getLogEntryBodyCase() != CONFIGURATIONENTRY;
      }
      Assert.assertTrue(newState);
    } finally {
      cluster.shutdown();
    }
  }
}
