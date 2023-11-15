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
package org.apache.raft;

import org.apache.hadoop.fs.FileUtil;
import org.apache.raft.client.RaftClient;
import org.apache.raft.client.RaftClientRequestSender;
import org.apache.raft.conf.RaftProperties;
import org.apache.raft.protocol.RaftPeer;
import org.apache.raft.server.RaftConfiguration;
import org.apache.raft.server.RaftServerConstants;
import org.apache.raft.server.RaftServer;
import org.apache.raft.server.RaftServerConfigKeys;
import org.apache.raft.server.storage.MemoryRaftLog;
import org.apache.raft.server.storage.RaftLog;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public abstract class MiniRaftCluster {

  static final Logger LOG = LoggerFactory.getLogger(MiniRaftCluster.class);

  public static class PeerChanges {
    public final RaftPeer[] allPeersInNewConf;
    public final RaftPeer[] newPeers;
    public final RaftPeer[] removedPeers;

    public PeerChanges(RaftPeer[] all, RaftPeer[] newPeers, RaftPeer[] removed) {
      this.allPeersInNewConf = all;
      this.newPeers = newPeers;
      this.removedPeers = removed;
    }
  }

  public static RaftConfiguration initConfiguration(int num) {
    RaftPeer[] peers = new RaftPeer[num];
    for (int i = 0; i < num; i++) {
      peers[i] = new RaftPeer("s" + i);
    }
    return RaftConfiguration.composeConf(peers, 0);
  }

  private static String getBaseDirectory() {
    return System.getProperty("test.build.data", "target/test/data") + "/raft/";
  }

  private static void formatDir(String dirStr) {
    final File serverDir = new File(dirStr);
    FileUtil.fullyDelete(serverDir);
  }

  private RaftConfiguration conf;
  private final RaftProperties properties;
  private final String testBaseDir;
  private final Map<String, RaftServer> servers = new LinkedHashMap<>();

  public MiniRaftCluster(int numServers, RaftProperties properties,
      boolean formatted) {
    this.conf = initConfiguration(numServers);
    this.properties = properties;
    this.testBaseDir = getBaseDirectory();

    for (RaftPeer p : conf.getPeers()) {
      final RaftServer s = newRaftServer(p.getId(), conf, formatted);
      servers.put(p.getId(), s);
    }
  }

  public void start() {
    servers.values().forEach((server) -> server.start(conf));
  }

  public RaftConfiguration getConf() {
    return conf;
  }

  private RaftServer newRaftServer(String id, RaftConfiguration conf,
      boolean format) {
    final RaftServer s;
    try {
      final String dirStr = testBaseDir + id;
      if (format) {
        formatDir(dirStr);
      }
      properties.set(RaftServerConfigKeys.RAFT_SERVER_STORAGE_DIR_KEY, dirStr);
      s = new RaftServer(id, conf, properties);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return s;
  }

  public abstract RaftClientRequestSender getRaftClientRequestSender()
      throws IOException;

  public abstract void addNewPeers(Collection<RaftPeer> newPeers,
                                   Collection<RaftServer> newServers)
      throws IOException;

  public PeerChanges addNewPeers(int number, boolean startNewPeer)
      throws IOException {
    List<RaftPeer> newPeers = new ArrayList<>(number);
    final int oldSize = conf.getPeers().size();
    for (int i = oldSize; i < oldSize + number; i++) {
      newPeers.add(new RaftPeer("s" + i));
    }


    // create and add new RaftServers
    final List<RaftServer> newServers = new ArrayList<>(number);
    for (RaftPeer p : newPeers) {
      RaftServer newServer = newRaftServer(p.getId(), conf, true);
      servers.put(p.getId(), newServer);
      newServers.add(newServer);
    }

    addNewPeers(newPeers, newServers);

    if (startNewPeer) {
      newServers.forEach(s -> s.start(null));
    }

    final RaftPeer[] np = newPeers.toArray(new RaftPeer[newPeers.size()]);
    newPeers.addAll(conf.getPeers());
    RaftPeer[] p = newPeers.toArray(new RaftPeer[newPeers.size()]);
    conf = RaftConfiguration.composeConf(p, 0);
    return new PeerChanges(p, np, new RaftPeer[0]);
  }

  public void startServer(String id, RaftConfiguration conf) {
    RaftServer server = servers.get(id);
    assert server != null;
    server.start(conf);
  }

  /**
   * prepare the peer list when removing some peers from the conf
   */
  public PeerChanges removePeers(int number, boolean removeLeader,
      Collection<RaftPeer> excluded) {
    Collection<RaftPeer> peers = new ArrayList<>(conf.getPeers());
    List<RaftPeer> removedPeers = new ArrayList<>(number);
    if (removeLeader) {
      final RaftPeer leader = new RaftPeer(getLeader().getId());
      assert !excluded.contains(leader);
      peers.remove(leader);
      removedPeers.add(leader);
    }
    List<RaftServer> followers = getFollowers();
    for (int i = 0, removed = 0; i < followers.size() &&
        removed < (removeLeader ? number - 1 : number); i++) {
      RaftPeer toRemove = new RaftPeer(followers.get(i).getId());
      if (!excluded.contains(toRemove)) {
        peers.remove(toRemove);
        removedPeers.add(toRemove);
        removed++;
      }
    }
    RaftPeer[] p = peers.toArray(new RaftPeer[peers.size()]);
    conf = RaftConfiguration.composeConf(p, 0);
    return new PeerChanges(p, new RaftPeer[0],
        removedPeers.toArray(new RaftPeer[removedPeers.size()]));
  }

  public void killServer(String id) {
    servers.get(id).kill();
  }

  public String printServers() {
    StringBuilder b = new StringBuilder("\n#servers = " + servers.size() + "\n");
    for (RaftServer s : servers.values()) {
      b.append("  ");
      b.append(s).append("\n");
    }
    return b.toString();
  }

  public String printAllLogs() {
    StringBuilder b = new StringBuilder("\n#servers = " + servers.size() + "\n");
    for (RaftServer s : servers.values()) {
      b.append("  ");
      b.append(s).append("\n");

      final RaftLog log = s.getState().getLog();
      if (log instanceof MemoryRaftLog) {
        b.append("    ");
        b.append(((MemoryRaftLog) log).getEntryString());
      }
    }
    return b.toString();
  }

  public RaftServer getLeader() {
    final List<RaftServer> leaders = new ArrayList<>();
    for(RaftServer s : servers.values()) {
      if (s.isRunning() && s.isLeader()) {
        if (leaders.isEmpty()) {
          leaders.add(s);
        } else {
          final long leaderTerm = leaders.get(0).getState().getCurrentTerm();
          final long term = s.getState().getCurrentTerm();
          if (term >= leaderTerm) {
            if (term > leaderTerm) {
              leaders.clear();
            }
            leaders.add(s);
          }
        }
      }
    }
    if (leaders.isEmpty()) {
      return null;
    } else if (leaders.size() != 1) {
      Assert.fail(printServers() + leaders.toString()
          + "leaders.size() = " + leaders.size() + " != 1");
    }
    return leaders.get(0);
  }

  public boolean isLeader(String leaderId) throws InterruptedException {
    final RaftServer leader = getLeader();
    return leader != null && leader.getId().equals(leaderId);
  }

  public List<RaftServer> getFollowers() {
    return servers.values().stream()
        .filter(s -> s.isRunning() && s.isFollower())
        .collect(Collectors.toList());
  }

  public Collection<RaftServer> getServers() {
    return servers.values();
  }

  public RaftServer getServer(String id) {
    return servers.get(id);
  }

  public Collection<RaftPeer> getPeers() {
    return getServers().stream().map(s ->
        new RaftPeer(s.getId(), s.getServerRpc().getInetSocketAddress()))
        .collect(Collectors.toList());
  }

  public RaftClient createClient(String clientId, String leaderId)
      throws IOException {
    return new RaftClient(clientId, conf.getPeers(),
        getRaftClientRequestSender(), leaderId);
  }

  public void shutdown() {
    servers.values().stream().filter(RaftServer::isRunning)
        .forEach(RaftServer::kill);
  }

  public abstract void blockQueueAndSetDelay(String leaderId, int delayMs)
      throws InterruptedException;

  /**
   * Try to enforce the leader of the cluster.
   * @param leaderId ID of the targeted leader server.
   * @return true if server has been successfully enforced to the leader, false
   *         otherwise.
   */
  public boolean tryEnforceLeader(String leaderId) throws InterruptedException {
    // do nothing and see if the given id is already a leader.
    if (isLeader(leaderId)) {
      return true;
    }

    // Blocking all other server's RPC read process to make sure a read takes at
    // least ELECTION_TIMEOUT_MIN. In this way when the target leader request a
    // vote, all non-leader servers can grant the vote.
    // Disable the target leader server RPC so that it can request a vote.
    blockQueueAndSetDelay(leaderId, RaftServerConstants.ELECTION_TIMEOUT_MIN_MS);

    // Reopen queues so that the vote can make progress.
    blockQueueAndSetDelay(leaderId, 0);

    return isLeader(leaderId);
  }

  /** Block/unblock the requests sent from the given source. */
  public abstract void setBlockRequestsFrom(String src, boolean block);
}
