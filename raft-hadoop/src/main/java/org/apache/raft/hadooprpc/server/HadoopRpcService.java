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
package org.apache.raft.hadooprpc.server;

import com.google.common.base.Preconditions;
import com.google.protobuf.BlockingService;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.raft.hadooprpc.Proxy;
import org.apache.raft.hadooprpc.client.RaftClientProtocolPB;
import org.apache.raft.hadooprpc.client.RaftClientProtocolServerSideTranslatorPB;
import org.apache.raft.proto.RaftClientProtocolProtos.RaftClientProtocolService;
import org.apache.raft.proto.RaftProtos.*;
import org.apache.raft.proto.RaftServerProtocolProtos.RaftServerProtocolService;
import org.apache.raft.protocol.RaftPeer;
import org.apache.raft.server.*;
import org.apache.raft.util.CodeInjectionForTesting;
import org.apache.raft.util.PeerProxyMap;
import org.apache.raft.util.ProtoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

/** Server side Hadoop RPC service. */
public class HadoopRpcService implements RaftServerRpc {
  public static final Logger LOG = LoggerFactory.getLogger(HadoopRpcService.class);
  static final String CLASS_NAME = HadoopRpcService.class.getSimpleName();
  public static final String SEND_SERVER_REQUEST = CLASS_NAME + ".sendServerRequest";

  private final Configuration conf;
  private final RaftServerRpcService raftService;
  private final String id;
  private final RPC.Server ipcServer;
  private final InetSocketAddress ipcServerAddress;

  private final PeerProxyMap<Proxy<RaftServerProtocolPB>> proxies
      = new PeerProxyMap<Proxy<RaftServerProtocolPB>>() {
    @Override
    public Proxy<RaftServerProtocolPB> createProxy(RaftPeer p)
        throws IOException {
      return new Proxy(RaftServerProtocolPB.class, p.getAddress(), conf);
    }
  };

  public HadoopRpcService(RaftServer server, Configuration conf)
      throws IOException {
    this.conf = conf;
    this.raftService = new RaftServerRpcService(new RequestDispatcher(server));
    this.id = server.getId();
    this.ipcServer = newRpcServer(conf);
    this.ipcServerAddress = ipcServer.getListenerAddress();

    addRaftClientProtocol(conf);

    LOG.info(getClass().getSimpleName() + " created RPC.Server at "
        + ipcServerAddress);
  }

  @Override
  public InetSocketAddress getInetSocketAddress() {
    return ipcServerAddress;
  }

  private RPC.Server newRpcServer(Configuration conf) throws IOException {
    final RaftServerConfigKeys.Get get = new RaftServerConfigKeys.Get(conf);
    final int handlerCount = get.ipc().handlers();
    final InetSocketAddress address = get.ipc().address();

    final BlockingService service
        = RaftServerProtocolService.newReflectiveBlockingService(
            new RaftServerProtocolServerSideTranslatorPB(raftService));
    RPC.setProtocolEngine(conf, RaftServerProtocolPB.class, ProtobufRpcEngine.class);
    return new RPC.Builder(conf)
        .setProtocol(RaftServerProtocolPB.class)
        .setInstance(service)
        .setBindAddress(address.getHostName())
        .setPort(address.getPort())
        .setNumHandlers(handlerCount)
        .setVerbose(false)
        .build();
  }

  private void addRaftClientProtocol(Configuration conf) {
    final Class<?> protocol = RaftClientProtocolPB.class;
    RPC.setProtocolEngine(conf,protocol, ProtobufRpcEngine.class);

    final BlockingService service
        = RaftClientProtocolService.newReflectiveBlockingService(
        new RaftClientProtocolServerSideTranslatorPB(raftService));
    ipcServer.addProtocol(RPC.RpcKind.RPC_PROTOCOL_BUFFER, protocol, service);
  }

  @Override
  public void start() {
    ipcServer.start();
  }

  @Override
  public void shutdown() {
    ipcServer.stop();
  }

  @Override
  public AppendEntriesReplyProto sendAppendEntries(
      AppendEntriesRequestProto request) throws IOException {
    Preconditions.checkArgument(id.equals(request.getServerRequest().getRequestorId()));
    CodeInjectionForTesting.execute(SEND_SERVER_REQUEST, id, null, request);

    final RaftServerProtocolPB proxy = proxies.getProxy(
        request.getServerRequest().getReplyId()).getProtocol();
    try {
      return proxy.appendEntries(null, request);
    } catch (ServiceException se) {
      throw ProtoUtils.toIOException(se);
    }
  }

  @Override
  public InstallSnapshotReplyProto sendInstallSnapshot(
      InstallSnapshotRequestProto request) throws IOException {
    Preconditions.checkArgument(id.equals(request.getServerRequest().getRequestorId()));
    CodeInjectionForTesting.execute(SEND_SERVER_REQUEST, id, null, request);

    final RaftServerProtocolPB proxy = proxies.getProxy(
        request.getServerRequest().getReplyId()).getProtocol();
    try {
      return proxy.installSnapshot(null, request);
    } catch (ServiceException se) {
      throw ProtoUtils.toIOException(se);
    }
  }

  @Override
  public RequestVoteReplyProto sendRequestVote(
      RequestVoteRequestProto request) throws IOException {
    Preconditions.checkArgument(id.equals(request.getServerRequest().getRequestorId()));
    CodeInjectionForTesting.execute(SEND_SERVER_REQUEST, id, null, request);

    final RaftServerProtocolPB proxy = proxies.getProxy(
        request.getServerRequest().getReplyId()).getProtocol();
    try {
      return proxy.requestVote(null, request);
    } catch (ServiceException se) {
      throw ProtoUtils.toIOException(se);
    }
  }

  @Override
  public void addPeers(Iterable<RaftPeer> peers) {
    proxies.addPeers(peers);
  }
}
