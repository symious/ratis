/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.raft.netty;

import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import org.apache.raft.netty.proto.NettyProtos.RaftNettyServerReplyProto;
import org.apache.raft.netty.proto.NettyProtos.RaftNettyServerRequestProto;
import org.apache.raft.proto.RaftProtos.RaftRpcRequestProto;
import org.apache.raft.protocol.RaftPeer;
import org.apache.raft.util.PeerProxyMap;
import org.apache.raft.util.ProtoUtils;
import org.apache.raft.util.RaftUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.raft.netty.proto.NettyProtos.RaftNettyServerReplyProto.RaftNettyServerReplyCase.EXCEPTIONREPLY;

public class NettyRpcProxy implements Closeable {
  public static class PeerMap extends PeerProxyMap<NettyRpcProxy> {
    private final EventLoopGroup group = new NioEventLoopGroup();

    @Override
    public NettyRpcProxy createProxyImpl(RaftPeer peer)
        throws IOException {
      try {
        return new NettyRpcProxy(peer, group);
      } catch (InterruptedException e) {
        throw RaftUtils.toInterruptedIOException("Failed connecting to " + peer, e);
      }
    }

    @Override
    public void close() {
      super.close();
      group.shutdownGracefully();
    }
  }

  public static long getSeqNum(RaftNettyServerReplyProto proto) {
    switch (proto.getRaftNettyServerReplyCase()) {
      case REQUESTVOTEREPLY:
        return proto.getRequestVoteReply().getServerReply().getSeqNum();
      case APPENDENTRIESREPLY:
        return proto.getAppendEntriesReply().getServerReply().getSeqNum();
      case INSTALLSNAPSHOTREPLY:
        return proto.getInstallSnapshotReply().getServerReply().getSeqNum();
      case RAFTCLIENTREPLY:
        return proto.getRaftClientReply().getRpcReply().getSeqNum();
      case EXCEPTIONREPLY:
        return proto.getExceptionReply().getRpcReply().getSeqNum();
      case RAFTNETTYSERVERREPLY_NOT_SET:
        throw new IllegalArgumentException("Reply case not set in proto: "
            + proto.getRaftNettyServerReplyCase());
      default:
        throw new UnsupportedOperationException("Reply case not supported: "
            + proto.getRaftNettyServerReplyCase());
    }
  }


  class Connection implements Closeable {
    private final NettyClient client = new NettyClient();
    private final Queue<CompletableFuture<RaftNettyServerReplyProto>> replies
        = new LinkedList<>();

    Connection(EventLoopGroup group) throws InterruptedException {
      final ChannelInboundHandler inboundHandler
          = new SimpleChannelInboundHandler<RaftNettyServerReplyProto>() {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx,
                                    RaftNettyServerReplyProto proto) {
          final CompletableFuture<RaftNettyServerReplyProto> future = pollReply();
          if (future == null) {
            throw new IllegalStateException("Request #" + getSeqNum(proto)
                + " not found");
          }
          if (proto.getRaftNettyServerReplyCase() == EXCEPTIONREPLY) {
            final Object ioe = ProtoUtils.toObject(proto.getExceptionReply().getException());
            future.completeExceptionally((IOException)ioe);
          } else {
            future.complete(proto);
          }
        }
      };
      final ChannelInitializer<SocketChannel> initializer
          = new ChannelInitializer<SocketChannel>() {
        @Override
        protected void initChannel(SocketChannel ch) throws Exception {
          final ChannelPipeline p = ch.pipeline();

          p.addLast(new ProtobufVarint32FrameDecoder());
          p.addLast(new ProtobufDecoder(RaftNettyServerReplyProto.getDefaultInstance()));
          p.addLast(new ProtobufVarint32LengthFieldPrepender());
          p.addLast(new ProtobufEncoder());

          p.addLast(inboundHandler);
        }
      };

      client.connect(peer.getAddress(), group, initializer);
    }

    synchronized ChannelFuture offer(RaftNettyServerRequestProto request,
        CompletableFuture<RaftNettyServerReplyProto> reply) {
      replies.offer(reply);
      return client.writeAndFlush(request);
    }

    synchronized CompletableFuture<RaftNettyServerReplyProto> pollReply() {
      return replies.poll();
    }

    @Override
    public synchronized void close() {
      client.close();
      if (!replies.isEmpty()) {
        final IOException e = new IOException("Connection to " + peer + " is closed.");
        replies.stream().forEach(f -> f.completeExceptionally(e));
        replies.clear();
      }
    }
  }

  private final RaftPeer peer;
  private final Connection connection;

  public NettyRpcProxy(RaftPeer peer, EventLoopGroup group) throws InterruptedException {
    this.peer = peer;
    this.connection = new Connection(group);
  }

  @Override
  public void close() {
    connection.close();
  }

  public RaftNettyServerReplyProto send(
      RaftRpcRequestProto request, RaftNettyServerRequestProto proto)
      throws IOException {
    final CompletableFuture<RaftNettyServerReplyProto> reply = new CompletableFuture<>();
    final ChannelFuture channelFuture = connection.offer(proto, reply);

    try {
      channelFuture.sync();
      return reply.get();
    } catch (InterruptedException e) {
      throw RaftUtils.toInterruptedIOException(ProtoUtils.toString(request)
          + " sending from " + peer + " is interrupted.", e);
    } catch (ExecutionException e) {
      throw RaftUtils.toIOException(e);
    }
  }
}
