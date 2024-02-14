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
package org.apache.ratis.client.impl;

import org.apache.ratis.client.DataStreamClient;
import org.apache.ratis.client.DataStreamClientRpc;
import org.apache.ratis.client.DataStreamOutputRpc;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.io.FilePositionCount;
import org.apache.ratis.proto.RaftProtos.DataStreamPacketHeaderProto.Type;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.ClientInvocationId;
import org.apache.ratis.protocol.DataStreamReply;
import org.apache.ratis.protocol.DataStreamRequestHeader;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.exceptions.AlreadyClosedException;
import org.apache.ratis.util.IOUtils;
import org.apache.ratis.protocol.*;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.MemoizedSupplier;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Streaming client implementation
 * allows client to create streams and send asynchronously.
 */
public class DataStreamClientImpl implements DataStreamClient {
  private final ClientId clientId;
  private final RaftGroupId groupId;

  private final RaftPeer dataStreamServer;
  private final DataStreamClientRpc dataStreamClientRpc;
  private final OrderedStreamAsync orderedStreamAsync;

  DataStreamClientImpl(ClientId clientId, RaftGroupId groupId, RaftPeer dataStreamServer,
      DataStreamClientRpc dataStreamClientRpc, RaftProperties properties) {
    this.clientId = clientId;
    this.groupId = groupId;
    this.dataStreamServer = dataStreamServer;
    this.dataStreamClientRpc = dataStreamClientRpc;
    this.orderedStreamAsync = new OrderedStreamAsync(clientId, dataStreamClientRpc, properties);
  }

  public final class DataStreamOutputImpl implements DataStreamOutputRpc {
    private final RaftClientRequest header;
    private final CompletableFuture<DataStreamReply> headerFuture;
    private final CompletableFuture<RaftClientReply> raftClientReplyFuture = new CompletableFuture<>();
    private final MemoizedSupplier<CompletableFuture<DataStreamReply>> closeSupplier = JavaUtils.memoize(() -> {
      final CompletableFuture<DataStreamReply> f = send(Type.STREAM_CLOSE);
      f.thenApply(ClientProtoUtils::getRaftClientReply).whenComplete(JavaUtils.asBiConsumer(raftClientReplyFuture));
      return f;
    });
    private final MemoizedSupplier<WritableByteChannel> writableByteChannelSupplier
        = JavaUtils.memoize(() -> new WritableByteChannel() {
      @Override
      public int write(ByteBuffer src) throws IOException {
        final int remaining = src.remaining();
        final DataStreamReply reply = IOUtils.getFromFuture(writeAsync(src),
            () -> "write(" + remaining + " bytes for " + ClientInvocationId.valueOf(header) + ")");
        return Math.toIntExact(reply.getBytesWritten());
      }

      @Override
      public boolean isOpen() {
        return !isClosed();
      }

      @Override
      public void close() throws IOException {
        IOUtils.getFromFuture(closeAsync(), () -> "close(" + ClientInvocationId.valueOf(header) + ")");
      }
    });

    private long streamOffset = 0;

    private DataStreamOutputImpl(RaftClientRequest request) {
      this.header = request;
      final ByteBuffer buffer = ClientProtoUtils.toRaftClientRequestProtoByteBuffer(header);
      this.headerFuture = send(Type.STREAM_HEADER, buffer, buffer.remaining());
    }

    private CompletableFuture<DataStreamReply> send(Type type, Object data, long length) {
      final DataStreamRequestHeader h = new DataStreamRequestHeader(type, header.getCallId(), streamOffset, length);
      return orderedStreamAsync.sendRequest(h, data);
    }

    private CompletableFuture<DataStreamReply> send(Type type) {
      return combineHeader(send(type, null, 0));
    }

    private CompletableFuture<DataStreamReply> combineHeader(CompletableFuture<DataStreamReply> future) {
      return future.thenCombine(headerFuture, (reply, headerReply) -> headerReply.isSuccess()? reply : headerReply);
    }

    private CompletableFuture<DataStreamReply> writeAsyncImpl(Object data, long length, boolean sync) {
      if (isClosed()) {
        return JavaUtils.completeExceptionally(new AlreadyClosedException(
            clientId + ": stream already closed, request=" + header));
      }
      final CompletableFuture<DataStreamReply> f = send(sync ? Type.STREAM_DATA_SYNC : Type.STREAM_DATA, data, length);
      streamOffset += length;
      return combineHeader(f);
    }

    @Override
    public CompletableFuture<DataStreamReply> writeAsync(ByteBuffer src, boolean sync) {
      return writeAsyncImpl(src, src.remaining(), sync);
    }

    @Override
    public CompletableFuture<DataStreamReply> writeAsync(FilePositionCount src, boolean sync) {
      return writeAsyncImpl(src, src.getCount(), sync);
    }

    @Override
    public CompletableFuture<DataStreamReply> closeAsync() {
      return closeSupplier.get();
    }

    boolean isClosed() {
      return closeSupplier.isInitialized();
    }

    public RaftClientRequest getHeader() {
      return header;
    }

    @Override
    public CompletableFuture<DataStreamReply> getHeaderFuture() {
      return headerFuture;
    }

    @Override
    public CompletableFuture<RaftClientReply> getRaftClientReplyFuture() {
      return raftClientReplyFuture;
    }

    @Override
    public WritableByteChannel getWritableByteChannel() {
      return writableByteChannelSupplier.get();
    }
  }

  @Override
  public DataStreamClientRpc getClientRpc() {
    return dataStreamClientRpc;
  }

  @Override
  public DataStreamOutputRpc stream() {
    final RaftClientRequest request = new RaftClientRequest(clientId, dataStreamServer.getId(), groupId,
        RaftClientImpl.nextCallId(), RaftClientRequest.dataStreamRequestType());
    return new DataStreamOutputImpl(request);
  }

  @Override
  public DataStreamOutputRpc stream(RaftClientRequest request) {
    return new DataStreamOutputImpl(request);
  }

  @Override
  public DataStreamOutputRpc stream(ByteBuffer headerMessage) {
    final Message message =
        Optional.ofNullable(headerMessage).map(ByteString::copyFrom).map(Message::valueOf).orElse(null);
    RaftClientRequest request = new RaftClientRequest(clientId, dataStreamServer.getId(), groupId,
        RaftClientImpl.nextCallId(), message, RaftClientRequest.dataStreamRequestType(), null);
    return new DataStreamOutputImpl(request);
  }

  @Override
  public void close() throws IOException {
    dataStreamClientRpc.close();
  }
}
