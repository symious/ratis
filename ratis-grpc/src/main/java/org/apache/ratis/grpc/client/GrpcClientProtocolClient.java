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
package org.apache.ratis.grpc.client;

import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.client.impl.ClientProtoUtils;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.grpc.GrpcTlsConfig;
import org.apache.ratis.grpc.GrpcUtil;
import org.apache.ratis.proto.RaftProtos.GroupInfoReplyProto;
import org.apache.ratis.proto.RaftProtos.GroupInfoRequestProto;
import org.apache.ratis.proto.RaftProtos.GroupListReplyProto;
import org.apache.ratis.proto.RaftProtos.GroupListRequestProto;
import org.apache.ratis.proto.RaftProtos.GroupManagementRequestProto;
import org.apache.ratis.proto.RaftProtos.RaftClientReplyProto;
import org.apache.ratis.proto.RaftProtos.RaftClientRequestProto;
import org.apache.ratis.proto.RaftProtos.SetConfigurationRequestProto;
import org.apache.ratis.protocol.LeaderNotReadyException;
import org.apache.ratis.protocol.TimeoutIOException;
import org.apache.ratis.thirdparty.io.grpc.netty.GrpcSslContexts;
import org.apache.ratis.thirdparty.io.netty.handler.ssl.SslContextBuilder;
import org.apache.ratis.proto.grpc.AdminProtocolServiceGrpc;
import org.apache.ratis.proto.grpc.AdminProtocolServiceGrpc.AdminProtocolServiceBlockingStub;
import org.apache.ratis.proto.grpc.RaftClientProtocolServiceGrpc;
import org.apache.ratis.proto.grpc.RaftClientProtocolServiceGrpc.RaftClientProtocolServiceBlockingStub;
import org.apache.ratis.proto.grpc.RaftClientProtocolServiceGrpc.RaftClientProtocolServiceStub;
import org.apache.ratis.protocol.AlreadyClosedException;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.NotLeaderException;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.thirdparty.io.grpc.ManagedChannel;
import org.apache.ratis.thirdparty.io.grpc.StatusRuntimeException;
import org.apache.ratis.thirdparty.io.grpc.netty.NegotiationType;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyChannelBuilder;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.apache.ratis.util.CollectionUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.TimeoutScheduler;
import org.apache.ratis.util.function.CheckedSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class GrpcClientProtocolClient implements Closeable {
  public static final Logger LOG = LoggerFactory.getLogger(GrpcClientProtocolClient.class);

  private final Supplier<String> name;
  private final RaftPeer target;
  private final ManagedChannel channel;

  private final TimeDuration requestTimeoutDuration;
  private final TimeDuration watchRequestTimeoutDuration;
  private final TimeoutScheduler scheduler = TimeoutScheduler.getInstance();

  private final RaftClientProtocolServiceBlockingStub blockingStub;
  private final RaftClientProtocolServiceStub asyncStub;
  private final AdminProtocolServiceBlockingStub adminBlockingStub;

  private final AtomicReference<AsyncStreamObservers> orderedStreamObservers = new AtomicReference<>();

  private final AtomicReference<AsyncStreamObservers> unorderedStreamObservers = new AtomicReference<>();

  GrpcClientProtocolClient(ClientId id, RaftPeer target, RaftProperties properties, GrpcTlsConfig tlsConf) {
    this.name = JavaUtils.memoize(() -> id + "->" + target.getId());
    this.target = target;
    final SizeInBytes flowControlWindow = GrpcConfigKeys.flowControlWindow(properties, LOG::debug);
    final SizeInBytes maxMessageSize = GrpcConfigKeys.messageSizeMax(properties, LOG::debug);
    NettyChannelBuilder channelBuilder =
        NettyChannelBuilder.forTarget(target.getAddress());

    if (tlsConf!= null) {
      SslContextBuilder sslContextBuilder = GrpcSslContexts.forClient();
      if (tlsConf.isFileBasedConfig()) {
        sslContextBuilder.trustManager(tlsConf.getTrustStoreFile());
      } else {
        sslContextBuilder.trustManager(tlsConf.getTrustStore());
      }
      if (tlsConf.getMtlsEnabled()) {
        if (tlsConf.isFileBasedConfig()) {
          sslContextBuilder.keyManager(tlsConf.getCertChainFile(),
              tlsConf.getPrivateKeyFile());
        } else {
          sslContextBuilder.keyManager(tlsConf.getPrivateKey(),
              tlsConf.getCertChain());
        }
      }
      try {
        channelBuilder.useTransportSecurity().sslContext(
            sslContextBuilder.build());
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    } else {
      channelBuilder.negotiationType(NegotiationType.PLAINTEXT);
    }
    channel = channelBuilder.flowControlWindow(flowControlWindow.getSizeInt())
        .maxInboundMessageSize(maxMessageSize.getSizeInt())
        .build();
    blockingStub = RaftClientProtocolServiceGrpc.newBlockingStub(channel);
    asyncStub = RaftClientProtocolServiceGrpc.newStub(channel);
    adminBlockingStub = AdminProtocolServiceGrpc.newBlockingStub(channel);
    this.requestTimeoutDuration = RaftClientConfigKeys.Rpc.requestTimeout(properties);
    this.watchRequestTimeoutDuration =
        RaftClientConfigKeys.Rpc.watchRequestTimeout(properties);
  }

  String getName() {
    return name.get();
  }

  @Override
  public void close() {
    Optional.ofNullable(orderedStreamObservers.getAndSet(null)).ifPresent(AsyncStreamObservers::close);
    Optional.ofNullable(unorderedStreamObservers.getAndSet(null)).ifPresent(AsyncStreamObservers::close);
    channel.shutdown();
    try {
      channel.awaitTermination(5, TimeUnit.SECONDS);
    } catch (Exception e) {
      LOG.error("Unexpected exception while waiting for channel termination", e);
    }
    scheduler.close();
  }

  RaftClientReplyProto groupAdd(GroupManagementRequestProto request) throws IOException {
    return blockingCall(() -> adminBlockingStub
        .withDeadlineAfter(requestTimeoutDuration.getDuration(), requestTimeoutDuration.getUnit())
        .groupManagement(request));
  }

  GroupListReplyProto groupList(GroupListRequestProto request) {
    return adminBlockingStub
        .withDeadlineAfter(requestTimeoutDuration.getDuration(), requestTimeoutDuration.getUnit())
        .groupList(request);
  }

  GroupInfoReplyProto groupInfo(GroupInfoRequestProto request) {
    return adminBlockingStub
        .withDeadlineAfter(requestTimeoutDuration.getDuration(), requestTimeoutDuration.getUnit())
        .groupInfo(request);
  }


  RaftClientReplyProto setConfiguration(
      SetConfigurationRequestProto request) throws IOException {
    return blockingCall(() -> blockingStub
        .withDeadlineAfter(requestTimeoutDuration.getDuration(), requestTimeoutDuration.getUnit())
        .setConfiguration(request));
  }

  private static RaftClientReplyProto blockingCall(
      CheckedSupplier<RaftClientReplyProto, StatusRuntimeException> supplier
      ) throws IOException {
    try {
      return supplier.get();
    } catch (StatusRuntimeException e) {
      throw GrpcUtil.unwrapException(e);
    }
  }

  StreamObserver<RaftClientRequestProto> ordered(StreamObserver<RaftClientReplyProto> responseHandler) {
    return asyncStub.ordered(responseHandler);
  }

  StreamObserver<RaftClientRequestProto> orderedWithTimeout(StreamObserver<RaftClientReplyProto> responseHandler) {
    return asyncStub.withDeadlineAfter(requestTimeoutDuration.getDuration(), requestTimeoutDuration.getUnit())
        .unordered(responseHandler);
  }

  AsyncStreamObservers getOrderedStreamObservers() {
    return orderedStreamObservers.updateAndGet(
        a -> a != null? a : new AsyncStreamObservers(this::ordered));
  }

  AsyncStreamObservers getUnorderedAsyncStreamObservers() {
    return unorderedStreamObservers.updateAndGet(
        a -> a != null? a : new AsyncStreamObservers(asyncStub::unordered));
  }

  public RaftPeer getTarget() {
    return target;
  }

  class ReplyMap {
    private final AtomicReference<Map<Long, CompletableFuture<RaftClientReply>>> map
        = new AtomicReference<>(new ConcurrentHashMap<>());

    // synchronized to avoid putNew after getAndSetNull
    synchronized CompletableFuture<RaftClientReply> putNew(long callId) {
      return Optional.ofNullable(map.get())
          .map(m -> CollectionUtils.putNew(callId, new CompletableFuture<>(), m, this::toString))
          .orElse(null);
    }

    Optional<CompletableFuture<RaftClientReply>> remove(long callId) {
      return Optional.ofNullable(map.get()).map(m -> m.remove(callId));
    }

    // synchronized to avoid putNew after getAndSetNull
    synchronized Map<Long, CompletableFuture<RaftClientReply>> getAndSetNull() {
      return map.getAndSet(null);
    }

    @Override
    public String toString() {
      return getName() + ":" + getClass().getSimpleName();
    }
  }

  static class RequestStreamer {
    private final AtomicReference<StreamObserver<RaftClientRequestProto>> streamObserver;

    RequestStreamer(StreamObserver<RaftClientRequestProto> streamObserver) {
      this.streamObserver = new AtomicReference<>(streamObserver);
    }

    synchronized boolean onNext(RaftClientRequestProto request) {
      final StreamObserver<RaftClientRequestProto> s = streamObserver.get();
      if (s != null) {
        s.onNext(request);
        return true;
      }
      return false;
    }

    synchronized void onCompleted() {
      final StreamObserver<RaftClientRequestProto> s = streamObserver.getAndSet(null);
      if (s != null) {
        s.onCompleted();
      }
    }
  }

  class AsyncStreamObservers {
    /** Request map: callId -> future */
    private final ReplyMap replies = new ReplyMap();

    private final StreamObserver<RaftClientReplyProto> replyStreamObserver
        = new StreamObserver<RaftClientReplyProto>() {
      @Override
      public void onNext(RaftClientReplyProto proto) {
        final long callId = proto.getRpcReply().getCallId();
        try {
          final RaftClientReply reply = ClientProtoUtils.toRaftClientReply(proto);
          LOG.trace("{}: receive {}", getName(), reply);
          final NotLeaderException nle = reply.getNotLeaderException();
          if (nle != null) {
            completeReplyExceptionally(nle, NotLeaderException.class.getName());
            return;
          }
          final LeaderNotReadyException lnre = reply.getLeaderNotReadyException();
          if (lnre != null) {
            completeReplyExceptionally(lnre, LeaderNotReadyException.class.getName());
            return;
          }
          handleReplyFuture(callId, f -> f.complete(reply));
        } catch (Throwable t) {
          handleReplyFuture(callId, f -> f.completeExceptionally(t));
        }
      }

      @Override
      public void onError(Throwable t) {
        final IOException ioe = GrpcUtil.unwrapIOException(t);
        completeReplyExceptionally(ioe, "onError");
      }

      @Override
      public void onCompleted() {
        completeReplyExceptionally(null, "completed");
      }
    };
    private final RequestStreamer requestStreamer;

    AsyncStreamObservers(Function<StreamObserver<RaftClientReplyProto>, StreamObserver<RaftClientRequestProto>> f) {
      this.requestStreamer = new RequestStreamer(f.apply(replyStreamObserver));
    }

    CompletableFuture<RaftClientReply> onNext(RaftClientRequest request) {
      final long callId = request.getCallId();
      final CompletableFuture<RaftClientReply> f = replies.putNew(callId);
      if (f == null) {
        return JavaUtils.completeExceptionally(new AlreadyClosedException(getName() + " is closed."));
      }
      try {
        if (!requestStreamer.onNext(ClientProtoUtils.toRaftClientRequestProto(request))) {
          return JavaUtils.completeExceptionally(new AlreadyClosedException(getName() + ": the stream is closed."));
        }
      } catch(Throwable t) {
        handleReplyFuture(request.getCallId(), future -> future.completeExceptionally(t));
        return f;
      }

      if (RaftClientRequestProto.TypeCase.WATCH.equals(request.getType().getTypeCase())) {
        scheduler.onTimeout(watchRequestTimeoutDuration, () ->
                timeoutCheck(callId, watchRequestTimeoutDuration), LOG,
            () -> "Timeout check failed for client request #" + callId);
      } else {
        scheduler.onTimeout(requestTimeoutDuration,
            () -> timeoutCheck(callId, requestTimeoutDuration), LOG,
            () -> "Timeout check failed for client request #" + callId);
      }
      return f;
    }

    private void timeoutCheck(long callId, TimeDuration timeOutDuration) {
      handleReplyFuture(callId, f -> f.completeExceptionally(
          new TimeoutIOException("Request #" + callId + " timeout " + timeOutDuration)));
    }

    private void handleReplyFuture(long callId, Consumer<CompletableFuture<RaftClientReply>> handler) {
      replies.remove(callId).ifPresent(handler);
    }

    private void close() {
      requestStreamer.onCompleted();
      completeReplyExceptionally(null, "close");
    }

    private void completeReplyExceptionally(Throwable t, String event) {
      final Map<Long, CompletableFuture<RaftClientReply>> map = replies.getAndSetNull();
      if (map == null) {
        return;
      }
      for (Map.Entry<Long, CompletableFuture<RaftClientReply>> entry : map.entrySet()) {
        final CompletableFuture<RaftClientReply> f = entry.getValue();
        if (!f.isDone()) {
          f.completeExceptionally(t != null? t
              : new AlreadyClosedException(getName() + ": Stream " + event
                  + ": no reply for async request cid=" + entry.getKey()));
        }
      }
    }
  }
}
