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

import java.nio.ByteBuffer;
import java.util.Optional;

import org.apache.ratis.datastream.impl.DataStreamReplyByteBuffer;
import org.apache.ratis.protocol.*;
import org.apache.ratis.protocol.exceptions.AlreadyClosedException;
import org.apache.ratis.protocol.exceptions.DataStreamException;
import org.apache.ratis.protocol.exceptions.LeaderNotReadyException;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.protocol.exceptions.NotReplicatedException;
import org.apache.ratis.protocol.exceptions.RaftException;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.proto.RaftProtos.*;
import org.apache.ratis.util.ProtoUtils;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.ratis.proto.RaftProtos.RaftClientReplyProto.ExceptionDetailsCase.ALREADYCLOSEDEXCEPTION;
import static org.apache.ratis.proto.RaftProtos.RaftClientReplyProto.ExceptionDetailsCase.DATASTREAMEXCEPTION;
import static org.apache.ratis.proto.RaftProtos.RaftClientReplyProto.ExceptionDetailsCase.LEADERNOTREADYEXCEPTION;
import static org.apache.ratis.proto.RaftProtos.RaftClientReplyProto.ExceptionDetailsCase.NOTLEADEREXCEPTION;
import static org.apache.ratis.proto.RaftProtos.RaftClientReplyProto.ExceptionDetailsCase.NOTREPLICATEDEXCEPTION;
import static org.apache.ratis.proto.RaftProtos.RaftClientReplyProto.ExceptionDetailsCase.STATEMACHINEEXCEPTION;

public interface ClientProtoUtils {

  static RaftRpcReplyProto.Builder toRaftRpcReplyProtoBuilder(
      ByteString requestorId, ByteString replyId, RaftGroupId groupId,
      long callId, boolean success) {
    return RaftRpcReplyProto.newBuilder()
        .setRequestorId(requestorId)
        .setReplyId(replyId)
        .setRaftGroupId(ProtoUtils.toRaftGroupIdProtoBuilder(groupId))
        .setCallId(callId)
        .setSuccess(success);
  }

  static RaftRpcRequestProto.Builder toRaftRpcRequestProtoBuilder(
      ByteString requesterId, ByteString replyId, RaftGroupId groupId, long callId,
      SlidingWindowEntry slidingWindowEntry) {
    if (slidingWindowEntry == null) {
      slidingWindowEntry = SlidingWindowEntry.getDefaultInstance();
    }
    return RaftRpcRequestProto.newBuilder()
        .setRequestorId(requesterId)
        .setReplyId(replyId)
        .setRaftGroupId(ProtoUtils.toRaftGroupIdProtoBuilder(groupId))
        .setCallId(callId)
        .setSlidingWindowEntry(slidingWindowEntry);
  }

  static RaftRpcRequestProto.Builder toRaftRpcRequestProtoBuilder(
      ClientId requesterId, RaftPeerId replyId, RaftGroupId groupId, long callId,
      SlidingWindowEntry slidingWindowEntry) {
    return toRaftRpcRequestProtoBuilder(
        requesterId.toByteString(), replyId.toByteString(), groupId, callId, slidingWindowEntry);
  }

  static RaftRpcRequestProto.Builder toRaftRpcRequestProtoBuilder(
      RaftClientRequest request) {
    return toRaftRpcRequestProtoBuilder(
        request.getClientId(),
        request.getServerId(),
        request.getRaftGroupId(),
        request.getCallId(),
        request.getSlidingWindowEntry());
  }

  static RaftClientRequest.Type toRaftClientRequestType(RaftClientRequestProto p) {
    switch (p.getTypeCase()) {
      case WRITE:
        return RaftClientRequest.Type.valueOf(p.getWrite());
      case DATASTREAM:
        return RaftClientRequest.Type.valueOf(p.getDataStream());
      case FORWARD:
        return RaftClientRequest.Type.valueOf(p.getForward());
      case MESSAGESTREAM:
        return RaftClientRequest.Type.valueOf(p.getMessageStream());
      case READ:
        return RaftClientRequest.Type.valueOf(p.getRead());
      case STALEREAD:
        return RaftClientRequest.Type.valueOf(p.getStaleRead());
      case WATCH:
        return RaftClientRequest.Type.valueOf(p.getWatch());
      default:
        throw new IllegalArgumentException("Unexpected request type: " + p.getTypeCase()
            + " in request proto " + p);
    }
  }

  static RaftClientRequest toRaftClientRequest(RaftClientRequestProto p) {
    final RaftClientRequest.Type type = toRaftClientRequestType(p);
    final RaftRpcRequestProto request = p.getRpcRequest();
    return new RaftClientRequest(
        ClientId.valueOf(request.getRequestorId()),
        RaftPeerId.valueOf(request.getReplyId()),
        ProtoUtils.toRaftGroupId(request.getRaftGroupId()),
        request.getCallId(),
        toMessage(p.getMessage()),
        type,
        request.getSlidingWindowEntry());
  }

  static ByteBuffer toRaftClientRequestProtoByteBuffer(RaftClientRequest request) {
    return toRaftClientRequestProto(request).toByteString().asReadOnlyByteBuffer();
  }

  static RaftClientRequestProto toRaftClientRequestProto(RaftClientRequest request) {
    final RaftClientRequestProto.Builder b = RaftClientRequestProto.newBuilder()
        .setRpcRequest(toRaftRpcRequestProtoBuilder(request));
    if (request.getMessage() != null) {
      b.setMessage(toClientMessageEntryProtoBuilder(request.getMessage()));
    }

    final RaftClientRequest.Type type = request.getType();
    switch (type.getTypeCase()) {
      case WRITE:
        b.setWrite(type.getWrite());
        break;
      case DATASTREAM:
        b.setDataStream(type.getDataStream());
        break;
      case FORWARD:
        b.setForward(type.getForward());
        break;
      case MESSAGESTREAM:
        b.setMessageStream(type.getMessageStream());
        break;
      case READ:
        b.setRead(type.getRead());
        break;
      case STALEREAD:
        b.setStaleRead(type.getStaleRead());
        break;
      case WATCH:
        b.setWatch(type.getWatch());
        break;
      default:
        throw new IllegalArgumentException("Unexpected request type: " + request.getType()
            + " in request " + request);
    }

    return b.build();
  }

  static RaftClientRequestProto toRaftClientRequestProto(
      ClientId clientId, RaftPeerId serverId, RaftGroupId groupId, long callId,
      long seqNum, ByteString content) {
    return RaftClientRequestProto.newBuilder()
        .setRpcRequest(toRaftRpcRequestProtoBuilder(
            clientId, serverId, groupId, callId, ProtoUtils.toSlidingWindowEntry(seqNum, false)))
        .setWrite(WriteRequestTypeProto.getDefaultInstance())
        .setMessage(toClientMessageEntryProtoBuilder(content))
        .build();
  }

  static RaftClientReplyProto toRaftClientReplyProto(RaftClientReply reply) {
    final RaftClientReplyProto.Builder b = RaftClientReplyProto.newBuilder();
    if (reply != null) {
      b.setRpcReply(toRaftRpcReplyProtoBuilder(reply.getClientId().toByteString(),
          reply.getServerId().toByteString(), reply.getRaftGroupId(),
          reply.getCallId(), reply.isSuccess()));
      b.setLogIndex(reply.getLogIndex());
      if (reply.getMessage() != null) {
        b.setMessage(toClientMessageEntryProtoBuilder(reply.getMessage()));
      }
      ProtoUtils.addCommitInfos(reply.getCommitInfos(), b::addCommitInfos);

      final NotLeaderException nle = reply.getNotLeaderException();
      if (nle != null) {
        NotLeaderExceptionProto.Builder nleBuilder =
            NotLeaderExceptionProto.newBuilder();
        final RaftPeer suggestedLeader = nle.getSuggestedLeader();
        if (suggestedLeader != null) {
          nleBuilder.setSuggestedLeader(suggestedLeader.getRaftPeerProto());
        }
        nleBuilder.addAllPeersInConf(ProtoUtils.toRaftPeerProtos(nle.getPeers()));
        b.setNotLeaderException(nleBuilder.build());
      }

      final NotReplicatedException nre = reply.getNotReplicatedException();
      if (nre != null) {
        final NotReplicatedExceptionProto.Builder nreBuilder = NotReplicatedExceptionProto.newBuilder()
            .setCallId(nre.getCallId())
            .setReplication(nre.getRequiredReplication())
            .setLogIndex(nre.getLogIndex());
        b.setNotReplicatedException(nreBuilder);
      }

      final LeaderNotReadyException lnre = reply.getLeaderNotReadyException();
      if (lnre != null) {
        LeaderNotReadyExceptionProto.Builder lnreBuilder = LeaderNotReadyExceptionProto.newBuilder()
            .setServerId(ProtoUtils.toRaftGroupMemberIdProtoBuilder(lnre.getServerId()));
        b.setLeaderNotReadyException(lnreBuilder);
      }

      Optional.ofNullable(reply.getStateMachineException())
          .map(ProtoUtils::toThrowableProto)
          .ifPresent(b::setStateMachineException);

      Optional.ofNullable(reply.getDataStreamException())
          .map(ProtoUtils::toThrowableProto)
          .ifPresent(b::setDataStreamException);

      Optional.ofNullable(reply.getAlreadyClosedException())
          .map(ProtoUtils::toThrowableProto)
          .ifPresent(b::setAlreadyClosedException);

      final RaftClientReplyProto serialized = b.build();
      final RaftException e = reply.getException();
      if (e != null) {
        final RaftClientReply deserialized = toRaftClientReply(serialized);
        if (!Optional.ofNullable(deserialized.getException())
            .map(Object::getClass).filter(e.getClass()::equals).isPresent()) {
          throw new AssertionError("Corruption while serializing reply= " + reply
              + " but serialized=" + serialized + " and deserialized=" + deserialized, e);
        }
      }
      return serialized;
    }
    return b.build();
  }

  static GroupListReplyProto toGroupListReplyProto(
      GroupListReply reply) {
    final GroupListReplyProto.Builder b =
        GroupListReplyProto.newBuilder();
    if (reply != null) {
      b.setRpcReply(toRaftRpcReplyProtoBuilder(reply.getClientId().toByteString(),
          reply.getServerId().toByteString(), reply.getRaftGroupId(),
          reply.getCallId(), reply.isSuccess()));
      if (reply.getGroupIds() != null) {
        reply.getGroupIds().forEach(groupId -> b.addGroupId(ProtoUtils.toRaftGroupIdProtoBuilder(groupId)));
      }
    }
    return b.build();
  }

  static GroupInfoReplyProto toGroupInfoReplyProto(GroupInfoReply reply) {
    final GroupInfoReplyProto.Builder b =
        GroupInfoReplyProto.newBuilder();
    if (reply != null) {
      b.setRpcReply(toRaftRpcReplyProtoBuilder(reply.getClientId().toByteString(),
          reply.getServerId().toByteString(), reply.getRaftGroupId(),
          reply.getCallId(), reply.isSuccess()));
      if (reply.getRaftGroupId() != null) {
        b.setGroup(ProtoUtils.toRaftGroupProtoBuilder(reply.getGroup()));
        b.setIsRaftStorageHealthy(reply.isRaftStorageHealthy());
        b.setRole(reply.getRoleInfoProto());
        ProtoUtils.addCommitInfos(reply.getCommitInfos(), b::addCommitInfos);
      }
    }
    return b.build();
  }

  static RaftClientReply getRaftClientReply(DataStreamReply reply) {
    if (!(reply instanceof DataStreamReplyByteBuffer)) {
      throw new IllegalStateException("Unexpected " + reply.getClass() + ": reply is " + reply);
    }
    try {
      return toRaftClientReply(((DataStreamReplyByteBuffer) reply).slice());
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalStateException("Failed to getRaftClientReply from " + reply, e);
    }
  }

  static RaftClientReply toRaftClientReply(ByteBuffer buffer) throws InvalidProtocolBufferException {
    return toRaftClientReply(RaftClientReplyProto.parseFrom(buffer));
  }

  static RaftClientReply toRaftClientReply(RaftClientReplyProto replyProto) {
    final RaftRpcReplyProto rp = replyProto.getRpcReply();
    final RaftGroupMemberId serverMemberId = ProtoUtils.toRaftGroupMemberId(rp.getReplyId(), rp.getRaftGroupId());

    final RaftException e;
    if (replyProto.getExceptionDetailsCase().equals(NOTLEADEREXCEPTION)) {
      NotLeaderExceptionProto nleProto = replyProto.getNotLeaderException();
      final RaftPeer suggestedLeader = nleProto.hasSuggestedLeader() ?
          ProtoUtils.toRaftPeer(nleProto.getSuggestedLeader()) : null;
      final List<RaftPeer> peers = ProtoUtils.toRaftPeers(nleProto.getPeersInConfList());
      e = new NotLeaderException(serverMemberId, suggestedLeader, peers);
    } else if (replyProto.getExceptionDetailsCase() == NOTREPLICATEDEXCEPTION) {
      final NotReplicatedExceptionProto nre = replyProto.getNotReplicatedException();
      e = new NotReplicatedException(nre.getCallId(), nre.getReplication(), nre.getLogIndex());
    } else if (replyProto.getExceptionDetailsCase().equals(STATEMACHINEEXCEPTION)) {
      e = ProtoUtils.toThrowable(replyProto.getStateMachineException(), StateMachineException.class);
    } else if (replyProto.getExceptionDetailsCase().equals(DATASTREAMEXCEPTION)) {
      e = ProtoUtils.toThrowable(replyProto.getDataStreamException(), DataStreamException.class);
    } else if (replyProto.getExceptionDetailsCase().equals(LEADERNOTREADYEXCEPTION)) {
      LeaderNotReadyExceptionProto lnreProto = replyProto.getLeaderNotReadyException();
      e = new LeaderNotReadyException(ProtoUtils.toRaftGroupMemberId(lnreProto.getServerId()));
    } else if (replyProto.getExceptionDetailsCase().equals(ALREADYCLOSEDEXCEPTION)) {
      e = ProtoUtils.toThrowable(replyProto.getAlreadyClosedException(), AlreadyClosedException.class);
    } else {
      e = null;
    }

    return RaftClientReply.newBuilder()
        .setClientId(ClientId.valueOf(rp.getRequestorId()))
        .setServerId(serverMemberId)
        .setCallId(rp.getCallId())
        .setSuccess(rp.getSuccess())
        .setMessage(toMessage(replyProto.getMessage()))
        .setException(e)
        .setLogIndex(replyProto.getLogIndex())
        .setCommitInfos(replyProto.getCommitInfosList())
        .build();
  }

  static GroupListReply toGroupListReply(GroupListReplyProto replyProto) {
    final RaftRpcReplyProto rpc = replyProto.getRpcReply();
    final List<RaftGroupId> groupIds = replyProto.getGroupIdList().stream()
        .map(ProtoUtils::toRaftGroupId)
        .collect(Collectors.toList());
    return new GroupListReply(ClientId.valueOf(rpc.getRequestorId()),
        RaftPeerId.valueOf(rpc.getReplyId()),
        ProtoUtils.toRaftGroupId(rpc.getRaftGroupId()),
        rpc.getCallId(),
        groupIds);
  }

  static GroupInfoReply toGroupInfoReply(GroupInfoReplyProto replyProto) {
    final RaftRpcReplyProto rpc = replyProto.getRpcReply();
    return new GroupInfoReply(ClientId.valueOf(rpc.getRequestorId()),
        RaftPeerId.valueOf(rpc.getReplyId()),
        ProtoUtils.toRaftGroupId(rpc.getRaftGroupId()),
        rpc.getCallId(),
        replyProto.getCommitInfosList(),
        ProtoUtils.toRaftGroup(replyProto.getGroup()),
        replyProto.getRole(),
        replyProto.getIsRaftStorageHealthy());
  }

  static Message toMessage(final ClientMessageEntryProto p) {
    return Message.valueOf(p.getContent());
  }

  static ClientMessageEntryProto.Builder toClientMessageEntryProtoBuilder(ByteString message) {
    return ClientMessageEntryProto.newBuilder().setContent(message);
  }

  static ClientMessageEntryProto.Builder toClientMessageEntryProtoBuilder(Message message) {
    return toClientMessageEntryProtoBuilder(message.getContent());
  }

  static SetConfigurationRequest toSetConfigurationRequest(
      SetConfigurationRequestProto p) {
    final RaftRpcRequestProto m = p.getRpcRequest();
    final List<RaftPeer> peers = ProtoUtils.toRaftPeers(p.getPeersList());
    return new SetConfigurationRequest(
        ClientId.valueOf(m.getRequestorId()),
        RaftPeerId.valueOf(m.getReplyId()),
        ProtoUtils.toRaftGroupId(m.getRaftGroupId()),
        p.getRpcRequest().getCallId(), peers);
  }

  static SetConfigurationRequestProto toSetConfigurationRequestProto(
      SetConfigurationRequest request) {
    return SetConfigurationRequestProto.newBuilder()
        .setRpcRequest(toRaftRpcRequestProtoBuilder(request))
        .addAllPeers(ProtoUtils.toRaftPeerProtos(request.getPeersInNewConf()))
        .build();
  }

  static GroupManagementRequest toGroupManagementRequest(GroupManagementRequestProto p) {
    final RaftRpcRequestProto m = p.getRpcRequest();
    final ClientId clientId = ClientId.valueOf(m.getRequestorId());
    final RaftPeerId serverId = RaftPeerId.valueOf(m.getReplyId());
    switch(p.getOpCase()) {
      case GROUPADD:
        return GroupManagementRequest.newAdd(clientId, serverId, m.getCallId(),
            ProtoUtils.toRaftGroup(p.getGroupAdd().getGroup()));
      case GROUPREMOVE:
        final GroupRemoveRequestProto remove = p.getGroupRemove();
        return GroupManagementRequest.newRemove(clientId, serverId, m.getCallId(),
            ProtoUtils.toRaftGroupId(remove.getGroupId()),
            remove.getDeleteDirectory(), remove.getRenameDirectory());
      default:
        throw new IllegalArgumentException("Unexpected op " + p.getOpCase() + " in " + p);
    }
  }

  static GroupInfoRequest toGroupInfoRequest(
      GroupInfoRequestProto p) {
    final RaftRpcRequestProto m = p.getRpcRequest();
    return new GroupInfoRequest(
        ClientId.valueOf(m.getRequestorId()),
        RaftPeerId.valueOf(m.getReplyId()),
        ProtoUtils.toRaftGroupId(m.getRaftGroupId()),
        m.getCallId());
  }

  static GroupListRequest toGroupListRequest(
      GroupListRequestProto p) {
    final RaftRpcRequestProto m = p.getRpcRequest();
    return new GroupListRequest(
        ClientId.valueOf(m.getRequestorId()),
        RaftPeerId.valueOf(m.getReplyId()),
        ProtoUtils.toRaftGroupId(m.getRaftGroupId()),
        m.getCallId());
  }


  static GroupManagementRequestProto toGroupManagementRequestProto(GroupManagementRequest request) {
    final GroupManagementRequestProto.Builder b = GroupManagementRequestProto.newBuilder()
        .setRpcRequest(toRaftRpcRequestProtoBuilder(request));
    final GroupManagementRequest.Add add = request.getAdd();
    if (add != null) {
      b.setGroupAdd(GroupAddRequestProto.newBuilder().setGroup(
          ProtoUtils.toRaftGroupProtoBuilder(add.getGroup())).build());
    }
    final GroupManagementRequest.Remove remove = request.getRemove();
    if (remove != null) {
      b.setGroupRemove(GroupRemoveRequestProto.newBuilder()
          .setGroupId(ProtoUtils.toRaftGroupIdProtoBuilder(remove.getGroupId()))
          .setDeleteDirectory(remove.isDeleteDirectory())
          .setRenameDirectory(remove.isRenameDirectory())
          .build());
    }
    return b.build();
  }

  static GroupInfoRequestProto toGroupInfoRequestProto(
      GroupInfoRequest request) {
    return GroupInfoRequestProto.newBuilder()
        .setRpcRequest(toRaftRpcRequestProtoBuilder(request))
        .build();
  }

  static GroupListRequestProto toGroupListRequestProto(
      GroupListRequest request) {
    return GroupListRequestProto.newBuilder()
        .setRpcRequest(toRaftRpcRequestProtoBuilder(request))
        .build();
  }

  static String toString(RaftClientRequestProto proto) {
    final RaftRpcRequestProto rpc = proto.getRpcRequest();
    return ClientId.valueOf(rpc.getRequestorId()) + "->" + rpc.getReplyId().toStringUtf8()
        + "#" + rpc.getCallId() + "-" + ProtoUtils.toString(rpc.getSlidingWindowEntry());
  }

  static String toString(RaftClientReplyProto proto) {
    final RaftRpcReplyProto rpc = proto.getRpcReply();
    return ClientId.valueOf(rpc.getRequestorId()) + "<-" + rpc.getReplyId().toStringUtf8()
        + "#" + rpc.getCallId();
  }
}
