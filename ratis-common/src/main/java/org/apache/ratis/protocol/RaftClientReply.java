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
package org.apache.ratis.protocol;

/**
 * Reply from server to client
 */
public class RaftClientReply extends RaftClientMessage {
  private final boolean success;
  private final long seqNum;

  /** non-null if the server is not leader */
  private final NotLeaderException notLeaderException;
  private final Message message;

  public RaftClientReply(ClientId clientId, RaftPeerId serverId, long seqNum,
      boolean success, Message message, NotLeaderException notLeaderException) {
    super(clientId, serverId);
    this.success = success;
    this.seqNum = seqNum;
    this.message = message;
    this.notLeaderException = notLeaderException;
  }

  public RaftClientReply(RaftClientRequest request,
      NotLeaderException notLeaderException) {
    this(request.getClientId(), request.getServerId(), request.getSeqNum(),
        false, null, notLeaderException);
  }

  public RaftClientReply(RaftClientRequest request, Message message) {
    this(request.getClientId(), request.getServerId(), request.getSeqNum(),
        true, message, null);
  }

  @Override
  public final boolean isRequest() {
    return false;
  }

  public long getSeqNum() {
    return seqNum;
  }

  @Override
  public String toString() {
    return super.toString() + ", seqNum: " + getSeqNum()
        + ", success: " + isSuccess();
  }

  public boolean isSuccess() {
    return success;
  }

  public Message getMessage() {
    return message;
  }

  public NotLeaderException getNotLeaderException() {
    return notLeaderException;
  }

  public boolean isNotLeader() {
    return notLeaderException != null;
  }
}
