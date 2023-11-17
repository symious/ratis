/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.raft.server;

import com.google.common.base.Preconditions;
import org.apache.raft.protocol.Message;
import org.apache.raft.protocol.RaftClientReply;
import org.apache.raft.protocol.RaftClientRequest;
import org.apache.raft.protocol.SetConfigurationRequest;
import org.apache.raft.statemachine.TrxContext;

import java.util.concurrent.CompletableFuture;

public class PendingRequest implements Comparable<PendingRequest> {
  private final Long index;
  private final RaftClientRequest request;
  private final TrxContext entry;
  private final CompletableFuture<RaftClientReply> future;

  PendingRequest(long index, RaftClientRequest request,
                 TrxContext entry) {
    this.index = index;
    this.request = request;
    this.entry = entry;
    this.future = new CompletableFuture<>();
  }

  PendingRequest(SetConfigurationRequest request) {
    this(RaftServerConstants.INVALID_LOG_INDEX, request, null);
  }

  long getIndex() {
    return index;
  }

  RaftClientRequest getRequest() {
    return request;
  }

  public CompletableFuture<RaftClientReply> getFuture() {
    return future;
  }

  TrxContext getEntry() {
    return entry;
  }

  synchronized void setException(Throwable e) {
    Preconditions.checkArgument(e != null);
    future.completeExceptionally(e);
  }

  synchronized void setReply(RaftClientReply r) {
    Preconditions.checkArgument(r != null);
    future.complete(r);
  }

  void setSuccessReply(Message message) {
    setReply(new RaftClientReply(getRequest(), message));
  }

  @Override
  public int compareTo(PendingRequest that) {
    return Long.compare(this.index, that.index);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(index=" + index
        + ", request=" + request;
  }
}
