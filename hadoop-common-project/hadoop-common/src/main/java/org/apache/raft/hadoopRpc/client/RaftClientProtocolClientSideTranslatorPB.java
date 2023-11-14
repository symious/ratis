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
package org.apache.raft.hadoopRpc.client;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.ipc.RPC;
import org.apache.raft.proto.RaftClientProtocolProtos.RaftClientRequestProto;
import org.apache.raft.proto.RaftClientProtocolProtos.SetConfigurationRequestProto;
import org.apache.raft.protocol.RaftClientProtocol;
import org.apache.raft.protocol.RaftClientRequest;
import org.apache.raft.protocol.SetConfigurationRequest;
import org.apache.raft.util.ProtoUtils;

import java.io.Closeable;
import java.io.IOException;

@InterfaceAudience.Private
public class RaftClientProtocolClientSideTranslatorPB
    implements RaftClientProtocol, Closeable {
  private final RaftClientProtocolPB rpcProxy;

  public RaftClientProtocolClientSideTranslatorPB(RaftClientProtocolPB rpcProxy) {
    this.rpcProxy = rpcProxy;
  }

  @Override
  public void close() throws IOException {
    RPC.stopProxy(rpcProxy);
  }

  @Override
  public void submitClientRequest(RaftClientRequest request)
      throws IOException {
    final RaftClientRequestProto p = ProtoUtils.toRaftClientRequestProto(request);
    try {
      rpcProxy.submitClientRequest(null, p);
    } catch (ServiceException se) {
      throw ProtoUtils.toIOException(se);
    }
  }

  @Override
  public void setConfiguration(SetConfigurationRequest request)
      throws IOException {
    final SetConfigurationRequestProto p
        = ProtoUtils.toSetConfigurationRequestProto(request);
    try {
      rpcProxy.setConfiguration(null, p);
    } catch (ServiceException se) {
      throw ProtoUtils.toIOException(se);
    }
  }
}
