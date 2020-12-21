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
package org.apache.ratis.client.api;

import org.apache.ratis.protocol.RoutingTable;

import java.nio.ByteBuffer;

/**
 * Stream data asynchronously to all the servers in the {@link org.apache.ratis.protocol.RaftGroup}.
 * Clients may stream data to the nearest server and then the server will forward the data to the other servers.
 * Once all the servers have received all the data of a request,
 * the leader (may or may not be the nearest server) creates a log entry with a data ID generated by the state machine.
 * Then, the leader sends the log entry to the followers.
 * Since the followers already have received the data from the stream,
 * they may lookup the data from the ID.
 *
 * Since this API allows clients to send data to the nearest server which is not necessarily the leader,
 * this API is more efficient for network-topology-aware clusters
 * than the other APIs that require clients to send data/messages to the leader.
 *
 * Note that this API is different from {@link MessageStreamApi} in the sense that
 * this API streams data to all the servers in the {@link org.apache.ratis.protocol.RaftGroup}
 * but {@link MessageStreamApi} streams messages only to the leader.
 */
public interface DataStreamApi {
  /** Create a stream to write data. */
  default DataStreamOutput stream() {
    return stream(null);
  }

  /** Create a stream by providing a customized header message. */
  DataStreamOutput stream(ByteBuffer headerMessage);

  /** Create a stream by providing a customized header message and route table. */
  DataStreamOutput stream(ByteBuffer headerMessage, RoutingTable routingTable);
}
