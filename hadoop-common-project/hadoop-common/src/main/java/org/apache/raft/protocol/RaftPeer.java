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
package org.apache.raft.protocol;

import java.net.InetSocketAddress;

public class RaftPeer {
  private final String id;
  private final String address;

  public RaftPeer(String id) {
    this(id, id);
  }

  public RaftPeer(String id, InetSocketAddress address) {
    this(id, address == null? null: address.toString());
  }

  public RaftPeer(String id, String address) {
    this.id = id;
    this.address = address;
  }

  public String getId() {
    return id;
  }

  public String getAddress() {
    return address;
  }

  @Override
  public String toString() {
    return id + ":" + address;
  }

  @Override
  public boolean equals(Object o) {
    return (o instanceof RaftPeer) && id.equals(((RaftPeer) o).getId());
  }

  @Override
  public int hashCode() {
    return id.hashCode();
  }
}
