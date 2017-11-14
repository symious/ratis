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
package org.apache.ratis.grpc;

import org.apache.log4j.Level;
import org.apache.ratis.RaftBasicTests;
import org.apache.ratis.server.impl.BlockRequestHandlingInjection;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.LogUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class TestRaftWithGrpc extends RaftBasicTests {
  static {
    LogUtils.setLogLevel(RaftServerImpl.LOG, Level.DEBUG);
    LogUtils.setLogLevel(FileUtils.LOG, Level.DEBUG);
  }

  private final MiniRaftClusterWithGRpc cluster;

  public TestRaftWithGrpc() throws IOException {
    cluster = MiniRaftClusterWithGRpc.FACTORY.newCluster(
        NUM_SERVERS, properties);
    Assert.assertNull(cluster.getLeader());
  }

  @Override
  public MiniRaftClusterWithGRpc getCluster() {
    return cluster;
  }

  @Override
  @Test
  public void testEnforceLeader() throws Exception {
    super.testEnforceLeader();

    MiniRaftClusterWithGRpc.sendServerRequestInjection.clear();
    BlockRequestHandlingInjection.getInstance().unblockAll();
  }

  @Test
  public void testBasicAppendEntriesAsync() throws Exception {
    super.testBasicAppendEntries(true);
  }

  @Override
  @Test
  public void testWithLoad() throws Exception {
    super.testWithLoad();
    BlockRequestHandlingInjection.getInstance().unblockAll();
  }
}
