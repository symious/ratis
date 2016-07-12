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
package org.apache.hadoop.raft.server;

import org.apache.hadoop.raft.protocol.RaftRpcMessage;
import org.apache.hadoop.util.Daemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;

import static org.apache.hadoop.util.ExitUtil.terminate;

public class RequestHandler<REQUEST extends RaftRpcMessage,
    REPLY extends RaftRpcMessage> {
  public static final Logger LOG = LoggerFactory.getLogger(RequestHandler.class);

  interface HandlerInterface<REQUEST extends RaftRpcMessage,
      REPLY extends RaftRpcMessage> {

    boolean isRunning();

    REPLY handleRequest(REQUEST r) throws IOException;
  }

  private final String serverId;
  private final String name;
  private final RaftRpc<REQUEST, REPLY> rpc;
  private final HandlerInterface<REQUEST, REPLY> handlerImpl;
  private final HandlerDaemon daemon;

  RequestHandler(String serverId, String name,
                 RaftRpc<REQUEST, REPLY> rpc,
                 HandlerInterface<REQUEST, REPLY> handlerImpl) {
    this.serverId = serverId;
    this.name = name;
    this.rpc = rpc;
    this.handlerImpl = handlerImpl;
    this.daemon = new HandlerDaemon();
  }

  void startDaemon() {
    daemon.start();
  }

  void shutdown() throws IOException {
    rpc.shutdown(serverId);
  }

  void interruptAndJoinDaemon() throws InterruptedException {
    daemon.interrupt();
    daemon.join();
  }

  RaftRpc<REQUEST, REPLY> getRpc() {
    return rpc;
  }

  void handleRequest(REQUEST request) throws IOException {
    final REPLY reply;
    try {
      reply = handlerImpl.handleRequest(request);
    } catch (IOException ioe) {
      LOG.debug("IOException for " + request, ioe);
      rpc.sendReply(request, null, ioe);
      return;
    }
    if (reply != null) {
      rpc.sendReply(request, reply, null);
    }
  }

  /**
   * A thread keep polling requests from the request queue. Used for simulation.
   */
  class HandlerDaemon extends Daemon {
    @Override
    public String toString() {
      return serverId + "." + name;
    }

    @Override
    public void run() {
      while (handlerImpl.isRunning()) {
        try {
          handleRequest(rpc.takeRequest(serverId));
        } catch (InterruptedIOException e) {
          LOG.info(this + " is interrupted by " + e);
          LOG.trace("TRACE", e);
          break;
        } catch (IOException e) {
          LOG.error(this + " has " + e);
          LOG.trace("TRACE", e);
        } catch(Throwable t) {
          if (!handlerImpl.isRunning()) {
            LOG.info(this + " is stopped.");
            break;
          }
          LOG.error(this + " is terminating due to", t);
          terminate(1, t);
        }
      }
    }
  }
}
