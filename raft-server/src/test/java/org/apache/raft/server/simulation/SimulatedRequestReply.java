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
package org.apache.raft.server.simulation;

import com.google.common.base.Preconditions;
import org.apache.hadoop.util.Time;
import org.apache.raft.RaftTestUtil;
import org.apache.raft.protocol.RaftPeer;
import org.apache.raft.protocol.RaftRpcMessage;
import org.apache.raft.server.RaftServerConstants;
import org.apache.raft.util.RaftUtils;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class SimulatedRequestReply<REQUEST extends RaftRpcMessage,
    REPLY extends RaftRpcMessage> implements RequestReply<REQUEST, REPLY> {
  public static final String SIMULATE_LATENCY_KEY
      = SimulatedRequestReply.class.getName() + ".simulateLatencyMs";
  public static final int SIMULATE_LATENCY_DEFAULT
      = RaftServerConstants.ELECTION_TIMEOUT_MIN_MS;
  public static final long TIMEOUT = 3000L;

  private static class ReplyOrException<REPLY> {
    private final REPLY reply;
    private final IOException ioe;

    ReplyOrException(REPLY reply, IOException ioe) {
      Preconditions.checkArgument(reply == null ^ ioe == null);
      this.reply = reply;
      this.ioe = ioe;
    }
  }

  static class EventQueue<REQUEST, REPLY> {
    private final BlockingQueue<REQUEST> requestQueue
        = new LinkedBlockingQueue<>();
    private final Map<REQUEST, ReplyOrException<REPLY>> replyMap
        = new ConcurrentHashMap<>();

    /** Block takeRequest for the requests sent from this server. */
    final AtomicBoolean blockTakeRequestFrom = new AtomicBoolean();
    /** Block sendRequest for the requests sent to this server. */
    final AtomicBoolean blockSendRequestTo = new AtomicBoolean();
    /** Delay takeRequest for the requests sent to this server. */
    final AtomicInteger delayTakeRequestTo = new AtomicInteger();

    REPLY request(REQUEST request) throws InterruptedException, IOException {
      requestQueue.put(request);
      synchronized (this) {
        final long startTime = Time.monotonicNow();
        while (Time.monotonicNow() - startTime < TIMEOUT &&
            !replyMap.containsKey(request)) {
          this.wait(TIMEOUT); // no need to be precise here
        }
      }

      if (!replyMap.containsKey(request)) {
        throw new IOException("Timeout while waiting for reply of request "
            + request);
      }
      final ReplyOrException<REPLY> re = replyMap.remove(request);
      if (re.ioe != null) {
        throw re.ioe;
      }
      return re.reply;
    }

    REQUEST takeRequest() throws InterruptedException {
      return requestQueue.take();
    }

    void reply(REQUEST request, REPLY reply, IOException ioe)
        throws IOException {
      replyMap.put(request, new ReplyOrException<>(reply, ioe));
      synchronized (this) {
        this.notifyAll();
      }
    }
  }

  private final Map<String, EventQueue<REQUEST, REPLY>> queues;
  private final int simulateLatencyMs;

  SimulatedRequestReply(Collection<RaftPeer> allPeers, int simulateLatencyMs) {
    queues = new ConcurrentHashMap<>();
    for (RaftPeer peer : allPeers) {
      queues.put(peer.getId(), new EventQueue<>());
    }

    this.simulateLatencyMs = simulateLatencyMs;
  }

  EventQueue<REQUEST, REPLY> getQueue(String qid) {
    return queues.get(qid);
  }

  @Override
  public REPLY sendRequest(REQUEST request) throws IOException {
    final String qid = request.getReplierId();
    final EventQueue<REQUEST, REPLY> q = queues.get(qid);
    if (q == null) {
      throw new IOException("The peer " + qid + " is not alive.");
    }
    try {
      RaftTestUtil.block(() -> q.blockSendRequestTo.get());
      return q.request(request);
    } catch (InterruptedException e) {
      throw RaftUtils.toInterruptedIOException("", e);
    }
  }

  @Override
  public REQUEST takeRequest(String qid) throws IOException {
    final EventQueue<REQUEST, REPLY> q = queues.get(qid);
    if (q == null) {
      throw new IOException("The RPC of " + qid + " has already shutdown.");
    }

    final REQUEST request;
    try {
      // delay request for testing
      RaftTestUtil.delay(q.delayTakeRequestTo::get);

      request = q.takeRequest();
      Preconditions.checkState(qid.equals(request.getReplierId()));

      // block request for testing
      final EventQueue<REQUEST, REPLY> reqQ = queues.get(request.getRequestorId());
      if (reqQ != null) {
        RaftTestUtil.block(reqQ.blockTakeRequestFrom::get);
      }
    } catch (InterruptedException e) {
      throw RaftUtils.toInterruptedIOException("", e);
    }
    return request;
  }

  @Override
  public void sendReply(REQUEST request, REPLY reply, IOException ioe)
      throws IOException {
    if (reply != null) {
      Preconditions.checkArgument(
          request.getRequestorId().equals(reply.getRequestorId()));
      Preconditions.checkArgument(
          request.getReplierId().equals(reply.getReplierId()));
    }
    simulateLatency();
    final String qid = request.getReplierId();
    EventQueue<REQUEST, REPLY> q = queues.get(qid);
    if (q != null) {
      q.reply(request, reply, ioe);
    }
  }

  @Override
  public void shutdown(String id) {
    queues.remove(id);
  }

  public void addPeers(Collection<RaftPeer> newPeers) {
    for (RaftPeer peer : newPeers) {
      queues.put(peer.getId(), new EventQueue<>());
    }
  }

  private void simulateLatency() throws IOException {
    if (simulateLatencyMs > 0) {
      int waitExpetation = simulateLatencyMs / 10;
      int waitHalfRange = waitExpetation / 3;
      int randomSleepMs = ThreadLocalRandom.current().nextInt(2 * waitHalfRange)
          + waitExpetation - waitHalfRange;
      try {
        Thread.sleep(randomSleepMs);
      } catch (InterruptedException ie) {
        throw RaftUtils.toInterruptedIOException("", ie);
      }
    }
  }
}
