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
package org.apache.ratis.grpc.metrics;

import java.util.Optional;

import org.apache.ratis.metrics.MetricRegistries;
import org.apache.ratis.metrics.MetricRegistryInfo;
import org.apache.ratis.metrics.RatisMetricRegistry;
import org.apache.ratis.thirdparty.com.google.common.annotations.VisibleForTesting;

import com.codahale.metrics.Timer;

public class GrpcServerMetrics {
  private final RatisMetricRegistry registry;

  private static final String RATIS_GRPC_METRICS_APP_NAME = "ratis_grpc";
  private static final String RATIS_GRPC_METRICS_COMP_NAME = "log_appender";
  private static final String RATIS_GRPC_METRICS_DESC = "Metrics for Ratis Grpc Log Appender";

  public static final String RATIS_GRPC_METRICS_LOG_APPENDER_LATENCY =
      "%s_latency";
  public static final String RATIS_GRPC_METRICS_LOG_APPENDER_SUCCESS =
      "%s_success_reply_count";
  public static final String RATIS_GRPC_METRICS_LOG_APPENDER_NOT_LEADER =
      "%s_not_leader_reply_count";
  public static final String RATIS_GRPC_METRICS_LOG_APPENDER_INCONSISTENCY =
      "%s_inconsistency_reply_count";
  public static final String RATIS_GRPC_METRICS_LOG_APPENDER_TIMEOUT =
      "%s_append_entry_timeout_count";
  public static final String RATIS_GRPC_METRICS_REQUEST_RETRY_COUNT = "num_retries";
  public static final String RATIS_GRPC_METRICS_REQUESTS_TOTAL = "num_requests";
  public static final String RATIS_GRPC_INSTALL_SNAPSHOT_COUNT = "num_install_snapshot";

  public GrpcServerMetrics(String serverId) {
    MetricRegistryInfo info = new MetricRegistryInfo(serverId, RATIS_GRPC_METRICS_APP_NAME,
        RATIS_GRPC_METRICS_COMP_NAME, RATIS_GRPC_METRICS_DESC);
    Optional<RatisMetricRegistry> metricRegistry = MetricRegistries.global().get(info);

    registry = metricRegistry.orElseGet(() -> MetricRegistries.global().create(info));
  }

  public Timer getGrpcLogAppenderLatencyTimer(String follower) {
    return registry.timer(String.format(RATIS_GRPC_METRICS_LOG_APPENDER_LATENCY, follower));
  }

  public void onRequestRetry() {
    registry.counter(RATIS_GRPC_METRICS_REQUEST_RETRY_COUNT).inc();
  }

  public void onRequestCreate() {
    registry.counter(RATIS_GRPC_METRICS_REQUESTS_TOTAL).inc();
  }

  public void onRequestSuccess(String follower) {
    registry.counter(String.format(RATIS_GRPC_METRICS_LOG_APPENDER_SUCCESS, follower)).inc();
  }

  public void onRequestNotLeader(String follower) {
    registry.counter(String.format(RATIS_GRPC_METRICS_LOG_APPENDER_NOT_LEADER, follower)).inc();
  }

  public void onRequestInconsistency(String follower) {
    registry.counter(String.format(RATIS_GRPC_METRICS_LOG_APPENDER_INCONSISTENCY, follower)).inc();
  }

  public void onRequestTimeout(String follower) {
    registry.counter(String.format(RATIS_GRPC_METRICS_LOG_APPENDER_TIMEOUT, follower)).inc();
  }

  public void onInstallSnapshot() {
    registry.counter(RATIS_GRPC_INSTALL_SNAPSHOT_COUNT).inc();
  }

  @VisibleForTesting
  public RatisMetricRegistry getRegistry() {
    return registry;
  }
}
