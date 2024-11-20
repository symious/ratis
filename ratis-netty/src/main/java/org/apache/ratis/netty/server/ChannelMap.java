/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.ratis.netty.server;

import org.apache.ratis.protocol.ClientInvocationId;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelId;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/** Map: {@link ChannelId} -> {@link ClientInvocationId}s. */
//class ChannelMap {
//  private final Map<ChannelId, Map<ClientInvocationId, ClientInvocationId>> map = new ConcurrentHashMap<>();
//
//  void add(ChannelId channelId, ClientInvocationId clientInvocationId) {
//    map.computeIfAbsent(channelId, (e) -> new ConcurrentHashMap<>())
//        .put(clientInvocationId, clientInvocationId);
//  }
//
//  void remove(ChannelId channelId, ClientInvocationId clientInvocationId) {
//    Optional.ofNullable(map.get(channelId))
//        .ifPresent((ids) -> ids.remove(clientInvocationId));
//  }
//
//  Set<ClientInvocationId> remove(ChannelId channelId) {
//    return Optional.ofNullable(map.remove(channelId))
//        .map(Map::keySet)
//        .orElse(Collections.emptySet());
//  }
//}

class ChannelMap {
  private final Map<ChannelId, Map<ClientInvocationId, Integer>> map = new ConcurrentHashMap<>();

  void add(ChannelId channelId, ClientInvocationId clientInvocationId) {
    map.computeIfAbsent(channelId, (e) -> new ConcurrentHashMap<>())
        .merge(clientInvocationId, 1, Integer::sum); // 如果存在，计数加1；否则初始化为1
  }

  void remove(ChannelId channelId, ClientInvocationId clientInvocationId) {
    Optional.ofNullable(map.get(channelId))
        .ifPresent(ids -> {
          ids.computeIfPresent(clientInvocationId, (id, count) -> count > 1 ? count - 1 : null);
          if (ids.isEmpty()) {
            map.remove(channelId); // 如果内部Map为空，移除ChannelId
          }
        });
  }

  Set<ClientInvocationId> remove(ChannelId channelId) {
    return Optional.ofNullable(map.remove(channelId))
        .map(ids -> {
          Set<ClientInvocationId> keys = new HashSet<>(ids.keySet());
          ids.clear(); // 清空内部Map
          return keys;
        })
        .orElse(Collections.emptySet());
  }

  String getChannelSizes() {
    return map.entrySet().stream()
        .map(entry -> entry.getKey() + ": " + entry.getValue().size())
        .collect(Collectors.joining(", "));
  }

  int getTotalSize() {
    return map.values().stream()
        .mapToInt(Map::size)
        .sum();
  }
}