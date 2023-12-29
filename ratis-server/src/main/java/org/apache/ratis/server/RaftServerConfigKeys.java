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
package org.apache.ratis.server;

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.apache.ratis.conf.ConfUtils.*;

public interface RaftServerConfigKeys {
  Logger LOG = LoggerFactory.getLogger(RaftServerConfigKeys.class);
  static Consumer<String> getDefaultLog() {
    return LOG::info;
  }

  String PREFIX = "raft.server";

  String STORAGE_DIR_KEY = PREFIX + ".storage.dir";
  List<File> STORAGE_DIR_DEFAULT = Collections.singletonList(new File("/tmp/raft-server/"));
  static List<File> storageDirs(RaftProperties properties) {
    return getFiles(properties::getFiles, STORAGE_DIR_KEY, STORAGE_DIR_DEFAULT, getDefaultLog());
  }
  static void setStorageDirs(RaftProperties properties, List<File> storageDir) {
    setFiles(properties::setFiles, STORAGE_DIR_KEY, storageDir);
  }

  /**
   * When bootstrapping a new peer, If the gap between the match index of the
   * peer and the leader's latest committed index is less than this gap, we
   * treat the peer as caught-up.
   */
  String STAGING_CATCHUP_GAP_KEY = PREFIX + ".staging.catchup.gap";
  int STAGING_CATCHUP_GAP_DEFAULT = 1000; // increase this number when write throughput is high
  static int stagingCatchupGap(RaftProperties properties) {
    return getInt(properties::getInt,
        STAGING_CATCHUP_GAP_KEY, STAGING_CATCHUP_GAP_DEFAULT, getDefaultLog(), requireMin(0));
  }
  static void setStagingCatchupGap(RaftProperties properties, int stagingCatchupGap) {
    setInt(properties::setInt, STAGING_CATCHUP_GAP_KEY, stagingCatchupGap);
  }

  /**
   * Timeout for leader election after which the statemachine of the server is notified
   * about leader election pending for a long time.
   */
  String LEADER_ELECTION_TIMEOUT_KEY = PREFIX + ".leader.election.timeout";
  TimeDuration LEADER_ELECTION_TIMEOUT_DEFAULT = TimeDuration.valueOf(60, TimeUnit.SECONDS);
  static TimeDuration leaderElectionTimeout(RaftProperties properties) {
    return getTimeDuration(properties.getTimeDuration(LEADER_ELECTION_TIMEOUT_DEFAULT.getUnit()),
        LEADER_ELECTION_TIMEOUT_KEY, LEADER_ELECTION_TIMEOUT_DEFAULT, getDefaultLog());
  }
  static void setLeaderElectionTimeout(RaftProperties properties, TimeDuration leaderElectionTimeout) {
    setTimeDuration(properties::setTimeDuration, LEADER_ELECTION_TIMEOUT_KEY, leaderElectionTimeout);

  }

  String WATCH_TIMEOUT_DENOMINATION_KEY = PREFIX + ".watch.timeout.denomination";
  TimeDuration WATCH_TIMEOUT_DENOMINATION_DEFAULT = TimeDuration.valueOf(1, TimeUnit.SECONDS);
  static TimeDuration watchTimeoutDenomination(RaftProperties properties) {
    return getTimeDuration(properties.getTimeDuration(WATCH_TIMEOUT_DENOMINATION_DEFAULT.getUnit()),
        WATCH_TIMEOUT_DENOMINATION_KEY, WATCH_TIMEOUT_DENOMINATION_DEFAULT, getDefaultLog(), requirePositive());
  }
  static void setWatchTimeoutDenomination(RaftProperties properties, TimeDuration watchTimeout) {
    setTimeDuration(properties::setTimeDuration, WATCH_TIMEOUT_DENOMINATION_KEY, watchTimeout);
  }

  /**
   * Timeout for watch requests.
   */
  String WATCH_TIMEOUT_KEY = PREFIX + ".watch.timeout";
  TimeDuration WATCH_TIMEOUT_DEFAULT = TimeDuration.valueOf(10, TimeUnit.SECONDS);
  static TimeDuration watchTimeout(RaftProperties properties) {
    return getTimeDuration(properties.getTimeDuration(WATCH_TIMEOUT_DEFAULT.getUnit()),
        WATCH_TIMEOUT_KEY, WATCH_TIMEOUT_DEFAULT, getDefaultLog(), requirePositive());
  }
  static void setWatchTimeout(RaftProperties properties, TimeDuration watchTimeout) {
    setTimeDuration(properties::setTimeDuration, WATCH_TIMEOUT_KEY, watchTimeout);
  }

  interface Log {
    String PREFIX = RaftServerConfigKeys.PREFIX + ".log";

    String USE_MEMORY_KEY = PREFIX + ".use.memory";
    boolean USE_MEMORY_DEFAULT = false;
    static boolean useMemory(RaftProperties properties) {
      return getBoolean(properties::getBoolean, USE_MEMORY_KEY, USE_MEMORY_DEFAULT, getDefaultLog());
    }
    static void setUseMemory(RaftProperties properties, boolean useMemory) {
      setBoolean(properties::setBoolean, USE_MEMORY_KEY, useMemory);
    }

    String QUEUE_ELEMENT_LIMIT_KEY = PREFIX + ".queue.element-limit";
    int QUEUE_ELEMENT_LIMIT_DEFAULT = 4096;
    static int queueElementLimit(RaftProperties properties) {
      return getInt(properties::getInt, QUEUE_ELEMENT_LIMIT_KEY, QUEUE_ELEMENT_LIMIT_DEFAULT, getDefaultLog(),
          requireMin(1));
    }
    static void setElementLimit(RaftProperties properties, int queueSize) {
      setInt(properties::setInt, QUEUE_ELEMENT_LIMIT_KEY, queueSize, requireMin(1));
    }

    String QUEUE_BYTE_LIMIT_KEY = PREFIX + ".queue.byte-limit";
    SizeInBytes QUEUE_BYTE_LIMIT_DEFAULT = SizeInBytes.valueOf("64MB");
    static SizeInBytes queueByteLimit(RaftProperties properties) {
      return getSizeInBytes(properties::getSizeInBytes,
          QUEUE_BYTE_LIMIT_KEY, QUEUE_BYTE_LIMIT_DEFAULT, getDefaultLog());
    }
    static void setByteLimit(RaftProperties properties, int queueSize) {
      setInt(properties::setInt, QUEUE_BYTE_LIMIT_KEY, queueSize, requireMin(1));
    }

    String SEGMENT_SIZE_MAX_KEY = PREFIX + ".segment.size.max";
    SizeInBytes SEGMENT_SIZE_MAX_DEFAULT = SizeInBytes.valueOf("8MB");
    static SizeInBytes segmentSizeMax(RaftProperties properties) {
      return getSizeInBytes(properties::getSizeInBytes,
          SEGMENT_SIZE_MAX_KEY, SEGMENT_SIZE_MAX_DEFAULT, getDefaultLog());
    }
    static void setSegmentSizeMax(RaftProperties properties, SizeInBytes segmentSizeMax) {
      setSizeInBytes(properties::set, SEGMENT_SIZE_MAX_KEY, segmentSizeMax);
    }

    /**
     * Besides the open segment, the max number of segments caching log entries.
     */
    String SEGMENT_CACHE_MAX_NUM_KEY = PREFIX + ".segment.cache.num.max";
    int SEGMENT_CACHE_MAX_NUM_DEFAULT = 6;
    static int maxCachedSegmentNum(RaftProperties properties) {
      return getInt(properties::getInt, SEGMENT_CACHE_MAX_NUM_KEY,
          SEGMENT_CACHE_MAX_NUM_DEFAULT, getDefaultLog(), requireMin(0));
    }
    static void setMaxCachedSegmentNum(RaftProperties properties, int maxCachedSegmentNum) {
      setInt(properties::setInt, SEGMENT_CACHE_MAX_NUM_KEY, maxCachedSegmentNum);
    }

    String PREALLOCATED_SIZE_KEY = PREFIX + ".preallocated.size";
    SizeInBytes PREALLOCATED_SIZE_DEFAULT = SizeInBytes.valueOf("4MB");
    static SizeInBytes preallocatedSize(RaftProperties properties) {
      return getSizeInBytes(properties::getSizeInBytes,
          PREALLOCATED_SIZE_KEY, PREALLOCATED_SIZE_DEFAULT, getDefaultLog());
    }
    static void setPreallocatedSize(RaftProperties properties, SizeInBytes preallocatedSize) {
      setSizeInBytes(properties::set, PREALLOCATED_SIZE_KEY, preallocatedSize);
    }

    String WRITE_BUFFER_SIZE_KEY = PREFIX + ".write.buffer.size";
    SizeInBytes WRITE_BUFFER_SIZE_DEFAULT =SizeInBytes.valueOf("64KB");
    static SizeInBytes writeBufferSize(RaftProperties properties) {
      return getSizeInBytes(properties::getSizeInBytes,
          WRITE_BUFFER_SIZE_KEY, WRITE_BUFFER_SIZE_DEFAULT, getDefaultLog());
    }
    static void setWriteBufferSize(RaftProperties properties, SizeInBytes writeBufferSize) {
      setSizeInBytes(properties::set, WRITE_BUFFER_SIZE_KEY, writeBufferSize);
    }

    String FORCE_SYNC_NUM_KEY = PREFIX + ".force.sync.num";
    int FORCE_SYNC_NUM_DEFAULT = 128;
    static int forceSyncNum(RaftProperties properties) {
      return getInt(properties::getInt,
          FORCE_SYNC_NUM_KEY, FORCE_SYNC_NUM_DEFAULT, getDefaultLog(), requireMin(0));
    }
    static void setForceSyncNum(RaftProperties properties, int forceSyncNum) {
      setInt(properties::setInt, FORCE_SYNC_NUM_KEY, forceSyncNum);
    }

    interface StateMachineData {
      String PREFIX = Log.PREFIX + ".statemachine.data";

      String SYNC_KEY = PREFIX + ".sync";
      boolean SYNC_DEFAULT = true;
      static boolean sync(RaftProperties properties) {
        return getBoolean(properties::getBoolean,
            SYNC_KEY, SYNC_DEFAULT, getDefaultLog());
      }
      static void setSync(RaftProperties properties, boolean sync) {
        setBoolean(properties::setBoolean, SYNC_KEY, sync);
      }
      String CACHING_ENABLED_KEY = PREFIX + ".caching.enabled";
      boolean CACHING_ENABLED_DEFAULT = false;
      static boolean cachingEnabled(RaftProperties properties) {
        return getBoolean(properties::getBoolean,
            CACHING_ENABLED_KEY, CACHING_ENABLED_DEFAULT, getDefaultLog());
      }
      static void setCachingEnabled(RaftProperties properties, boolean enable) {
        setBoolean(properties::setBoolean, CACHING_ENABLED_KEY, enable);
      }

      String SYNC_TIMEOUT_KEY = PREFIX + ".sync.timeout";
      TimeDuration SYNC_TIMEOUT_DEFAULT = TimeDuration.valueOf(10, TimeUnit.SECONDS);
      static TimeDuration syncTimeout(RaftProperties properties) {
        return getTimeDuration(properties.getTimeDuration(SYNC_TIMEOUT_DEFAULT.getUnit()),
            SYNC_TIMEOUT_KEY, SYNC_TIMEOUT_DEFAULT, getDefaultLog());
      }
      static void setSyncTimeout(RaftProperties properties, TimeDuration syncTimeout) {
        setTimeDuration(properties::setTimeDuration, SYNC_TIMEOUT_KEY, syncTimeout);
      }

      /**
       * -1: retry indefinitely
       *  0: no retry
       * >0: the number of retries
       */
      String SYNC_TIMEOUT_RETRY_KEY = PREFIX + ".sync.timeout.retry";
      int SYNC_TIMEOUT_RETRY_DEFAULT = -1;
      static int syncTimeoutRetry(RaftProperties properties) {
        return getInt(properties::getInt, SYNC_TIMEOUT_RETRY_KEY, SYNC_TIMEOUT_RETRY_DEFAULT, getDefaultLog(),
            requireMin(-1));
      }
      static void setSyncTimeoutRetry(RaftProperties properties, int syncTimeoutRetry) {
        setInt(properties::setInt, SYNC_TIMEOUT_RETRY_KEY, syncTimeoutRetry, requireMin(-1));
      }
    }

    interface Appender {
      String PREFIX = Log.PREFIX + ".appender";

      String BUFFER_ELEMENT_LIMIT_KEY = PREFIX + ".buffer.element-limit";
      /** 0 means no limit. */
      int BUFFER_ELEMENT_LIMIT_DEFAULT = 0;
      static int bufferElementLimit(RaftProperties properties) {
        return getInt(properties::getInt,
            BUFFER_ELEMENT_LIMIT_KEY, BUFFER_ELEMENT_LIMIT_DEFAULT, getDefaultLog(), requireMin(0));
      }
      static void setBufferElementLimit(RaftProperties properties, int bufferElementLimit) {
        setInt(properties::setInt, BUFFER_ELEMENT_LIMIT_KEY, bufferElementLimit);
      }

      String BUFFER_BYTE_LIMIT_KEY = PREFIX + ".buffer.byte-limit";
      SizeInBytes BUFFER_BYTE_LIMIT_DEFAULT = SizeInBytes.valueOf("4MB");
      static SizeInBytes bufferByteLimit(RaftProperties properties) {
        return getSizeInBytes(properties::getSizeInBytes,
            BUFFER_BYTE_LIMIT_KEY, BUFFER_BYTE_LIMIT_DEFAULT, getDefaultLog());
      }
      static void setBufferByteLimit(RaftProperties properties, SizeInBytes bufferByteLimit) {
        setSizeInBytes(properties::set, BUFFER_BYTE_LIMIT_KEY, bufferByteLimit);
      }

      String SNAPSHOT_CHUNK_SIZE_MAX_KEY = PREFIX + ".snapshot.chunk.size.max";
      SizeInBytes SNAPSHOT_CHUNK_SIZE_MAX_DEFAULT =SizeInBytes.valueOf("16MB");
      static SizeInBytes snapshotChunkSizeMax(RaftProperties properties) {
        return getSizeInBytes(properties::getSizeInBytes,
            SNAPSHOT_CHUNK_SIZE_MAX_KEY, SNAPSHOT_CHUNK_SIZE_MAX_DEFAULT, getDefaultLog());
      }
      static void setSnapshotChunkSizeMax(RaftProperties properties, SizeInBytes maxChunkSize) {
        setSizeInBytes(properties::set, SNAPSHOT_CHUNK_SIZE_MAX_KEY, maxChunkSize);
      }
    }
  }

  interface Snapshot {
    String PREFIX = RaftServerConfigKeys.PREFIX + ".snapshot";

    /** whether trigger snapshot when log size exceeds limit */
    String AUTO_TRIGGER_ENABLED_KEY = PREFIX + ".auto.trigger.enabled";
    /** by default let the state machine to decide when to do checkpoint */
    boolean AUTO_TRIGGER_ENABLED_DEFAULT = false;
    static boolean autoTriggerEnabled(RaftProperties properties) {
      return getBoolean(properties::getBoolean,
          AUTO_TRIGGER_ENABLED_KEY, AUTO_TRIGGER_ENABLED_DEFAULT, getDefaultLog());
    }
    static void setAutoTriggerEnabled(RaftProperties properties, boolean autoTriggerThreshold) {
      setBoolean(properties::setBoolean, AUTO_TRIGGER_ENABLED_KEY, autoTriggerThreshold);
    }

    /** log size limit (in number of log entries) that triggers the snapshot */
    String AUTO_TRIGGER_THRESHOLD_KEY = PREFIX + ".auto.trigger.threshold";
    long AUTO_TRIGGER_THRESHOLD_DEFAULT = 400000L;
    static long autoTriggerThreshold(RaftProperties properties) {
      return getLong(properties::getLong,
          AUTO_TRIGGER_THRESHOLD_KEY, AUTO_TRIGGER_THRESHOLD_DEFAULT, getDefaultLog(), requireMin(0L));
    }
    static void setAutoTriggerThreshold(RaftProperties properties, long autoTriggerThreshold) {
      setLong(properties::setLong, AUTO_TRIGGER_THRESHOLD_KEY, autoTriggerThreshold);
    }
  }

  /** server rpc timeout related */
  interface Rpc {
    String PREFIX = RaftServerConfigKeys.PREFIX + ".rpc";

    String TIMEOUT_MIN_KEY = PREFIX + ".timeout.min";
    TimeDuration TIMEOUT_MIN_DEFAULT = TimeDuration.valueOf(150, TimeUnit.MILLISECONDS);
    static TimeDuration timeoutMin(RaftProperties properties) {
      return getTimeDuration(properties.getTimeDuration(TIMEOUT_MIN_DEFAULT.getUnit()),
          TIMEOUT_MIN_KEY, TIMEOUT_MIN_DEFAULT, getDefaultLog());
    }
    static void setTimeoutMin(RaftProperties properties, TimeDuration minDuration) {
      setTimeDuration(properties::setTimeDuration, TIMEOUT_MIN_KEY, minDuration);
    }

    String TIMEOUT_MAX_KEY = PREFIX + ".timeout.max";
    TimeDuration TIMEOUT_MAX_DEFAULT = TimeDuration.valueOf(300, TimeUnit.MILLISECONDS);
    static TimeDuration timeoutMax(RaftProperties properties) {
      return getTimeDuration(properties.getTimeDuration(TIMEOUT_MAX_DEFAULT.getUnit()),
          TIMEOUT_MAX_KEY, TIMEOUT_MAX_DEFAULT, getDefaultLog());
    }
    static void setTimeoutMax(RaftProperties properties, TimeDuration maxDuration) {
      setTimeDuration(properties::setTimeDuration, TIMEOUT_MAX_KEY, maxDuration);
    }

    String REQUEST_TIMEOUT_KEY = PREFIX + ".request.timeout";
    TimeDuration REQUEST_TIMEOUT_DEFAULT = TimeDuration.valueOf(3000, TimeUnit.MILLISECONDS);
    static TimeDuration requestTimeout(RaftProperties properties) {
      return getTimeDuration(properties.getTimeDuration(REQUEST_TIMEOUT_DEFAULT.getUnit()),
          REQUEST_TIMEOUT_KEY, REQUEST_TIMEOUT_DEFAULT, getDefaultLog());
    }
    static void setRequestTimeout(RaftProperties properties, TimeDuration timeoutDuration) {
      setTimeDuration(properties::setTimeDuration, REQUEST_TIMEOUT_KEY, timeoutDuration);
    }

    String SLEEP_TIME_KEY = PREFIX + ".sleep.time";
    TimeDuration SLEEP_TIME_DEFAULT = TimeDuration.valueOf(25, TimeUnit.MILLISECONDS);
    static TimeDuration sleepTime(RaftProperties properties) {
      return getTimeDuration(properties.getTimeDuration(SLEEP_TIME_DEFAULT.getUnit()),
          SLEEP_TIME_KEY, SLEEP_TIME_DEFAULT, getDefaultLog());
    }
    static void setSleepTime(RaftProperties properties, TimeDuration sleepTime) {
      setTimeDuration(properties::setTimeDuration, SLEEP_TIME_KEY, sleepTime);
    }

    String SLOWNESS_TIMEOUT_KEY = PREFIX + "slowness.timeout";
    TimeDuration SLOWNESS_TIMEOUT_DEFAULT = TimeDuration.valueOf(60, TimeUnit.SECONDS);
    static TimeDuration slownessTimeout(RaftProperties properties) {
      return getTimeDuration(properties.getTimeDuration(SLOWNESS_TIMEOUT_DEFAULT.getUnit()),
          SLOWNESS_TIMEOUT_KEY, SLOWNESS_TIMEOUT_DEFAULT, getDefaultLog());
    }
    static void setSlownessTimeout(RaftProperties properties, TimeDuration expiryTime) {
      setTimeDuration(properties::setTimeDuration, SLOWNESS_TIMEOUT_KEY, expiryTime);
    }
  }

  /** server retry cache related */
  interface RetryCache {
    String PREFIX = RaftServerConfigKeys.PREFIX + ".retrycache";

    String EXPIRY_TIME_KEY = PREFIX + ".expirytime";
    TimeDuration EXPIRY_TIME_DEFAULT = TimeDuration.valueOf(60, TimeUnit.SECONDS);
    static TimeDuration expiryTime(RaftProperties properties) {
      return getTimeDuration(properties.getTimeDuration(EXPIRY_TIME_DEFAULT.getUnit()),
          EXPIRY_TIME_KEY, EXPIRY_TIME_DEFAULT, getDefaultLog());
    }
    static void setExpiryTime(RaftProperties properties, TimeDuration expiryTime) {
      setTimeDuration(properties::setTimeDuration, EXPIRY_TIME_KEY, expiryTime);
    }
  }

  static void main(String[] args) {
    printAll(RaftServerConfigKeys.class);
  }
}
