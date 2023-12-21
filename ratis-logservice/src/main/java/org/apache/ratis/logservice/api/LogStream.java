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
package org.apache.ratis.logservice.api;

import java.io.IOException;
import java.util.Set;

/**
 * A distributed log with "infinite" length that supports reads and writes.
 */
public interface LogStream {

  /**
   * An enumeration that defines the current state of a LogStream
   */
  public enum State {
    OPEN,
    CLOSED;
  }

  /**
   * Returns the unique name to identify this log.
   */
  LogName getName();

  /**
   * Returns the current state of this log.
   */
  State getState();

  /**
   * Returns the size of this LogStream in bytes.
   */
  long getSize();

  /**
   * Creates a reader to read this LogStream.
   *
   * @return A synchronous reader
   */
  LogReader createReader();

  /**
   * Creates a write to write to this LogStream.
   *
   * @return A synchronous writer
   */
  LogWriter createWriter();

  /**
   * Returns the recordId of the last record in this LogStream. For an empty log, the recordId is {@code 0}.
   */
  long getLastRecordId();

  /**
   * Returns all {@link RecordListeners} for this LogStream.
   */
  Set<RecordListener> getRecordListeners();

  /**
   * Returns a copy of the Configuration for this LogStream.
   */
  LogStreamConfiguration getConfiguration();
}
