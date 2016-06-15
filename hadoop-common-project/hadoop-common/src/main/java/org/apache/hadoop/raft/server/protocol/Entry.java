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
package org.apache.hadoop.raft.server.protocol;

import com.google.common.base.Preconditions;
import org.apache.hadoop.raft.protocol.Message;

public class Entry extends TermIndex {
  public static final Entry[] EMPTY_ARRAY = {};

  public static void validateEntries(long expectedTerm, TermIndex previous,
      Entry... entries) {
    if (entries != null && entries.length > 0) {
      final long index0 = entries[0].getIndex();

      if (previous == null) {
        Preconditions.checkArgument(index0 == 0,
            "Unexpected Index: previous is null but entries[%s].getIndex()=%s",
            0, index0);
      } else {
        Preconditions.checkArgument(previous.getIndex() == index0 - 1,
            "Unexpected Index: previous is %s but entries[%s].getIndex()=%s",
            previous, 0, index0);
      }

      for (int i = 0; i < entries.length; i++) {
        final long t = entries[i].getTerm();
        Preconditions.checkArgument(expectedTerm >= t,
            "Unexpected Term: entries[%s].getTerm()=%s but expectedTerm=%s",
            i, t, expectedTerm);

        final long indexi = entries[i].getIndex();
        Preconditions.checkArgument(indexi == index0 + i, "Unexpected Index: " +
                "entries[%s].getIndex()=%s but entries[0].getIndex()=%s",
            i, indexi, index0);
      }
    }
  }

  private final Message message;

  public Entry(long term, long logIndex, Message message) {
    super(term, logIndex);
    this.message = message;
  }

  public Message getMessage() {
    return message;
  }

  public boolean isConfigurationEntry() {
    return false;
  }
}
