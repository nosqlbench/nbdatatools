package io.nosqlbench.nbvectors.jjq.outputs;

/*
 * Copyright (c) nosqlbench
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import com.fasterxml.jackson.databind.JsonNode;
import net.thisptr.jackson.jq.Output;
import net.thisptr.jackson.jq.exception.JsonQueryException;
import net.thisptr.jackson.jq.path.Path;

import java.util.concurrent.LinkedBlockingDeque;

/// An {@link Output} implementation which simply buffers results in a blocking queue.
/// This can be actively consumed to unblock producers, or it can be oversized for raw buffering.
public class BufferOutput implements Output {
  private final LinkedBlockingDeque<JsonNode> buffer;

  /// create a buffer output
  /// @param bufferSize
  ///     the total buffer size to fill before blocking
  public BufferOutput(int bufferSize) {
    this.buffer = new LinkedBlockingDeque<>(bufferSize);
  }

  @Override
  public void emit(JsonNode out) throws JsonQueryException {
    try {
      buffer.putLast(out);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void emit(JsonNode out, Path opath) throws JsonQueryException {
    try {
      buffer.putLast(out);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /// Get the result stream. Reading from this may unblock upstream writers if the stream is full.
  /// @return the result stream
  public LinkedBlockingDeque<JsonNode> getResultStream() {
    return this.buffer;
  }
}
