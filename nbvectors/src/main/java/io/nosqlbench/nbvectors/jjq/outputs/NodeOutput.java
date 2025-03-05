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

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/// An implementation of output which buffers results in a blocking queue.
public class NodeOutput implements Output  {

  private final BlockingQueue<JsonNode> bq;

  /// create a node output
  /// @param limit the maximum number of nodes to buffer
  public NodeOutput(int limit) {
    this.bq = new ArrayBlockingQueue<>(limit);
  }

  /// {@inheritDoc}
  @Override
  public void emit(JsonNode out) throws JsonQueryException {
    try {
      bq.put(out);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /// Take the next node from the queue
  /// @return the next node
  public JsonNode take() {
    try {
      return bq.take();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /// Determine whether the queue is empty
  /// @return true if the queue is empty
  public boolean isEmpty() {
    return bq.isEmpty();
  }
}
