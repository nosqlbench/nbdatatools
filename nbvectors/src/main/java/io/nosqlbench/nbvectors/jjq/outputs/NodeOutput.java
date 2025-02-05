package io.nosqlbench.nbvectors.jjq.outputs;

import com.fasterxml.jackson.databind.JsonNode;
import net.thisptr.jackson.jq.Output;
import net.thisptr.jackson.jq.exception.JsonQueryException;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class NodeOutput implements Output  {

  private final BlockingQueue<JsonNode> bq;

  public NodeOutput(int limit) {
    this.bq = new ArrayBlockingQueue<>(limit);
  }

  @Override
  public void emit(JsonNode out) throws JsonQueryException {
    try {
      bq.put(out);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public JsonNode take() {
    try {
      return bq.take();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
  public boolean isEmpty() {
    return bq.isEmpty();
  }
}
