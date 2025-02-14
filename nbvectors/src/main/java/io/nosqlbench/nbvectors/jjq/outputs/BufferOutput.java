package io.nosqlbench.nbvectors.jjq.outputs;

import com.fasterxml.jackson.databind.JsonNode;
import net.thisptr.jackson.jq.Output;
import net.thisptr.jackson.jq.exception.JsonQueryException;
import net.thisptr.jackson.jq.path.Path;

import java.util.concurrent.LinkedBlockingDeque;

public class BufferOutput implements Output {
  private final LinkedBlockingDeque<JsonNode> buffer;

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

  public LinkedBlockingDeque<JsonNode> getResultStream() {
    return this.buffer;
  }
}
