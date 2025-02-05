package io.nosqlbench.nbvectors.jjq.outputs;

import com.fasterxml.jackson.databind.JsonNode;
import net.thisptr.jackson.jq.Output;
import net.thisptr.jackson.jq.exception.JsonQueryException;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class NullOutput implements Output  {
  @Override
  public void emit(JsonNode out) throws JsonQueryException {
  }
}
