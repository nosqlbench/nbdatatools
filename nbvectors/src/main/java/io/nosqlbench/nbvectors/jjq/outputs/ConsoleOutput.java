package io.nosqlbench.nbvectors.jjq.outputs;

import com.fasterxml.jackson.databind.JsonNode;
import net.thisptr.jackson.jq.Output;
import net.thisptr.jackson.jq.exception.JsonQueryException;

public class ConsoleOutput implements Output  {

  @Override
  public void emit(JsonNode out) throws JsonQueryException {
    System.out.println(out);
  }

}
