package io.nosqlbench.nbvectors.jsonalyze;

import com.fasterxml.jackson.databind.JsonNode;
import net.thisptr.jackson.jq.Output;
import net.thisptr.jackson.jq.exception.JsonQueryException;

import java.util.ArrayList;
import java.util.List;

public class NodeOutput implements Output  {
  public final List<JsonNode> output = new ArrayList<>();

  @Override
  public void emit(JsonNode out) throws JsonQueryException {
    output.add(out);
  }
}
