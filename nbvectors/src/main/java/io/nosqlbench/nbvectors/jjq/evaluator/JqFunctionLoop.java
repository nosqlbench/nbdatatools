package io.nosqlbench.nbvectors.jjq.evaluator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import net.thisptr.jackson.jq.JsonQuery;
import net.thisptr.jackson.jq.Output;
import net.thisptr.jackson.jq.Scope;

import java.util.function.Function;

public class JqFunctionLoop implements Runnable {

  private final Scope scope;
  private final Iterable<String> jsonlSource;
  private final Function<String, JsonNode> mapper;
  private final JsonQuery query;
  private final Output output;

  public JqFunctionLoop(
      Scope scope,
      Iterable<String> jsonlSource,
      Function<String, JsonNode> mapper,
      JsonQuery query,
      Output output
  )
  {
    this.scope = scope;
    this.jsonlSource = jsonlSource;
    this.mapper = mapper;
    this.query = query;
    this.output = output;
  }

  @Override
  public void run() {
    for (String inputJson : jsonlSource) {
      JsonNode node = mapper.apply(inputJson);
      try {
        query.apply(scope, node, output);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }

  }
}
