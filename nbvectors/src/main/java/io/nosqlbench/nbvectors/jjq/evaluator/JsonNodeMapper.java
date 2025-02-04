package io.nosqlbench.nbvectors.jjq.evaluator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.function.Function;

public class JsonNodeMapper implements Function<String, JsonNode> {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override
  public JsonNode apply(String s) {
    try {
      return MAPPER.readTree(s);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
