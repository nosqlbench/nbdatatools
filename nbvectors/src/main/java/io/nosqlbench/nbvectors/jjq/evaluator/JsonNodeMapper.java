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
      JsonNode jsonNode = MAPPER.readTree(s);
      if (jsonNode.isEmpty()) {
        System.out.println("EMPTY NODE from '" + s + "'");
      }
      return jsonNode;
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Error parsing JSON:" + e);
    }
  }
}
