package io.nosqlbench.nbvectors.commands.jjq.evaluator;

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


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.function.Function;

/// A function to convert a string to a JSON node
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
