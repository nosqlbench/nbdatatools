package io.nosqlbench.command.jjq.evaluator;

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
import net.thisptr.jackson.jq.JsonQuery;
import net.thisptr.jackson.jq.Output;
import net.thisptr.jackson.jq.Scope;

import java.util.function.Function;
import java.util.function.Supplier;

/// A simple runnable implementation of a jq processor
public class JqProc implements Runnable {

  private final Scope scope;
  private final Supplier<String> jsonlSource;
  private final Function<String, JsonNode> mapper;
  private final JsonQuery query;
  private final Output output;
  private final String id;

  /// create a jq processor
  /// @param id a label for this processor, for debugging purposes
  /// @param scope the jq scope to use for processing
  /// @param jsonlSource the source of jsonl data to process
  /// @param mapper a function to convert jsonl to json nodes
  /// @param query the jq query to apply to the json nodes
  /// @param output the output to write results to
  public JqProc(
      String id,
      Scope scope,
      Supplier<String> jsonlSource,
      Function<String, JsonNode> mapper,
      JsonQuery query,
      Output output
  )
  {
    this.id = id;
    this.scope = scope;
    this.jsonlSource = jsonlSource;
    this.mapper = mapper;
    this.query = query;
    this.output = output;
  }

  @Override
  public void run() {
    JsonNode node;
    try {
      int count = 0;
      String inputJson = null;
      while ((inputJson = jsonlSource.get()) != null) {
        count++;
        try {
          node = mapper.apply(inputJson);
        } catch (Exception e) {
          throw new RuntimeException("error parsing input:\n>>" + inputJson + "\n>>\n");
        }
        try {
          query.apply(scope, node, output);
        } catch (JsonProcessingException e) {
          throw new RuntimeException(e);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

  }

}
