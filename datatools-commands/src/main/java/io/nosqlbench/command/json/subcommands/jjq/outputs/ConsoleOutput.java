package io.nosqlbench.command.json.subcommands.jjq.outputs;

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


import com.fasterxml.jackson.databind.JsonNode;
import net.thisptr.jackson.jq.Output;
import net.thisptr.jackson.jq.exception.JsonQueryException;

/// An implementation of output which writes all nodes to the console with {@link JsonNode#toString()}
public class ConsoleOutput implements Output  {

  /// create a console output
  public ConsoleOutput() {
  }
  @Override
  public void emit(JsonNode out) throws JsonQueryException {
    System.out.println(out);
  }

}
