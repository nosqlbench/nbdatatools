package io.nosqlbench.command.json.subcommands.jjq.nbfunctions;

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
import com.fasterxml.jackson.databind.node.JsonNodeType;
import io.nosqlbench.command.json.subcommands.jjq.apis.NBBaseJQFunction;
import io.nosqlbench.command.json.subcommands.jjq.apis.NBStateContext;
import io.nosqlbench.command.json.subcommands.jjq.contexts.NBIdMapper;
import io.nosqlbench.command.json.subcommands.jjq.contexts.NBTriesContext;
import net.thisptr.jackson.jq.BuiltinFunction;
import net.thisptr.jackson.jq.Expression;
import net.thisptr.jackson.jq.PathOutput;
import net.thisptr.jackson.jq.Scope;
import net.thisptr.jackson.jq.Version;
import net.thisptr.jackson.jq.exception.JsonQueryException;
import net.thisptr.jackson.jq.internal.misc.Preconditions;
import net.thisptr.jackson.jq.path.Path;

import java.util.List;
import java.util.Map;

/// an implementation of a jjq function `nbindex("fieldname";"statefile.json")`
/// which will index the given fieldname in the JSON object stream,
/// recording the enumerated index in the provided state file.
/// ---
/// The state for this should be instanced according to the user's requirements,
/// and this needs to be made more flexible.
@BuiltinFunction({"nbindex/2"})
public class nbindex extends NBBaseJQFunction {
  private NBIdMapper mapper;
  private String filepath;
  private String fieldName;

  @Override
  public void doApply(
      Scope scope,
      List<Expression> args,
      JsonNode in,
      Path path,
      PathOutput output,
      Version version
  ) throws JsonQueryException
  {
    if (in.isNull()||in.isEmpty()) {
      return;
    }
    if (in.has(fieldName)) {
      JsonNode node = in.get(fieldName);
      mapper.addInstance(fieldName, node.asText());
    }
    output.emit(in, path);
  }

  @Override
  public void start(Scope scope, List<Expression> args, JsonNode in, NBStateContext nbctx)
      throws JsonQueryException
  {
    args.get(1).apply(
        scope, in, (path) -> {
          Preconditions.checkArgumentType("nbindex/2", 1, path, JsonNodeType.STRING);
          args.get(0).apply(
              scope, in, (expr) -> {
                Preconditions.checkArgumentType("nbindex/2", 2, path, JsonNodeType.STRING);
                this.fieldName = expr.asText();
              }
          );
        }
    );
    Expression fileExpr = args.get(1);
    fileExpr.apply(
        scope, in, (n) -> {
          if (!n.isTextual()) {
            throw new RuntimeException("file expr must yield a string");
          } else {
            this.filepath = n.asText();
          }
        }
    );

    Map<String, Object> state = getState();
    this.mapper = (NBIdMapper) state.computeIfAbsent(
        "mapper_context",
        k -> new NBTriesContext(this.filepath).registerShutdownHook(nbctx)
    );
  }

}
