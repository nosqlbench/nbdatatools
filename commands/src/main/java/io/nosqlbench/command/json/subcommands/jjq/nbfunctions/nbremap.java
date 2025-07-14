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
import com.fasterxml.jackson.databind.node.LongNode;
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

/// an implementation of a jjq function `nbremap("fieldName";"remapfile.json)`
/// The changes the value of the provided field name in the JSON stream to
/// the index ordinal from the provided remap file.
@BuiltinFunction({"nbremap/2"})
public class nbremap extends NBBaseJQFunction {
  private NBIdMapper mapper;
  private String file;
  private String dirpath;
  private String fieldName;

  /// {@inheritDoc}
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
    if (in.isNull()) {
      return;
    }
    if (!in.isTextual()) {
      throw new RuntimeException("field value must be textual, but it is " + in.toPrettyString());
    }
    long value = mapper.lookupId(fieldName, in.asText());
    LongNode out = LongNode.valueOf(value);
    output.emit(out,path);
  }

  /// {@inheritDoc}
  @Override
  public void start(Scope scope, List<Expression> args, JsonNode in, NBStateContext nbctx) throws JsonQueryException {
    args.get(1).apply(
        scope,in, (path) -> {
          Preconditions.checkArgumentType("nbindex/2", 1, path, JsonNodeType.STRING);
          args.get(0).apply(scope, in, (fieldname) -> {
            System.out.println("path:"+path+", fieldname:"+fieldname);
            this.fieldName = fieldname.asText();
          });

        }
    );
    Expression fileExpr = args.get(1);
    fileExpr.apply(
        scope, in, (n) -> {
          if (!n.isTextual()) {
            throw new RuntimeException("file expr must yield a string");
          } else {
            this.dirpath = n.asText();
          }
        }
    );

    Map<String, Object> state = getState();
    this.mapper =
        (NBIdMapper) state.computeIfAbsent("mapper_context", k -> new NBTriesContext(dirpath));
  }
}
