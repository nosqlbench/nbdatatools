package io.nosqlbench.nbvectors.commands.jjq.nbfunctions;

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
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.nosqlbench.nbvectors.commands.jjq.apis.NBBaseJQFunction;
import io.nosqlbench.nbvectors.commands.jjq.apis.NBStateContext;
import io.nosqlbench.nbvectors.commands.jjq.contexts.NBEnumContext;
import io.nosqlbench.nbvectors.commands.jjq.contexts.NBIdEnumerator;
import net.thisptr.jackson.jq.BuiltinFunction;
import net.thisptr.jackson.jq.Expression;
import net.thisptr.jackson.jq.PathOutput;
import net.thisptr.jackson.jq.Scope;
import net.thisptr.jackson.jq.Version;
import net.thisptr.jackson.jq.exception.JsonQueryException;
import net.thisptr.jackson.jq.path.Path;

import java.util.List;
import java.util.Map;
/// an implementation of a jjq function `nbenum("fieldName")`
/// This enumerates distinct values in the order of presentation
/// in the `enumerator_context` context variable but seems to do nothing else yet.
/// ---
/// This does not instance its state per function, and this needs to be fixed.
@BuiltinFunction({"nbenum/1"})
public class nbenum extends NBBaseJQFunction {
  private NBIdEnumerator enumer;
  private String dirpath;
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
    if (in.isNull() || in.isEmpty()) {
      return;
    }
    if (in.has(fieldName)) {
      System.err.println(
          "object already contains enumerated field '" + fieldName + "', all " + "fields:");
      in.fieldNames().forEachRemaining(System.err::println);
    }
    long value = enumer.getAsLong();
    JsonNode out = in;
    if (out instanceof ObjectNode onode) {
      onode.set(fieldName, new LongNode(value));
    } else {
      throw new RuntimeException(
          "Unable to modify node of type '" + out.getClass().getCanonicalName());
    }

    output.emit(out, path);
  }

  /// {@inheritDoc}
  @Override
  public void start(Scope scope, List<Expression> args, JsonNode in, NBStateContext nbctx)
      throws JsonQueryException
  {
    args.getFirst().apply(
        scope, in, (fieldname) -> {
          System.out.println("enumerating fieldname:" + fieldname);
          this.fieldName = fieldname.asText();
        }
    );

    Map<String, Object> state = getState();
    this.enumer = (NBIdEnumerator) state.computeIfAbsent(
        "enumerator_context",
        k -> new NBEnumContext(fieldName)
    );
  }
}
