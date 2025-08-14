package io.nosqlbench.command.json.subcommands.jjq.apis;

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
import io.nosqlbench.command.json.subcommands.jjq.CMD_jjq;
import net.thisptr.jackson.jq.*;
import net.thisptr.jackson.jq.exception.JsonQueryException;
import net.thisptr.jackson.jq.path.Path;

import java.util.List;
import java.util.Map;

/// A base type for building new functions for jackson-jq within the
/// {@link CMD_jjq} codebase
public abstract class NBBaseJQFunction implements Function, StatefulShutdown {
  private boolean registered = false;
  private Map<String, Object> state;

  /// The state context for this function
  protected NBStateContext context;

  /// apply the function to an incoming [JsonNode]
  @Override
  public final void apply(
      Scope scope,
      List<Expression> args,
      JsonNode in,
      Path path,
      PathOutput output,
      Version version
  ) throws JsonQueryException
  {
    if (!registered) {
      synchronized (this) {
        this.context = NBJJQ.getContext(scope);
        context.register(this);
        this.state = NBJJQ.getState(scope);
        start(scope, args, in, context);
        this.registered = true;
      }
    }
    doApply(scope, args, in, path, output, version);

  }

  /// {@inheritDoc}
  @Override
  public void shutdown() {
    System.err.println("shutting down this " + this);
  }

  /// get the state map for this function
  /// @return the state map for this function
  public Map<String, Object> getState() {
    return state;
  }

  /// Implement this method to have an _exactly-once_ initializer.
  /// This is called once per function instance before it processes its first node.
  /// @param scope the jq scope
  /// @param args the arguments to the function
  /// @param in the input node
  /// @param nbctx the state context
  /// @throws JsonQueryException when any error occurs during node processing
  public abstract void start(Scope scope, List<Expression> args, JsonNode in, NBStateContext nbctx)
      throws JsonQueryException;

  /// This is the actual function implementation. It is applied to each incoming node.
  /// @param scope the jq scope
  /// @param args the arguments to the function
  /// @param in the input node
  /// @param path the jq path
  /// @param output the output to write results to
  /// @param version the jq version
  /// @throws JsonQueryException when any error occurs during node processing
  public abstract void doApply(
      Scope scope,
      List<Expression> args,
      JsonNode in,
      Path path,
      PathOutput output,
      Version version
  ) throws JsonQueryException;

  /// get the name of this function
  /// @return the name of this function
  public String getName() {
    BuiltinFunction bifa = this.getClass().getAnnotation(BuiltinFunction.class);
    String[] names = bifa.value();
    return names[0].split("/")[0];
  }

  @Override
  public String toString() {
    return getName();
  }
}
