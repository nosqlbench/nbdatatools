package io.nosqlbench.nbvectors.jjq.apis;

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
import net.thisptr.jackson.jq.*;
import net.thisptr.jackson.jq.exception.JsonQueryException;
import net.thisptr.jackson.jq.path.Path;

import java.util.List;
import java.util.Map;

public abstract class NBBaseJQFunction implements Function, StatefulShutdown {
  private boolean registered = false;
  private Map<String, Object> state;
  protected NBStateContext context;

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

  @Override
  public void shutdown() {
    System.err.println("shutting down this " + this);
  }

  public Map<String, Object> getState() {
    return state;
  }

  public abstract void start(Scope scope, List<Expression> args, JsonNode in, NBStateContext nbctx)
      throws JsonQueryException;

  public abstract void doApply(
      Scope scope,
      List<Expression> args,
      JsonNode in,
      Path path,
      PathOutput output,
      Version version
  ) throws JsonQueryException;

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
