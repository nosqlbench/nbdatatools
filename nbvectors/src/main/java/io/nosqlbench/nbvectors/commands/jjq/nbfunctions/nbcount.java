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
import io.nosqlbench.nbvectors.commands.jjq.apis.NBBaseJQFunction;
import io.nosqlbench.nbvectors.commands.jjq.apis.NBStateContext;
import net.thisptr.jackson.jq.BuiltinFunction;
import net.thisptr.jackson.jq.Expression;
import net.thisptr.jackson.jq.PathOutput;
import net.thisptr.jackson.jq.Scope;
import net.thisptr.jackson.jq.Version;
import net.thisptr.jackson.jq.exception.JsonQueryException;
import net.thisptr.jackson.jq.path.Path;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/// an implementation of a jjq function `nbcount()`
/// This counts the number of objects which are not null
@BuiltinFunction({"nbcount/0"})
public class nbcount extends NBBaseJQFunction {

  /// create a nbcount function
  public nbcount() {
  }

  private AtomicLong counter;
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
    if (!in.isNull()) {
      counter.incrementAndGet();
    }
    output.emit(in,path);
  }

  @Override
  public void start(Scope scope, List<Expression> args, JsonNode in, NBStateContext nbctx) {
    Map<String, Object> state = getState();
    this.counter = (AtomicLong) state.computeIfAbsent("nbcount_count",k -> new AtomicLong());
  }

  @Override
  public void shutdown() {
    System.out.println("found " + counter.get() + " objects");
  }
}
