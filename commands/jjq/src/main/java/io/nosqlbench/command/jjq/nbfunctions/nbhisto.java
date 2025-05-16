package io.nosqlbench.command.jjq.nbfunctions;

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
import io.nosqlbench.command.jjq.apis.NBBaseJQFunction;
import io.nosqlbench.command.jjq.apis.NBStateContext;
import io.nosqlbench.command.jjq.contexts.NBHistogramContext;
import net.thisptr.jackson.jq.BuiltinFunction;
import net.thisptr.jackson.jq.Expression;
import net.thisptr.jackson.jq.PathOutput;
import net.thisptr.jackson.jq.Scope;
import net.thisptr.jackson.jq.Version;
import net.thisptr.jackson.jq.exception.JsonQueryException;
import net.thisptr.jackson.jq.path.Path;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/// an implementation of a jjq function `nbhisto("")`
/// This tallies the histogram for all values of the given field
/// in the `nbhistogram_counts` context variable, and then
/// prints them out at shutdown.
/// ---
/// This does not instance its state per call, and this needs to be fixed.
@BuiltinFunction({"nbhisto/1"})
public class nbhisto extends NBBaseJQFunction {
  private ConcurrentHashMap<String, AtomicLong> counts;
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
    if (in.isNull()) {
      return;
    }
    args.get(0).apply(
        scope, in, (n) -> {
          String text = n.asText();
          AtomicLong along = counts.computeIfAbsent(text, t -> new AtomicLong());
          along.incrementAndGet();
        }
    );
    output.emit(in, path);
  }

  /// {@inheritDoc}
  @Override
  public void start(Scope scope, List<Expression> args, JsonNode in, NBStateContext nbctx) {
    Map<String, Object> state = getState();
    this.counts =
        (ConcurrentHashMap<String, AtomicLong>) state.computeIfAbsent(
            "nbhistogram_counts",
            k -> new NBHistogramContext().registerShutdownHook(nbctx)
        );
  }

  /// {@inheritDoc}
  @Override
  public void shutdown() {
    System.out.println(counts);
  }
}
