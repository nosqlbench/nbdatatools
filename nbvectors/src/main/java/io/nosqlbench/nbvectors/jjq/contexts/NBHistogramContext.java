package io.nosqlbench.nbvectors.jjq.contexts;

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


import io.nosqlbench.nbvectors.jjq.apis.NBStateContext;
import io.nosqlbench.nbvectors.jjq.apis.StatefulShutdown;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class NBHistogramContext extends ConcurrentHashMap<String, AtomicLong>
    implements StatefulShutdown
{
  @Override
  public void shutdown() {
    System.err.println("histogram shutting down:\n" + this.numericSummary());
  }

  public NBHistogramContext registerShutdownHook(NBStateContext ctx) {
    ctx.registerShutdownHook(this);
    return this;
  }

  public String numericSummary() {
    List<entry> ids = new ArrayList<>();
    this.forEach((k, v) -> {
      ids.add(new entry(Long.parseLong(k), v.get()));
    });
    Collections.sort(ids, Comparator.comparingLong(entry::freq).reversed());
    StringBuilder sb = new StringBuilder();

    for (entry id : ids) {
      sb.append(String.format("% 20d", id.id())).append(": ")
          .append(String.format("% 15d", id.freq)).append("\n");
    }
    sb.append("base frequencies:\n")
        .append(ids.stream().map(s -> String.valueOf(s.freq)).collect(Collectors.joining(",")));
    return sb.toString();
  }

  public record entry(long id, long freq) {
  }

}
