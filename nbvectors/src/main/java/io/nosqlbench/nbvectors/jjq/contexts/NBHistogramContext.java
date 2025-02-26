package io.nosqlbench.nbvectors.jjq.contexts;

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
