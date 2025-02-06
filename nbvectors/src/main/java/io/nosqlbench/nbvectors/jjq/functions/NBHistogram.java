package io.nosqlbench.nbvectors.jjq.functions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.auto.service.AutoService;
import io.nosqlbench.nbvectors.jjq.apis.NBJQFunction;
import net.thisptr.jackson.jq.*;
import net.thisptr.jackson.jq.exception.JsonQueryException;
import net.thisptr.jackson.jq.path.Path;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@AutoService(Function.class)
@BuiltinFunction({"nbhistogram/1"})
public class NBHistogram extends NBJQFunction {
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

  }

  @Override
  public void start() {
    Map<String, Object> state = getState();
    this.counts =
        (ConcurrentHashMap<String, AtomicLong>) state.computeIfAbsent(
            "nbhistogram_counts",
            k -> new ConcurrentHashMap<String, AtomicLong>()
        );
  }

  @Override
  public void finish() {
    System.out.println(counts);
  }
}
