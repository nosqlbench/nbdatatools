package io.nosqlbench.nbvectors.jjq.nbfunctions;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.auto.service.AutoService;
import io.nosqlbench.nbvectors.jjq.apis.NBBaseJQFunction;
import io.nosqlbench.nbvectors.jjq.apis.NBStateContext;
import io.nosqlbench.nbvectors.jjq.contexts.NBHistogramContext;
import net.thisptr.jackson.jq.*;
import net.thisptr.jackson.jq.exception.JsonQueryException;
import net.thisptr.jackson.jq.path.Path;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@AutoService(Function.class)
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

  @Override
  public void start(Scope scope, List<Expression> args, JsonNode in, NBStateContext nbctx) {
    Map<String, Object> state = getState();
    this.counts =
        (ConcurrentHashMap<String, AtomicLong>) state.computeIfAbsent(
            "nbhistogram_counts",
            k -> new NBHistogramContext().registerShutdownHook(nbctx)
        );
  }

  @Override
  public void shutdown() {
    System.out.println(counts);
  }
}
