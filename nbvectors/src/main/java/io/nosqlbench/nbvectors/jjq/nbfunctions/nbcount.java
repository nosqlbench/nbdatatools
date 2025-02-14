package io.nosqlbench.nbvectors.jjq.nbfunctions;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.auto.service.AutoService;
import io.nosqlbench.nbvectors.jjq.apis.NBBaseJQFunction;
import io.nosqlbench.nbvectors.jjq.apis.NBStateContext;
import net.thisptr.jackson.jq.*;
import net.thisptr.jackson.jq.exception.JsonQueryException;
import net.thisptr.jackson.jq.path.Path;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

@AutoService(Function.class)
@BuiltinFunction({"nbcount/0"})
public class nbcount extends NBBaseJQFunction {
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
