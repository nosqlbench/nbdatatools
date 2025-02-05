package io.nosqlbench.nbvectors.jjq.functions;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.auto.service.AutoService;
import io.nosqlbench.nbvectors.jjq.NBJJQ;
import net.thisptr.jackson.jq.*;
import net.thisptr.jackson.jq.exception.JsonQueryException;
import net.thisptr.jackson.jq.path.Path;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

@AutoService(Function.class)
@BuiltinFunction({"nbcount/1","nbcount/0"})
public class NBCount extends NBJQFunction  {
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
    counter.incrementAndGet();
  }

  @Override
  public void start() {
    Map<String, Object> state = getState();
    this.counter = (AtomicLong) state.computeIfAbsent("nbcount_count",k -> new AtomicLong());
  }

  @Override
  public void finish() {
    System.out.println("found " + counter.get() + " objects");
  }
}
