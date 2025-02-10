package io.nosqlbench.nbvectors.jjq.apis;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.auto.service.AutoService;
import io.nosqlbench.nbvectors.jjq.functions.NBHistogramContext;
import io.nosqlbench.nbvectors.jjq.functions.mappers.StatefulShutdown;
import net.thisptr.jackson.jq.*;
import net.thisptr.jackson.jq.exception.JsonQueryException;
import net.thisptr.jackson.jq.path.Path;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@AutoService(Function.class)
@BuiltinFunction("nbstate/0")
public class NBStateContextHolderHack implements NBStateContext, Function, AutoCloseable {
  private final Deque<StatefulShutdown> statefulShutdowns = new ArrayDeque<>();
  private final ConcurrentHashMap<String,Object> state = new ConcurrentHashMap<>();
  private final List<NBBaseJQFunction> registered = new ArrayList();

  @Override
  public void apply(
      Scope scope,
      List<Expression> args,
      JsonNode in,
      Path path,
      PathOutput output,
      Version version
  ) throws JsonQueryException
  {}

  @Override
  public void register(NBBaseJQFunction f) {
    this.registered.add(f);
  }

  @Override
  public List<NBBaseJQFunction> getRegisteredFunctions() {
    return registered;
  }

  @Override
  public ConcurrentHashMap<String, Object> getState() {
    return state;
  }

  @Override
  public synchronized void registerShutdownHook(StatefulShutdown nbHistogramContext) {
    this.statefulShutdowns.add(nbHistogramContext);
  }

  @Override
  public void close() throws Exception {
    for (StatefulShutdown statefulShutdown : statefulShutdowns.reversed()) {
      System.out.println("shutting down " + statefulShutdown);
      statefulShutdown.shutdown();
    }
  }
}
