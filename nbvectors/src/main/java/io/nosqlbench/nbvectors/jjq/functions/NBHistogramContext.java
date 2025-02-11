package io.nosqlbench.nbvectors.jjq.functions;

import io.nosqlbench.nbvectors.jjq.apis.NBStateContext;
import io.nosqlbench.nbvectors.jjq.functions.mappers.StatefulShutdown;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class NBHistogramContext extends ConcurrentHashMap<String, AtomicLong>
    implements StatefulShutdown
{
  @Override
  public void shutdown() {
    System.out.println("histogram shutting down: " + this);
  }
  public NBHistogramContext registerShutdownHook(NBStateContext ctx) {
    ctx.registerShutdownHook(this);
    return this;
  }
}
