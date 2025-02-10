package io.nosqlbench.nbvectors.jjq.apis;

import io.nosqlbench.nbvectors.jjq.functions.NBHistogramContext;
import io.nosqlbench.nbvectors.jjq.functions.mappers.StatefulShutdown;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public interface NBStateContext {
  void register(NBBaseJQFunction f);

  List<NBBaseJQFunction> getRegisteredFunctions();

  ConcurrentHashMap<String, Object> getState();

  void registerShutdownHook(StatefulShutdown statefulShutdown);
}
