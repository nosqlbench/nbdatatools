package io.nosqlbench.nbvectors.jjq.apis;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public interface NBStateContext extends AutoCloseable {
  void register(NBBaseJQFunction f);

  List<NBBaseJQFunction> getRegisteredFunctions();

  ConcurrentHashMap<String, Object> getState();

  void registerShutdownHook(StatefulShutdown statefulShutdown);
}
