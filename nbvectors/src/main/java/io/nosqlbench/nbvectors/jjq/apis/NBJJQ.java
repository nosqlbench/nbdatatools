package io.nosqlbench.nbvectors.jjq.apis;

import net.thisptr.jackson.jq.Function;
import net.thisptr.jackson.jq.Scope;

import java.util.List;
import java.util.Map;

public class NBJJQ {
  public static synchronized NBStateContext getContext(Scope scope) {
    Function nbs = scope.getFunction("nbstate", 0);
    if (nbs instanceof NBStateContextHolderHack nbsf) {
      return nbsf;
    } else {
      throw new RuntimeException("Missing state holder function");
    }

  }
  public static Map<String, Object> getState(Scope scope) {
    return getContext(scope).getState();
  }

  public synchronized static void register(NBBaseJQFunction f, Scope scope) {
    getContext(scope).register(f);
  }

  public static List<NBBaseJQFunction> getRegisteredFunctions(Scope scope) {
    return getContext(scope).getRegisteredFunctions();
  }

}
