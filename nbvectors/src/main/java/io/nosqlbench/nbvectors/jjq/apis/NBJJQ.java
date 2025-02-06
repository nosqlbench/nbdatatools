package io.nosqlbench.nbvectors.jjq.apis;

import net.thisptr.jackson.jq.Function;
import net.thisptr.jackson.jq.Scope;

import java.util.List;
import java.util.Map;

public class NBJJQ {
  public static Map<String, Object> getState(Scope scope) {
    Function nbs = scope.getFunction("nbstate", 0);
    if (nbs instanceof NBStateFunction nbsf) {
      return (Map<String, Object>) nbsf.getState();
    } else {
      throw new RuntimeException("error loading function named nbstate for state map");
    }
  }

  public static void register(NBJQFunction f, Scope scope) {
    Function nbs = scope.getFunction("nbstate", 0);
    if (nbs instanceof NBStateFunction nbsf) {
      nbsf.register(f);
    } else {
      throw new RuntimeException("error loading function named nbstate for function registration");
    }
  }

  public static List<NBJQFunction> getRegisteredFunctions(Scope scope) {
    Function nbs = scope.getFunction("nbstate", 0);
    if (nbs instanceof NBStateFunction nbsf) {
      return nbsf.getRegisteredFunctions();
    }
    throw new RuntimeException("error loading function named nbstate for function registration");

  }

}
