package io.nosqlbench.nbvectors.jjq.apis;

import com.fasterxml.jackson.databind.JsonNode;
import io.nosqlbench.nbvectors.jjq.functions.mappers.StatefulShutdown;
import net.thisptr.jackson.jq.*;
import net.thisptr.jackson.jq.exception.JsonQueryException;
import net.thisptr.jackson.jq.path.Path;

import java.util.List;
import java.util.Map;

public abstract class NBBaseJQFunction implements Function, StatefulShutdown {
  private boolean registered = false;
  private Map<String, Object> state;

  @Override
  public final void apply(
      Scope scope,
      List<Expression> args,
      JsonNode in,
      Path path,
      PathOutput output,
      Version version
  ) throws JsonQueryException
  {
    if (!registered) {
      synchronized (this) {
        NBStateContext nbctx = NBJJQ.getContext(scope);
        nbctx.register(this);
        this.state = NBJJQ.getState(scope);
        start(scope, args, in, nbctx);
        this.registered = true;
      }
    }
    doApply(scope, args, in, path, output, version);

  }

  @Override
  public void shutdown() {
    System.out.println("shutting down this " + this);
  }

  public Map<String, Object> getState() {
    return state;
  }

  public abstract void start(Scope scope, List<Expression> args, JsonNode in, NBStateContext nbctx)
      throws JsonQueryException;

  public abstract void doApply(
      Scope scope,
      List<Expression> args,
      JsonNode in,
      Path path,
      PathOutput output,
      Version version
  ) throws JsonQueryException;

  public String getName() {
    BuiltinFunction bifa = this.getClass().getAnnotation(BuiltinFunction.class);
    String[] names = bifa.value();
    return names[0].split("/")[0];
  }

  @Override
  public String toString() {
    return getName();
  }
}
