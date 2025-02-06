package io.nosqlbench.nbvectors.jjq.apis;

import com.fasterxml.jackson.databind.JsonNode;
import net.thisptr.jackson.jq.*;
import net.thisptr.jackson.jq.exception.JsonQueryException;
import net.thisptr.jackson.jq.path.Path;

import java.util.List;
import java.util.Map;

public abstract class NBJQFunction implements Function {
  private boolean registered = false;
  private Map<String,Object> state;
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
      synchronized(this) {
        NBJJQ.register(this,scope);
        this.state = NBJJQ.getState(scope);
        start();
        this.registered=true;
      }
    }
    doApply(scope,args,in,path,output,version);

  }

  public void finish() {

  };
  public Map<String, Object> getState() {
    return state;
  }

  public abstract void start();
  public abstract void doApply (
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
