package io.nosqlbench.nbvectors.jjq.functions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.auto.service.AutoService;
import io.nosqlbench.nbvectors.taghdf.traversal.visitors.HdfCompoundVisitor;
import net.thisptr.jackson.jq.*;
import net.thisptr.jackson.jq.exception.JsonQueryException;
import net.thisptr.jackson.jq.path.Path;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@AutoService(Function.class)
@BuiltinFunction("nbstate/0")
public class NBStateFunction implements Function {
  private final ConcurrentHashMap<String,Object> state = new ConcurrentHashMap<>();
  private final List<NBJQFunction> registered = new ArrayList();

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

  public void register(NBJQFunction f) {
    this.registered.add(f);
  }

  public List<NBJQFunction> getRegisteredFunctions() {
    return registered;
  }

  public ConcurrentHashMap<String, Object> getState() {
    return state;
  }
}
