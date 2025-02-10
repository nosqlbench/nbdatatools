package io.nosqlbench.nbvectors.jjq.functions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.google.auto.service.AutoService;
import io.nosqlbench.nbvectors.jjq.apis.NBBaseJQFunction;
import io.nosqlbench.nbvectors.jjq.apis.NBStateContext;
import io.nosqlbench.nbvectors.jjq.functions.mappers.NBIdMapper;
import io.nosqlbench.nbvectors.jjq.functions.mappers.NBTriesContext;
import net.thisptr.jackson.jq.*;
import net.thisptr.jackson.jq.exception.JsonQueryException;
import net.thisptr.jackson.jq.internal.misc.Preconditions;
import net.thisptr.jackson.jq.path.Path;

import java.util.List;
import java.util.Map;

@AutoService(Function.class)
@BuiltinFunction({"nbindex/2"})
public class NBIndexingFunction extends NBBaseJQFunction {
  private NBIdMapper mapper;
  private String filepath;
  private String fieldName;

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
    if (in.isNull()) {
      return;
    }
    if (in.has(fieldName)) {
      JsonNode node = in.get(fieldName);
      mapper.addInstance(fieldName,node.asText());
    }
    output.emit(in, path);
  }

  @Override
  public void start(Scope scope, List<Expression> args, JsonNode in, NBStateContext nbctx) throws JsonQueryException {
    args.get(1).apply(
        scope,in, (path) -> {
          Preconditions.checkArgumentType("nbindex/2", 1, path, JsonNodeType.STRING);
          args.get(0).apply(scope, in, (expr) -> {
            Preconditions.checkArgumentType("nbindex/2", 2, path, JsonNodeType.STRING);
            this.fieldName = expr.asText();
          });
        }
    );
    Expression fileExpr = args.get(1);
    fileExpr.apply(
        scope, in, (n) -> {
          if (!n.isTextual()) {
            throw new RuntimeException("file expr must yield a string");
          } else {
            this.filepath = n.asText();
          }
        }
    );

    Map<String, Object> state = getState();
    this.mapper =
        (NBIdMapper) state.computeIfAbsent("mapper_context",
            k -> new NBTriesContext(this.filepath).register(nbctx));
  }

}
