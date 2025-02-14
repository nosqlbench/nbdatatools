package io.nosqlbench.nbvectors.jjq.nbfunctions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.auto.service.AutoService;
import io.nosqlbench.nbvectors.jjq.apis.NBBaseJQFunction;
import io.nosqlbench.nbvectors.jjq.apis.NBStateContext;
import io.nosqlbench.nbvectors.jjq.contexts.NBIdMapper;
import io.nosqlbench.nbvectors.jjq.contexts.NBTriesContext;
import net.thisptr.jackson.jq.*;
import net.thisptr.jackson.jq.exception.JsonQueryException;
import net.thisptr.jackson.jq.internal.misc.Preconditions;
import net.thisptr.jackson.jq.path.Path;

import java.util.List;
import java.util.Map;

@AutoService(Function.class)
@BuiltinFunction({"nbremap/2"})
public class nbremap extends NBBaseJQFunction {
  private NBIdMapper mapper;
  private String file;
  private String dirpath;
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
    if (!in.isTextual()) {
      throw new RuntimeException("field value must be textual, but it is " + in.toPrettyString());
    }
    long value = mapper.lookupId(fieldName, in.asText());
    LongNode out = LongNode.valueOf(value);
    output.emit(out,path);
  }

  @Override
  public void start(Scope scope, List<Expression> args, JsonNode in, NBStateContext nbctx) throws JsonQueryException {
    args.get(1).apply(
        scope,in, (path) -> {
          Preconditions.checkArgumentType("nbindex/2", 1, path, JsonNodeType.STRING);
          args.get(0).apply(scope, in, (fieldname) -> {
            System.out.println("path:"+path+", fieldname:"+fieldname);
            this.fieldName = fieldname.asText();
          });

        }
    );
    Expression fileExpr = args.get(1);
    fileExpr.apply(
        scope, in, (n) -> {
          if (!n.isTextual()) {
            throw new RuntimeException("file expr must yield a string");
          } else {
            this.dirpath = n.asText();
          }
        }
    );

    Map<String, Object> state = getState();
    this.mapper =
        (NBIdMapper) state.computeIfAbsent("mapper_context", k -> new NBTriesContext(dirpath));
  }
}
