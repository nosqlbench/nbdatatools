package io.nosqlbench.nbvectors.jjq.nbfunctions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.auto.service.AutoService;
import io.nosqlbench.nbvectors.jjq.apis.NBBaseJQFunction;
import io.nosqlbench.nbvectors.jjq.contexts.NBEnumContext;
import io.nosqlbench.nbvectors.jjq.contexts.NBIdEnumerator;
import io.nosqlbench.nbvectors.jjq.apis.NBStateContext;
import io.nosqlbench.nbvectors.jjq.contexts.NBTriesContext;
import io.nosqlbench.nbvectors.jjq.contexts.NBIdMapper;
import net.thisptr.jackson.jq.*;
import net.thisptr.jackson.jq.exception.JsonQueryException;
import net.thisptr.jackson.jq.internal.misc.Preconditions;
import net.thisptr.jackson.jq.path.Path;

import java.util.List;
import java.util.Map;

@AutoService(Function.class)
@BuiltinFunction({"nbenum/1"})
public class nbenum extends NBBaseJQFunction {
  private NBIdEnumerator enumer;
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
    if (in.isNull() || in.isEmpty()) {
      return;
    }
    if (in.has(fieldName)) {
      System.err.println(
          "object already contains enumerated field '" + fieldName + "', all " + "fields:");
      in.fieldNames().forEachRemaining(System.err::println);
    }
    long value = enumer.getAsLong();
    JsonNode out = in;
    if (out instanceof ObjectNode onode) {
      onode.set(fieldName, new LongNode(value));
    } else {
      throw new RuntimeException(
          "Unable to modify node of type '" + out.getClass().getCanonicalName());
    }

    output.emit(out, path);
  }

  @Override
  public void start(Scope scope, List<Expression> args, JsonNode in, NBStateContext nbctx)
      throws JsonQueryException
  {
    args.getFirst().apply(
        scope, in, (fieldname) -> {
          System.out.println("enumerating fieldname:" + fieldname);
          this.fieldName = fieldname.asText();
        }
    );

    Map<String, Object> state = getState();
    this.enumer = (NBIdEnumerator) state.computeIfAbsent(
        "enumerator_context",
        k -> new NBEnumContext(fieldName)
    );
  }
}
