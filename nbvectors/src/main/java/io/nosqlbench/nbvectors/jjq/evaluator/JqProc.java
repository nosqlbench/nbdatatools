package io.nosqlbench.nbvectors.jjq.evaluator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import io.nosqlbench.nbvectors.jjq.bulkio.ConcurrentSupplier;
import net.thisptr.jackson.jq.JsonQuery;
import net.thisptr.jackson.jq.Output;
import net.thisptr.jackson.jq.Scope;

import java.util.function.Function;

public class JqProc implements Runnable {

  private final Scope scope;
  private final ConcurrentSupplier<String> jsonlSource;
  private final Function<String, JsonNode> mapper;
  private final JsonQuery query;
  private final Output output;
  private final String id;

  public JqProc(
      String id,
      Scope scope,
      ConcurrentSupplier<String> jsonlSource,
      Function<String, JsonNode> mapper,
      JsonQuery query,
      Output output
  )
  {
    this.id = id;
    this.scope = scope;
    this.jsonlSource = jsonlSource;
    this.mapper = mapper;
    this.query = query;
    this.output = output;
  }

  @Override
  public void run() {
    JsonNode node;
    try {
      int count = 0;
      String inputJson = null;
      while ((inputJson = jsonlSource.get()) != null) {
        count++;
//        if ((count % 100) == 0) {
//          System.out.print(".");
//          //          System.out.println("id:" + id + " count:" + count);
//          if ((count % 500) == 0) {
//            System.out.println("\n");
//          }
//          System.out.flush();
//        }
        try {
          node = mapper.apply(inputJson);
        } catch (Exception e) {
          throw new RuntimeException("error parsing input:\n" + inputJson);
        }
        try {
          query.apply(scope, node, output);
        } catch (JsonProcessingException e) {
          throw new RuntimeException(e);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

  }

}
