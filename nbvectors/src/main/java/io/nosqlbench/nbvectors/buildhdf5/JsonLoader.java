package io.nosqlbench.nbvectors.buildhdf5;

import com.fasterxml.jackson.databind.JsonNode;
import io.nosqlbench.nbvectors.buildhdf5.predicates.types.PNode;
import io.nosqlbench.nbvectors.jjq.evaluator.JJQInvoker;
import io.nosqlbench.nbvectors.jjq.bulkio.ConvertingIterable;
import io.nosqlbench.nbvectors.jjq.outputs.BufferOutput;
import io.nosqlbench.nbvectors.buildhdf5.predicates.PredicateParser;
import io.nosqlbench.nbvectors.verifyknn.datatypes.LongIndexedFloatVector;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

public class JsonLoader {
  public static Iterator<LongIndexedFloatVector> readTrainingStream(MapperConfig config) {

    Supplier<String> input = JJQSupplier.path(config.getTrainingJsonFile());
    String expr = config.getTrainingJqExpr();
    BufferOutput output = new BufferOutput(5000000);

    JJQInvoker invoker = new JJQInvoker(input, expr, output);

    invoker.run();
    ConvertingIterable<JsonNode, LongIndexedFloatVector> converter =
        new ConvertingIterable<>(output.getResultStream(), JsonNodeIntoLongIndexedFloatVector);
    return converter.iterator();
  }

  public static Iterator<LongIndexedFloatVector> readTestStream(MapperConfig config) {
    Supplier<String> input = JJQSupplier.path(config.getTestJsonFile());
    String expr = config.getTestJqExpr();
    BufferOutput output = new BufferOutput(5000000);
    JJQInvoker invoker = new JJQInvoker(input, expr, output);
    invoker.run();
    ConvertingIterable<JsonNode, LongIndexedFloatVector> converter =
        new ConvertingIterable<>(output.getResultStream(), JsonNodeIntoLongIndexedFloatVector);
    return converter.iterator();

  }

  public static Iterator<long[]> readNeighborsStream(MapperConfig config) {
    Supplier<String> input = JJQSupplier.path(config.getNeighborhoodJsonFile());
    String expr = config.getNeighborhoodTestExpr();
    BufferOutput output = new BufferOutput(5000000);
    JJQInvoker invoker = new JJQInvoker(input, expr, output);
    invoker.run();
    ConvertingIterable<JsonNode, long[]> converter =
        new ConvertingIterable<>(output.getResultStream(), JsonNodeIntoLongNeighborIndices);
    return converter.iterator();
  }

  public static Iterator<float[]> readDistancesStream(MapperConfig config) {
    Supplier<String> input = JJQSupplier.path(config.getDistancesJsonFile());
    String expr = config.getDistancesExpr();
    BufferOutput output = new BufferOutput(5000000);
    JJQInvoker invoker = new JJQInvoker(input, expr, output);
    invoker.run();
    ConvertingIterable<JsonNode, float[]> converter =
        new ConvertingIterable<>(output.getResultStream(), JsonNodeIntoFloatNeighborScoreDistances);
    return converter.iterator();

  }


  public static Function<JsonNode, long[]> JsonNodeIntoLongNeighborIndices = n -> {
    JsonNode vnode = n.get("ids");
    if (vnode == null) {
      List<String> names = new ArrayList<>();
      n.fieldNames().forEachRemaining(names::add);
      throw new RuntimeException("ids node was null from node:\n" + n.toPrettyString());
    }
    long[] longs = new long[vnode.size()];
    int i = 0;
    for (JsonNode node : vnode) {
      longs[i++] = node.longValue();
    }
    return longs;
  };

  public static Function<JsonNode, float[]> JsonNodeIntoFloatNeighborScoreDistances = n -> {
    JsonNode vnode = n.get("scores");
    if (vnode != null) {
      float[] floats = new float[vnode.size()];
      for (int i = 0; i < floats.length; i++) {
        // scores are presumed to be on the unit interval and need to be converted back to
        // cosine similarity
        floats[i] = (vnode.get(i).floatValue() * 2) - 1;
      }
      return floats;
    }

    vnode = n.get("distances");
    if (vnode != null) {
      float[] floats = new float[vnode.size()];
      for (int i = 0; i < floats.length; i++) {
        floats[i] = vnode.get(i).floatValue();
      }
      return floats;
    }
    List<String> names = new ArrayList<>();
    n.fieldNames().forEachRemaining(names::add);
    throw new RuntimeException("scores node was null from node:\n" + n.toPrettyString());
  };

  public static Function<JsonNode, LongIndexedFloatVector> JsonNodeIntoLongIndexedFloatVector =
      n -> {
        JsonNode vnode = n.get("vector");
        if (vnode == null) {
          List<String> names = new ArrayList<>();
          n.fieldNames().forEachRemaining(names::add);
          throw new RuntimeException("vector node was null, keys:" + names);
        }
        float[] floats = new float[vnode.size()];
        int i = 0;
        for (JsonNode node : vnode) {
          floats[i++] = node.floatValue();
        }
        return new LongIndexedFloatVector(n.get("id").asLong(), floats);
      };

  public static Iterator<PNode<?>> readFiltersStream(MapperConfig config) {
    Optional<Path> filtersFile = config.getFiltersFile();
    Optional<String> filtersExpr = config.getFiltersExpr();
    if (filtersExpr.isEmpty() || filtersFile.isEmpty()) {
      throw new RuntimeException(
          "filters expr and filters file must both be defined in the config");
    }
    Supplier<String> input = JJQSupplier.path(filtersFile.get());
    String expr = filtersExpr.get();
    BufferOutput output = new BufferOutput(5000000);

    JJQInvoker invoker = new JJQInvoker(input, expr, output);
    invoker.run();
    ConvertingIterable<JsonNode, PNode<?>> converter =
        new ConvertingIterable<>(output.getResultStream(), PredicateParser::parse);
    return converter.iterator();
  }


}
