package io.nosqlbench.nbvectors.buildhdf5;

/*
 * Copyright (c) nosqlbench
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


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

/// A data loader for JSON data which uses the jjq syntax to load data from JSON files.
public class JsonLoader {

  /// get an iterator for training vectors
  /// @param config the configuration to use for loading the data
  /// @return an iterator for {@link LongIndexedFloatVector}
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

  /// get an iterator for test vectors
  /// @param config the configuration to use for loading the data
  /// @return an iterator for {@link LongIndexedFloatVector}
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

  /// get an iterator for neighbors
  /// @param config the configuration to use for loading the data
  /// @return an iterator for {@link LongIndexedFloatVector}
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

  /// get an iterator for distances
  /// @param config the configuration to use for loading the data
  /// @return an iterator for {@link LongIndexedFloatVector}
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


  /// a converter for json nodes into `long[]` indices
  /// ---
  /// # required node structure
  /// ```json
  /// {
  ///   "ids": [0,3,9]
  /// }
  /// ```
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

  /// A converter for json nodes into `float[]` distances
  /// ---
  /// # required node structure (pick one)
  /// ```json
  /// {
  ///   "distances": [0.23,-0.12,0.01]
  /// }
  ///
  /// {
  ///   "scores": [0.23,0.12,0.01]
  /// }
  /// ```
  /// If _distances_ is provided, then the values are presumed to be cosine similarity values, as
  ///  in _not_ converted to scalar distance values.
  /// If _scores_ is provided, then the values are presumed to be in unit-interval scores, and
  /// are converted to equivalent cosine similarity values.
  /// Other conversions may be added as needed, and should each be distinguished by a specific
  /// property name.
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

  /// a converter for json nodes into {@link LongIndexedFloatVector}
  /// ---
  /// # required node structure
  /// ```json
  /// {
  ///   "id": 0,
  ///   "vector": [0.23,-0.12,0.01]
  /// }
  /// ```
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

  /// get an iterator for predicate filters
  /// @param config the configuration to use for loading the data
  /// @return an iterator for {@link PNode}
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
