package io.nosqlbench.nbvectors.commands.build_hdf5.datasource.json;

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
import io.nosqlbench.nbvectors.commands.build_hdf5.datasource.JJQSupplier;
import io.nosqlbench.nbvectors.commands.build_hdf5.datasource.MapperConfig;
import io.nosqlbench.vectordata.spec.datasets.types.DistanceFunction;
import io.nosqlbench.vectordata.spec.predicates.PNode;
import io.nosqlbench.nbvectors.commands.jjq.evaluator.JJQInvoker;
import io.nosqlbench.nbvectors.commands.jjq.bulkio.iteration.ConvertingIterable;
import io.nosqlbench.nbvectors.commands.jjq.outputs.BufferOutput;
import io.nosqlbench.nbvectors.commands.build_hdf5.predicates.PredicateParser;
import io.nosqlbench.vectordata.spec.attributes.RootGroupAttributes;
import io.nosqlbench.vectordata.spec.tokens.SpecDataSource;
import io.nosqlbench.nbvectors.commands.verify_knn.datatypes.LongIndexedFloatVector;

import java.nio.file.Path;
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;

/// A data loader for JSON data which uses the jjq syntax to load data from JSON files.
public class JsonLoader implements SpecDataSource {

  private final MapperConfig config;

  /// create a new JSON loader
  /// @param config
  ///     the configuration to use for loading the data
  public JsonLoader(MapperConfig config) {
    this.config = config;
  }

  /// get an iterator for training vectors
  /// @return an iterator for {@link LongIndexedFloatVector}
  @Override
  public Optional<Iterable<float[]>> getBaseVectors() {

    Optional<Path> baseJsonFile = config.getBaseVectorsJsonFile();
    if (baseJsonFile.isEmpty()) {
      return Optional.empty();
    }
    Supplier<String> input = JJQSupplier.path(baseJsonFile.get());
    String expr = config.getTrainingJqExpr();

    BufferOutput output = new BufferOutput(5000000);

    JJQInvoker invoker = new JJQInvoker(input, expr, output);

    invoker.run();
    ConvertingIterable<JsonNode, float[]> converter =
        new ConvertingIterable<>(output.getResultStream(), JsonNodeIntoVectorFloatAry);
    return Optional.of(converter);
  }

  @Override
  public Optional<Iterable<?>> getBaseContent() {
    return Optional.empty();
  }

  /// get an iterator for test vectors
  /// @return an iterator for {@link LongIndexedFloatVector}
  @Override
  public Optional<Iterable<float[]>> getQueryVectors() {

    Optional<Path> testJsonFile = config.getTestJsonFile();
    if (testJsonFile.isEmpty()) {
      return Optional.empty();
    }
    Supplier<String> input = JJQSupplier.path(testJsonFile.get());
    String expr = config.getTestJqExpr();
    BufferOutput output = new BufferOutput(5000000);
    try (JJQInvoker invoker = new JJQInvoker(input, expr, output)) {
      invoker.run();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    ConvertingIterable<JsonNode, float[]> converter =
        new ConvertingIterable<>(output.getResultStream(), JsonNodeIntoVectorFloatAry);
    return Optional.of(converter);

  }

  @Override
  public Optional<Iterable<?>> getQueryTerms() {
    return Optional.empty();
  }

  /// get an iterator for neighbors
  /// @return an iterator for {@link LongIndexedFloatVector}
  @Override
  public Optional<Iterable<int[]>> getNeighborIndices() {

    Optional<Path> neighborhoodJsonFile = config.getNeighborhoodJsonFile();
    if (neighborhoodJsonFile.isEmpty()) {
      return Optional.empty();
    }
    Supplier<String> input = JJQSupplier.path(neighborhoodJsonFile.get());
    String expr = config.getNeighborhoodTestExpr();
    BufferOutput output = new BufferOutput(5000000);
    try (JJQInvoker invoker = new JJQInvoker(input, expr, output)) {
      invoker.run();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    ConvertingIterable<JsonNode, int[]> converter =
        new ConvertingIterable<>(output.getResultStream(), JsonNodeIntoIntegerNeighborIndices);
    return Optional.of(converter);
  }

  /// get an iterator for distances
  /// @return an iterator for {@link LongIndexedFloatVector}
  @Override
  public Optional<Iterable<float[]>> getNeighborDistances() {
    Optional<Path> optionalDistancesJsonFile = config.getDistancesJsonFile();
    if (optionalDistancesJsonFile.isEmpty()) {
      return Optional.empty();
    }
    Supplier<String> input = JJQSupplier.path(optionalDistancesJsonFile.get());
    String expr = config.getDistancesExpr();
    BufferOutput output = new BufferOutput(5000000);
    try (JJQInvoker invoker = new JJQInvoker(input, expr, output)) {
      invoker.run();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    ConvertingIterable<JsonNode, float[]> converter =
        new ConvertingIterable<>(output.getResultStream(), JsonNodeIntoFloatNeighborScoreDistances);
    return Optional.of(converter);

  }

  @Override
  public RootGroupAttributes getMetadata() {
    return new RootGroupAttributes(
        config.getModel(),
        config.getUrl(),
        DistanceFunction.valueOf(config.getDistanceFunction()),
        config.getNotes(),
        config.getDatasetMeta().license(),
        config.getDatasetMeta().vendor(),
        config.getDatasetMeta().tags()
    );
  }

  /// a converter for json nodes into `long[]` indices
  /// ---
  /// # required node structure
  /// ```json
  ///{
  ///   "ids": [0,3,9]
  ///}
  ///```
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

  /// a converter for json nodes into `int[]` indices
  public static Function<JsonNode, int[]> JsonNodeIntoIntegerNeighborIndices = n -> {
    JsonNode vnode = n.get("ids");
    if (vnode == null) {
      List<String> names = new ArrayList<>();
      n.fieldNames().forEachRemaining(names::add);
      throw new RuntimeException("ids node was null from node:\n" + n.toPrettyString());
    }
    int[] ints = new int[vnode.size()];
    int i = 0;
    for (JsonNode node : vnode) {
      ints[i++] = node.intValue();
    }
    return ints;
  };

  /// A converter for json nodes into `float[]` distances
  /// ---
  /// # required node structure (pick one)
  /// ```json
  ///{
  ///   "distances": [0.23,-0.12,0.01]
  ///}
  ///
  ///{
  ///   "scores": [0.23,0.12,0.01]
  ///}
  ///```
  /// If _distances_ is provided, then the values are presumed to be cosine similarity values, as
  ///  in _not_ converted to scalar distance values.
  /// If _scores_ is provided, then the values are presumed to be in unit-intervals scores, and
  /// are converted to equivalent cosine similarity values.
  /// Other conversions may be added as needed, and should each be distinguished by a specific
  /// property name.
  public static Function<JsonNode, float[]> JsonNodeIntoFloatNeighborScoreDistances = n -> {
    JsonNode vnode = n.get("scores");
    if (vnode != null) {
      float[] floats = new float[vnode.size()];
      for (int i = 0; i < floats.length; i++) {
        // scores are presumed to be on the unit intervals and need to be converted back to
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
  ///{
  ///   "id": 0,
  ///   "vector": [0.23,-0.12,0.01]
  ///}
  ///```
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

  /// a converter for json nodes into `float[]` vectors
  /// ---
  /// # required node structure
  /// ```json
  /// {
  ///   "vector": [0.23,-0.12,0.01]
  /// }
  /// ```
  public static Function<JsonNode, float[]> JsonNodeIntoVectorFloatAry =      n -> {
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
    return floats;
  };

  /// get an iterator for predicate filters
  /// @return an iterator for {@link PNode}
  public Optional<Iterable<PNode<?>>> getQueryFilters() {
    Optional<Path> filtersFile = config.getFiltersFile();
    Optional<String> filtersExpr = config.getFiltersExpr();
    if (filtersExpr.isEmpty() || filtersFile.isEmpty()) {
      throw new RuntimeException("filters expr and filters file must both be defined in the config");
    }
    Supplier<String> input = JJQSupplier.path(filtersFile.get());
    String expr = filtersExpr.get();
    BufferOutput output = new BufferOutput(5000000);

    JJQInvoker invoker = new JJQInvoker(input, expr, output);
    invoker.run();
    ConvertingIterable<JsonNode, PNode<?>> converter =
        new ConvertingIterable<>(output.getResultStream(), PredicateParser::parse);
    return Optional.of(converter);
  }


}
