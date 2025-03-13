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


import io.nosqlbench.nbvectors.spec.attributes.SpecAttributes;
import io.nosqlbench.nbvectors.verifyknn.options.DistanceFunction;
import org.snakeyaml.engine.v2.api.Load;
import org.snakeyaml.engine.v2.api.LoadSettings;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/// A basic config wrapper for the buildhdf5 command's mapping logic.
public class MapperConfig {
  private final Map<String, Object> cfgmap;

  /// create a mapper config
  /// @param cfgmap
  ///     the config map
  public MapperConfig(Map<String, Object> cfgmap) {
    this.cfgmap = cfgmap;
  }

  /// create a mapper config from a file
  /// @param layoutPath
  ///     the path to the config file
  /// @return a mapper config
  public static MapperConfig file(Path layoutPath) {
    BufferedReader reader = null;
    Map<String, String> cfgData = null;
    LoadSettings loadSettings = LoadSettings.builder().build();
    Load yaml = new Load(loadSettings);
    try {
      reader = Files.newBufferedReader(layoutPath, StandardCharsets.UTF_8);
      Map<String, Object> config = (Map<String, Object>) yaml.loadFromReader(reader);
      return new MapperConfig(config);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /// get the training expression
  /// @return the training expression
  public String getTrainingExpr() {
    return (String) cfgmap.get("training_expr");
  }

  /// get the training JSON file
  /// @return the training JSON file
  public Path getTrainingJsonFile() {
    return Path.of((String) this.cfgmap.get("training_file"));
  }

  /// get the training jq expression
  /// @return the training jq expression
  public String getTrainingJqExpr() {
    return (String) this.cfgmap.get("training_expr");
  }

  /// get the test JSON file
  /// @return the test JSON file
  public Path getTestJsonFile() {
    return Path.of((String) this.cfgmap.get("test_file"));
  }

  /// get the test jq expression
  /// @return the test jq expression
  public String getTestJqExpr() {
    return (String) this.cfgmap.get("test_expr");
  }

  /// get the neighborhood JSON file
  /// @return the neighborhood JSON file
  public Path getNeighborhoodJsonFile() {
    return Path.of((String) this.cfgmap.get("neighborhood_file"));
  }

  /// get the neighborhood jq expression
  /// @return the neighborhood jq expression
  public String getNeighborhoodTestExpr() {
    return (String) this.cfgmap.get("neighborhood_expr");
  }

  /// get the distances JSON file
  /// @return the distances JSON file
  public Path getDistancesJsonFile() {
    return Path.of((String) this.cfgmap.get("distances_file"));
  }

  /// get the distances jq expression
  /// @return the distances jq expression
  public String getDistancesExpr() {
    return (String) cfgmap.get("distances_expr");
  }

  /// get the filters JSON file
  /// @return the filters JSON file
  public Optional<Path> getFiltersFile() {
    return Optional.ofNullable((String) cfgmap.get("filters_file")).map(Path::of);
  }

  /// get the dataset metadata
  ///
  public SpecAttributes getDatasetMeta() {
    return new SpecAttributes(
        (String) cfgmap.get("model"),
        (String) cfgmap.get("url"),
        DistanceFunction.valueOf(cfgmap.get("distance_function").toString()),
        Optional.ofNullable(cfgmap.get("notes")).map(String::valueOf)
    );
  }

  /// get the model attribute value
  /// @return the model attribute value
  public String getModel() {
    return cfgmap.getOrDefault("model", "random").toString();
  }

  /// get the distance attribute value
  /// @return the distance attribute value
  public String getDistanceFunction() {
    return cfgmap.getOrDefault("distance_function", "cosine").toString();
  }

  /// get the url of this dataset
  /// @return a URL of the dataset, of available
  public String getUrl() {
    return cfgmap.getOrDefault("url", "none provided").toString();
  }


  /// get the filters jq expression
  /// @return the filters jq expression
  public Optional<String> getFiltersExpr() {
    return Optional.ofNullable((String) cfgmap.get("filters_expr"));
  }

  /// get a list of remapper configurations
  /// @return a list of remapper configurations
  public List<RemapConfig> getMappers() {
    List<RemapConfig> mapperConfigs = new ArrayList<>();

    Map<String, Map<String, String>> remaps =
        (Map<String, Map<String, String>>) this.cfgmap.get("remappers");
    remaps = remaps == null ? Map.of() : remaps;
    remaps.forEach((k, v) -> mapperConfigs.add(new RemapConfig(k, v)));

    return mapperConfigs;
  }

  /// return notes associated with this dataset
  /// @return notes, if provided
  public Optional<String> getNotes() {
    return Optional.ofNullable(cfgmap.get("notes")).map(String::valueOf);
  }

  /// a remap configuration
  /// @param name
  ///     the name of a remapper configuration
  /// @param file
  ///     the file to remap
  /// @param expr
  ///     the jq expression to remap
  public record RemapConfig(String name, Path file, String expr) {
    /// create a remapper configuration
    /// @param name
    ///     the name of the remapper configuration
    /// @param props
    ///     the properties of the remapper configuration
    public RemapConfig(String name, Map<String, String> props) {
      this(name, Path.of(props.get("input_file")), props.get("expression"));
    }
  }
}
