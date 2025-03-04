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


import io.jhdf.api.Node;
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

public class MapperConfig {
  private final Map<String, Object> cfgmap;

  public MapperConfig(Map<String, Object> cfgmap) {
    this.cfgmap = cfgmap;
  }

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

  public String getTrainingExpr() {
    return (String) cfgmap.get("training_expr");
  }

  public Path getTrainingJsonFile() {
    return Path.of((String) this.cfgmap.get("training_file"));
  }

  public String getTrainingJqExpr() {
    return (String) this.cfgmap.get("training_expr");
  }

  public Path getTestJsonFile() {
    return Path.of((String) this.cfgmap.get("test_file"));
  }

  public String getTestJqExpr() {
    return (String) this.cfgmap.get("test_expr");
  }

  public Path getNeighborhoodJsonFile() {
    return Path.of((String) this.cfgmap.get("neighborhood_file"));
  }

  public String getNeighborhoodTestExpr() {
    return (String) this.cfgmap.get("neighborhood_expr");
  }

  public Path getDistancesJsonFile() {
    return Path.of((String) this.cfgmap.get("distances_file"));
  }

  public String getDistancesExpr() {
    return (String) cfgmap.get("distances_expr");
  }

  public Optional<Path> getFiltersFile() {
    return Optional.ofNullable((String) cfgmap.get("filters_file")).map(Path::of);
  }

  public String getModel() {
    return cfgmap.getOrDefault("model", "random").toString();
  }

  public String getDistanceFunction() {
    return cfgmap.getOrDefault("distance_function", "cosine").toString();
  }

  public Optional<String> getFiltersExpr() {
    return Optional.ofNullable((String) cfgmap.get("filters_expr"));
  }

  public List<RemapConfig> getMappers() {
    List<RemapConfig> mapperConfigs = new ArrayList<>();

    Map<String, Map<String, String>> remaps =
        (Map<String, Map<String, String>>) this.cfgmap.get("remappers");
    remaps = remaps == null ? Map.of() : remaps;
    remaps.forEach((k,v)-> {
      mapperConfigs.add(new RemapConfig(k,v));
    });

    return mapperConfigs;
  }

  public record RemapConfig(String name, Path file, String expr){
    public RemapConfig(String name, Map<String, String> props) {
      this(name, Path.of(props.get("input_file")), props.get("expression"));
    }
  }
}
