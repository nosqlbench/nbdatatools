package io.nosqlbench.nbvectors.buildhdf5;

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
