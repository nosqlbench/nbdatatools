package io.nosqlbench.nbvectors.buildhdf5;

import org.snakeyaml.engine.v2.api.Load;
import org.snakeyaml.engine.v2.api.LoadSettings;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

public class MapperConfig {
  private final Map<String, String> cfgmap;
  public MapperConfig(Map<String, String> cfgmap) {
    this.cfgmap = cfgmap;
  }

  public static MapperConfig file(Path layoutPath) {
    BufferedReader reader = null;
    Map<String,String> cfgData = null;
    LoadSettings loadSettings = LoadSettings.builder().build();
    Load yaml = new Load(loadSettings);
    try {
      reader = Files.newBufferedReader(layoutPath, StandardCharsets.UTF_8);
      Map<String,String> config = (Map<String,String>) yaml.loadFromReader(reader);
      return new MapperConfig(config);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public String getTrainingExpr() {
    return cfgmap.get("training_expr");
  }

  public Path getTrainingJsonFile() {
    return Path.of(this.cfgmap.get("training_file"));
  }

  public String getTrainingJqExpr() {
    return this.cfgmap.get("training_expr");
  }

  public Path getTestJsonFile() {
    return Path.of(this.cfgmap.get("test_file"));
  }

  public String getTestJqExpr() {
    return this.cfgmap.get("test_expr");
  }

  public Path getNeighborhoodJsonFile() {
    return Path.of(this.cfgmap.get("neighborhood_file"));
  }

  public String getNeighborhoodTestExpr() {
    return this.cfgmap.get("neighborhood_expr");
  }

  public Path getDistancesJsonFile() {
    return Path.of(this.cfgmap.get("distances_file"));
  }

  public String getDistancesExpr() {
    return cfgmap.get("distances_expr");
  }
}
