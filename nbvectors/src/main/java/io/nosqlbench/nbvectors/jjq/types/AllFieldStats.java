package io.nosqlbench.nbvectors.jjq.types;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/// Contains stats for all fields of an object stream, independently for each field
public class AllFieldStats {

  @JsonProperty()
  private Map<String,SingleFieldStats> stats = new HashMap<>();

  public AllFieldStats() {
  }


  @JsonGetter("stats")
  public Map<String, SingleFieldStats> getStatsForField() {
    return stats;
  }

  @JsonSetter("stats")
  public void setStats(Map<String, SingleFieldStats> stats) {
    this.stats = stats;
  }

  public String summary() {
    return stats.values().stream().map(SingleFieldStats::toString).collect(Collectors.joining("\n"));
  }

}
