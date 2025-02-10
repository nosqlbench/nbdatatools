package io.nosqlbench.nbvectors.jjq.types;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.commons.collections4.trie.PatriciaTrie;

import java.util.Map;

///  Contains stats for a single field of an object stream for all values
public class SingleFieldStats {

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  @JsonProperty
  private String name;

  @JsonDeserialize(as = PatriciaTrie.class)
  private Map<String, SingleValueStats> statsForValues = new PatriciaTrie<>();

  public SingleFieldStats() {}
  public SingleFieldStats(String name) {
    this.name = name;
  }

  @JsonGetter("stats")
  public Map<String, SingleValueStats> getStatsForValues() {
    return statsForValues;
  }

  @JsonSetter("stats")
  public void setStatsForValues(Map<String, SingleValueStats> statsForValues) {
    this.statsForValues = statsForValues;
  }

  @Override
  public String toString() {
    return "  " + this.name + ": " + this.statsForValues.size() + " values";
  }
}

