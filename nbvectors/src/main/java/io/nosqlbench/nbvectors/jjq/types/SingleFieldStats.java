package io.nosqlbench.nbvectors.jjq.types;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.commons.collections4.trie.PatriciaTrie;

import java.util.Map;

public class SingleFieldStats {

  @JsonDeserialize(as = PatriciaTrie.class)
  private Map<String, SingleValueStats> statsForValues = new PatriciaTrie<>();

  public SingleFieldStats() {}

  @JsonGetter("stats")
  public Map<String, SingleValueStats> getStatsForValues() {
    return statsForValues;
  }

  @JsonSetter("stats")
  public void setStatsForValues(Map<String, SingleValueStats> statsForValues) {
    this.statsForValues = statsForValues;
  }
}

