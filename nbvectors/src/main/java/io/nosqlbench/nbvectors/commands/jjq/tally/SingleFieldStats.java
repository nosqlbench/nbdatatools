package io.nosqlbench.nbvectors.commands.jjq.tally;

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


import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.commons.collections4.trie.PatriciaTrie;

import java.util.Map;

///  Contains stats for a single field of an object stream for all values
public class SingleFieldStats {

  /// get the name of the field
  /// @return the name of the field
  public String getName() {
    return name;
  }

  /// set the name of the field
  /// @param name the name of the field
  public void setName(String name) {
    this.name = name;
  }

  @JsonProperty
  private String name;

  @JsonDeserialize(as = PatriciaTrie.class)
  private Map<String, SingleValueStats> statsForValues = new PatriciaTrie<>();

  /// create an empty stats props
  public SingleFieldStats() {}

  /// create a stats props for a specific field
  /// @param name the name of the field
  public SingleFieldStats(String name) {
    this.name = name;
  }

  /// get the stats for all values of this field
  /// @return the stats for all values of this field
  @JsonGetter("stats")
  public Map<String, SingleValueStats> getStatsForValues() {
    return statsForValues;
  }

  /// set the stats for all values of this field
  /// @param statsForValues the stats for all values of this field
  @JsonSetter("stats")
  public void setStatsForValues(Map<String, SingleValueStats> statsForValues) {
    this.statsForValues = statsForValues;
  }

  /// return a summary string that describes this field stats
  @Override
  public String toString() {
    return "  " + this.name + ": " + this.statsForValues.size() + " values";
  }
}

