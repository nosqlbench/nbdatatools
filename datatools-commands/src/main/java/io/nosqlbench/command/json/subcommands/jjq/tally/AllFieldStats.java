package io.nosqlbench.command.json.subcommands.jjq.tally;

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

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/// Contains stats for all fields of an object stream, independently for each field
public class AllFieldStats {

  @JsonProperty()
  private Map<String,SingleFieldStats> stats = new HashMap<>();

  /// create an empty stats props
  public AllFieldStats() {
  }


  /// get the stats for a specific field
  /// @param fieldName the name of the field to get stats for
  /// @return the stats for the field
  public SingleFieldStats getStatsForField(String fieldName) {
    return stats.get(fieldName);
  }

  /// get the stats for all fields
  /// @return the stats for all fields
  @JsonGetter("stats")
  public Map<String, SingleFieldStats> getStatsForField() {
    return stats;
  }

  /// set the stats for all fields
  /// @param stats the stats for all fields
  @JsonSetter("stats")
  public void setStats(Map<String, SingleFieldStats> stats) {
    this.stats = stats;
  }

  /// return a summary string that describes all fields stats
  /// @return a descriptive readout
  public String summary() {
    return stats.values().stream().map(SingleFieldStats::toString).collect(Collectors.joining("\n"));
  }

}
