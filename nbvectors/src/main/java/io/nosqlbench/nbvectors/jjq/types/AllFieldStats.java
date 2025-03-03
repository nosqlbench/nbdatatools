package io.nosqlbench.nbvectors.jjq.types;

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
