package io.nosqlbench.command.jjq.tally;

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


import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;

///  Contains stats for a specific value of a specific field
public class SingleValueStats {

  @JsonProperty
  private long idx;
  @JsonProperty
  private long count;

  /// create an empty stats props
  public SingleValueStats() {}

  /// create a stats props for a specific value of a specific field
  /// @param idx the index of the value in the field
  /// @param count the number of times the value was seen
  public SingleValueStats(long idx, long count) {
    this.idx = idx;
    this.count = count;
  }

  /// set the count of the value
  /// @param count the count of the value
  @JsonSetter("count")
  public void setCount(long count) {
    this.count = count;
  }

  /// set the index of the value
  /// @param idx the index of the value
  @JsonSetter("idx")
  public void setIdx(long idx) {
    this.idx = idx;
  }

  /// get the count of the value
  /// @return the count of the value
  public long getCount() {
    return this.count;
  }

  /// get the index of the value
  /// @return the index of the value
  public long getIdx() {
    return idx;
  }

  /// increment the count of the value
  public synchronized void increment() {
    this.count+=1;
  }
}
