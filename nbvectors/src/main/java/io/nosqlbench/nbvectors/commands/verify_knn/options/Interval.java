package io.nosqlbench.nbvectors.commands.verify_knn.options;

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


/// Allow either `min_incl..max_excl` or simply `max_excl` format
/// @param min the min value, inclusive
/// @param max the max value, exclusive
public record Interval(long min, long max)  {

  /// get the number of values in the intervals
  /// @return the number of values in the intervals
  public int count() {
    return (int) (max() - min());
  }

  public String toString() {
    return min() + ".." + max();
  }
}
