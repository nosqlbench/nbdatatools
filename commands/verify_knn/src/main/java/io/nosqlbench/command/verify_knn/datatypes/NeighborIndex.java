package io.nosqlbench.command.verify_knn.datatypes;

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


/// a neighbor index containing both the dataset index and the associated distance
/// @param index the index of the neighbor in the dataset
/// @param distance the distance of the neighbor
public record NeighborIndex(
    long index, double distance
) implements Comparable<NeighborIndex>
{
  @Override
  public int compareTo(NeighborIndex o) {
    return Double.compare(distance, o.distance);
  }

  @Override
  public String toString() {
    return "("+index+","+String.format("%.3f",distance)+")";
  }
}
