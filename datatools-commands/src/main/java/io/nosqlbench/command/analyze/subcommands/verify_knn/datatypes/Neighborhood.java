package io.nosqlbench.command.analyze.subcommands.verify_knn.datatypes;

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


import java.util.ArrayList;
import java.util.Arrays;

/// A neighborhood is a list of neighbors, each with an index and distance
public class Neighborhood extends ArrayList<NeighborIndex> {
  /// create a neighborhood from an array of neighbor indices
  /// @param lastResults the neighbor indices
  /// @see NeighborIndex
  public Neighborhood(NeighborIndex[] lastResults) {
    this.addAll(Arrays.asList(lastResults));
  }

  /// create an empty neighborhood
  public Neighborhood() {
    super();
  }

  /// add a neighbor to the neighborhood
  /// @param index the index of the neighbor
  /// @param distance the distance of the neighbor
  /// @return this neighborhood, for method chaining
  /// @see NeighborIndex
  public Neighborhood add(long index, double distance) {
    this.add(new NeighborIndex(index, distance));
    return this;
  }

  /// get the indices of the neighbors in this neighborhood
  /// @return the indices of the neighbors in this neighborhood
  /// @see NeighborIndex
  public long[] getIndices() {
    return this.stream().mapToLong(NeighborIndex::index).toArray();
  }
}
