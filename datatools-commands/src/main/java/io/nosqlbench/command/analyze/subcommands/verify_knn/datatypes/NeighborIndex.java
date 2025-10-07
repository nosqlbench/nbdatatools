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


/// a neighbor index containing both the dataset index and the associated distance
public class NeighborIndex implements Comparable<NeighborIndex> {
    /// the index of the neighbor in the dataset
    private final long index;
    /// the distance of the neighbor
    private final double distance;
    
    public NeighborIndex(long index, double distance) {
        this.index = index;
        this.distance = distance;
    }
    
    /// @return the index of the neighbor in the dataset
    public long index() {
        return index;
    }
    
    /// @return the distance of the neighbor
    public double distance() {
        return distance;
    }
  @Override
  public int compareTo(NeighborIndex o) {
    return Double.compare(distance, o.distance);
  }

  @Override
  public String toString() {
    return "("+index+","+String.format("%.3f",distance)+")";
  }
}
