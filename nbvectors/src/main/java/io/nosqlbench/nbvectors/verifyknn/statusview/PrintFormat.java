package io.nosqlbench.nbvectors.verifyknn.statusview;

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


import io.nosqlbench.nbvectors.verifyknn.datatypes.LongIndexedFloatVector;
import io.nosqlbench.nbvectors.verifyknn.datatypes.NeighborIndex;
import io.nosqlbench.nbvectors.verifyknn.datatypes.Neighborhood;

import java.util.Arrays;

public enum PrintFormat {
  all;

  public static StringBuilder format(
      String description,
      LongIndexedFloatVector testVector,
      Neighborhood neighbors
  )
  {
    StringBuilder sb = new StringBuilder(description).append(":\n");
    sb.append("query index: ").append(testVector.index()).append("\n");
    sb.append("query vector: ").append(Arrays.toString(testVector.vector())).append("\n");
    sb.append("neighbors:\n").append(neighbors).append("\n\n");

    long[] indices =
        neighbors.stream().mapToLong(NeighborIndex::index).toArray();
    sb.append(" indices:\n").append(Arrays.toString(indices)).append("\n\n");

    double[] distances =
        neighbors.stream().mapToDouble(NeighborIndex::distance).toArray();
    sb.append(" distances:\n").append(Arrays.toString(distances)).append("\n\n");
    return sb;
  }
}
