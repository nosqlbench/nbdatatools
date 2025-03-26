package io.nosqlbench.nbvectors.commands.verify_knn.computation;

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


import io.nosqlbench.nbvectors.commands.verify_knn.datatypes.LongIndexedFloatVector;
import io.nosqlbench.nbvectors.commands.verify_knn.datatypes.Neighborhood;
import io.nosqlbench.nbvectors.spec.access.datasets.types.Indexed;

import static io.nosqlbench.nbvectors.commands.verify_knn.statusview.Glyphs.braille;

/// encapsulate the result of a vector query, including both the known correct result
/// and the actual result
/// @param testVector the test vector
/// @param providedNeighborhood the neighborhood provided by the system under test
/// @param expectedNeighborhood the neighborhood expected by the test, based on KNN data
public record NeighborhoodComparison(
    Indexed<float[]> testVector,
    int[] providedNeighborhood,
    int[] expectedNeighborhood
)
{
  /// determine if the provided neighborhood is an error
  /// @return true if the provided neighborhood is an error
  public boolean isError() {
    int[][] partitions = Computations.partitions(
        providedNeighborhood,
        expectedNeighborhood
    );
    return partitions[Computations.SET_A].length > 0 || partitions[Computations.SET_B].length > 0;
  }

  /// render the comparison as a string
  public String toString() {
    StringBuilder sb = new StringBuilder();

    int[][] partitions = Computations.partitions(
        providedNeighborhood,
        expectedNeighborhood
    );

    sb.append(partitions[Computations.SET_A].length==0? "PASS " : "FAIL ");

    sb.append("[");
    sb.append(braille(Computations.matchingImage(
        expectedNeighborhood(),
        providedNeighborhood
    )));
    sb.append("]");

    sb.append(String.format(
        " (extra,matching,missing)=(%d,%d,%d)",
        partitions[Computations.SET_A].length,
        partitions[Computations.SET_BOTH].length,
        partitions[Computations.SET_B].length
    ));

    sb.append("\n");

    return sb.toString();
  }
}
