package io.nosqlbench.verifyknn.computation;

import io.nosqlbench.verifyknn.datatypes.IndexedFloatVector;
import io.nosqlbench.verifyknn.datatypes.Neighborhood;

import static io.nosqlbench.verifyknn.statusview.Glyphs.braille;

public record NeighborhoodComparison(
    IndexedFloatVector testVector,
    Neighborhood providedNeighborhood,
    Neighborhood expectedNeighborhood
)
{
  public boolean isError() {
    long[][] partitions = Computations.partitions(
        providedNeighborhood.getIndices(),
        expectedNeighborhood.getIndices()
    );
    return partitions[Computations.SET_A].length > 0 || partitions[Computations.SET_B].length > 0;
  }


  public String toString() {
    StringBuilder sb = new StringBuilder();
//    sb.append(testVector).append("\n");

    long[][] partitions = Computations.partitions(
        providedNeighborhood.getIndices(),
        expectedNeighborhood.getIndices()
    );

    sb.append(partitions[Computations.SET_A].length==0? "PASS " : "FAIL ");

    sb.append("[");
    sb.append(braille(Computations.matchingImage(
        expectedNeighborhood().getIndices(),
        providedNeighborhood.getIndices()
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
