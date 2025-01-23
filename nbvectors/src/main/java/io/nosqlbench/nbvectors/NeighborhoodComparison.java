package io.nosqlbench.nbvectors;

import java.util.BitSet;

public record NeighborhoodComparison(
    IndexedFloatVector testVector,
    Neighborhood providedNeighborhood,
    Neighborhood expectedNeighborhood,
    long[] providedView,
    long[] expectedView,
    BitSet providedBits,
    BitSet expectedBits
) {
  public String toString() {
    StringBuilder sb = new StringBuilder();
    // TODO: convert the bitset to visual form a la https://unicode.org/charts/nameslist/c_2800.html
    return sb.toString();
  }
}
