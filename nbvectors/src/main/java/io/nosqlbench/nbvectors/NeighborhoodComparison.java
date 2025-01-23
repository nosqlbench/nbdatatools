package io.nosqlbench.nbvectors;

import java.util.BitSet;

import static io.nosqlbench.nbvectors.Glyphs.braille;

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
    Computations.BitSetDelta bitmaps =
        Computations.bitmaps(providedNeighborhood.getIndices(), expectedNeighborhood.getIndices());
    sb.append("query index: ").append(testVector).append("\n");
    sb.append("matching: ");
    sb.append(braille(Computations.matchingImage(
        providedNeighborhood.getIndices(),
        expectedNeighborhood().getIndices()
    )));
    long[][] partitions = Computations.partitions(
        providedNeighborhood.getIndices(),
        expectedNeighborhood.getIndices()
    );
    sb.append(String.format(
        " (extra,matching,missing)=(%d,%d,%d)",
        partitions[Computations.SET_A].length,
        partitions[Computations.SET_BOTH].length,
        partitions[Computations.SET_B].length
    ));
    sb.append("\n");

    byte[] expectedByteArray = bitmaps.expected().toByteArray();
    return sb.toString();
  }
}
