package io.nosqlbench.nbvectors;

import java.util.BitSet;

public record NeighborhoodComparison(
    IndexedFloatVector testVector,
    Neighborhood providedNeighborhood,
    Neighborhood expectedNeighborhood
) {
  final static int BRAILLE_BASE = 0x2800;

  public static char braille(int value) {
    return (char) (BRAILLE_BASE + value);
  }

  public boolean isError() {
    long[][] partitions = Computations.partitions(
        providedNeighborhood.getIndices(),
        expectedNeighborhood.getIndices()
    );
    return partitions[Computations.SET_A].length > 0 || partitions[Computations.SET_B].length > 0;
  }

  public static String braille(byte[] bytes) {
    StringBuilder sb = new StringBuilder();
    for (byte b : bytes) {
      char braille = braille(Byte.toUnsignedInt(b));
      sb.append(braille);
    }
    return sb.toString();
  }

  public static String braille(BitSet bits) {
    byte[] bary = bits.toByteArray();
    return braille(bary);
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    Computations.BitSetDelta bitmaps =
        Computations.bitmaps(providedNeighborhood.getIndices(), expectedNeighborhood.getIndices());
    sb.append("query index: ").append(testVector).append("\n");
    sb.append("provided: ").append(braille(bitmaps.provided())).append("\n");
    sb.append("expected: ").append(braille(bitmaps.expected())).append("\n");
    sb.append("matching: ").append(braille(Computations.matchingImage(providedNeighborhood.getIndices(),
        expectedNeighborhood().getIndices()))).append("\n");
//    sb.append("provided: ").append(providedNeighborhood).append("\n");
//    sb.append("expected: ").append(expectedNeighborhood).append("\n");

    byte[] expectedByteArray = bitmaps.expected().toByteArray();



    // TODO: convert the bitset to visual form a la https://unicode.org/charts/nameslist/c_2800.html
    return sb.toString();
  }
}
