package io.nosqlbench.nbvectors;

import io.nosqlbench.nbvectors.computation.NeighborhoodComparison;
import io.nosqlbench.nbvectors.datatypes.IndexedFloatVector;
import io.nosqlbench.nbvectors.datatypes.Neighborhood;
import org.junit.jupiter.api.Test;

import static io.nosqlbench.nbvectors.statusview.Glyphs.braille;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.BitSet;


public class NeighborhoodComparisonTest {

  @Test
  public void testGlyphs() {
    for (int i = 0; i < 256; i++) {
      System.out.print(braille(i));
    }
    System.out.println();

    BitSet maxBits = BitSet.valueOf(new byte[]{-1});
    assertThat(braille(maxBits)).isEqualTo("⣿");
  }

  @Test
  public void testErrorDetection() {
    NeighborhoodComparison c1 = new NeighborhoodComparison(
        new IndexedFloatVector(0, new float[]{1, 2, 3}),
        new Neighborhood().add(1, 0.1).add(2, 0.2).add(3, 0.3),
        new Neighborhood().add(1, 0.1).add(2, 0.2).add(3, 0.3)
    );
    assertThat(c1.isError()).isFalse();

    NeighborhoodComparison c2 = new NeighborhoodComparison(
        new IndexedFloatVector(0, new float[]{1, 2, 3}),
        new Neighborhood().add(1, 0.1).add(2, 0.2).add(4, 0.4),
        new Neighborhood().add(1, 0.1).add(2, 0.2).add(3, 0.3)
    );
    assertThat(c2.isError()).isTrue();

    NeighborhoodComparison c3 = new NeighborhoodComparison(
        new IndexedFloatVector(0, new float[]{1, 2, 3}),
        new Neighborhood().add(1, 0.1).add(2, 0.2).add(3, 0.3),
        new Neighborhood().add(1, 0.1).add(2, 0.2).add(4, 0.4)
    );
    assertThat(c3.isError()).isTrue();

  }
}