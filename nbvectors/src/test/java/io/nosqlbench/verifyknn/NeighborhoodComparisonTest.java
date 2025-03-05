package io.nosqlbench.verifyknn;

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


import io.nosqlbench.nbvectors.verifyknn.computation.NeighborhoodComparison;
import io.nosqlbench.nbvectors.verifyknn.datatypes.LongIndexedFloatVector;
import io.nosqlbench.nbvectors.verifyknn.datatypes.Neighborhood;
import org.junit.jupiter.api.Test;

import static io.nosqlbench.nbvectors.verifyknn.statusview.Glyphs.braille;
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
        new LongIndexedFloatVector(0, new float[]{1, 2, 3}),
        new Neighborhood().add(1, 0.1).add(2, 0.2).add(3, 0.3),
        new Neighborhood().add(1, 0.1).add(2, 0.2).add(3, 0.3)
    );
    assertThat(c1.isError()).isFalse();

    NeighborhoodComparison c2 = new NeighborhoodComparison(
        new LongIndexedFloatVector(0, new float[]{1, 2, 3}),
        new Neighborhood().add(1, 0.1).add(2, 0.2).add(4, 0.4),
        new Neighborhood().add(1, 0.1).add(2, 0.2).add(3, 0.3)
    );
    assertThat(c2.isError()).isTrue();

    NeighborhoodComparison c3 = new NeighborhoodComparison(
        new LongIndexedFloatVector(0, new float[]{1, 2, 3}),
        new Neighborhood().add(1, 0.1).add(2, 0.2).add(3, 0.3),
        new Neighborhood().add(1, 0.1).add(2, 0.2).add(4, 0.4)
    );
    assertThat(c3.isError()).isTrue();

  }
}
