package io.nosqlbench.command.verify_knn;

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


import io.nosqlbench.command.analyze.subcommands.verify_knn.computation.NeighborhoodComparison;
import io.nosqlbench.vectordata.spec.datasets.types.Indexed;
import org.junit.jupiter.api.Test;

import java.util.BitSet;

import static io.nosqlbench.nbdatatools.api.types.bitimage.Glyphs.braille;
import static org.assertj.core.api.Assertions.assertThat;


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
        new Indexed<>(0, new float[]{1, 2, 3}),
        new int[]{1, 2, 3},
        new int[]{1, 2, 3}
    );
    assertThat(c1.isError()).isFalse();

    NeighborhoodComparison c2 = new NeighborhoodComparison(
        new Indexed<>(0, new float[]{1, 2, 3}),
        new int[]{1, 2, 3},
        new int[]{1, 2, 4}
    );
    assertThat(c2.isError()).isTrue();

    NeighborhoodComparison c3 = new NeighborhoodComparison(
        new Indexed<>(0, new float[]{1, 2, 3}),
        new int[]{1, 2, 3},
        new int[]{1, 2, 4}
    );
    assertThat(c3.isError()).isTrue();

  }
}
