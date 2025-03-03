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


import io.nosqlbench.nbvectors.verifyknn.computation.Computations;
import io.nosqlbench.nbvectors.verifyknn.datatypes.BitImage;
import org.junit.jupiter.api.Test;

import static io.nosqlbench.nbvectors.verifyknn.statusview.Glyphs.braille;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.BitSet;

public class ComputationsTest {

  @Test
  public void testMatchingFull() {
    BitImage image = Computations.matchingImage(
        new long[]{1, 2, 3, 4, 5, 6, 7, 8},
        new long[]{1, 2, 3, 4, 5, 6, 7, 8}
    );
    BitSet expectedBits = new BitSet() {{
      set(0, 8);
    }};
    assertThat(image.getOnes()).isEqualTo(expectedBits);

    String glyph = braille(image);
    assertThat(glyph).isEqualTo("⣿");
  }

  @Test
  public void testMatchingPartial() {
    BitImage image = Computations.matchingImage(
        new long[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}, new long[]{
            1, 3, 5, 7
        }
    );
    BitSet expectedBits = new BitSet() {{
      set(0);
      set(2);
      set(4);
      set(6);
    }};
    assertThat(image.getOnes()).isEqualTo(expectedBits);

    String glyph = braille(image);
    assertThat(image.length()).isEqualTo(12);
    assertThat(glyph).isEqualTo("⠭⠀");

  }

}
