package io.nosqlbench.nbvectors;

import io.nosqlbench.nbvectors.computation.Computations;
import io.nosqlbench.nbvectors.datatypes.BitImage;
import org.junit.jupiter.api.Test;

import static io.nosqlbench.nbvectors.statusview.Glyphs.braille;
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