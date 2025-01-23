package io.nosqlbench.nbvectors;
import org.junit.jupiter.api.Test;

import static io.nosqlbench.nbvectors.Glyphs.braille;
import static org.assertj.core.api.Assertions.assertThat;
import java.util.BitSet;

public class ComputationsTest {

  @Test
  public void testMatchingImage() {
    BitSet bitSet = Computations.matchingImage(
        new long[]{1, 2, 3, 4, 5, 6, 7, 8},
        new long[]{1, 2, 3, 4, 5, 6, 7, 8}
    );
    BitSet expectedBits = new BitSet() {{ set(0,8);}};
    assertThat(bitSet).isEqualTo(expectedBits);
    String glyph = braille(bitSet);
    assertThat(glyph).isEqualTo("⣿");

    bitSet = Computations.matchingImage(
        new long[]{1, 3, 5, 7},
        new long[]{1, 2, 3, 4}
    );
    expectedBits = new BitSet() {{ set(0); set(2);}};
    assertThat(bitSet).isEqualTo(expectedBits);

    glyph = braille(bitSet);
    assertThat(glyph).isEqualTo("⠅");
  }
}