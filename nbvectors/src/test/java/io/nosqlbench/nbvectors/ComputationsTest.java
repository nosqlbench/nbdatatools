package io.nosqlbench.nbvectors;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import java.util.BitSet;

public class ComputationsTest {

  /// ```
  /// 0x00:0x20⠀  ⠁⠂⠃⠄⠅⠆⠇⠈⠉⠊⠋⠌⠍⠎⠏⠐⠑⠒⠓⠔⠕⠖⠗⠘⠙⠚⠛⠜⠝⠞⠟
  /// 0x20:0x40  ⠠⠡⠢⠣⠤⠥⠦⠧⠨⠩⠪⠫⠬⠭⠮⠯⠰⠱⠲⠳⠴⠵⠶⠷⠸⠹⠺⠻⠼⠽⠾⠿
  /// 0x40:0x60  ⡀⡁⡂⡃⡄⡅⡆⡇⡈⡉⡊⡋⡌⡍⡎⡏⡐⡑⡒⡓⡔⡕⡖⡗⡘⡙⡚⡛⡜⡝⡞⡟
  /// 0x60:0x80  ⡠⡡⡢⡣⡤⡥⡦⡧⡨⡩⡪⡫⡬⡭⡮⡯⡰⡱⡲⡳⡴⡵⡶⡷⡸⡹⡺⡻⡼⡽⡾⡿
  /// 0x80:0xa0  ⢀⢁⢂⢃⢄⢅⢆⢇⢈⢉⢊⢋⢌⢍⢎⢏⢐⢑⢒⢓⢔⢕⢖⢗⢘⢙⢚⢛⢜⢝⢞⢟
  /// 0xa0:0xc0  ⢠⢡⢢⢣⢤⢥⢦⢧⢨⢩⢪⢫⢬⢭⢮⢯⢰⢱⢲⢳⢴⢵⢶⢷⢸⢹⢺⢻⢼⢽⢾⢿
  /// 0xc0:0xe0  ⣀⣁⣂⣃⣄⣅⣆⣇⣈⣉⣊⣋⣌⣍⣎⣏⣐⣑⣒⣓⣔⣕⣖⣗⣘⣙⣚⣛⣜⣝⣞⣟
  /// 0xe0:0x100 ⣠⣡⣢⣣⣤⣥⣦⣧⣨⣩⣪⣫⣬⣭⣮⣯⣰⣱⣲⣳⣴⣵⣶⣷⣸⣹⣺⣻⣼⣽⣾⣿
  /// ```
  @Test
  public void testMatchingImage() {
    BitSet bitSet = Computations.matchingImage(
        new long[]{1, 2, 3, 4, 5, 6, 7, 8},
        new long[]{1, 2, 3, 4, 5, 6, 7, 8}
    );
    BitSet expectedBits = new BitSet() {{ set(0,8);}};
    assertThat(bitSet).isEqualTo(expectedBits);
    String glyph = NeighborhoodComparison.braille(bitSet);
    assertThat(glyph).isEqualTo("⣿");

    bitSet = Computations.matchingImage(
        new long[]{1, 3, 5, 7},
        new long[]{1, 2, 3, 4}
    );
    expectedBits = new BitSet() {{ set(0); set(2);}};
    glyph = NeighborhoodComparison.braille(bitSet);
    assertThat(glyph).isEqualTo("⣿");
  }
}