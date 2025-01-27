package io.nosqlbench.verifyknn.statusview;

import io.nosqlbench.verifyknn.datatypes.BitImage;

import java.util.BitSet;

/// #unicode braille
/// ```
/// 0x00:0x20⠀  ⠁⠂⠃⠄⠅⠆⠇⠈⠉⠊⠋⠌⠍⠎⠏⠐⠑⠒⠓⠔⠕⠖⠗⠘⠙⠚⠛⠜⠝⠞⠟
/// 0x20:0x40  ⠠⠡⠢⠣⠤⠥⠦⠧⠨⠩⠪⠫⠬⠭⠮⠯⠰⠱⠲⠳⠴⠵⠶⠷⠸⠹⠺⠻⠼⠽⠾⠿
/// 0x40:0x60  ⡀⡁⡂⡃⡄⡅⡆⡇⡈⡉⡊⡋⡌⡍⡎⡏⡐⡑⡒⡓⡔⡕⡖⡗⡘⡙⡚⡛⡜⡝⡞⡟
/// 0x60:0x80  ⡠⡡⡢⡣⡤⡥⡦⡧⡨⡩⡪⡫⡬⡭⡮⡯⡰⡱⡲⡳⡴⡵⡶⡷⡸⡹⡺⡻⡼⡽⡾⡿
/// 0x80:0xa0  ⢀⢁⢂⢃⢄⢅⢆⢇⢈⢉⢊⢋⢌⢍⢎⢏⢐⢑⢒⢓⢔⢕⢖⢗⢘⢙⢚⢛⢜⢝⢞⢟
/// 0xa0:0xc0  ⢠⢡⢢⢣⢤⢥⢦⢧⢨⢩⢪⢫⢬⢭⢮⢯⢰⢱⢲⢳⢴⢵⢶⢷⢸⢹⢺⢻⢼⢽⢾⢿
/// 0xc0:0xe0  ⣀⣁⣂⣃⣄⣅⣆⣇⣈⣉⣊⣋⣌⣍⣎⣏⣐⣑⣒⣓⣔⣕⣖⣗⣘⣙⣚⣛⣜⣝⣞⣟
/// 0xe0:0x100 ⣠⣡⣢⣣⣤⣥⣦⣧⣨⣩⣪⣫⣬⣭⣮⣯⣰⣱⣲⣳⣴⣵⣶⣷⣸⣹⣺⣻⣼⣽⣾⣿
///```
/// ## braille bit positions
/// ```
/// 1 4
/// 2 5
/// 3 6
/// 7 8
///```
/// ## ordinal transform
/// lsb pos 3 -> 7
/// lsb pos 4-6 -> 3-5

public class Glyphs {

  /// This method will only use enough characters to represent the defined
  /// 1-bits. If you want to represent additional unset values, use [#braille(BitImage)]
  public static String braille(BitSet bits) {
    byte[] bary = bits.toByteArray();
    return braille(bary);
  }

  /// Convert bit positions to a visual representation, 4-bits per column, down and then right
  public static String braille(BitImage bits) {
    byte[] bary = bits.toByteArray();
    return braille(bary);
  }

  /// Convert byte values to a visual representation, 4-bits per column, down and then right
  public static String braille(byte[] bytes) {
    StringBuilder sb = new StringBuilder();
    for (byte b : bytes) {
      sb.append(braille(b));
    }
    return sb.toString();
  }

  public static char braille(int v) {
    return braille((byte) v);
  }
  public static char braille(byte b) {
    int bits = Byte.toUnsignedInt(b);
    int image = bits & 7 | ((bits & 8) << 3) | ((bits & 112) >> 1) | bits & 128;
    char braille = (char) (BRAILLE_BASE + image);
    return braille;
  }

  final static int BRAILLE_BASE = 0x2800;

}
