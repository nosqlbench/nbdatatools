package io.nosqlbench.nbvectors.statusview;

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
/// ```
/// ## braille bit positions
/// ```
/// 1 4
/// 2 5
/// 3 6
/// 7 8
/// ```
/// ## ordinal transform
/// lsb pos 3 -> 7
/// lsb pos 4-6 -> 3-5

public class Glyphs {

  public static String braille(BitSet bits) {
    byte[] bary = bits.toByteArray();
    return braille(bary);
  }

  public static String braille(byte[] bytes) {
    StringBuilder sb = new StringBuilder();
    for (byte b : bytes) {
      int bits = Byte.toUnsignedInt(b);
      int image = bits & 7 | ((bits & 8)<<3) | ((bits & 112)>>1) | bits & 128;

      char braille = braille(image);
      sb.append(braille);
    }
    return sb.toString();
  }

  final static int BRAILLE_BASE = 0x2800;

  public static char braille(int value) {
    return (char) (BRAILLE_BASE + value);
  }

}
