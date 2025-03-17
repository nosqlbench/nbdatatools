package io.nosqlbench.nbvectors.commands.verify_knn.statusview;

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


import io.nosqlbench.nbvectors.commands.verify_knn.datatypes.BitImage;

import java.util.BitSet;

/// Various braille formatting and conversion functions,
/// used to crate a visual bitwise image of array values
/// ---
/// ## unicode braille table
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
public class Glyphs {

  /// This method will only use enough characters to represent the defined
  /// 1-bits. If you want to represent additional unset values, use [#braille(BitImage)]
  /// @param bits the bit set to render
  /// @return a braille string containing only the active bits
  public static String braille(BitSet bits) {
    byte[] bary = bits.toByteArray();
    return braille(bary);
  }

  /// Produce a dot-by-dot picture the provide bit image. This uses
  /// [unicode Braille](https://en.wikipedia.org/wiki/Braille_Patterns#Block) patterns
  /// The visual order of values goes from top to bottom, then left to right.
  /// @return a braille string containing positions for all active and inactive bits, up to the
  /// known length of the image
  /// @param bits the source image
  public static String braille(BitImage bits) {
    byte[] bary = bits.toByteArray();
    return braille(bary);
  }

  /// Convert byte values to a visual representation, 4-bits per column, down and then right
  /// @return a braille string for the given bytes
  /// @param bytes the byte array to render
  public static String braille(byte[] bytes) {
    StringBuilder sb = new StringBuilder();
    for (byte b : bytes) {
      sb.append(braille(b));
    }
    return sb.toString();
  }

  /// Render the given integer value as an equivalent braille character,
  /// with the order going down first column, then down second column.
  /// @param v the integer value to convert
  /// @return a braille char representing the values of a int
  public static char braille(int v) {
    return braille((byte) v);
  }

  /// Similar to {@link #braille(int)}, but for byte values.
  /// Byte values here are converted to unsigned form.
  /// @return a braille char representing the values of a byte
  /// @param b the byte to render
  public static char braille(byte b) {
    int bits = Byte.toUnsignedInt(b);
    int image = bits & 7 | ((bits & 8) << 3) | ((bits & 112) >> 1) | bits & 128;
    char braille = (char) (BRAILLE_BASE + image);
    return braille;
  }

  final static int BRAILLE_BASE = 0x2800;

  /// Render the byte array in visual hex form, with spaces between
  /// groups of 8 bytes and a visual `.` in between groups of 4 bytes
  /// @param bytes the byte array to render
  /// @return a hex string
  public static String hex(byte[] bytes) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < bytes.length; i++) {
      if ((i%4)==0) {
        sb.append(" ");
      } else {
        sb.append(".");
      }
      sb.append(String.format("%02X",bytes[i]));
    }
    return sb.toString();
  }
}
