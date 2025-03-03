package io.nosqlbench.nbvectors.verifyknn.statusview;

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


import io.nosqlbench.nbvectors.verifyknn.datatypes.BitImage;

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
