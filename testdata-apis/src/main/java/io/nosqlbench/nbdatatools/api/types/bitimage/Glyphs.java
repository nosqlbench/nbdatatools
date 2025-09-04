package io.nosqlbench.nbdatatools.api.types.bitimage;

/// Copyright (c) nosqlbench
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///   http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing,
/// software distributed under the License is distributed on an
/// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
/// KIND, either express or implied.  See the License for the
/// specific language governing permissions and limitations
/// under the License.


import java.util.BitSet;

/// Various braille formatting and conversion functions,
/// used to crate a visual bitwise image of array values
/// ---
/// ## unicode braille table
/// ```
/// 0x00:0x20⠀ ⠀⠁⠂⠃⠄⠅⠆⠇⠈⠉⠊⠋⠌⠍⠎⠏⠐⠑⠒⠓⠔⠕⠖⠗⠘⠙⠚⠛⠜⠝⠞⠟
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
///
/// ## dot order by bit position
/// ```
/// byte = 0b a b c d e f g h
///           │ │ │ │ │ │ │ │
/// pixels    0 1 2 3 4 5 6 7
/// dots      1 2 3 7 4 5 6 8
/// bits      0 1 2 6 3 4 5 7
///```
///
/// ## Unicode dot mapping
/// ```
/// | Dot | Unicode bit (binary) | Hex bit |
/// | --- | -------------------- | ------- |
/// | 1   | 00000001 (bit 0)     | 0x01    |
/// | 2   | 00000010 (bit 1)     | 0x02    |
/// | 3   | 00000100 (bit 2)     | 0x04    |
/// | 4   | 00001000 (bit 3)     | 0x08    |
/// | 5   | 00010000 (bit 4)     | 0x10    |
/// | 6   | 00100000 (bit 5)     | 0x20    |
/// | 7   | 01000000 (bit 6)     | 0x40    |
/// | 8   | 10000000 (bit 7)     | 0x80    |
///```
///
/// # In-place character mapping
///
/// Supposed I want to be able to address into the char buffer with uniform indexing and the represent unicode like braille characters?
///
/// To achieve a uniform indexing scheme (one index per character, directly addressable), and represent Unicode characters like Braille uniformly, your simplest and most efficient solution is:
/// * Use UTF-16 encoding (Java's internal encoding for char).
/// * Map the file using a MappedByteBuffer.
/// * View the underlying buffer directly as a CharBuffer.
///
/// This approach allows you to have each Unicode character represented exactly by one char (for
///  code points in the Basic Multilingual Plane, including all Braille characters at U+2800 to
/// U+28FF). Because each Java char is uniformly 2 bytes, you can directly use integer indices
/// into the CharBuffer
///
/// ## Example Approach
///
/// 1) Create and size the file:
///
/// ```Java
/// RandomAccessFile raf = new RandomAccessFile("braille_data.bin", "rw");
/// FileChannel channel = raf.getChannel();
///
/// // Decide how many Braille chars you need. Example: 1024 characters
/// int charCount = 1024;
/// int byteSize = charCount * Character.BYTES; // each char = 2 bytes
///
/// // Resize the file to the exact byte size needed
/// raf.setLength(byteSize);
///```
///
/// 2) Memory-map the file into a MappedByteBuffer:
/// ```Java
/// MappedByteBuffer mappedBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, byteSize);
/// mappedBuffer.order(ByteOrder.BIG_ENDIAN); // Java chars default to big-endian UTF-16
///```
///
/// 3) Obtain a uniform CharBuffer view:
/// ```Java
/// CharBuffer charBuffer = mappedBuffer.asCharBuffer();
///```
///
/// 4) Place a braille character by offset
/// ```Java
/// // Fill with empty Braille cells (U+2800)
/// for (int i = 0; i < charBuffer.capacity(); i++){
///     charBuffer.put(i, '\u2800');
///}
///
/// // Example: Toggle dots for character at index 42
/// int idx = 42;
/// char current = charBuffer.get(idx);
/// char toggled = (char)(current ^ 0x04); // Toggle dot 3
/// charBuffer.put(idx, toggled);
///```
///
public class Glyphs {

  /// Default constructor for the Glyphs utility class.
  /// This class provides static methods for working with braille characters and bit representations.
  public Glyphs() {
    // Default constructor
  }

  /// all the braille characters in perceptual column-row order
  public static String table = ("⠀⠁⠂⠃⠄⠅⠆⠇⡀⡁⡂⡃⡄⡅⡆⡇\n" +
      "⠈⠉⠊⠋⠌⠍⠎⠏⡈⡉⡊⡋⡌⡍⡎⡏\n" +
      "⠐⠑⠒⠓⠔⠕⠖⠗⡐⡑⡒⡓⡔⡕⡖⡗\n" +
      "⠘⠙⠚⠛⠜⠝⠞⠟⡘⡙⡚⡛⡜⡝⡞⡟\n" +
      "⠠⠡⠢⠣⠤⠥⠦⠧⡠⡡⡢⡣⡤⡥⡦⡧\n" +
      "⠨⠩⠪⠫⠬⠭⠮⠯⡨⡩⡪⡫⡬⡭⡮⡯\n" +
      "⠰⠱⠲⠳⠴⠵⠶⠷⡰⡱⡲⡳⡴⡵⡶⡷\n" +
      "⠸⠹⠺⠻⠼⠽⠾⠿⡸⡹⡺⡻⡼⡽⡾⡿\n" +
      "⢀⢁⢂⢃⢄⢅⢆⢇⣀⣁⣂⣃⣄⣅⣆⣇\n" +
      "⢈⢉⢊⢋⢌⢍⢎⢏⣈⣉⣊⣋⣌⣍⣎⣏\n" +
      "⢐⢑⢒⢓⢔⢕⢖⢗⣐⣑⣒⣓⣔⣕⣖⣗\n" +
      "⢘⢙⢚⢛⢜⢝⢞⢟⣘⣙⣚⣛⣜⣝⣞⣟\n" +
      "⢠⢡⢢⢣⢤⢥⢦⢧⣠⣡⣢⣣⣤⣥⣦⣧\n" +
      "⢨⢩⢪⢫⢬⢭⢮⢯⣨⣩⣪⣫⣬⣭⣮⣯\n" +
      "⢰⢱⢲⢳⢴⢵⢶⢷⣰⣱⣲⣳⣴⣵⣶⣷\n" +
      "⢸⢹⢺⢻⢼⢽⢾⢿⣸⣹⣺⣻⣼⣽⣾⣿\n").replaceAll("\n", "");
  ///  all the braille characters in perceptual column-row order
  public static char[] chars = table.toCharArray();
  /// all the ordinal offsets of characters based on their disordered magnitude over the base
  ///
  public static int[] lookupOffset = invertWithBase(chars);


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

  /// Return a braille glyph representing the numeric value (bitwise and) the provided numeric mask
  /// position
  /// @param c the char
  /// @param mask the mask to apply
  /// @return the modified char
  public static char andMask(char c, int mask) {
    if (c == ' ')
      c = '\u2800';
    // People may think a space and the "zero" braille glyph are the same but they are not.
    int order = lookupOffset[c-BRAILLE_BASE];
    int offset = mask & order;
    return chars[offset];
  }

  /// Return a braille glyph representing the numeric value (bitwise or) the provided numeric mask
  /// position
  /// @param c the char
  /// @param mask the mask to apply
  /// @return the modified char
  public static char orMask(char c, int mask) {
    if (c == ' ')
      c = '\u2800';
    // People may think a space and the "zero" braille glyph are the same but they are not.
    int order = lookupOffset[c-BRAILLE_BASE];
    int offset = mask | order;
    return chars[offset];
  }

  /// Return a braille glyph representing the numeric value (bitwise and not) the provided numeric
  /// mask
  /// position
  /// @param c the char
  /// @param mask the mask to apply
  /// @return the modified char
  public static char andNotMask(char c, int mask) {
    if (c == ' ')
      c = '\u2800';
    // People may think a space and the "zero" braille glyph are the same but they are not.
    int order = lookupOffset[c-BRAILLE_BASE];
    int offset = (~mask) & order;
    return chars[offset];
  }

  /// These masks determine the active bit relative to the left shift order
  public static int[] maskByShift = new int[]{0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80};

  final static int BRAILLE_BASE = 0x2800;

  /// Render the byte array in visual hex form, with spaces between
  /// groups of 8 bytes and a visual `.` in between groups of 4 bytes
  /// @param bytes the byte array to render
  /// @return a hex string
  public static String hex(byte[] bytes) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < bytes.length; i++) {
      if ((i % 4) == 0) {
        sb.append(" ");
      } else {
        sb.append(".");
      }
      sb.append(String.format("%02X", bytes[i]));
    }
    return sb.toString();
  }

  private static int[] invertWithBase(char[] perm) {
    int base = perm[0];
    int[] inverse = new int[perm.length];
    for (int i = 0; i < perm.length; i++) {

      char c = perm[i];
      int v = c - base;
      inverse[v] = i;
    }
    return inverse;
  }


}
