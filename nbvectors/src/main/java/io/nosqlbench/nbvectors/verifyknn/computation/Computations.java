package io.nosqlbench.nbvectors.verifyknn.computation;

import io.nosqlbench.nbvectors.verifyknn.datatypes.BitImage;

import java.util.Arrays;
import java.util.BitSet;

public class Computations {
  /// Take two arrays of unordered ints and compute the three
  /// sets: a-only, common, b-only, AKA a-b, a∩b, b-a
  /// with all elements in order
  public static int SET_A = 0;
  public static int SET_BOTH = 1;
  public static int SET_B = 2;

  public static long[][] partitions(long[] provided, long[] expected) {
    Arrays.sort(provided);
    Arrays.sort(expected);

    int a_index = 0, b_index = 0;
    long a_element, b_element;

    long[] common_view = new long[provided.length + expected.length];
    long[] a_view = new long[provided.length + expected.length];
    long[] b_view = new long[provided.length + expected.length];
    int cursor = 0;
    int a_bufidx = 0, b_bufidx = 0, c_bufidx = 0;
    while (a_index < provided.length && b_index < expected.length) {

      a_element = provided[a_index];
      b_element = expected[b_index];
      if (a_element == b_element) {
        common_view[c_bufidx++] = a_element;
        a_index++;
        b_index++;
      } else if (b_element < a_element) {
        b_view[b_bufidx++] = b_element;
        b_index++;
      } else { // b_element > a_element
        a_view[a_bufidx++] = a_element;
        a_index++;
      }
      cursor++;
    }
    a_view = Arrays.copyOf(a_view, a_bufidx);
    b_view = Arrays.copyOf(b_view, b_bufidx);
    common_view = Arrays.copyOf(common_view, c_bufidx);
    return new long[][] {a_view, common_view, b_view};
  }

  /// Produce a dot-by-dot picture of the indices of the expected array
  /// which are also present in the provided array. This uses
  /// [unicode Braille](https://en.wikipedia.org/wiki/Braille_Patterns#Block) patterns
  /// The visual order of values goes from top to bottom, then left to right.
  /// ---
  /// ### examples
  /// ```
  /// provided[0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ] expected[0 ,1, 2 ,3 ,4 ,5 ,6 ,7 ]  image[⣿]
  /// provided[3 ,5 ,9 ,17,32,33,88,99] expected[3 ,5 ,9 ,17,32,33,88,99]  image[⣿]
  /// provided[0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ] expected[33]                       image[⠀]
  /// provided[0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ] expected[0 ,1, 2 ,3 ,4 ,5 ,6 ,7 ]  image[⡊]
  /// provided[0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ] expected[            4 ,5 ,6 ,7 ]  image[⡇]
  /// provided[0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ] expected[7 ,   5 ,   3 ,2 ,1, 0 ]  image[⣗]
  /// provided[0..15]                   expected[0..3,5,7,9,11,12]         image[⣗⡊]
  /// provided[0..31]                   expected[]                         image[⠀⠀⠀⠀]
  /// ```
  ///
  public static BitImage matchingImage(long[] expected_ary, long[] provided_ary) {
    Arrays.sort(expected_ary);
    int expected_idx = 0;
    long expected_data;

    Arrays.sort(provided_ary);
    int provided_idx = 0;
    long provided_data;

    BitImage matching_bits = new BitImage(expected_ary.length);
    while (provided_idx < provided_ary.length && expected_idx < expected_ary.length) {
      provided_data = provided_ary[provided_idx];
      expected_data = expected_ary[expected_idx];
      if (provided_data == expected_data) {
        matching_bits.set(expected_idx);
        provided_idx++;
        expected_idx++;
      } else if (expected_data < provided_data) {
        expected_idx++;
      } else {
        provided_idx++;
      }
    }
    while (expected_idx < expected_ary.length) {
      matching_bits.setZero(expected_idx);
      expected_idx++;
    }
    return matching_bits;
  }

  public static BitSetDelta bitmaps(long[] a, long[] b) {
    Arrays.sort(a);
    Arrays.sort(b);

    int a_index = 0, b_index = 0;
    long a_element, b_element;
    BitSet aBits = new BitSet(a.length);
    BitSet bBits = new BitSet(b.length);
    int position = 0;
    while (a_index < a.length && b_index < b.length) {
      a_element = a[a_index];
      b_element = b[b_index];
      if (a_element == b_element) {
        aBits.set(position);
        bBits.set(position);
        a_index++;
        b_index++;
      } else if (b_element < a_element) {
        bBits.set(position);
        b_index++;
      } else {
        aBits.set(position);
        a_index++;
      }
      position++;
    }
    return new BitSetDelta(aBits, bBits);
  }

  public static record BitSetDelta(BitSet provided, BitSet expected) {
  }


}
