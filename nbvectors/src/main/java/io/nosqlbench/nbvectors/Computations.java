package io.nosqlbench.nbvectors;

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

  public static BitSet matchingImage(long[] provided_ary, long[] expected_ary) {
    Arrays.sort(provided_ary);
    Arrays.sort(expected_ary);
    int a_index = 0, b_index = 0;
    long a_element, b_element;
    BitSet aBits = new BitSet(provided_ary.length);
    while (a_index < provided_ary.length && b_index < expected_ary.length) {
      a_element = provided_ary[a_index];
      b_element = expected_ary[b_index];
      if (a_element == b_element) {
        aBits.set(b_index);
        a_index++;
        b_index++;
      } else if (b_element < a_element) {
        b_index++;
      } else {
        a_index++;
      }
    }
    return aBits;
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
