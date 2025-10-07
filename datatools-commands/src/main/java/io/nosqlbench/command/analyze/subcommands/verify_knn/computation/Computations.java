package io.nosqlbench.command.analyze.subcommands.verify_knn.computation;

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


import io.nosqlbench.nbdatatools.api.types.bitimage.BitImage;

import java.util.Arrays;
import java.util.BitSet;

/// Static computation methods used by various nbvectors classes
public class Computations {
  /// the first of three sets returned by {@link #partitions(long[], long[])}
  public static int SET_A = 0;
  /// the second of three sets returned by {@link #partitions(long[], long[])}
  public static int SET_BOTH = 1;
  /// the third of three sets returned by {@link #partitions(long[], long[])}
  public static int SET_B = 2;

  /// given two sets of unordered longs, compute a-only, common, b-only, AKA a-b, a∩b, b-a
  /// compute three sets of values and return them all
  /// The returned arrays are enumerated as
  /// result\[[#SET_A]\],
  /// result\[[#SET_BOTH]\], and
  /// result\[[#SET_B]\]:
  /// 0. values only found in the first provided set; _A∖B_; _A-B_; _provided∖expected_
  /// 1. values common to both sets; _A∩B_; _A intersect B_; _provided ∩ expected_
  /// 2. values only found in the second provided set; _B∖A_; _B-A_; _expected∖provided_
  /// @param provided The indices which were set
  /// @param expected The indices which were expected to be set
  /// @return the effective venn diagram members of A\B, A∩B, and B\A
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

  /// given two sets of unordered longs, compute a-only, common, b-only, AKA a-b, a∩b, b-a
  /// compute three sets of values and return them all
  /// The returned arrays are enumerated as
  /// result\[[#SET_A]\],
  /// result\[[#SET_BOTH]\], and
  /// result\[[#SET_B]\]:
  /// 0. values only found in the first provided set; _A∖B_; _A-B_; _provided∖expected_
  /// 1. values common to both sets; _A∩B_; _A intersect B_; _provided ∩ expected_
  /// 2. values only found in the second provided set; _B∖A_; _B-A_; _expected∖provided_
  /// @param provided The indices which were set
  /// @param expected The indices which were expected to be set
  /// @return the effective venn diagram members of A\B, A∩B, and B\A
  public static int[][] partitions(int[] provided, int[] expected) {
    Arrays.sort(provided);
    Arrays.sort(expected);

    int a_index = 0, b_index = 0;
    int a_element, b_element;

    int[] common_view = new int[provided.length + expected.length];
    int[] a_view = new int[provided.length + expected.length];
    int[] b_view = new int[provided.length + expected.length];
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
    return new int[][] {a_view, common_view, b_view};
  }


  /// Produce a bit image which represents a picture of expected array indices which are
  /// also present in the provided array.
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
  /// @param expected_ary The array of bits which are expected to be set
  /// @param provided_ary The array of bits which were actually set
  /// @return a bit image which
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

  /// Produce a bit image which represents a picture of expected array indices which are
  /// also present in the provided array.
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
  /// @param expected_ary The array of bits which are expected to be set
  /// @param provided_ary The array of bits which were actually set
  /// @return a bit image which
  ///
  public static BitImage matchingImage(int[] expected_ary, int[] provided_ary) {
    Arrays.sort(expected_ary);
    int expected_idx = 0;
    int expected_data;

    Arrays.sort(provided_ary);
    int provided_idx = 0;
    int provided_data;

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

  /// compute the difference between two sets of unsorted longs, covering over
  /// only the values which are provided in either set, but with the sorted position of each of
  /// represented solely by their rank.
  /// ---
  /// ### examples
  /// ```text
  ///   a [0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ] △ b [0 ,1, 2 ,3 ,4 ,5 ,6 ,7 ]
  /// = a'[0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ]   b'[0 ,1, 2 ,3 ,4 ,5 ,6 ,7 ]
  ///
  ///   a [3 ,5 ,9 ,17,32,33,88,99] △ b [3 ,5 ,9 ,17,32,33,88,99]
  /// = a'[0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ]   b'[0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ]
  ///
  ///   a [0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ] △ b [33]
  /// = a'[0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ]   b'[ 8]
  ///
  ///   a [0 ,1 ,2 ,3 ] △ b [0 ,4 ,5 ]
  /// = a'[0 ,1 ,2 ,3, 4, 5 ], b'[0 ,4 ,5 ]
  ///
  ///   a [8, 6, 7, 5, 3, 0, 9 ] △ b [2 ,4, 6, 0, 1]
  /// = a'[0 ,3 ,5 ,6 ,7 ,8 ,9 ]   b'[0, 1, 2, 4, 6]
  /// ```
  ///
  /// @param a a set of long values
  /// @param b a set of long values
  /// @return the reduced sets of value indices
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

  /// Represents a delta △ between the provided and expected bitsets, like
  /// {@link #bitmaps(long[], long[])}, but for bitsets instead
  public static class BitSetDelta {
    /// the actual indices, in the form of a bit mask
    private final BitSet provided;
    /// the expected indices, in the form of a bit mask
    private final BitSet expected;
    
    public BitSetDelta(BitSet provided, BitSet expected) {
      this.provided = provided;
      this.expected = expected;
    }
    
    public BitSet provided() { return provided; }
    public BitSet expected() { return expected; }
  }


}
