package io.nosqlbench.command.verify_knn.datatypes;

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


import java.util.BitSet;

/// Like [[BitSet]], but only for capturing a bit image
/// with its length, including affirmatively set zero and one values,
/// rather than just implicitly tracking size based on 1-values
public class BitImage {
  private BitSet ones = new BitSet();
  private BitSet zeros = new BitSet();

  /// create a bit image
  /// @param length initial size of the bit set
  public BitImage(int length) {
    zeros.set(length - 1);
  }

  /// create a bit image
  /// @param ones initial one-value positions
  /// @param zeros initial zero-value positions
  public BitImage(BitSet zeros, BitSet ones) {
    this.ones = ones;
    this.zeros = zeros;
  }

  /// Affirmatively set a given index to one
  /// @param idx index to set to one
  public void set(int idx) {
    ones.set(idx);
  }

  /// Affirmatively set a given index to zero
  /// @param idx index to set to zero
  public void setZero(int idx) {
    zeros.set(idx);
  }

  /// Get the length of the bit set
  /// @return the length of this bitset, meaning the furthest position that has been affirmatively
  ///  set to either zero or one
  public int length() {
    return Math.max(ones.length(), zeros.length());
  }

  /// Get a mask containing all one-values, but including the length of both one-values and
  /// zero-values
  /// @return The bit mask of all positions which have been affirmatively set, whether zero or one
  public BitSet mask() {
    BitSet mask = new BitSet();
    mask.or(ones);
    mask.or(zeros);
    return mask;
  }

  /// Get a byte array representation of this bit field
  /// @return a byte array representation of this bit field, which will include bytes for all
  /// bits represented as either affirmatively set zero or ones. Up to 7 extra bits may be
  /// present since this is not bit-field length.
  public byte[] toByteArray() {
    byte[] oneBits = ones.toByteArray();
    byte[] zeroBits = zeros.toByteArray();
    byte[] allBits = new byte[Math.max(oneBits.length, zeroBits.length)];
    System.arraycopy(oneBits, 0, allBits, 0, oneBits.length);
    return allBits;
  }

  /// get a bitset representing the affirmatively set one-values
  /// @return the bit set
  public BitSet getOnes() {
    return ones;
  }

  /// get a bitset representing the affirmatively set zero-values
  ///  @return the zeroes bit set
  public BitSet getZeros() {
    return zeros;
  }

  /// get the bit at the given index
  /// @return the bit
  /// @param idx the index
  public boolean get(int idx) {
    return ones.get(idx);
  }
}
