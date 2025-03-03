package io.nosqlbench.nbvectors.verifyknn.datatypes;

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
/// with its length, including explicitly defined 0 and 1 values,
/// rather than just implicitly tracking size based on 1-values
public class BitImage {
  private BitSet ones = new BitSet();
  private BitSet zeros = new BitSet();

  public BitImage(int length) {
    zeros.set(length - 1);
  }

  public BitImage(BitSet zeros, BitSet ones) {
    this.ones = ones;
    this.zeros = zeros;
  }

  public void set(int expectedIdx) {
    ones.set(expectedIdx);
  }

  public void setZero(int idx) {
    zeros.set(idx);
  }

  public int length() {
    return Math.max(ones.length(), zeros.length());
  }

  public BitSet mask() {
    BitSet mask = new BitSet();
    mask.or(ones);
    mask.or(zeros);
    return mask;
  }

  public byte[] toByteArray() {
    byte[] oneBits = ones.toByteArray();
    byte[] zeroBits = zeros.toByteArray();
    byte[] allBits = new byte[Math.max(oneBits.length, zeroBits.length)];
    for (int bit = 0; bit < oneBits.length; bit++) {
      allBits[bit] = oneBits[bit];
    }
    return allBits;
  }

  public BitSet getOnes() {
    return ones;
  }

  public BitSet getZeros() {
    return zeros;
  }

  public boolean get(int idx) {
    return ones.get(idx);
  }
}
