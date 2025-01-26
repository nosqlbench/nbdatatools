package io.nosqlbench.nbvectors.datatypes;

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
