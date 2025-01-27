package io.nosqlbench.verifyknn.datatypes;

import org.junit.jupiter.api.Test;

import java.util.BitSet;

import static org.assertj.core.api.Assertions.assertThat;


public class BitImageTest {

  @Test
  public void testBasicImage() {
    BitSet ones = new BitSet();
    BitSet zeros = new BitSet();

    BitImage image = new BitImage(zeros, ones);
    ones.set(0);
    assertThat(image.getOnes()).isEqualTo(ones);
    assertThat(image.length()).isEqualTo(1);
    zeros.set(99);
    assertThat(image.length()).isEqualTo(100);
  }

}