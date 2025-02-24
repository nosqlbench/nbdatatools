package io.nosqlbench.verifyknn.experiments.pencodertest;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;

public class EncoderTest {

  @Test
  public void testEncoder() {
    Predicate p1 = new Predicate(new Term(0, Op.in, 23L), new Term(1, Op.eq, 234L));
    System.out.println(p1);
    ByteBuffer b1 = p1.encode();
    Predicate p2 = Predicate.decode(b1);
    System.out.println(p2);
    assertThat(p1).isEqualTo(p2);
  }
}
