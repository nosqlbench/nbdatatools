package io.nosqlbench.nbvectors.buildhdf5.predicates.types;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

public class NodeTest {

  @Test
  public void testLoading() {
    var pn1 = new PredicateNode(0,OpType.IN, 234, 343);
    var pn2 = new PredicateNode(1, OpType.LE, 5);
    var pn3 = new ConjugateNode(ConjugateType.OR,pn1,pn2);

    ByteBuffer bb = ByteBuffer.allocate(100);

    pn1.encode(bb.clear()).flip();
    var rn1 = Node.fromBuffer(bb);
    assertThat(rn1).isEqualTo(pn1);

    pn2.encode(bb.clear()).flip();
    var rn2 = Node.fromBuffer(bb);
    assertThat(rn2).isEqualTo(pn2);

    pn3.encode(bb.clear()).flip();
    var rn3 = Node.fromBuffer(bb);
    assertThat(rn3).isEqualTo(pn3);



  }

}