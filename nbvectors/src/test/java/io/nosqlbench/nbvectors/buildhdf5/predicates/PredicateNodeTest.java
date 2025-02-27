package io.nosqlbench.nbvectors.buildhdf5.predicates;

import io.nosqlbench.verifyknn.experiments.nodewalk.repr.CqlNodeRenderer;
import io.nosqlbench.nbvectors.buildhdf5.predicates.types.ConjugateNode;
import io.nosqlbench.nbvectors.buildhdf5.predicates.types.PredicateNode;
import io.nosqlbench.nbvectors.buildhdf5.predicates.types.ConjugateType;
import io.nosqlbench.nbvectors.buildhdf5.predicates.types.Node;
import io.nosqlbench.nbvectors.buildhdf5.predicates.types.OpType;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;

public class PredicateNodeTest {
  @Test
  public void testPred() {
    ByteBuffer b = ByteBuffer.allocate(100);
    PredicateNode p1 = new PredicateNode(0, OpType.EQ, 777);
    p1.encode(b);
    b.flip();
    PredicateNode p2 = new PredicateNode(b);
    assertThat(p1).isEqualTo(p2);
    System.out.println(p1);
  }

  @Test
  public void testNesting() {
    Node tn = getTestNode1();
    ByteBuffer b = ByteBuffer.allocate(100);
    tn.encode(b);
    b.flip();
    Node<?> node = Node.fromBuffer(b);
    System.out.println(node);
  }

  private Node<?> getTestNode1() {
    PredicateNode inp = new PredicateNode(1, OpType.IN, new long[]{3L, 4L});
    PredicateNode e3 = new PredicateNode(0, OpType.EQ, 3L);
    ConjugateNode tn = new ConjugateNode(ConjugateType.OR, new Node[]{e3, inp});
    return tn;
  }

  @Test
  public void testRepresenter() {
    Node<?> tn1 = getTestNode1();
    System.out.println("tn1:\n"+tn1.toString());
    CqlNodeRenderer pr = new CqlNodeRenderer();
    String represented = pr.apply(tn1);
    System.out.println(represented);


  }


}