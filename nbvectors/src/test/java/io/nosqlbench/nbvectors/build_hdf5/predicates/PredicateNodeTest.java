package io.nosqlbench.nbvectors.build_hdf5.predicates;

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


import io.nosqlbench.nbvectors.commands.build_hdf5.predicates.repr.CqlNodeRenderer;
import io.nosqlbench.vectordata.internalapi.predicates.ConjugateNode;
import io.nosqlbench.vectordata.internalapi.predicates.PredicateNode;
import io.nosqlbench.vectordata.internalapi.predicates.ConjugateType;
import io.nosqlbench.vectordata.internalapi.predicates.PNode;
import io.nosqlbench.vectordata.internalapi.predicates.OpType;
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
    PNode tn = getTestNode1();
    ByteBuffer b = ByteBuffer.allocate(100);
    tn.encode(b);
    b.flip();
    PNode<?> node = PNode.fromBuffer(b);
    System.out.println(node);
  }

  private PNode<?> getTestNode1() {
    PredicateNode inp = new PredicateNode(1, OpType.IN, new long[]{3L, 4L});
    PredicateNode e3 = new PredicateNode(0, OpType.EQ, 3L);
    ConjugateNode tn = new ConjugateNode(ConjugateType.OR, new PNode[]{e3, inp});
    return tn;
  }

  @Test
  public void testRepresenter() {
    PNode<?> tn1 = getTestNode1();
    System.out.println("tn1:\n"+tn1.toString());
    CqlNodeRenderer pr = new CqlNodeRenderer();
    String represented = pr.apply(tn1);
    System.out.println(represented);


  }


}
