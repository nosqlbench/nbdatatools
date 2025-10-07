package io.nosqlbench.command.build_hdf5.predicates.types;

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


import io.nosqlbench.vectordata.spec.predicates.ConjugateNode;
import io.nosqlbench.vectordata.spec.predicates.ConjugateType;
import io.nosqlbench.vectordata.spec.predicates.OpType;
import io.nosqlbench.vectordata.spec.predicates.PNode;
import io.nosqlbench.vectordata.spec.predicates.PredicateNode;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;

public class NodeTest {

  @Test
  public void testLoading() {
    var pn1 = new PredicateNode(0, OpType.IN, 234, 343);
    var pn2 = new PredicateNode(1, OpType.LE, 5);
    var pn3 = new ConjugateNode(ConjugateType.OR,pn1,pn2);

    ByteBuffer bb = ByteBuffer.allocate(100);

    pn1.encode(bb.clear()).flip();
    var rn1 = PNode.fromBuffer(bb);
    assertThat(rn1).isEqualTo(pn1);

    pn2.encode(bb.clear()).flip();
    var rn2 = PNode.fromBuffer(bb);
    assertThat(rn2).isEqualTo(pn2);

    pn3.encode(bb.clear()).flip();
    var rn3 = PNode.fromBuffer(bb);
    assertThat(rn3).isEqualTo(pn3);



  }

}
