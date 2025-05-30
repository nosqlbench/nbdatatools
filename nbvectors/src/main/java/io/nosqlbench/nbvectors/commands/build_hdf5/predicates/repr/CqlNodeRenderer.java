package io.nosqlbench.nbvectors.commands.build_hdf5.predicates.repr;

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


import io.nosqlbench.vectordata.internalapi.predicates.ConjugateNode;
import io.nosqlbench.vectordata.internalapi.predicates.PredicateNode;
import io.nosqlbench.vectordata.internalapi.predicates.PNode;

/// A renderer for cql syntax
public class CqlNodeRenderer implements PredicateRepresenter {


  @Override
  public String apply(PNode<?> node) {
    return switch (node) {
      case ConjugateNode n -> reprConjugate(n);
      case PredicateNode p -> reprPredicate(p);
    };
  }

  private String reprPredicate(PredicateNode p) {
    StringBuilder sb = new StringBuilder();
    sb.append("F").append(p.field());
    if (isChar(p.op().symbol())) {
      sb.append(" ");
    }
    sb.append(p.op().symbol());
    if (p.v().length>1) {
      sb.append("(");
      String delim="";
      for (long v : p.v()) {
        sb.append(delim);
        sb.append(v);
        delim=",";
      }
      sb.append(")");
    } else {
      sb.append(p.v()[0]);
    }
    return sb.toString();
  }

  private boolean isChar(String symbol) {
    char c = symbol.charAt(0);
    return (c >= 'A' && c <= 'Z') || (c>='a' && c<='z');
  }

  private String reprConjugate(ConjugateNode n) {
    return switch (n.type()) {
      case AND,OR -> concatenate(n.type().name(),n.values());
      case PRED -> throw new RuntimeException("impossible unless broken code");
    };
  }

  private String concatenate(String name, PNode<?>[] values) {
    StringBuilder sb = new StringBuilder();

    for (PNode<?> value : values) {
      String nodeRep = apply(value);
      if (!sb.isEmpty()) {
        sb.append(" ").append(name).append(" ");
      }
      sb.append(nodeRep);
    }
    return sb.toString();
  }

}
