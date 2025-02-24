package io.nosqlbench.verifyknn.experiments.nodewalk.repr;

import io.nosqlbench.verifyknn.experiments.nodewalk.types.ConjugateNode;
import io.nosqlbench.verifyknn.experiments.nodewalk.types.NodeRepresenter;
import io.nosqlbench.verifyknn.experiments.nodewalk.types.PredicateNode;
import io.nosqlbench.verifyknn.experiments.nodewalk.types.Node;

public class CqlNodeRenderer implements NodeRepresenter {
  @Override
  public String apply(Node<?> node) {
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

  private String concatenate(String name, Node<?>[] values) {
    StringBuilder sb = new StringBuilder();

    for (int i = 0; i < values.length; i++) {
      Node<?> value = values[i];
      String nodeRep = apply(value);
      if (!sb.isEmpty()) {
        sb.append(" ").append(name).append(" ");
      }
      sb.append(nodeRep);
    }
    return sb.toString();
  }

}
