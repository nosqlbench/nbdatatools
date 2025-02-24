package io.nosqlbench.verifyknn.experiments.nodewalk.types;

import java.nio.ByteBuffer;
import java.util.Arrays;

public record ConjugateNode(ConjugateType type, Node<?>[] values)
    implements BBWriter<ConjugateNode>, Node<ConjugateNode>
{

  public ConjugateNode(ByteBuffer b) {
    this(ConjugateType.values()[b.get()], readValues(b));
  }

  private static Node<?>[] readValues(ByteBuffer b) {
    byte count = b.get();
    Node<?>[] elements = new Node[count];
    for (int i = 0; i < elements.length; i++) {
      elements[i] = readValue(b);
    }
    return elements;
  }

  private static Node readValue(ByteBuffer b) {
    ConjugateType eType = ConjugateType.values()[b.get()];
    return switch (eType) {
      case PRED -> new PredicateNode(b);
      default -> new ConjugateNode(b);
    };
  }

  @Override
  public ByteBuffer encode(ByteBuffer out) {
    out.put((byte) this.type.ordinal()).put((byte) this.values.length);
    for (Node<?> element : values) {
      element.encode(out);
    }
    return out;
  }

  @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer("ConjugateNode{");
    sb.append("type=").append(type);
    sb.append(", v=").append(values == null ? "null" : Arrays.asList(values).toString());
    sb.append('}');
    return sb.toString();
  }
}
