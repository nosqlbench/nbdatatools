package io.nosqlbench.nbvectors.buildhdf5.predicates.types;

import java.nio.ByteBuffer;
import java.util.Arrays;

public record ConjugateNode(ConjugateType type, Node<?>... values)
    implements BBWriter<ConjugateNode>, Node<ConjugateNode>
{

  public ConjugateNode(ByteBuffer b) {
    this(ConjugateType.values()[b.get()], readValues(b));
  }

  private static Node<?>[] readValues(ByteBuffer b) {
    byte count = b.get();
    Node<?>[] elements = new Node[count];
    for (int i = 0; i < elements.length; i++) {
      elements[i] = Node.fromBuffer(b);
    }
    return elements;
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

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof ConjugateNode(ConjugateType type1, Node<?>[] values1)))
      return false;

    return Arrays.equals(values, values1) && type == type1;
  }

  @Override
  public int hashCode() {
    int result = type.hashCode();
    result = 31 * result + Arrays.hashCode(values);
    return result;
  }
}
