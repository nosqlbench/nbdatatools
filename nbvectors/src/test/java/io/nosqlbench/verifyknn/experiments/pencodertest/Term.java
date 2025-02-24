package io.nosqlbench.verifyknn.experiments.pencodertest;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

public record Term(int field, Op op, long... values) {

  public static Term decode(ByteBuffer buf) {
    byte fieldOffset = buf.get();
    Op op = Op.values()[buf.get()];
    return switch (op) {
      case in -> {
        int count = buf.getInt();
        long[] values = new long[count];
        for (int i = 0; i < count; i++) {
          values[i]=buf.getLong();
        }
        yield new Term(fieldOffset,op,values);
      }
      default -> new Term(fieldOffset,op,new long[] {buf.getLong()});
    };
  }

  public void encode(ByteBuffer buf) {
    buf.put((byte)field).put((byte) op.ordinal());
    if (op==Op.in) {
      buf.putInt(values.length);
    }
    for (long l : values) {
      buf.putLong(l);
    }
  }

  @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer("Term{");
    sb.append("field=").append(field);
    sb.append(", op=").append(op);
    sb.append(", values=");
    if (values == null)
      sb.append("null");
    else {
      sb.append('[');
      for (int i = 0; i < values.length; ++i)
        sb.append(i == 0 ? "" : ", ").append(values[i]);
      sb.append(']');
    }
    sb.append('}');
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof Term term))
      return false;

    return field == term.field && op == term.op && Arrays.equals(values, term.values);
  }

  @Override
  public int hashCode() {
    int result = field;
    result = 31 * result + Objects.hashCode(op);
    result = 31 * result + Arrays.hashCode(values);
    return result;
  }
}
