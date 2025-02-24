package io.nosqlbench.verifyknn.experiments.pencodertest;

import java.nio.ByteBuffer;
import java.util.Arrays;

public record Predicate(Term... t) {
  public ByteBuffer encode(ByteBuffer buf) {
    for (Term term : t) {
      term.encode(buf);
    }
    return buf;
  }
  public ByteBuffer encode() {
    ByteBuffer buf = ByteBuffer.allocate(100);
    return encode(buf).flip();
  }
  static Predicate decode(ByteBuffer buf) {
    Term[] terms = new Term[0];
    if (buf.remaining()==0) {
      throw new RuntimeException("short read on predicate buffer");
    }
    while (buf.hasRemaining()) {
      Term term = Term.decode(buf);
      Term[] newTerms = new Term[terms.length+1];
      System.arraycopy(terms,0,newTerms,0,terms.length);
      newTerms[newTerms.length-1]=term;
      terms = newTerms;
    }
    return new Predicate(terms);
  }

  @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer("Predicate{");
    sb.append("t=").append(t == null ? "null" : Arrays.asList(t).toString());
    sb.append('}');
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof Predicate predicate))
      return false;

    return Arrays.equals(t, predicate.t);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(t);
  }
}
