package io.nosqlbench.verifyknn.experiments.pencodertest;

public enum Op {

  end(")"),
  eq("="),
  ne("!="),
  gt(">"),
  lt("<"),
  ge(">="),
  le("<="),
  in("in(");

  public final String symbol;

  Op(String symbol) {
    this.symbol = symbol;
  }

  public String format(long[] values) {
    StringBuilder sb = new StringBuilder();
    sb.append(this.name());
    String delim = "";
    for (long value : values) {
      sb.append(value);
      sb.append(delim);
      delim = ",";
    }
    return sb.toString();
  }
}
