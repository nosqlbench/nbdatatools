package io.nosqlbench.nbvectors.buildhdf5.predicates.types;

public enum OpType {
  GT(">"),
  LT("<"),
  EQ("="),
  NE("!="),
  GE(">="),
  LE("<="),
  IN("IN");

  private final String symbol;

  OpType(String symbol) {
    this.symbol=symbol;
  }

  public String symbol() {
    return this.symbol;
  }
}
