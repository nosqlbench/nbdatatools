package io.nosqlbench.nbvectors.verifyknn.options;

///  Allow either min_included..max_excluded or simply max_excluded format
public record Interval(long start, long end)  {
  public int count() {
    return (int) (end()-start());
  }

}
