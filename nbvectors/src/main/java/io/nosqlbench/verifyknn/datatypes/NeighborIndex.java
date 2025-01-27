package io.nosqlbench.verifyknn.datatypes;

public record NeighborIndex(
    long index, double distance
) implements Comparable<NeighborIndex>
{
  @Override
  public int compareTo(NeighborIndex o) {
    return Double.compare(distance, o.distance);
  }

  @Override
  public String toString() {
    return "("+index+","+String.format("%.3f",distance)+")";
  }
}
