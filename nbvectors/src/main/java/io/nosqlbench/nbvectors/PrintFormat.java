package io.nosqlbench.nbvectors;

import java.util.Arrays;
import java.util.List;

public enum PrintFormat {
  all;

  public static StringBuilder format(
      String description,
      long index,
      float[] queryV,
      List<NeighborIndex> neighbors
  )
  {
    StringBuilder sb = new StringBuilder(description).append(":\n");
    sb.append("query index: ").append(index).append("\n");
    sb.append("query vector: ").append(Arrays.toString(queryV)).append("\n");
    sb.append("neighbors:\n").append(neighbors).append("\n\n");

    long[] indices =
        neighbors.stream().mapToLong(NeighborIndex::index).toArray();
    sb.append(" indices:\n").append(Arrays.toString(indices)).append("\n\n");

    double[] distances =
        neighbors.stream().mapToDouble(NeighborIndex::distance).toArray();
    sb.append(" distances:\n").append(Arrays.toString(distances)).append("\n\n");
    return sb;
  }
}
