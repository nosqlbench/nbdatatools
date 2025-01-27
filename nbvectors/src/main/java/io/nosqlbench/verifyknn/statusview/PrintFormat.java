package io.nosqlbench.verifyknn.statusview;

import io.nosqlbench.verifyknn.datatypes.IndexedFloatVector;
import io.nosqlbench.verifyknn.datatypes.NeighborIndex;
import io.nosqlbench.verifyknn.datatypes.Neighborhood;

import java.util.Arrays;

public enum PrintFormat {
  all;

  public static StringBuilder format(
      String description,
      IndexedFloatVector testVector,
      Neighborhood neighbors
  )
  {
    StringBuilder sb = new StringBuilder(description).append(":\n");
    sb.append("query index: ").append(testVector.index()).append("\n");
    sb.append("query vector: ").append(Arrays.toString(testVector.vector())).append("\n");
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
