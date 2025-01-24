package io.nosqlbench.nbvectors;

import java.util.Comparator;

/// IndexedVector global ordering is based on index, not vector shape
/// If you want to compare by vector value ordering, use [#BY_VECTOR_SHAPE]
public record IndexedFloatVector(
    long index, float[] vector
) implements Comparable<IndexedFloatVector>
{
  @Override
  public int compareTo(IndexedFloatVector o) {
    return Long.compare(index, o.index);
  }

  public static Comparator<IndexedFloatVector> BY_VECTOR_SHAPE = new Comparator<IndexedFloatVector>() {

    @Override
    public int compare(IndexedFloatVector o1, IndexedFloatVector o2) {
      int range = Math.min(o1.vector.length, o2.vector.length);
      for (int i = 0; i < range; i++) {
        int diff = Double.compare(o1.vector[i], o2.vector[i]);
        if (diff != 0)
          return diff;
      }
      if (o1.vector.length > range)
        return -1;
      if (o2.vector.length > range)
        return 1;
      return 0;
    }
  };

  @Override
  public String toString() {

    return "query("+index+"):float["+vector.length+"] " + String.format("%+1.3f %+1.3f %+1.3f",
        vector[0], vector[1], vector[2]) + "...";
  }
}
