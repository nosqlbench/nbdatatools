package io.nosqlbench.nbvectors.verifyknn.datatypes;

/*
 * Copyright (c) nosqlbench
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import java.util.Comparator;

/// IndexedVector global ordering is based on index, not vector shape
/// If you want to compare by vector value ordering, use [#BY_VECTOR_SHAPE]
public record LongIndexedFloatVector(
    long index, float[] vector
) implements Comparable<LongIndexedFloatVector>
{
  @Override
  public int compareTo(LongIndexedFloatVector o) {
    return Long.compare(index, o.index);
  }

  public static Comparator<LongIndexedFloatVector> BY_VECTOR_SHAPE = new Comparator<LongIndexedFloatVector>() {

    @Override
    public int compare(LongIndexedFloatVector o1, LongIndexedFloatVector o2) {
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
