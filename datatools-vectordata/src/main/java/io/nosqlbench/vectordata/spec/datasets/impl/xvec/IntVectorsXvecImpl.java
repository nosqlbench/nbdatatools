package io.nosqlbench.vectordata.spec.datasets.impl.xvec;

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

import io.nosqlbench.vectordata.layoutv2.DSWindow;
import io.nosqlbench.vectordata.spec.datasets.types.IntVectors;

import java.nio.channels.AsynchronousFileChannel;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/// Implementation of IntVectors backed by xvec resources.
public class IntVectorsXvecImpl extends CoreXVecDatasetViewMethods<int[]> implements IntVectors {

  /// Create a new IntVectors view for xvec data.
  /// @param channel The AsynchronousFileChannel providing access to the dataset
  /// @param sourceSize The size of the source file in bytes
  /// @param window The configured window describing the slice of data to expose
  /// @param extension The file extension (used to derive the vector format)
  public IntVectorsXvecImpl(AsynchronousFileChannel channel, long sourceSize, DSWindow window, String extension) {
    super(channel, sourceSize, window, extension);
  }

  /// {@inheritDoc}
  @Override
  public List<Set<Integer>> asSets() {
    List<Set<Integer>> sets = new ArrayList<>(getCount());
    for (int[] vector : this) {
      LinkedHashSet<Integer> set = new LinkedHashSet<>(vector.length);
      for (int value : vector) {
        set.add(value);
      }
      sets.add(set);
    }
    return sets;
  }
}
