package io.nosqlbench.vectordata.spec.datasets.impl.hdf5;

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


import io.jhdf.api.Dataset;
import io.nosqlbench.vectordata.spec.datasets.types.IntVectors;
import io.nosqlbench.vectordata.layout.FWindow;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/// A view of integer vectors data
public class IntVectorsHdf5Impl extends CoreHdf5DatasetViewMethods<int[]> implements IntVectors {

  /// create a new integer vectors view
  /// @param dataset the dataset to view
  /// @param window the window to use
  public IntVectorsHdf5Impl(Dataset dataset, FWindow window) {
    super(dataset, window);
  }

  @Override
  public List<Set<Integer>> asSets() {
    List<Set<Integer>> sets = new ArrayList<>(this.getCount());
    for (int[] ints : this) {
      HashSet<Integer> set = new HashSet<>(ints.length);
      sets.add(set);
      for (int i : ints) {
        set.add(i);
      }
      sets.add(set);
    }
    return sets;
  }

}
