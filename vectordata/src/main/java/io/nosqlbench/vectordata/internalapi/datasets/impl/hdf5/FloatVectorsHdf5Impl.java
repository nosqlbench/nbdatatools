package io.nosqlbench.vectordata.internalapi.datasets.impl.hdf5;

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
import io.nosqlbench.vectordata.internalapi.datasets.api.FloatVectors;
import io.nosqlbench.vectordata.layout.FWindow;

/// a dataset consisting of arrays of float values
public class FloatVectorsHdf5Impl extends CoreHdf5DatasetViewMethods<float[]>
    implements FloatVectors
{
  /// create a new float vectors dataset
  /// @param dataset the dataset to wrap
  /// @see FloatVectors
  public FloatVectorsHdf5Impl(Dataset dataset, FWindow window) {
    super(dataset, window);
  }
}
