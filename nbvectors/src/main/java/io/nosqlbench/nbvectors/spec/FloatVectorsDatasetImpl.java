package io.nosqlbench.nbvectors.spec;

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
import io.nosqlbench.nbvectors.spec.access.datasets.BaseVectors;
import io.nosqlbench.nbvectors.spec.access.datasets.FloatVectorsDataset;

/// a dataset consisting of arrays of float values
public class FloatVectorsDatasetImpl extends BaseVectorsDataset<Float> implements FloatVectorsDataset  {

  /// create a float vectors dataset
  /// @param dataset the dataset
  public FloatVectorsDatasetImpl(Dataset dataset) {
    super(dataset);
  }

  /// get a vector by its ordinal
  /// @param ordinal the ordinal of the vector to get
  /// @return the vector
  @Override
  public float[] getVector(int ordinal) {
    return new float[0];
  }

}
