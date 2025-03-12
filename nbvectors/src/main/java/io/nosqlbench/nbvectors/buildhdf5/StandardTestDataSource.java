package io.nosqlbench.nbvectors.buildhdf5;

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


import io.nosqlbench.nbvectors.verifyknn.datatypes.LongIndexedFloatVector;

import java.util.Iterator;

/// Defines the provider interface for test data which may be used
/// to construct a valid HDF5 test data file.
///
/// Methods which return optional correspond to optional components of the
/// test data spec. Other methods are required to be implemented and return
/// a non-null value.
public interface StandardTestDataSource {
  Iterator<LongIndexedFloatVector> getBaseVectors();

  Iterator<LongIndexedFloatVector> getQueryVectors();

  Iterator<long[]> getNeighborIndices();

  Iterator<float[]> getDistances();
}
