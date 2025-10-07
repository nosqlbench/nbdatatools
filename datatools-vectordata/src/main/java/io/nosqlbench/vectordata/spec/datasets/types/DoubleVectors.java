package io.nosqlbench.vectordata.spec.datasets.types;

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


/// a dataset consisting of arrays of float values
public interface DoubleVectors extends DatasetView<double[]> {
  /// get a vector by its ordinal
  /// @param ordinal the ordinal of the vector to get
  /// @return the vector
  public double[] getVector(long ordinal);
  /// get a range of vectors by their ordinals
  /// @param startInclusive the startInclusive ordinal, inclusive
  /// @param endExclusive the end ordinal, exclusive
  /// @return the vectors
  public double[][] getVectors(long startInclusive, long endExclusive);

}

