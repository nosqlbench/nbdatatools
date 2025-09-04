package io.nosqlbench.command.analyze.subcommands.verify_knn.readers;

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


import io.jhdf.HdfFile;
import io.jhdf.api.Dataset;
import io.nosqlbench.nbdatatools.api.fileio.LongIndexedFloatVector;
import io.nosqlbench.command.analyze.subcommands.verify_knn.datatypes.NeighborIndex;
import io.nosqlbench.command.analyze.subcommands.verify_knn.datatypes.Neighborhood;
import io.nosqlbench.vectordata.spec.datasets.types.TestDataKind;

import java.util.Objects;

/// This class captures the basic requirements of a standard KNN answer key format.
///
/// Credits:
///
/// This starting point for this format was initially derived from the
/// [ann-benchmark](https://github.com/erikbern/ann-benchmarks) HDF5 format.
public class KNNData implements AutoCloseable {
  private final Dataset test;
  private final Dataset train;
  private final Dataset neighbors;
  private final Dataset distances;
  private final HdfFile hdfFile;

  public KNNData(Dataset test, Dataset train, Dataset neighbors, Dataset distances, HdfFile hdfFile) {
    this.test = test;
    this.train = train;
    this.neighbors = neighbors;
    this.distances = distances;
    this.hdfFile = hdfFile;
  }

  public Dataset test() {
    return test;
  }

  public Dataset train() {
    return train;
  }

  public Dataset neighbors() {
    return neighbors;
  }

  public Dataset distances() {
    return distances;
  }

  public HdfFile hdfFile() {
    return hdfFile;
  }
  /// create a knn data reader
  /// @param hdfFile the hdfFile containing the datasets
  public KNNData(HdfFile hdfFile) {
    this(
        hdfFile.getDatasetByPath(TestDataKind.query_vectors.name()),
        hdfFile.getDatasetByPath(TestDataKind.base_vectors.name()),
        hdfFile.getDatasetByPath(TestDataKind.neighbor_indices.name()),
        hdfFile.getDatasetByPath(TestDataKind.neighbor_distances.name()),
        hdfFile
    );
  }

  @Override
  public void close() throws Exception {
    hdfFile.close();
  }

  /// Read a specific test vector from the HDF5 file based on its order in the dataset
  /// @param index the index of the test vector to read
  /// @return the test vector
  public LongIndexedFloatVector readHdf5TestVector(long index) {
    int[] testVectorDimensions = test().getDimensions();
    long[] testVectorOffsets = new long[testVectorDimensions.length];
    // get 1 test vector row at a time; we simply re-use the dim array
    testVectorDimensions[0] = 1;

    testVectorOffsets[0] = index;
    Object testVectorData = test().getData(testVectorOffsets, testVectorDimensions);
    float[] testVector = ((float[][]) testVectorData)[0];
    return new LongIndexedFloatVector(index, testVector);
  }

  /// Read a specific range neighborhoods from the HDF5 file based on their order in the dataset
  /// @param minIncluded the index of the neighborhood to read, inclusive
  /// @param maxExcluded the last index of the neighborhood to read, exclusive
  /// @return the neighborhoods
  public Neighborhood[] neighborhoods(long minIncluded, long maxExcluded) {
    Neighborhood[] neighborhoods = new Neighborhood[(int) (maxExcluded - minIncluded)];
    for (int i = 0; i < neighborhoods.length; i++) {
      neighborhoods[i] = neighborhood(minIncluded + i);
    }
    return neighborhoods;
  }

  /// Read a specific neighborhood from the HDF5 file based on its order in the dataset
  /// @param ordinal the index of the neighborhood to read
  /// @return the neighborhood
  public Neighborhood neighborhood(long ordinal)
  {
    Neighborhood answerKey = new Neighborhood();
    Dataset neighbors = neighbors();
    Dataset distances = distances();

    int[] dimensions = neighbors.getDimensions();
    long[] sliceOffset = new long[dimensions.length];
    sliceOffset[0] = ordinal;
    dimensions[0] = 1;

    Object neighborsData = neighbors.getData(sliceOffset, dimensions);

    // normalize type to double, supporting maximum precision for both cases
    long[] neighborIndices;
    if (neighborsData instanceof long[][]) {
      long[][] longdata = (long[][]) neighborsData;
      neighborIndices = longdata[0];
    } else if (neighborsData instanceof int[][]) {
      int[][] intData = (int[][]) neighborsData;
      neighborIndices = new long[intData[0].length];
      for (int i = 0; i < neighborIndices.length; i++) {
        neighborIndices[i] = intData[0][i];
      }
    } else {
      throw new IllegalStateException("Unexpected value: " + neighborsData);
    }

    Object distancesData = distances.getData(sliceOffset, dimensions);
    float[] neighborDistances = ((float[][]) distancesData)[0];

    for (int i = 0; i < neighborDistances.length; i++) {
      answerKey.add(new NeighborIndex(neighborIndices[i], neighborDistances[i]));
    }

    return answerKey;
  }


  /// Read a specific training vector from the HDF5 file based on its order in the dataset
  /// @param testVectorOrdinal the index of the training vector to read
  /// @return the training vector
  public float[] train(int testVectorOrdinal) {
    int[] trainVectorDimensions = train().getDimensions();
    trainVectorDimensions[0] = 1;
    long[] trainVectorOffsets = new long[trainVectorDimensions.length];

    trainVectorOffsets[0] = testVectorOrdinal;
    Object trainVectorData = train().getData(trainVectorOffsets, trainVectorDimensions);
    return ((float[][]) trainVectorData)[0];
  }

  /// Get the number of training vectors in the HDF5 file
  /// @return the number of training vectors
  public int trainingVectorCount() {
    return train().getDimensions()[0];
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    KNNData knnData = (KNNData) o;
    return Objects.equals(test, knnData.test) &&
           Objects.equals(train, knnData.train) &&
           Objects.equals(neighbors, knnData.neighbors) &&
           Objects.equals(distances, knnData.distances) &&
           Objects.equals(hdfFile, knnData.hdfFile);
  }

  @Override
  public int hashCode() {
    return Objects.hash(test, train, neighbors, distances, hdfFile);
  }

  @Override
  public String toString() {
    return "KNNData{" +
           "test=" + test +
           ", train=" + train +
           ", neighbors=" + neighbors +
           ", distances=" + distances +
           ", hdfFile=" + hdfFile +
           '}';
  }
}
