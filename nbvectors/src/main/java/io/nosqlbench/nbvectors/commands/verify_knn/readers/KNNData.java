package io.nosqlbench.nbvectors.commands.verify_knn.readers;

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
import io.nosqlbench.nbvectors.commands.verify_knn.datatypes.LongIndexedFloatVector;
import io.nosqlbench.nbvectors.commands.verify_knn.datatypes.NeighborIndex;
import io.nosqlbench.nbvectors.commands.verify_knn.datatypes.Neighborhood;
import io.nosqlbench.nbvectors.spec.views.SpecDatasets;

/// This record type captures the basic requirements of a standard KNN answer key format.
///
/// Credits:
///
/// This starting point for this format was initially derived from the
/// [ann-benchmark](https://github.com/erikbern/ann-benchmarks) HDF5 format.
/// @param test the test dataset, containing query vectors, organized by index
/// @param train the train dataset, containing all vectors to search, organized by index
/// @param neighbors the neighbors dataset describing correct KNN results, organized by index
/// @param distances the distances dataset describing pair-wise distances for neighbors,
///  organized by index
/// @param hdfFile the hdfFile containing the datasets
public record KNNData(
    Dataset test,
    Dataset train,
    Dataset neighbors,
    Dataset distances,
    HdfFile hdfFile
) implements AutoCloseable
{
  /// create a knn data reader
  /// @param hdfFile the hdfFile containing the datasets
  public KNNData(HdfFile hdfFile) {
    this(
        hdfFile.getDatasetByPath(SpecDatasets.query_vectors.name()),
        hdfFile.getDatasetByPath(SpecDatasets.base_vectors.name()),
        hdfFile.getDatasetByPath(SpecDatasets.neighbor_indices.name()),
        hdfFile.getDatasetByPath(SpecDatasets.neighbor_distances.name()),
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
    long[] neighborIndices = switch (neighborsData) {
      case long[][] longdata -> longdata[0];
      case int[][] intData -> {
        long[] ary = new long[intData[0].length];
        for (int i = 0; i < ary.length; i++) {
          ary[i] = intData[0][i];
        }
        yield ary;
      }
      default -> throw new IllegalStateException("Unexpected value: " + neighborsData);
    };

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
}
