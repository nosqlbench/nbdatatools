package io.nosqlbench.nbvectors.verifyknn.readers;

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
import io.nosqlbench.nbvectors.verifyknn.datatypes.LongIndexedFloatVector;
import io.nosqlbench.nbvectors.verifyknn.datatypes.NeighborIndex;
import io.nosqlbench.nbvectors.verifyknn.datatypes.Neighborhood;

/// This record type captures the basic requirements of a standard KNN answer key format.
///
/// credits:
/// This starting point for this format was initially derived from the
/// [ann-benchmark](https://github.com/erikbern/ann-benchmarks) HDF5 format.
public record KNNData(
    Dataset test,
    Dataset train,
    Dataset neighbors,
    Dataset distances,
    HdfFile hdfFile
) implements AutoCloseable
{
  public KNNData(HdfFile hdfFile) {
    this(
        hdfFile.getDatasetByPath("/test"),
        hdfFile.getDatasetByPath("/train"),
        hdfFile.getDatasetByPath("/neighbors"),
        hdfFile.getDatasetByPath("/distances"),
        hdfFile
    );
  }

  @Override
  public void close() throws Exception {
    hdfFile.close();
  }

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

  public Neighborhood[] neighborhoods(long minIncluded, long maxExcluded) {
    Neighborhood[] neighborhoods = new Neighborhood[(int) (maxExcluded - minIncluded)];
    for (int i = 0; i < neighborhoods.length; i++) {
      neighborhoods[i] = neighborhood(minIncluded + i);
    }
    return neighborhoods;
  }

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
      case int[][] intdata -> {
        long[] ary = new long[intdata[0].length];
        for (int i = 0; i < ary.length; i++) {
          ary[i] = intdata[0][i];
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


  public float[] train(int testVectorOrdinal) {
    int[] trainVectorDimensions = train().getDimensions();
    trainVectorDimensions[0] = 1;
    long[] trainVectorOffsets = new long[trainVectorDimensions.length];

    trainVectorOffsets[0] = testVectorOrdinal;
    Object trainVectorData = train().getData(trainVectorOffsets, trainVectorDimensions);
    float[] trainVector = ((float[][]) trainVectorData)[0];
    return trainVector;
  }

  public int trainingVectorCount() {
    return train().getDimensions()[0];
  }
}
