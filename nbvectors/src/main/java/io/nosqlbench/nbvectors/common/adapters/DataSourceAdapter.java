package io.nosqlbench.nbvectors.common.adapters;

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


import io.nosqlbench.nbvectors.commands.export_hdf5.datasource.ivecfvec.FvecToFloatArray;
import io.nosqlbench.nbvectors.commands.export_hdf5.datasource.ivecfvec.FvecToIndexedFloatVector;
import io.nosqlbench.nbvectors.commands.export_hdf5.datasource.ivecfvec.IvecToIntArray;
import io.nosqlbench.nbvectors.commands.export_hdf5.datasource.parquet.ParquetVectorsReader;
import io.nosqlbench.nbvectors.commands.export_hdf5.datasource.parquet.traversal.ParquetDataAdapter;
import io.nosqlbench.nbvectors.commands.verify_knn.datatypes.LongIndexedFloatVector;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class DataSourceAdapter {

  public static Iterable<LongIndexedFloatVector> adaptQueryVectors(Path path) {
    return adaptLongIndexedFloatVector(path);
  }

  public static Iterable<Object> adaptBaseContent(Path path) {
    return null;
  }

  public static Iterable<float[]> adaptBaseVectors(Path path) {
    return adaptFloatArray(path);
  }

  private static Iterable<LongIndexedFloatVector> adaptLongIndexedFloatVector(Path path) {
    if (path.toString().endsWith(".fvec") || path.toString().endsWith(".fvecs")) {
      return new FvecToIndexedFloatVector(path);
    }

    throw new RuntimeException("Unsupported path type: " + path);
  }

  public static Iterable<int[]> adaptNeighborIndices(Path path) {
    return adaptIntArray(path);
  }

  private static Iterable<int[]> adaptIntArray(Path path) {
    if (path.toString().endsWith(".ivec") || path.toString().endsWith(".ivecs")) {
      return new IvecToIntArray(path);
    }

    throw new RuntimeException("Unsupported path type: " + path);
  }

  public static Iterable<float[]> adaptNeighborDistances(Path path) {
    return adaptFloatArray(path);
  }

  private static Iterable<float[]> adaptFloatArray(Path path) {
    if (path.toString().endsWith(".fvec") || path.toString().endsWith(".fvecs")) {
      return new FvecToFloatArray(path);
    } else if (Files.isDirectory(path)) {
      return new ParquetDataAdapter(List.of(path));
    } else {
      throw new RuntimeException("file type not recognized: " + path);
    }
  }
}
