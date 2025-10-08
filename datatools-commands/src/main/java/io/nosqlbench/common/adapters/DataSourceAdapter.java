package io.nosqlbench.common.adapters;

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


import io.nosqlbench.command.hdf5.subcommands.export_hdf5.datasource.ivecfvec.BvecToByteArray;
import io.nosqlbench.command.hdf5.subcommands.export_hdf5.datasource.ivecfvec.FvecToIndexedFloatVector;
import io.nosqlbench.command.hdf5.subcommands.export_hdf5.datasource.ivecfvec.IvecToIntArray;
import io.nosqlbench.nbdatatools.api.fileio.LongIndexedFloatVector;
import io.nosqlbench.nbdatatools.api.fileio.BoundedVectorFileStream;

import java.nio.file.Path;

/// Adapt a path to a data source, depending on the file name or contents
public class DataSourceAdapter {

  /// Adapt a path to a data source, depending on the file name or contents
  /// @param path
  ///     the path to adapt
  /// @return an iterable of query vectors
  public static Iterable<float[]> adaptQueryVectors(Path path) {
    return adaptFloatArray(path);
  }

  /// Adapt a path to a data source, depending on the file name or contents
  /// @param path
  ///     the path to adapt
  /// @return an iterable of base content
  public static Iterable<Object> adaptBaseContent(Path path) {
    return null;
  }

  /// Adapt a path to a data source, depending on the file name or contents
  /// @param path
  ///     the path to adapt
  /// @return an iterable of base vectors
  public static Iterable<float[]> adaptBaseVectors(Path path) {
    return adaptFloatArray(path);
  }

  /// Adapt a path to a data source, depending on the file name or contents
  /// @param path
  ///     the path to adapt
  /// @return an iterable of long indexed float vectors
  private static Iterable<LongIndexedFloatVector> adaptLongIndexedFloatVector(Path path) {
    if (path.toString().endsWith(".fvec") || path.toString().endsWith(".fvecs")) {
      return new FvecToIndexedFloatVector(path);
    }

    throw new RuntimeException("Unsupported path type: " + path);
  }

  /// Adapt a path to a data source, depending on the file name or contents
  /// @param path
  ///     the path to adapt
  /// @return an iterable of neighbor indices
  public static Iterable<int[]> adaptNeighborIndices(Path path) {
    return adaptIntArray(path);
  }

  /// Adapt a path to a data source, depending on the file name or contents
  /// @param path
  ///     the path to adapt
  /// @return an iterable of int arrays
  public static Iterable<int[]> adaptIntArray(Path path) {
    if (path.toString().endsWith(".ivec") || path.toString().endsWith(".ivecs")) {
      return new IvecToIntArray(path);
    }

    throw new RuntimeException("Unsupported path type: " + path);
  }

  public static Iterable<byte[]> adaptByteArray(Path path) {
    if (path.toString().endsWith(".bvec") || path.toString().endsWith(".bvecs")) {
      return new BvecToByteArray(path);
    } else {
      throw new RuntimeException("file type not recognized: " + path);
    }
  }

  /// Adapt a path to a data source, depending on the file name or contents
  /// @param path
  ///     the path to adapt
  /// @return an iterable of neighbor distances
  public static Iterable<float[]> adaptNeighborDistances(Path path) {
    return adaptFloatArray(path);
  }

  /// Adapt a path to a data source, depending on the file name or contents
  /// @param path
  ///     the path to adapt
  /// @return an iterable of float arrays
  public static Iterable<float[]> adaptFloatArray(Path path) {
    ReaderAndPath rp = new ReaderAndPath(path.toString());
    BoundedVectorFileStream<float[]> sizedReader = rp.getSizedReader(float[].class);
    return sizedReader;
  }

  public static Iterable<?> adaptAnyType(Path path) {
    ReaderAndPath rp = new ReaderAndPath(path.toString());
    BoundedVectorFileStream<?> sizedReader = rp.getSizedReader(Object.class);
    return sizedReader;
  }
}
