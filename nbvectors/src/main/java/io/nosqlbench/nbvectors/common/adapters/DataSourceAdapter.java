package io.nosqlbench.nbvectors.common.adapters;

import io.nosqlbench.nbvectors.commands.export_hdf5.datasource.ivecfvec.FvecToFloatArray;
import io.nosqlbench.nbvectors.commands.export_hdf5.datasource.ivecfvec.FvecToIndexedFloatVector;
import io.nosqlbench.nbvectors.commands.export_hdf5.datasource.ivecfvec.IvecToIntArray;
import io.nosqlbench.nbvectors.commands.verify_knn.datatypes.LongIndexedFloatVector;

import java.nio.file.Path;

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
    if (path.endsWith(".fvec") || path.endsWith(".fvecs")) {
      return new FvecToIndexedFloatVector(path);
    }

    throw new RuntimeException("Unsupported path type: " + path);
  }

  public static Iterable<int[]> adaptNeighborIndices(Path path) {
    return adaptIntArray(path);
  }

  private static Iterable<int[]> adaptIntArray(Path path) {
    if (path.endsWith(".ivec") || path.endsWith(".ivecs")) {
      return new IvecToIntArray(path);
    }

    throw new RuntimeException("Unsupported path type: " + path);
  }

  public static Iterable<float[]> adaptNeighborDistances(Path path) {
    return adaptFloatArray(path);
  }

  private static Iterable<float[]> adaptFloatArray(Path path) {
    if (path.endsWith(".fvec") || path.endsWith(".fvecs")) {
      return new FvecToFloatArray(path);
    } else {
      throw new RuntimeException("file type not recognized");
    }
  }
}
