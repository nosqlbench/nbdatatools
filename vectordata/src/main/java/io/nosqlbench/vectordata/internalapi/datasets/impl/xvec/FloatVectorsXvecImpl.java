package io.nosqlbench.vectordata.internalapi.datasets.impl.xvec;

import io.nosqlbench.vectordata.download.merkle.MerkleRAF;
import io.nosqlbench.vectordata.internalapi.datasets.impl.hdf5.CoreHdf5DatasetViewMethods;
import io.nosqlbench.vectordata.layout.manifest.DSWindow;

/// Implementation of float vector access for xvec file formats.
///
/// This class provides methods for accessing float vectors stored in xvec files,
/// extending the core functionality provided by CoreXVecDatasetViewMethods.
public class FloatVectorsXvecImpl extends CoreXVecDatasetViewMethods<float[]> {
  /// Creates a new FloatVectorsXvecImpl instance.
  ///
  /// @param randomio The random access file to read from
  /// @param sourceSize The size of the source file in bytes
  /// @param window The window to use for accessing the data
  /// @param extension The file extension indicating the vector format
  public FloatVectorsXvecImpl(
      MerkleRAF randomio,
      long sourceSize,
      DSWindow window,
      String extension
  )
  {
    super(randomio, sourceSize, window, extension);
  }
}
