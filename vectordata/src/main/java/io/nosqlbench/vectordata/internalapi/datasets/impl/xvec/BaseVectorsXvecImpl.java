package io.nosqlbench.vectordata.internalapi.datasets.impl.xvec;

import io.nosqlbench.vectordata.download.merkle.MerkleRAF;
import io.nosqlbench.vectordata.internalapi.datasets.api.BaseVectors;
import io.nosqlbench.vectordata.layout.manifest.DSWindow;

/// Implementation of BaseVectors interface for xvec file format.
/// This class extends FloatVectorsXvecImpl to provide base vector functionality
/// for xvec files, which store vectors in a binary format.
public class BaseVectorsXvecImpl extends FloatVectorsXvecImpl implements BaseVectors {
  /// Creates a new BaseVectorsXvecImpl instance.
  ///
  /// @param randomio The random access file to read from
  /// @param sourceSize The size of the source file in bytes
  /// @param window The window to use for accessing the data
  /// @param extension The file extension indicating the vector format
  public BaseVectorsXvecImpl(MerkleRAF randomio, long sourceSize, DSWindow window, String extension)
  {
    super(randomio, sourceSize, window, extension);
  }
}
