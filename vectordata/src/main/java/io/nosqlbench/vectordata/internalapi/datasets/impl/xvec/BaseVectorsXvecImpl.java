package io.nosqlbench.vectordata.internalapi.datasets.impl.xvec;

import io.nosqlbench.vectordata.download.merkle.MerkleRAF;
import io.nosqlbench.vectordata.internalapi.datasets.api.BaseVectors;
import io.nosqlbench.vectordata.layout.manifest.DSWindow;

public class BaseVectorsXvecImpl extends FloatVectorsXvecImpl implements BaseVectors {
  public BaseVectorsXvecImpl(MerkleRAF randomio, long sourceSize, DSWindow window, String extension)
  {
    super(randomio, sourceSize, window, extension);
  }
}
