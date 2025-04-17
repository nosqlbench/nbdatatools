package io.nosqlbench.vectordata.internalapi.datasets.impl.xvec;

import io.nosqlbench.vectordata.download.merkle.MerkleRAF;
import io.nosqlbench.vectordata.internalapi.datasets.impl.hdf5.CoreHdf5DatasetViewMethods;
import io.nosqlbench.vectordata.layout.manifest.DSWindow;

public class FloatVectorsXvecImpl extends CoreXVecDatasetViewMethods<float[]> {
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
