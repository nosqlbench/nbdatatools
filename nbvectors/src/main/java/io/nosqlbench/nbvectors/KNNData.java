package io.nosqlbench.nbvectors;

import io.jhdf.HdfFile;
import io.jhdf.api.Dataset;

public record KNNData(
    Dataset test, Dataset train, Dataset neighbors, Dataset distances, HdfFile hdfFile
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
}
