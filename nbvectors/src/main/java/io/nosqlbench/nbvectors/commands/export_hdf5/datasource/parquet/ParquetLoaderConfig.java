package io.nosqlbench.nbvectors.commands.export_hdf5.datasource.parquet;

import com.google.common.io.Files;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

public class ParquetLoaderConfig {
  public Optional<List<Path>> getBaseVectorsLayout() {
    return null;
  }
}
