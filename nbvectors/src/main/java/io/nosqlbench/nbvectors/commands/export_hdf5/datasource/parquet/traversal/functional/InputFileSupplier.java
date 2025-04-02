package io.nosqlbench.nbvectors.commands.export_hdf5.datasource.parquet.traversal.functional;

import io.nosqlbench.nbvectors.commands.jjq.bulkio.iteration.ConvertingIterable;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.LocalInputFile;

import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

public class InputFileSupplier implements Supplier<InputFile> {
  private final Iterator<InputFile> iterator;

  public InputFileSupplier(List<Path> files, boolean recurse) {

    ConvertingIterable<Path, InputFile> inputFileIterable =
        new ConvertingIterable<>(files, LocalInputFile::new);
    this.iterator = inputFileIterable.iterator();
  }

  @Override
  public InputFile get() {
    if (this.iterator.hasNext()) {
      return iterator.next();
    }
    return null;
  }

}
