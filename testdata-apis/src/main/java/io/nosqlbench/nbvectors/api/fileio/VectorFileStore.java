package io.nosqlbench.nbvectors.api.fileio;

import io.nosqlbench.nbvectors.api.noncore.VectorStreamStore;

import java.nio.file.Path;

public interface VectorFileStore<T> extends VectorStreamStore<T>  {
  /**
   Initialize the writer with a path
   @param path
   The path to initialize the writer with
   */
  public void open(Path path);

  default void flush() {};

  /// This should be called at the end of the writer lifecycle.
  void close();
}
