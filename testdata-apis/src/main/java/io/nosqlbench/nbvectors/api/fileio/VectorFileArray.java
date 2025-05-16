package io.nosqlbench.nbvectors.api.fileio;

import io.nosqlbench.nbvectors.api.noncore.VectorRandomAccessReader;

import java.nio.file.Path;

/// This interface represents random access to vector data that is read from a file
/// @param <T> The vector type, an array type
public interface VectorFileArray<T> extends VectorRandomAccessReader<T>, AutoCloseable {

  /// All file readers must be opened this way, even if the provided file path is an
  /// aggregator, like a containing directory
  /// @param filePath The source of vector data
  public void open(Path filePath);

  @Override
  void close();
}
