package io.nosqlbench.nbdatatools.api.fileio;

/*
 * Copyright (c) nosqlbench
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import java.nio.file.Path;
import io.nosqlbench.nbdatatools.api.fileio.VectorStreamStore;

/// A way to store a sequence of vectors in a file.
/// @param <T> The type of vector value
public interface VectorFileStreamStore<T> extends VectorStreamStore<T>  {
  /// Initialize the writer with a path
  /// @param path
  ///   The path to initialize the writer with
  public void open(Path path);

  /// Flush any buffered data to the underlying storage.
  /// This ensures that all data written so far is persisted.
  /// The default implementation does nothing, but implementations should override this
  /// if they buffer data.
  default void flush() {};

  /// This should be called at the end of the writer lifecycle.
  /// Implementations should ensure all data is flushed and resources are released.
  /// @throws RuntimeException if there is an error closing the writer
  void close();
}
