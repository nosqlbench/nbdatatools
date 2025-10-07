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
