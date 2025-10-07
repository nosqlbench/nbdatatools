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


import io.nosqlbench.nbdatatools.api.types.Named;

import java.nio.file.Path;

/// The base type for all readable streams of vectors, in contrast to [VectorRandomAccessReader]
/// @param <T> The vector type. This is typically an array type like `float[]` or `int[]`, so you
///  would expect to see `Iterable<float[]>`, **NOT** `Iterable<float>`
public interface VectorFileStream<T> extends Iterable<T>, Named, AutoCloseable {
  /// All vector data streams must be opened this way, even if the path is an aggregator, like a
  /// containing directory.
  /// @param path The path to open.
  public void open(Path path);

  /// close the stream
  default void close() {}
}
