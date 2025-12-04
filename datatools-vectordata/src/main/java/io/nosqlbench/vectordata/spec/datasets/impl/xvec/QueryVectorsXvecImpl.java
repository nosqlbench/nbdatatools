package io.nosqlbench.vectordata.spec.datasets.impl.xvec;

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

import io.nosqlbench.vectordata.layoutv2.DSWindow;
import io.nosqlbench.vectordata.spec.datasets.types.QueryVectors;

import java.nio.channels.AsynchronousFileChannel;

/// Query vector view backed by xvec formatted data.
///
/// This class extends FloatVectorsXvecImpl to provide query vector functionality.
/// It accepts any AsynchronousFileChannel implementation, including MAFileChannel,
/// following the decorator pattern for orthogonal channel implementation support.
public class QueryVectorsXvecImpl extends FloatVectorsXvecImpl implements QueryVectors {

  /// Create a new QueryVectors view for xvec data.
  /// @param channel The AsynchronousFileChannel providing access to the dataset (includes MAFileChannel)
  /// @param sourceSize The size of the source file in bytes
  /// @param window The configured window describing the slice of data to expose
  /// @param extension The file extension (used to derive the vector format)
  public QueryVectorsXvecImpl(AsynchronousFileChannel channel, long sourceSize, DSWindow window, String extension) {
    super(channel, sourceSize, window, extension);
  }
}
