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
import io.nosqlbench.vectordata.merklev2.MAFileChannel;
import io.nosqlbench.vectordata.spec.datasets.types.FloatVectors;

import java.nio.channels.AsynchronousFileChannel;

/// Implementation of float vector access for xvec file formats.
///
/// This class provides methods for accessing float vectors stored in xvec files,
/// extending the core functionality provided by CoreXVecDatasetViewMethods.
public class FloatVectorsXvecImpl extends CoreXVecDatasetViewMethods<float[]> implements FloatVectors {
  /// Creates a new FloatVectorsXvecImpl instance.
  ///
  /// @param channel The MAFileChannel to read from
  /// @param sourceSize The size of the source file in bytes
  /// @param window The window to use for accessing the data
  /// @param extension The file extension indicating the vector format
  public FloatVectorsXvecImpl(
      AsynchronousFileChannel channel,
      long sourceSize,
      DSWindow window,
      String extension
  )
  {
    super(channel, sourceSize, window, extension);
  }
}
