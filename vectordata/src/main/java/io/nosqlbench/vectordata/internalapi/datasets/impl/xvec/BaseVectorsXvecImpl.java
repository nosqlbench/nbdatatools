package io.nosqlbench.vectordata.internalapi.datasets.impl.xvec;

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


import io.nosqlbench.vectordata.download.merkle.MerkleRAF;
import io.nosqlbench.vectordata.internalapi.datasets.api.BaseVectors;
import io.nosqlbench.vectordata.layout.manifest.DSWindow;

/// Implementation of BaseVectors interface for xvec file format.
/// This class extends FloatVectorsXvecImpl to provide base vector functionality
/// for xvec files, which store vectors in a binary format.
public class BaseVectorsXvecImpl extends FloatVectorsXvecImpl implements BaseVectors {
  /// Creates a new BaseVectorsXvecImpl instance.
  ///
  /// @param randomio The random access file to read from
  /// @param sourceSize The size of the source file in bytes
  /// @param window The window to use for accessing the data
  /// @param extension The file extension indicating the vector format
  public BaseVectorsXvecImpl(MerkleRAF randomio, long sourceSize, DSWindow window, String extension)
  {
    super(randomio, sourceSize, window, extension);
  }
}
