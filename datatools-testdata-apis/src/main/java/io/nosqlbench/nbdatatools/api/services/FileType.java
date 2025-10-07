package io.nosqlbench.nbdatatools.api.services;

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


/// A canonical type name for an underlying data format and associated base vector type
public enum FileType {
  /// either ivec, fvec, or some other little-endian format containing a sequence of (vector length, vector
  /// length, vector values)
  xvec,
  /// The apache parquet formst
  parquet,
  /// Comma separated values
  csv;
}
