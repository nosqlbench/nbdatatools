/// Vector file readers for xvec formats (fvec, ivec, bvec, dvec).
///
/// This package provides implementations for reading vector data from xvec format files,
/// which store vectors in little-endian sequential format. Each vector consists of a
/// 4-byte dimension header followed by the vector values in the appropriate format.
///
/// ## Key Components
///
/// ### Readers
/// - {@link io.nosqlbench.readers.UniformFvecReader}: Read float vectors from fvec files
/// - {@link io.nosqlbench.readers.UniformIvecReader}: Read integer vectors from ivec files
/// - {@link io.nosqlbench.readers.UniformBvecReader}: Read byte vectors from bvec files
/// - {@link io.nosqlbench.readers.UniformDvecReader}: Read double vectors from dvec files
///
/// ### Streamers
/// - {@link io.nosqlbench.readers.UniformFvecStreamer}: Stream float vectors from fvec files
/// - {@link io.nosqlbench.readers.UniformIvecStreamer}: Stream integer vectors from ivec files
/// - {@link io.nosqlbench.readers.UniformBvecStreamer}: Stream byte vectors from bvec files
/// - {@link io.nosqlbench.readers.UniformDvecStreamer}: Stream double vectors from dvec files
///
/// ### Utilities
/// - {@link io.nosqlbench.readers.CsvJsonArrayStreamer}: Stream JSON arrays from CSV
/// - {@link io.nosqlbench.readers.ReaderUtils}: Shared reader utilities
///
/// ## xvec Format
///
/// The xvec format family stores vectors as:
/// ```
/// [dimension:4bytes][value1:Nbytes][value2:Nbytes]...[valueN:Nbytes]
/// [dimension:4bytes][value1:Nbytes][value2:Nbytes]...[valueN:Nbytes]
/// ...
/// ```
///
/// Where N depends on the format:
/// - fvec: 4 bytes per float value
/// - ivec: 4 bytes per int value
/// - bvec: 1 byte per byte value
/// - dvec: 8 bytes per double value
///
/// ## Usage Example
///
/// ```java
/// UniformFvecReader reader = new UniformFvecReader(path);
/// int count = reader.getCount();
/// float[] vector = reader.get(0);
/// ```
package io.nosqlbench.readers;

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
