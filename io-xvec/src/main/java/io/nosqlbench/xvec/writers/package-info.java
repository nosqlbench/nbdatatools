/// # REQUIREMENTS
/// This package contains writers implementing [[io.nosqlbench.nbdatatools.api.fileio.VectorStreamStore]] for ivec, and
/// fvec formats.
///
/// Each implementation of a VectorWriter in this package must do the following:
/// * Ensure that each vector written to the file is the same dimension. That is, after the first
///  one is written, the dimension of it should be remembered, and all later dimensions
/// should be checked against this.
/// * Each implementation should contain an explicit no-args constructor to support SPI.
/// * Each vector written should consist of a little-endian int dimension and a little-endian
/// buffer of values in the required format.
/// * There should be implementations for the following types:
///   * int[] as IvecVectorWriter
///   * float[] as FvecVectorWriter
///   * double[] as DvecVectorWriter
///   * short[] as SvecVectorWriter
///   * There should also be a special HvecVectorWriter which uses IEEE 754-2008 binary 16
/// encoding for JDK20 compatible 16-bit floating point types.
package io.nosqlbench.xvec.writers;

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

