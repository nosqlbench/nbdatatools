/// xvec format implementations of dataset view interfaces.
///
/// This package provides concrete implementations of the dataset view interfaces
/// ({@link io.nosqlbench.vectordata.spec.datasets.types}) specifically for xvec file formats
/// (fvec, ivec, bvec, etc.). These implementations handle reading and buffering vector data
/// from little-endian sequential vector files with optimized access patterns.
///
/// ## Key Components
///
/// - {@link io.nosqlbench.vectordata.spec.datasets.impl.xvec.BaseVectorsXvecImpl}: Base vectors from xvec
/// - {@link io.nosqlbench.vectordata.spec.datasets.impl.xvec.QueryVectorsXvecImpl}: Query vectors from xvec
/// - {@link io.nosqlbench.vectordata.spec.datasets.impl.xvec.NeighborIndicesXvecImpl}: Neighbor indices from xvec
/// - {@link io.nosqlbench.vectordata.spec.datasets.impl.xvec.NeighborDistancesXvecImpl}: Neighbor distances from xvec
/// - {@link io.nosqlbench.vectordata.spec.datasets.impl.xvec.FloatVectorsXvecImpl}: Float vectors from xvec
/// - {@link io.nosqlbench.vectordata.spec.datasets.impl.xvec.IntVectorsXvecImpl}: Integer vectors from xvec
/// - {@link io.nosqlbench.vectordata.spec.datasets.impl.xvec.CoreXVecDatasetViewMethods}: Shared xvec methods
///
/// ## xvec Format
///
/// The xvec format family (fvec, ivec, bvec) stores vectors as sequences of:
/// ```
/// [dimension:4bytes][value1:Nbytes][value2:Nbytes]...[valueN:Nbytes]
/// ```
/// where N depends on the specific format (4 for fvec/ivec, 1 for bvec).
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
