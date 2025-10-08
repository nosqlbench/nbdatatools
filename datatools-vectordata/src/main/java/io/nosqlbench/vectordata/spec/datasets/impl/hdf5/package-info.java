/// HDF5 format implementations of dataset view interfaces.
///
/// This package provides concrete implementations of the dataset view interfaces
/// ({@link io.nosqlbench.vectordata.spec.datasets.types}) specifically for HDF5 file format.
/// These implementations handle reading and buffering vector data from HDF5 files with
/// optimized access patterns.
///
/// ## Key Components
///
/// - {@link io.nosqlbench.vectordata.spec.datasets.impl.hdf5.BaseVectorsHdf5Impl}: Base vectors from HDF5
/// - {@link io.nosqlbench.vectordata.spec.datasets.impl.hdf5.QueryVectorsHdf5Impl}: Query vectors from HDF5
/// - {@link io.nosqlbench.vectordata.spec.datasets.impl.hdf5.NeighborIndicesHdf5Impl}: Neighbor indices from HDF5
/// - {@link io.nosqlbench.vectordata.spec.datasets.impl.hdf5.NeighborDistancesHdf5Impl}: Neighbor distances from HDF5
/// - {@link io.nosqlbench.vectordata.spec.datasets.impl.hdf5.FloatVectorsHdf5Impl}: Float vectors from HDF5
/// - {@link io.nosqlbench.vectordata.spec.datasets.impl.hdf5.IntVectorsHdf5Impl}: Integer vectors from HDF5
/// - {@link io.nosqlbench.vectordata.spec.datasets.impl.hdf5.DoubleVectorsHdf5Impl}: Double vectors from HDF5
/// - {@link io.nosqlbench.vectordata.spec.datasets.impl.hdf5.CoreHdf5DatasetViewMethods}: Shared HDF5 methods
package io.nosqlbench.vectordata.spec.datasets.impl.hdf5;

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
