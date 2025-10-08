/// Core dataset type interfaces for vector test data.
///
/// This package defines the fundamental interfaces for working with different types of vector datasets,
/// including base vectors, query vectors, neighbor indices, and distance metrics. These interfaces
/// are implemented by format-specific classes (HDF5, xvec, etc.) to provide uniform access patterns.
///
/// ## Key Components
///
/// ### Dataset Interfaces
/// - {@link io.nosqlbench.vectordata.spec.datasets.types.DatasetView}: Base interface for all dataset views
/// - {@link io.nosqlbench.vectordata.spec.datasets.types.BaseVectors}: Base vector dataset
/// - {@link io.nosqlbench.vectordata.spec.datasets.types.QueryVectors}: Query vector dataset
/// - {@link io.nosqlbench.vectordata.spec.datasets.types.NeighborIndices}: K-nearest neighbor indices
/// - {@link io.nosqlbench.vectordata.spec.datasets.types.NeighborDistances}: K-nearest neighbor distances
/// - {@link io.nosqlbench.vectordata.spec.datasets.types.FloatVectors}: Float-based vector datasets
/// - {@link io.nosqlbench.vectordata.spec.datasets.types.IntVectors}: Integer-based vector datasets
/// - {@link io.nosqlbench.vectordata.spec.datasets.types.DoubleVectors}: Double-based vector datasets
///
/// ### Supporting Types
/// - {@link io.nosqlbench.vectordata.spec.datasets.types.DistanceFunction}: Distance metric enumeration
/// - {@link io.nosqlbench.vectordata.spec.datasets.types.ViewKind}: Dataset view type classification
/// - {@link io.nosqlbench.vectordata.spec.datasets.types.TestDataKind}: Test data classification
/// - {@link io.nosqlbench.vectordata.spec.datasets.types.Indexed}: Interface for indexed elements
///
/// ## Usage Example
///
/// ```java
/// BaseVectors baseVectors = testDataView.getBaseVectors().orElseThrow();
/// int vectorCount = baseVectors.getCount();
/// float[] vector = baseVectors.get(0);
/// DistanceFunction metric = baseVectors.getDistanceFunction();
/// ```
package io.nosqlbench.vectordata.spec.datasets.types;

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
