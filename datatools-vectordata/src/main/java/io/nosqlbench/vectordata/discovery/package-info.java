/// Discovery and access APIs for vector test datasets.
///
/// This package provides the primary interfaces for discovering and accessing vector test data,
/// including profile selection, test data views, and data group organization. This is the main
/// entry point for consuming vector test data from various sources (HDF5, xvec, etc.).
///
/// ## Key Components
///
/// - {@link io.nosqlbench.vectordata.discovery.TestDataView}: Primary interface for accessing datasets
/// - {@link io.nosqlbench.vectordata.discovery.TestDataGroup}: Organized collection of related datasets
/// - {@link io.nosqlbench.vectordata.discovery.TestDataSources}: Factory for creating data sources
/// - {@link io.nosqlbench.vectordata.discovery.ProfileSelector}: Selector for specific dataset profiles
/// - {@link io.nosqlbench.vectordata.discovery.HDF5ProfileDataView}: HDF5-based dataset view implementation
///
/// ## Usage Example
///
/// ```java
/// TestDataView dataView = TestDataSources.open("path/to/dataset.yaml", "profile-name");
/// Optional<BaseVectors> baseVectors = dataView.getBaseVectors();
/// Optional<QueryVectors> queryVectors = dataView.getQueryVectors();
/// Optional<NeighborIndices> neighbors = dataView.getNeighborIndices();
/// ```
package io.nosqlbench.vectordata.discovery;

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
