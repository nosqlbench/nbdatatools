package io.nosqlbench.command.build_hdf5.writers;

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


import io.nosqlbench.vectordata.spec.datasets.types.TestDataKind;

import java.util.List;

/// This record type captures the source and bounds of data for a single dataset kind
/// in the standard HDF5 KNN answer key format.
/// @param kind
///     the kind of dataset (base, query, neighbor, distance, filter, ...)
/// @param ordered
///     the ordered list of dataset layouts for this kind
public record DatasetLayoutByKind(TestDataKind kind, List<TestDatasetLayout> ordered) {
}
