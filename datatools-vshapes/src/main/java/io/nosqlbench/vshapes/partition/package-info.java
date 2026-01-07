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

/// Memory-aware partitioning for large vector datasets.
///
/// ## Overview
///
/// This package provides utilities for processing vector datasets that exceed
/// available heap memory by partitioning them into manageable chunks.
///
/// ## Key Classes
///
/// - [MemoryAwarePartitioner] - Computes optimal partition sizes based on
///   available memory and vector dimensions
///
/// ## Usage
///
/// ```java
/// MemoryAwarePartitioner partitioner = MemoryAwarePartitioner.forDataset(
///     1_000_000, // totalVectors
///     384        // dimensions
/// );
///
/// for (MemoryAwarePartitioner.Partition partition : partitioner.partitions()) {
///     float[][] chunk = loadVectors(partition.start(), partition.end());
///     processPartition(chunk);
///     checkpoint(partition.index());
/// }
/// ```
///
/// @see io.nosqlbench.vshapes.partition.MemoryAwarePartitioner
package io.nosqlbench.vshapes.partition;
