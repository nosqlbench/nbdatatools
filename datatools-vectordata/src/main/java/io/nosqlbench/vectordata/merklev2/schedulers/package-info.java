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

/// Chunk download schedulers for MAFileChannel.
/// 
/// This package contains implementations of the {@link io.nosqlbench.vectordata.merklev2.ChunkScheduler}
/// interface that determine which merkle tree nodes to download to satisfy read requests.
/// 
/// All schedulers in this package are stateless by design, allowing them to be safely
/// swapped at runtime without affecting ongoing operations.
/// 
/// Available schedulers:
/// 
/// - {@link DefaultChunkScheduler} - Balanced scheduling strategy suitable for most use cases
/// - {@link AggressiveChunkScheduler} - Downloads larger regions, optimized for high bandwidth  
/// - {@link ConservativeChunkScheduler} - Downloads minimal chunks, optimized for limited bandwidth
/// - {@link AdaptiveChunkScheduler} - Adapts behavior based on performance metrics
/// 
/// Usage example:
/// ```java
/// MAFileChannel channel = new MAFileChannel(cachePath, statePath, remoteSource);
/// 
/// // Switch to aggressive scheduler for bulk operations
/// channel.setChunkScheduler(new AggressiveChunkScheduler());
/// 
/// // Later switch to conservative scheduler for sparse access
/// channel.setChunkScheduler(new ConservativeChunkScheduler());
/// ```
/// 
/// @since 0.1.5
package io.nosqlbench.vectordata.merklev2.schedulers;