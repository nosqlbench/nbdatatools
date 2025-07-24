package io.nosqlbench.vectordata.merklev2;

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

import java.util.concurrent.CompletableFuture;

/// Interface for scheduling operations that can accept tasks and manage futures.
/// 
/// This allows schedulers to work with either a full ChunkQueue or a tracking wrapper
/// without knowing the difference. It provides the minimal interface needed for
/// schedulers to add tasks and manage futures.
public interface SchedulingTarget {
    /// Adds a task to the target.
    /// 
    /// @param task The task to add
    /// @return true if the task was successfully added
    boolean offerTask(ChunkScheduler.NodeDownloadTask task);
    
    /// Gets or creates a future for the specified node index.
    /// 
    /// @param nodeIndex The merkle tree node index
    /// @return The future for this node's download
    CompletableFuture<Void> getOrCreateFuture(int nodeIndex);
}