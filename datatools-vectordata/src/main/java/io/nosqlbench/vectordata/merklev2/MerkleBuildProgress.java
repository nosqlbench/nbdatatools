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

/// Common interface for progress tracking during merkle tree building.
/// This interface allows different progress implementations to be used
/// interchangeably in command-line tools and progress reporters.
public interface MerkleBuildProgress<T> {
    
    /// Gets the CompletableFuture that completes when the merkle build is done.
    /// @return The CompletableFuture.
    CompletableFuture<T> getFuture();
    
    /// Gets the number of chunks processed so far.
    /// @return The number of chunks processed.
    int getProcessedChunks();
    
    /// Gets the total number of chunks to process.
    /// @return The total number of chunks.
    int getTotalChunks();
    
    /// Gets the total number of bytes to process.
    /// @return The total number of bytes.
    long getTotalBytes();
    
    /// Gets the current phase of the build process.
    /// @return The current phase.
    String getPhase();
    
    /// Gets a summary of performance metrics.
    /// @return A formatted string with performance metrics.
    String getPerformanceMetrics();
}