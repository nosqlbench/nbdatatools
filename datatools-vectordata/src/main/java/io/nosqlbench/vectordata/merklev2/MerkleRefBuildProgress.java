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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/// Tracks the progress of building a Merkle reference tree.
/// This class provides information about the number of chunks processed and the total number of chunks.
/// It also contains a CompletableFuture that completes when the Merkle reference is built.
public class MerkleRefBuildProgress implements MerkleBuildProgress<MerkleDataImpl> {
    /// Enum representing the different stages of Merkle reference creation
    public enum Stage {
        /// The initial stage before any processing has started
        INITIALIZING("Initializing"),
        /// Processing the leaf nodes
        LEAF_NODE_PROCESSING("Processing leaf nodes"),
        /// Processing the internal nodes
        INTERNAL_NODE_PROCESSING("Processing internal nodes"),
        /// The final stage when the Merkle reference is completed
        COMPLETED("Completed");

        private final String displayName;

        Stage(String displayName) {
            this.displayName = displayName;
        }

        /// Gets the display name for this stage.
        /// @return The display name.
        public String getDisplayName() {
            return displayName;
        }
    }

    private final CompletableFuture<MerkleDataImpl> future;
    private final AtomicInteger processedChunks;
    private final int totalChunks;
    private final long totalBytes;
    private volatile String phase;
    private volatile Stage currentStage;
    
    // Performance counters
    private final AtomicLong hashComputeTimeNanos = new AtomicLong(0);
    private final AtomicLong internalNodeComputeTimeNanos = new AtomicLong(0);

    /// Creates a new MerkleRefBuildProgress with the specified total chunks.
    /// @param totalChunks The total number of chunks to process.
    /// @param totalBytes The total number of bytes to process.
    public MerkleRefBuildProgress(int totalChunks, long totalBytes) {
        this.future = new CompletableFuture<>();
        this.processedChunks = new AtomicInteger(0);
        this.totalChunks = totalChunks;
        this.totalBytes = totalBytes;
        this.phase = Stage.INITIALIZING.getDisplayName();
        this.currentStage = Stage.INITIALIZING;
    }

    /// Gets the CompletableFuture that completes when the Merkle reference is built.
    /// @return The CompletableFuture.
    public CompletableFuture<MerkleDataImpl> getFuture() {
        return future;
    }

    /// Gets the number of chunks processed so far.
    /// @return The number of chunks processed.
    public int getProcessedChunks() {
        return processedChunks.get();
    }

    /// Gets the total number of chunks to process.
    /// @return The total number of chunks.
    public int getTotalChunks() {
        return totalChunks;
    }

    /// Gets the total number of bytes to process.
    /// @return The total number of bytes.
    public long getTotalBytes() {
        return totalBytes;
    }

    /// Gets the current phase of the build process.
    /// @return The current phase.
    public String getPhase() {
        return phase;
    }

    /// Gets the current stage of the build process.
    /// @return The current stage.
    public Stage getCurrentStage() {
        return currentStage;
    }

    /// Increments the number of chunks processed and returns the new value.
    /// @return The new number of chunks processed.
    public int incrementProcessedChunks() {
        return processedChunks.incrementAndGet();
    }

    /// Completes the future with the specified Merkle reference.
    /// @param merkleRef The Merkle reference to complete the future with.
    public void complete(MerkleDataImpl merkleRef) {
        setStage(Stage.COMPLETED);
        future.complete(merkleRef);
    }

    /// Completes the future exceptionally with the specified exception.
    /// @param ex The exception to complete the future with.
    public void completeExceptionally(Throwable ex) {
        future.completeExceptionally(ex);
    }

    /// Sets the current phase of the build process.
    /// @param phase The new phase.
    public void setPhase(String phase) {
        this.phase = phase;
    }

    /// Sets the current stage of the build process.
    /// @param stage The new stage.
    public void setStage(Stage stage) {
        this.currentStage = stage;
        this.phase = stage.getDisplayName();
    }

    /// Sets the current stage and a custom phase message.
    /// @param stage The new stage.
    /// @param customPhase A custom phase message to display.
    public void setStage(Stage stage, String customPhase) {
        this.currentStage = stage;
        this.phase = customPhase;
    }
    
    // Performance counter methods
    
    /// Adds to the hash compute time counter.
    /// @param nanos The time in nanoseconds to add.
    public void addHashComputeTime(long nanos) {
        hashComputeTimeNanos.addAndGet(nanos);
    }
    
    /// Adds to the internal node compute time counter.
    /// @param nanos The time in nanoseconds to add.
    public void addInternalNodeComputeTime(long nanos) {
        internalNodeComputeTimeNanos.addAndGet(nanos);
    }
    
    /// Gets the total hash compute time in nanoseconds.
    /// @return The total hash compute time.
    public long getHashComputeTimeNanos() {
        return hashComputeTimeNanos.get();
    }
    
    /// Gets the total internal node compute time in nanoseconds.
    /// @return The total internal node compute time.
    public long getInternalNodeComputeTimeNanos() {
        return internalNodeComputeTimeNanos.get();
    }
    
    /// Gets a summary of performance metrics.
    /// @return A formatted string with performance metrics.
    public String getPerformanceMetrics() {
        return String.format(
            "Performance Metrics:\n" +
            "  Hash Compute Time: %.3f ms\n" +
            "  Internal Node Compute Time: %.3f ms\n",
            hashComputeTimeNanos.get() / 1_000_000.0,
            internalNodeComputeTimeNanos.get() / 1_000_000.0
        );
    }
}