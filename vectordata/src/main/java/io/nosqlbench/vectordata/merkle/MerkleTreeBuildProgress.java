package io.nosqlbench.vectordata.merkle;

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

/// Tracks the progress of building a Merkle tree.
/// This class provides information about the number of chunks processed and the total number of chunks.
/// It also contains a CompletableFuture that completes when the Merkle tree is built.
/// The progress is tracked through different stages of processing, each with its own progress meter.
public class MerkleTreeBuildProgress {
    /// Enum representing the different stages of Merkle tree creation
    public enum Stage {
        /// The initial stage before any processing has started
        INITIALIZING("Initializing"),
        /// Checking if the file exists
        FILE_EXISTENCE_CHECK("Checking file existence"),
        /// Opening the file channel for reading
        FILE_CHANNEL_OPENING("Opening file channel"),
        /// Calculating the number of chunks
        CHUNK_CALCULATION("Calculating chunks"),
        /// Creating the job queue
        JOB_QUEUE_CREATION("Creating job queue"),
        /// Setting up the worker threads
        WORKER_THREAD_SETUP("Setting up worker threads"),
        /// Creating the feeder thread
        FEEDER_THREAD_CREATION("Creating feeder thread"),
        /// Processing the leaf nodes
        LEAF_NODE_PROCESSING("Processing leaf nodes"),
        /// Processing the internal nodes
        INTERNAL_NODE_PROCESSING("Processing internal nodes"),
        /// Saving the Merkle tree to a file
        SAVING_TO_FILE("Saving to file"),
        /// The final stage when the Merkle tree is completed
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

    private final CompletableFuture<MerkleTree> future;
    private final AtomicInteger processedChunks;
    private final int totalChunks;
    private final long totalBytes;
    private volatile String phase;
    private volatile Stage currentStage;
    
    // Performance counters
    private final AtomicLong hashComputeTimeNanos = new AtomicLong(0);
    private final AtomicLong fileReadTimeNanos = new AtomicLong(0);
    private final AtomicLong synchronizationWaitTimeNanos = new AtomicLong(0);
    private final AtomicLong internalNodeComputeTimeNanos = new AtomicLong(0);
    private final AtomicInteger totalBytesRead = new AtomicInteger(0);

    /// Creates a new MerkleTreeBuildProgress with the specified total chunks.
    /// @param totalChunks The total number of chunks to process.
    /// @param totalBytes The total number of bytes to process.
    public MerkleTreeBuildProgress(int totalChunks, long totalBytes) {
        this.future = new CompletableFuture<>();
        this.processedChunks = new AtomicInteger(0);
        this.totalChunks = totalChunks;
        this.totalBytes = totalBytes;
        this.phase = Stage.INITIALIZING.getDisplayName();
        this.currentStage = Stage.INITIALIZING;
    }

    /// Creates a new MerkleTreeBuildProgress with the specified total chunks and phase.
    /// @param totalChunks The total number of chunks to process.
    /// @param totalBytes The total number of bytes to process.
    /// @param phase The current phase of the build process.
    public MerkleTreeBuildProgress(int totalChunks, long totalBytes, String phase) {
        this.future = new CompletableFuture<>();
        this.processedChunks = new AtomicInteger(0);
        this.totalChunks = totalChunks;
        this.totalBytes = totalBytes;
        this.phase = phase;
        this.currentStage = Stage.INITIALIZING;
    }

    /// Creates a new MerkleTreeBuildProgress with the specified total chunks and stage.
    /// @param totalChunks The total number of chunks to process.
    /// @param totalBytes The total number of bytes to process.
    /// @param stage The current stage of the build process.
    public MerkleTreeBuildProgress(int totalChunks, long totalBytes, Stage stage) {
        this.future = new CompletableFuture<>();
        this.processedChunks = new AtomicInteger(0);
        this.totalChunks = totalChunks;
        this.totalBytes = totalBytes;
        this.currentStage = stage;
        this.phase = stage.getDisplayName();
    }

    /// Gets the CompletableFuture that completes when the Merkle tree is built.
    /// @return The CompletableFuture.
    public CompletableFuture<MerkleTree> getFuture() {
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

    /// Completes the future with the specified Merkle tree.
    /// @param merkleTree The Merkle tree to complete the future with.
    public void complete(MerkleTree merkleTree) {
        setStage(Stage.COMPLETED);
        future.complete(merkleTree);
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
    /// This also updates the phase to match the stage's display name.
    /// @param stage The new stage.
    public void setStage(Stage stage) {
        this.currentStage = stage;
        this.phase = stage.getDisplayName();
    }

    /// Sets the current stage of the build process with a custom phase message.
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
    
    /// Adds to the file read time counter.
    /// @param nanos The time in nanoseconds to add.
    public void addFileReadTime(long nanos) {
        fileReadTimeNanos.addAndGet(nanos);
    }
    
    /// Adds to the synchronization wait time counter.
    /// @param nanos The time in nanoseconds to add.
    public void addSynchronizationWaitTime(long nanos) {
        synchronizationWaitTimeNanos.addAndGet(nanos);
    }
    
    /// Adds to the internal node compute time counter.
    /// @param nanos The time in nanoseconds to add.
    public void addInternalNodeComputeTime(long nanos) {
        internalNodeComputeTimeNanos.addAndGet(nanos);
    }
    
    /// Adds to the total bytes read counter.
    /// @param bytes The number of bytes to add.
    public void addBytesRead(int bytes) {
        totalBytesRead.addAndGet(bytes);
    }
    
    /// Gets the total hash compute time in nanoseconds.
    /// @return The total hash compute time.
    public long getHashComputeTimeNanos() {
        return hashComputeTimeNanos.get();
    }
    
    /// Gets the total file read time in nanoseconds.
    /// @return The total file read time.
    public long getFileReadTimeNanos() {
        return fileReadTimeNanos.get();
    }
    
    /// Gets the total synchronization wait time in nanoseconds.
    /// @return The total synchronization wait time.
    public long getSynchronizationWaitTimeNanos() {
        return synchronizationWaitTimeNanos.get();
    }
    
    /// Gets the total internal node compute time in nanoseconds.
    /// @return The total internal node compute time.
    public long getInternalNodeComputeTimeNanos() {
        return internalNodeComputeTimeNanos.get();
    }
    
    /// Gets the total bytes read.
    /// @return The total bytes read.
    public int getTotalBytesRead() {
        return totalBytesRead.get();
    }
    
    /// Gets a summary of performance metrics.
    /// @return A formatted string with performance metrics.
    public String getPerformanceMetrics() {
        return String.format(
            "Performance Metrics:\n" +
            "  Hash Compute Time: %.3f ms\n" +
            "  File Read Time: %.3f ms\n" +
            "  Synchronization Wait Time: %.3f ms\n" +
            "  Internal Node Compute Time: %.3f ms\n" +
            "  Total Bytes Read: %d\n",
            hashComputeTimeNanos.get() / 1_000_000.0,
            fileReadTimeNanos.get() / 1_000_000.0,
            synchronizationWaitTimeNanos.get() / 1_000_000.0,
            internalNodeComputeTimeNanos.get() / 1_000_000.0,
            totalBytesRead.get()
        );
    }
}
