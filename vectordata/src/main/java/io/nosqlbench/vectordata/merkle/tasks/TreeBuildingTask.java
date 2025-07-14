package io.nosqlbench.vectordata.merkle.tasks;

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

import io.nosqlbench.vectordata.merkle.MerkleFooter;
import io.nosqlbench.vectordata.merkle.MerkleRange;
import io.nosqlbench.vectordata.merkle.MerkleTree;
import io.nosqlbench.vectordata.merkle.MerkleTreeBuildProgress;
import io.nosqlbench.vectordata.merklev2.MerkleShape;
import io.nosqlbench.vectordata.status.EventSink;
import io.nosqlbench.vectordata.status.NoOpDownloadEventSink;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/// Task for building the Merkle tree asynchronously
public class TreeBuildingTask implements Runnable {
    private final MerkleTreeBuildProgress progress;
    private final long effectiveLength;
    private final long chunkSize;
    private final Path filePath;
    private final MerkleRange range;
    private final MerkleShape calc;
    private final AsynchronousFileChannel fileChannel;
    private final EventSink eventSink;

    /// Creates a new tree building task
    /// @param progress
    ///     The progress tracker for the Merkle tree build
    /// @param effectiveLength
    ///     The effective length of the file to process
    /// @param filePath
    ///     The path to the file to process
    /// @param range
    ///     The range within the file to process
    /// @param fileSize
    ///     The size of the file
    /// @param calc
    ///     The calculated geometry descriptor for the Merkle tree
    /// @param fileChannel
    ///     The asynchronous file channel to use for reading the file
    public TreeBuildingTask(
        MerkleTreeBuildProgress progress,
        long effectiveLength, Path filePath,
        MerkleRange range,
        long fileSize,
        MerkleShape calc,
        AsynchronousFileChannel fileChannel
    )
    {
        this(progress, effectiveLength, filePath, range, calc, fileChannel, new NoOpDownloadEventSink());
    }

    /// Creates a new tree building task with a specified event sink
    /// @param progress
    ///     The progress tracker for the Merkle tree build
    /// @param effectiveLength
    ///     The effective length of the file to process
    /// @param filePath
    ///     The path to the file to process
    /// @param range
    ///     The range within the file to process
    /// @param calc
    ///     The calculated geometry descriptor for the Merkle tree
    /// @param fileChannel
    ///     The asynchronous file channel to use for reading the file
    /// @param eventSink
    ///     The event sink for instrumentation and testing
    public TreeBuildingTask(
        MerkleTreeBuildProgress progress,
        long effectiveLength, Path filePath,
        MerkleRange range, MerkleShape calc,
        AsynchronousFileChannel fileChannel,
        EventSink eventSink
    )
    {
        this.progress = progress;
        this.effectiveLength = effectiveLength;
        // Note: chunkSize parameter is ignored as it's now calculated automatically
        // We'll get it from the calc descriptor which already has the automatically calculated value
        this.chunkSize = calc.getChunkSize();
        this.filePath = filePath;
        this.range = range;
        this.calc = calc;
        this.fileChannel = fileChannel;
        this.eventSink = eventSink != null ? eventSink : new NoOpDownloadEventSink();
    }

    @Override
    public void run() {
        try {
            // Update stage to indicate we're checking file existence
            progress.setStage(MerkleTreeBuildProgress.Stage.FILE_EXISTENCE_CHECK);

            // Check if the file exists
            if (!Files.exists(filePath)) {
                // If the file doesn't exist, we cannot create a merkle tree
                progress.setStage(MerkleTreeBuildProgress.Stage.COMPLETED, "File does not exist");
                progress.completeExceptionally(new IOException("Cannot create merkle tree for non-existent file: " + filePath));
                return;
            }

            // Use the MerkleShape passed to the constructor

            // Create an empty BitSet for tracking valid nodes
            BitSet valid = new BitSet(calc.getNodeCount());
            int bitSetSize = valid.toByteArray().length;

            // Create a temporary file for the merkle tree
            Path tempFile = Files.createTempFile("merkle", ".mrkl");
            tempFile.toFile().deleteOnExit(); // Ensure the file is deleted when the JVM exits
            // Note: Using deleteOnExit() instead of shutdown hook for better performance

            // Calculate the total file size needed
            long dataRegionSize = (long) (calc.getCapLeaf() + calc.getOffset()) * 32; // SHA-256 hash size
            long footerSize = MerkleFooter.create(chunkSize, effectiveLength, bitSetSize).footerLength();
            long totalFileSize = dataRegionSize + bitSetSize + footerSize;

            // Create the file but don't physically allocate space
            try (FileChannel channel = FileChannel.open(
                tempFile,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING
            )) {
                // Write the footer with the BitSet size at the end of the file
                // Use absolute positioning for thread safety
                MerkleFooter footer = MerkleFooter.create(chunkSize, effectiveLength, bitSetSize);
                int written = channel.write(footer.toByteBuffer(), dataRegionSize + bitSetSize);
                assert written == footer.footerLength();

                // Don't force flush here - defer until final save for better performance
            }

            // Open the file channel for memory mapping
            FileChannel merkleFileChannel = null;
            ByteBuffer mappedBuffer = null;
            MerkleTree merkleTree = null;

            try {
                merkleFileChannel = 
                    FileChannel.open(tempFile, StandardOpenOption.READ, StandardOpenOption.WRITE);

                // Memory map the hash data region
                mappedBuffer = 
                    merkleFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, dataRegionSize);

                // Create the MerkleTree with memory-mapped access
                merkleTree = new MerkleTree(
                    valid,
                    calc,
                    merkleFileChannel,
                    mappedBuffer
                );

                // Update stage to indicate we're preparing file access
                progress.setStage(MerkleTreeBuildProgress.Stage.FILE_CHANNEL_OPENING);

                // Use the provided asynchronous file channel
                // Update stage to indicate we're calculating chunks
                progress.setStage(MerkleTreeBuildProgress.Stage.CHUNK_CALCULATION);

                // Get the total number of chunks from the calculated dimensions
                int totalChunks = calc.getTotalChunks();

                // Create a job queue with a limit of 1000 elements
                progress.setStage(MerkleTreeBuildProgress.Stage.JOB_QUEUE_CREATION);
                BlockingQueue<ChunkDescriptor> jobQueue = new LinkedBlockingQueue<>(1000);

                // Volatile flag to signal when all chunks have been added to the queue
                progress.setStage(MerkleTreeBuildProgress.Stage.WORKER_THREAD_SETUP);
                final AtomicBoolean feederComplete = new AtomicBoolean(false);

                // Create a separate thread to feed chunk descriptors into the queue
                progress.setStage(MerkleTreeBuildProgress.Stage.FEEDER_THREAD_CREATION);
                Thread feederThread = Thread.ofVirtual().start(() -> {
                    try {
                        // Add all chunk descriptors to the queue
                        for (int i = 0; i < totalChunks; i++) {
                            jobQueue.put(new ChunkDescriptor(i));
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        progress.completeExceptionally(new RuntimeException(
                            "Feeder thread was interrupted: " + e.getMessage(),
                            e
                        ));
                    } finally {
                        // Signal that all chunks have been added to the queue
                        feederComplete.set(true);
                    }
                });

                // Create a pool of virtual threads to process chunks
                // For I/O-bound operations, using more threads than CPU cores can improve throughput
                List<Thread> workerThreads = new ArrayList<>();
                // Use a higher number of workers for I/O-bound operations
                int workerCount = Math.max(1, Runtime.getRuntime().availableProcessors() * 2);

                // Create and start worker threads
                progress.setStage(MerkleTreeBuildProgress.Stage.LEAF_NODE_PROCESSING);
                for (int i = 0; i < workerCount; i++) {
                    Thread workerThread = Thread.ofVirtual().start(new ChunkWorker(
                        jobQueue,
                        feederComplete,
                        progress,
                        merkleTree,
                        fileChannel,
                        chunkSize,
                        range.start(),
                        effectiveLength,
                        eventSink
                    ));

                    workerThreads.add(workerThread);
                }

                // Wait for the feeder thread to complete
                feederThread.join();

                // Wait for all worker threads to complete
                for (Thread workerThread : workerThreads) {
                    workerThread.join();
                }

                // Update stage to indicate we're now processing internal nodes
                progress.setStage(MerkleTreeBuildProgress.Stage.INTERNAL_NODE_PROCESSING);

                // Compute all internal nodes now that all leaf processing is complete
                long internalNodeStartTime = System.nanoTime();
                try {
                    // Use the new comprehensive internal node computation method
                    merkleTree.computeAllInternalNodes();
                    // Now access the root hash to verify everything is computed correctly
                    merkleTree.getHash(0);
                } catch (Exception e) {
                    // Log the error but continue, as this is just to force computation
                    eventSink.warn("Failed to compute internal nodes: {}", e.getMessage());
                }
                long internalNodeEndTime = System.nanoTime();
                progress.addInternalNodeComputeTime(internalNodeEndTime - internalNodeStartTime);

                // Update progress for internal nodes (they are computed automatically by accessing root hash)
                // We'll increment the progress counter for each internal node without sleeping
                int internalNodeCount = calc.getInternalNodeCount();
                for (int i = 0; i < internalNodeCount; i++) {
                    progress.incrementProcessedChunks();
                }

                // Set the stage to SAVING_TO_FILE before completing
                progress.setStage(MerkleTreeBuildProgress.Stage.SAVING_TO_FILE);

                // Log performance metrics if event sink supports it
                if (eventSink != null) {
                    eventSink.info("Merkle tree build performance metrics:\n{}", progress.getPerformanceMetrics());
                }

                // Complete the progress with the built merkle tree
                // This will automatically set the stage to COMPLETED
                progress.complete(merkleTree);

            } catch (Exception e) {
                // Close resources if an exception occurs
                if (merkleFileChannel != null) {
                    try {
                        merkleFileChannel.close();
                    } catch (IOException closeException) {
                        eventSink.warn("Failed to close merkle file channel: {}", closeException.getMessage());
                    }
                }

                // Delete the temporary file if an exception occurs
                try {
                    Files.deleteIfExists(tempFile);
                } catch (IOException deleteException) {
                    eventSink.warn("Failed to delete temporary file: {}", deleteException.getMessage());
                }

                progress.completeExceptionally(new RuntimeException(
                    "Error in parallel processing: " + e.getMessage(),
                    e
                ));
                return;
            }
        } catch (Exception e) {
            progress.completeExceptionally(new RuntimeException(
                "Failed to create Merkle tree: " + e.getMessage(),
                e
            ));
        } finally {
            // Close the file channel if it's not null
            if (fileChannel != null) {
                try {
                    fileChannel.close();
                } catch (IOException e) {
                    // Log the error but don't fail the whole operation
                    eventSink.warn("Failed to close file channel: {}", e.getMessage());
                }
            }
        }
    }
}
