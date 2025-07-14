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

import io.nosqlbench.vectordata.merkle.MerkleTree;
import io.nosqlbench.vectordata.merkle.MerkleTreeBuildProgress;
import io.nosqlbench.vectordata.status.EventSink;
import io.nosqlbench.vectordata.status.NoOpDownloadEventSink;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/// Worker that processes chunks from the job queue
/// This class has been consolidated with FileChunkProcessor to improve efficiency by:
/// 1. Eliminating the need to create a new FileChunkProcessor instance for each chunk
/// 2. Reusing the same ByteBuffer for all chunks processed by this worker
/// 3. Reducing memory allocation and garbage collection overhead
/// 4. Using hashData() method directly for leaf nodes
/// 5. Using AsynchronousFileChannel for non-blocking I/O operations
public class ChunkWorker implements Runnable {
    private final BlockingQueue<ChunkDescriptor> jobQueue;
    private final AtomicBoolean feederComplete;
    private final MerkleTreeBuildProgress progress;
    private final MerkleTree merkleTree;
    private final AsynchronousFileChannel fileChannel;
    private final long chunkSize;
    private final long fileOffset;
    private final long effectiveLength;
    // Remove shared buffer; allocate per chunk to avoid reuse issues
    private final EventSink eventSink;

    /// Creates a new chunk worker
    /// @param jobQueue The queue of chunks to process
    /// @param feederComplete Flag indicating when all chunks have been added to the queue
    /// @param progress The progress tracker for the Merkle tree build
    /// @param merkleTree The Merkle tree being built
    /// @param fileChannel The file channel to read from
    /// @param chunkSize The size of each chunk
    /// @param fileOffset The offset in the file to start reading from
    /// @param effectiveLength The effective length of the file to process
    public ChunkWorker(
        BlockingQueue<ChunkDescriptor> jobQueue,
        AtomicBoolean feederComplete,
        MerkleTreeBuildProgress progress,
        MerkleTree merkleTree,
        AsynchronousFileChannel fileChannel,
        long chunkSize,
        long fileOffset,
        long effectiveLength
    )
    {
        this(jobQueue, feederComplete, progress, merkleTree, fileChannel, chunkSize, fileOffset, effectiveLength, new NoOpDownloadEventSink());
    }

    /// Creates a new chunk worker with a specified event sink
    /// @param jobQueue The queue of chunks to process
    /// @param feederComplete Flag indicating when all chunks have been added to the queue
    /// @param progress The progress tracker for the Merkle tree build
    /// @param merkleTree The Merkle tree being built
    /// @param fileChannel The file channel to read from
    /// @param chunkSize The size of each chunk
    /// @param fileOffset The offset in the file to start reading from
    /// @param effectiveLength The effective length of the file to process
    /// @param eventSink The event sink for logging
    public ChunkWorker(
        BlockingQueue<ChunkDescriptor> jobQueue,
        AtomicBoolean feederComplete,
        MerkleTreeBuildProgress progress,
        MerkleTree merkleTree,
        AsynchronousFileChannel fileChannel,
        long chunkSize,
        long fileOffset,
        long effectiveLength,
        EventSink eventSink
    )
    {
        this.jobQueue = jobQueue;
        this.feederComplete = feederComplete;
        this.progress = progress;
        this.merkleTree = merkleTree;
        this.fileChannel = fileChannel;
        this.chunkSize = chunkSize;
        this.fileOffset = fileOffset;
        this.effectiveLength = effectiveLength;
        this.eventSink = eventSink != null ? eventSink : new NoOpDownloadEventSink();
    }

    @Override
    public void run() {
        try {
            // Use a batch approach to reduce contention and improve throughput
            List<ChunkDescriptor> batch = new ArrayList<>();
            int maxBatchSize = 10; // Process up to 10 chunks at a time

            while (true) {
                batch.clear();

                // Try to get a batch of chunk descriptors from the queue
                jobQueue.drainTo(batch, maxBatchSize);

                // If no descriptors are available, try polling with timeout
                if (batch.isEmpty()) {
                    ChunkDescriptor descriptor = jobQueue.poll(100, TimeUnit.MILLISECONDS);
                    if (descriptor == null) {
                        // If no descriptor is available and the feeder is complete, exit the loop
                        if (feederComplete.get() && jobQueue.isEmpty()) {
                            break;
                        }
                        continue;
                    }
                    batch.add(descriptor);
                }

                // Process each chunk in the batch
                for (ChunkDescriptor descriptor : batch) {
                    int chunkIndex = descriptor.chunkIndex;

                    // Calculate the byte range for this chunk
                    long chunkStartOffset = chunkIndex * chunkSize;
                    long chunkEndOffset = Math.min(chunkStartOffset + chunkSize, effectiveLength);
                    int bytesToRead = (int) (chunkEndOffset - chunkStartOffset);

                    // Calculate the file position for this chunk
                    long filePosition = fileOffset + chunkStartOffset;
                    // Read the chunk into a fresh buffer
                    ByteBuffer readBuffer = ByteBuffer.allocate(bytesToRead);
                    long readStartTime = System.nanoTime();
                    try {
                        long pos = filePosition;
                        while (readBuffer.hasRemaining()) {
                            Future<Integer> future = fileChannel.read(readBuffer, pos);
                            int n = future.get();
                            if (n <= 0) break;
                            pos += n;
                        }
                    } catch (Exception e) {
                        throw new RuntimeException("Error reading chunk " + chunkIndex + ": " + e.getMessage(), e);
                    }
                    long readEndTime = System.nanoTime();
                    progress.addFileReadTime(readEndTime - readStartTime);
                    progress.addBytesRead(bytesToRead);
                    
                    readBuffer.flip();
                    
                    // Submit the data directly to the merkle tree
                    long syncStartTime = System.nanoTime();
                    merkleTree.hashData(chunkStartOffset, chunkEndOffset, readBuffer);
                    long syncEndTime = System.nanoTime();
                    progress.addSynchronizationWaitTime(syncEndTime - syncStartTime);

                    // Update progress
                    progress.incrementProcessedChunks();
                }
            }
        } catch (Exception e) {
            progress.completeExceptionally(new RuntimeException(
                "Worker thread error: " + e.getMessage(),
                e
            ));
        }
    }
}
