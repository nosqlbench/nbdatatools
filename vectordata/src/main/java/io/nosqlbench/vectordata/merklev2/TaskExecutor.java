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

import io.nosqlbench.nbdatatools.api.transport.ChunkedTransportClient;
import io.nosqlbench.nbdatatools.api.transport.FetchResult;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

/// Task executor that processes node download tasks from a queue.
/// 
/// This class observes a task queue and automatically executes download tasks
/// as they are added. It manages the actual download operations, verification,
/// and caching of data, while maintaining futures for task completion tracking.
/// 
/// The executor runs in a separate thread pool and processes tasks concurrently
/// up to a configurable parallelism limit. It handles both leaf nodes (single chunks)
/// and internal nodes (multiple chunks) appropriately.
public class TaskExecutor implements AutoCloseable {
    private static final Logger logger = Logger.getLogger(TaskExecutor.class.getName());
    
    private final OptimizedChunkQueue chunkQueue;
    private final ChunkedTransportClient transport;
    private final MerkleState merkleState;
    private final MerkleShape shape;
    private final AsynchronousFileChannel localCache;
    private final ExecutorService executorService;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Thread observerThread;
    private final int maxConcurrentDownloads;
    private final Semaphore downloadSemaphore;
    
    /// Creates a new task executor.
    /// 
    /// @param chunkQueue The OptimizedChunkQueue to observe for tasks and manage state
    /// @param transport The transport client for downloading
    /// @param merkleState The merkle state for verification
    /// @param shape The merkle shape for conversions
    /// @param localCache The local cache file channel
    /// @param maxConcurrentDownloads Maximum number of concurrent downloads
    public TaskExecutor(OptimizedChunkQueue chunkQueue,
                       ChunkedTransportClient transport,
                       MerkleState merkleState,
                       MerkleShape shape,
                       AsynchronousFileChannel localCache,
                       int maxConcurrentDownloads) {
        this.chunkQueue = chunkQueue;
        this.transport = transport;
        this.merkleState = merkleState;
        this.shape = shape;
        this.localCache = localCache;
        this.maxConcurrentDownloads = maxConcurrentDownloads;
        this.downloadSemaphore = new Semaphore(maxConcurrentDownloads);
        
        // Create executor service with bounded thread pool
        this.executorService = new ThreadPoolExecutor(
            1, maxConcurrentDownloads,
            60L, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(),
            r -> {
                Thread t = new Thread(r);
                t.setName("MAFileChannel-TaskExecutor-" + t.getId());
                t.setDaemon(true);
                return t;
            }
        );
        
        // Start observer thread
        this.observerThread = new Thread(this::observeQueue);
        this.observerThread.setName("MAFileChannel-TaskObserver");
        this.observerThread.setDaemon(true);
        this.observerThread.start();
    }
    
    /// Observes the task queue and submits tasks for execution.
    private void observeQueue() {
        while (running.get()) {
            try {
                // Wait for a task with timeout to allow periodic checks
                ChunkScheduler.NodeDownloadTask task = chunkQueue.taskQueue().poll(100, TimeUnit.MILLISECONDS);
                if (task != null) {
                    submitTask(task);
                }
            } catch (InterruptedException e) {
                // Thread interrupted, check if we should stop
                if (!running.get()) {
                    Thread.currentThread().interrupt();
                    break;
                }
            } catch (Exception e) {
                logger.severe("Error in task observer: " + e.getMessage());
            }
        }
    }
    
    /// Submits a task for execution.
    private void submitTask(ChunkScheduler.NodeDownloadTask task) {
        // Check if task is already completed
        if (task.getFuture().isDone()) {
            return;
        }
        
        // Store the future in our map
        // Future is already managed by OptimizedChunkQueue
        
        // Submit for execution
        executorService.submit(() -> {
            try {
                // Acquire semaphore to limit concurrent downloads
                downloadSemaphore.acquire();
                try {
                    executeTask(task);
                } finally {
                    downloadSemaphore.release();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                completeTaskExceptionally(task, e);
            } catch (Exception e) {
                completeTaskExceptionally(task, e);
            }
        });
    }
    
    /// Executes a single download task.
    private void executeTask(ChunkScheduler.NodeDownloadTask task) throws Exception {
        int nodeIndex = task.getNodeIndex();
        long start = task.getOffset();
        long length = task.getSize();
        
        logger.fine("Executing download task for node " + nodeIndex + 
                   " (offset=" + start + ", length=" + length + ")");
        
        // Download data from remote source
        CompletableFuture<? extends FetchResult<?>> downloadFuture = 
            transport.fetchRange(start, length);
        
        // Chain the processing as a continuation of the download - this enables true concurrency
        downloadFuture.thenAccept(result -> {
            try {
                ByteBuffer nodeData = result.getData();
                
                // Process the node based on whether it's a leaf or internal node
                if (task.isLeafNode()) {
                    processLeafNode(task, nodeIndex, start, nodeData);
                } else {
                    processInternalNode(task, nodeData);
                }
                
                // Complete the task successfully
                completeTaskSuccessfully(task);
            } catch (Exception e) {
                completeTaskExceptionally(task, e);
            }
        }).exceptionally(throwable -> {
            completeTaskExceptionally(task, throwable);
            return null;
        });
        
        // Note: We don't block here - the worker thread is free to handle other tasks
        // while the HTTP download proceeds asynchronously
    }
    
    /// Processes a leaf node download.
    private void processLeafNode(ChunkScheduler.NodeDownloadTask task, int nodeIndex, 
                                long start, ByteBuffer nodeData) throws IOException {
        // For leaf nodes, convert node index to chunk index
        if (!shape.isLeafNode(nodeIndex)) {
            throw new IllegalArgumentException("Expected leaf node, got internal node: " + nodeIndex);
        }
        
        int chunkIndex = shape.leafNodeToChunkIndex(nodeIndex);
        boolean saved = merkleState.saveIfValid(chunkIndex, nodeData, data -> {
            try {
                // Save to local cache
                localCache.write(data, start).get();
            } catch (Exception e) {
                throw new RuntimeException("Failed to save chunk to cache", e);
            }
        });
        
        if (!saved) {
            throw new IOException("Chunk " + chunkIndex + " failed verification");
        }
        logger.fine("Successfully downloaded and verified chunk " + chunkIndex);
    }
    
    /// Processes an internal node download.
    private void processInternalNode(ChunkScheduler.NodeDownloadTask task, 
                                   ByteBuffer nodeData) throws IOException {
        // For internal nodes, split the data and verify each chunk
        // Use MerkleShape to get the authoritative leaf range for this node
        MerkleShape.MerkleNodeRange leafRange = shape.getLeafRangeForNode(task.getNodeIndex());
        int dataOffset = 0;
        
        for (long leafIndex = leafRange.getStart(); leafIndex < leafRange.getEnd(); leafIndex++) {
            // In internal nodes, leaf indices directly correspond to chunk indices
            int chunkIndex = (int) leafIndex;
            if (chunkIndex >= shape.getTotalChunks()) {
                continue; // Skip chunks beyond the actual file content
            }
            
            
            long chunkStart = shape.getChunkStartPosition(chunkIndex);
            int actualChunkSize = (int) shape.getActualChunkSize(chunkIndex);
            
            // Extract chunk data from the larger download
            if (dataOffset + actualChunkSize > nodeData.remaining()) {
                throw new IOException("Internal node data too small for chunk " + chunkIndex);
            }
            
            ByteBuffer chunkData = nodeData.slice();
            chunkData.position(dataOffset);
            chunkData.limit(dataOffset + actualChunkSize);
            ByteBuffer chunkSlice = chunkData.slice();
            
            // Verify and save this chunk
            boolean saved = merkleState.saveIfValid(chunkIndex, chunkSlice, data -> {
                try {
                    // Save to local cache
                    localCache.write(data, chunkStart).get();
                } catch (Exception e) {
                    throw new RuntimeException("Failed to save chunk to cache: " + e, e);
                }
            });
            
            if (!saved) {
                throw new IOException("Chunk " + chunkIndex + " failed verification");
            }
            
            dataOffset += actualChunkSize;
        }
        
        logger.fine("Successfully downloaded and verified internal node " + 
                   task.getNodeIndex() + " covering " + leafRange.getLength() + " chunks");
    }
    
    /// Completes a task successfully.
    private void completeTaskSuccessfully(ChunkScheduler.NodeDownloadTask task) {
        CompletableFuture<Void> future = chunkQueue.removeFuture(task.getNodeIndex());
        if (future != null && !future.isDone()) {
            future.complete(null);
        }
        // Record successful completion in history
        chunkQueue.markCompleted(task, task.getSize());
    }
    
    /// Completes a task exceptionally.
    private void completeTaskExceptionally(ChunkScheduler.NodeDownloadTask task, Throwable throwable) {
        CompletableFuture<Void> future = chunkQueue.removeFuture(task.getNodeIndex());
        if (future != null && !future.isDone()) {
            future.completeExceptionally(throwable);
        }
        // Record failed completion in history
        chunkQueue.markCompleted(task, java.time.Instant.now(), false, 0);
        logger.severe("Task failed for node " + task.getNodeIndex() + ": " + throwable.getMessage());
    }
    
    /// Gets the number of tasks currently in the queue.
    public int getQueueSize() {
        return chunkQueue.getPendingTaskCount();
    }
    
    /// Gets the number of active downloads.
    public int getActiveDownloads() {
        return maxConcurrentDownloads - downloadSemaphore.availablePermits();
    }
    
    /// Gets the number of pending task futures.
    public int getPendingFutures() {
        return chunkQueue.getInFlightCount();
    }
    
    @Override
    public void close() {
        // Stop accepting new tasks
        running.set(false);
        
        // Interrupt observer thread
        observerThread.interrupt();
        try {
            observerThread.join(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        // Shutdown executor
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        // Cancel any remaining futures and clear pending tasks
        chunkQueue.inFlightFutures().values().forEach(future -> future.cancel(true));
        chunkQueue.clearPending();
    }
}