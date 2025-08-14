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

import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.Timer.Sample;

import java.time.Instant;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;

/// Optimized version of ChunkQueue that reduces synchronization contention.
/// 
/// Key optimizations:
/// 1. Uses ReadWriteLock instead of synchronized methods for better concurrency
/// 2. Minimizes critical sections to reduce lock contention
/// 3. Separates read-only operations from mutating operations
/// 4. Allows concurrent scheduling operations where safe
/// 
/// This implementation maintains the same interface as ChunkQueue but provides
/// better performance under high concurrency by reducing unnecessary blocking.
public record OptimizedChunkQueue(
    BlockingQueue<ChunkScheduler.NodeDownloadTask> taskQueue,
    ConcurrentMap<Integer, CompletableFuture<Void>> inFlightFutures,
    Deque<CompletedTask> completedTasksHistory,
    int maxHistorySize,
    ReadWriteLock schedulingLock,
    MeterRegistry meterRegistry
) implements SchedulingTarget {
    
    /// Creates a new OptimizedChunkQueue with the specified history size.
    /// 
    /// @param maxHistorySize Maximum number of completed tasks to retain in history
    /// @param meterRegistry Micrometer registry for metrics collection
    public OptimizedChunkQueue(int maxHistorySize, MeterRegistry meterRegistry) {
        this(
            new LinkedBlockingQueue<>(),
            new ConcurrentHashMap<>(),
            new ConcurrentLinkedDeque<>(),
            maxHistorySize,
            new ReentrantReadWriteLock(),
            meterRegistry
        );
        
        // Initialize metrics
        initializeMetrics();
    }
    
    /// Creates a new OptimizedChunkQueue with the specified history size and default metrics.
    /// 
    /// @param maxHistorySize Maximum number of completed tasks to retain in history
    public OptimizedChunkQueue(int maxHistorySize) {
        this(maxHistorySize, Metrics.globalRegistry);
    }
    
    /// Creates a new OptimizedChunkQueue with default history size of 1000.
    public OptimizedChunkQueue() {
        this(1000);
    }
    
    /// Initializes Micrometer metrics for performance monitoring.
    private void initializeMetrics() {
        // Counter for tasks added
        Counter.builder("chunk_queue.tasks.added")
            .description("Total number of tasks added to the queue")
            .register(meterRegistry);
            
        // Counter for tasks completed
        Counter.builder("chunk_queue.tasks.completed")
            .description("Total number of tasks completed")
            .register(meterRegistry);
            
        // Timer for scheduling operations
        Timer.builder("chunk_queue.scheduling.duration")
            .description("Time spent in scheduling operations")
            .register(meterRegistry);
            
        // Gauge for pending task count
        Gauge.builder("chunk_queue.tasks.pending", this, OptimizedChunkQueue::getPendingTaskCount)
            .description("Number of tasks currently pending")
            .register(meterRegistry);
            
        // Gauge for in-flight task count
        Gauge.builder("chunk_queue.tasks.in_flight", this, OptimizedChunkQueue::getInFlightCount)
            .description("Number of tasks currently in flight")
            .register(meterRegistry);
            
        // Gauge for completed task history size
        Gauge.builder("chunk_queue.tasks.history_size", this, OptimizedChunkQueue::getCompletedTaskCount)
            .description("Number of completed tasks in history")
            .register(meterRegistry);
    }
    
    /// Offers a task to the pending task queue.
    /// 
    /// This is thread-safe and non-blocking.
    /// 
    /// @param task The task to add to the queue
    /// @return true if the task was successfully added
    public boolean offerTask(ChunkScheduler.NodeDownloadTask task) {
        boolean added = taskQueue.offer(task);
        if (added) {
            meterRegistry.counter("chunk_queue.tasks.added").increment();
        }
        return added;
    }
    
    /// Polls a task from the pending task queue.
    /// 
    /// This is thread-safe and non-blocking.
    /// 
    /// @return The next task to process, or null if the queue is empty
    public ChunkScheduler.NodeDownloadTask pollTask() {
        return taskQueue.poll();
    }
    
    /// Gets or creates a future for the specified node index.
    /// 
    /// This uses ConcurrentHashMap.computeIfAbsent which is thread-safe and optimized.
    /// 
    /// @param nodeIndex The merkle tree node index
    /// @return The future for this node's download
    public CompletableFuture<Void> getOrCreateFuture(int nodeIndex) {
        return inFlightFutures.computeIfAbsent(nodeIndex, index -> new CompletableFuture<>());
    }
    
    /// Removes the future for a completed node.
    /// 
    /// @param nodeIndex The node index that completed
    /// @return The future that was removed, or null if not present
    public CompletableFuture<Void> removeFuture(int nodeIndex) {
        return inFlightFutures.remove(nodeIndex);
    }
    
    /// Marks a task as completed and adds it to the history.
    /// 
    /// Uses write lock only for the history update portion.
    /// 
    /// @param task The task that completed
    /// @param completionTime When the task completed
    /// @param success Whether the task completed successfully
    /// @param bytesTransferred Number of bytes actually transferred
    public void markCompleted(ChunkScheduler.NodeDownloadTask task, Instant completionTime, 
                             boolean success, long bytesTransferred) {
        // Remove from in-flight map (thread-safe)
        removeFuture(task.getNodeIndex());
        
        // Add to completed history (requires synchronization)
        CompletedTask completedTask = new CompletedTask(
            task.getNodeIndex(),
            task.getOffset(),
            task.getSize(),
            task.isLeafNode(),
            completionTime,
            success,
            bytesTransferred
        );
        
        // Only synchronize the history update
        schedulingLock.writeLock().lock();
        try {
            completedTasksHistory.addLast(completedTask);
            while (completedTasksHistory.size() > maxHistorySize) {
                completedTasksHistory.removeFirst();
            }
        } finally {
            schedulingLock.writeLock().unlock();
        }
        
        // Update metrics
        meterRegistry.counter("chunk_queue.tasks.completed").increment();
        if (success) {
            meterRegistry.counter("chunk_queue.tasks.completed", "status", "success").increment();
        } else {
            meterRegistry.counter("chunk_queue.tasks.completed", "status", "failed").increment();
        }
    }
    
    /// Convenience method to mark a task as successfully completed.
    /// 
    /// @param task The task that completed successfully
    /// @param bytesTransferred Number of bytes transferred
    public void markCompleted(ChunkScheduler.NodeDownloadTask task, long bytesTransferred) {
        markCompleted(task, Instant.now(), true, bytesTransferred);
    }
    
    /// Gets the number of pending tasks in the queue.
    /// 
    /// @return Number of tasks waiting to be processed
    public int getPendingTaskCount() {
        return taskQueue.size();
    }
    
    /// Gets the number of in-flight downloads.
    /// 
    /// @return Number of downloads currently in progress
    public int getInFlightCount() {
        return inFlightFutures.size();
    }
    
    /// Gets the number of completed tasks in history.
    /// 
    /// @return Number of completed tasks retained in history
    public int getCompletedTaskCount() {
        schedulingLock.readLock().lock();
        try {
            return completedTasksHistory.size();
        } finally {
            schedulingLock.readLock().unlock();
        }
    }
    
    /// Checks if there are any pending tasks.
    /// 
    /// @return true if the task queue is empty
    public boolean isEmpty() {
        return taskQueue.isEmpty();
    }
    
    /// Clears all pending tasks and in-flight futures.
    /// 
    /// This does not clear the completed task history.
    public void clearPending() {
        taskQueue.clear();
        inFlightFutures.clear();
    }
    
    /// Clears the completed task history.
    public void clearHistory() {
        schedulingLock.writeLock().lock();
        try {
            completedTasksHistory.clear();
        } finally {
            schedulingLock.writeLock().unlock();
        }
    }
    
    /// Clears all data structures.
    public void clearAll() {
        clearPending();
        clearHistory();
    }
    
    /// Executes a scheduling operation with minimal locking for better concurrency.
    /// 
    /// This method uses a read lock during scheduling to allow multiple concurrent
    /// scheduling operations, only taking a write lock if history needs to be read.
    /// 
    /// @param schedulingOperation The operation to execute that will add tasks to this queue
    /// @return List of futures for tasks that were added during this scheduling operation
    public List<CompletableFuture<Void>> executeScheduling(SchedulingOperation schedulingOperation) {
        Timer.Sample sample = Timer.start(meterRegistry);
        try {
            SchedulingResult result = executeSchedulingWithTasks(schedulingOperation);
            return result.futures();
        } finally {
            sample.stop(Timer.builder("chunk_queue.scheduling.duration")
                .register(meterRegistry));
        }
    }
    
    /// Executes a scheduling operation with optimized locking and returns both tasks and futures.
    /// 
    /// Uses minimal locking strategy:
    /// 1. No lock needed for task tracking (uses thread-safe wrapper)
    /// 2. No lock needed for task queue operations (ConcurrentLinkedQueue is thread-safe)
    /// 3. No lock needed for future operations (ConcurrentHashMap is thread-safe)
    /// 
    /// @param schedulingOperation The operation to execute that will add tasks to this queue
    /// @return SchedulingResult containing both tasks and futures that were added
    public SchedulingResult executeSchedulingWithTasks(SchedulingOperation schedulingOperation) {
        Timer.Sample sample = Timer.start(meterRegistry);
        try {
            // Track tasks added during this operation without any locking
            List<ChunkScheduler.NodeDownloadTask> addedTasks = new ArrayList<>();
            
            // Create a wrapper that tracks added tasks
            SchedulingWrapper wrapper = new SchedulingWrapper(this, addedTasks);
            
            // Execute the scheduling operation (no lock needed - operations are thread-safe)
            schedulingOperation.schedule(wrapper);
            
            // Return futures for all tasks that were added
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            for (ChunkScheduler.NodeDownloadTask task : addedTasks) {
                futures.add(task.getFuture());
            }
            
            // Record metrics for this scheduling operation
            meterRegistry.counter("chunk_queue.scheduling.operations").increment();
            meterRegistry.counter("chunk_queue.scheduling.tasks_scheduled")
                .increment(addedTasks.size());
            
            return new SchedulingResult(addedTasks, futures);
        } finally {
            sample.stop(Timer.builder("chunk_queue.scheduling.duration")
                .register(meterRegistry));
        }
    }
    
    /// Result of a scheduling operation containing both tasks and futures.
    /// 
    /// @param tasks The tasks that were added during scheduling
    /// @param futures The futures for the tasks that were added
    public record SchedulingResult(
        List<ChunkScheduler.NodeDownloadTask> tasks,
        List<CompletableFuture<Void>> futures
    ) {}
    
    /// Functional interface for scheduling operations.
    /// 
    /// This allows callers to pass scheduling logic that will be executed
    /// with optimized access to the ChunkQueue.
    @FunctionalInterface
    public interface SchedulingOperation {
        /// Executes the scheduling logic with the provided wrapper.
        /// 
        /// @param wrapper The SchedulingWrapper to use for adding tasks
        void schedule(SchedulingWrapper wrapper);
    }
    
    
    /// Wrapper that tracks tasks added during a scheduling operation.
    /// 
    /// This class provides the same interface as ChunkQueue but tracks
    /// which tasks were added during the current scheduling operation.
    public static class SchedulingWrapper implements SchedulingTarget {
        private final OptimizedChunkQueue chunkQueue;
        private final List<ChunkScheduler.NodeDownloadTask> addedTasks;
        
        private SchedulingWrapper(OptimizedChunkQueue chunkQueue, List<ChunkScheduler.NodeDownloadTask> addedTasks) {
            this.chunkQueue = chunkQueue;
            this.addedTasks = addedTasks;
        }
        
        /// Adds a task to the queue and tracks it in this scheduling operation.
        /// 
        /// @param task The task to add
        /// @return true if the task was successfully added
        public boolean offerTask(ChunkScheduler.NodeDownloadTask task) {
            if (chunkQueue.offerTask(task)) {
                addedTasks.add(task);
                return true;
            }
            return false;
        }
        
        /// Gets or creates a future for the specified node index.
        /// 
        /// @param nodeIndex The merkle tree node index
        /// @return The future for this node's download
        public CompletableFuture<Void> getOrCreateFuture(int nodeIndex) {
            return chunkQueue.getOrCreateFuture(nodeIndex);
        }
        
        /// Gets the underlying ChunkQueue for other operations.
        /// 
        /// @return The ChunkQueue this wrapper is tracking
        public OptimizedChunkQueue getChunkQueue() {
            return chunkQueue;
        }
    }
    
    
    /// Represents a completed download task with timing and success information.
    /// 
    /// This record is used to track the history of completed tasks for analysis
    /// and performance monitoring.
    public record CompletedTask(
        int nodeIndex,
        long offset,
        long size,
        boolean isLeafNode,
        Instant completionTime,
        boolean success,
        long bytesTransferred
    ) {
        
        /// Gets the efficiency ratio of this completed task.
        /// 
        /// Efficiency is calculated as the ratio of bytes requested to bytes transferred.
        /// A ratio of 1.0 means perfect efficiency (no waste), values > 1.0 indicate
        /// over-downloading.
        /// 
        /// @return The efficiency ratio (bytes transferred / bytes requested)
        public double getEfficiency() {
            return size > 0 ? (double) bytesTransferred / size : 0.0;
        }
        
        /// Checks if this task represents over-downloading.
        /// 
        /// @return true if more bytes were transferred than requested
        public boolean isOverDownload() {
            return bytesTransferred > size;
        }
        
        /// Gets the amount of waste (over-downloaded bytes).
        /// 
        /// @return Number of bytes downloaded but not needed (0 if no waste)
        public long getWasteBytes() {
            return Math.max(0, bytesTransferred - size);
        }
        
        @Override
        public String toString() {
            return String.format("CompletedTask{node=%d, offset=%d, size=%d, leaf=%s, success=%s, transferred=%d, efficiency=%.2f}", 
                nodeIndex, offset, size, isLeafNode, success, bytesTransferred, getEfficiency());
        }
    }
}