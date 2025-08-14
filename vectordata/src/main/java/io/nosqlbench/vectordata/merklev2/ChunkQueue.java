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

import java.time.Instant;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;

/// Encapsulates all data structures used for chunk download queuing and tracking.
/// 
/// This record owns and manages:
/// 1. The task queue for pending download tasks
/// 2. The in-flight futures map for duplicate prevention
/// 3. A history queue of recently completed tasks for analysis
/// 
/// The ChunkQueue provides thread-safe operations for managing the complete
/// lifecycle of download tasks from scheduling through completion.
/// 
/// Usage example:
/// ```java
/// ChunkQueue chunkQueue = new ChunkQueue(100); // Keep history of last 100 completed tasks
/// 
/// // Schedule tasks
/// scheduler.scheduleDownloads(shape, state, offset, length, chunkQueue);
/// 
/// // Process pending tasks
/// ChunkScheduler.NodeDownloadTask task = chunkQueue.pollTask();
/// 
/// // Mark task as completed
/// chunkQueue.markCompleted(task);
/// ```
public record ChunkQueue(
    BlockingQueue<ChunkScheduler.NodeDownloadTask> taskQueue,
    ConcurrentMap<Integer, CompletableFuture<Void>> inFlightFutures,
    Deque<CompletedTask> completedTasksHistory,
    int maxHistorySize
) implements SchedulingTarget {
    
    /// Creates a new ChunkQueue with the specified history size.
    /// 
    /// @param maxHistorySize Maximum number of completed tasks to retain in history
    public ChunkQueue(int maxHistorySize) {
        this(
            new LinkedBlockingQueue<>(),
            new ConcurrentHashMap<>(),
            new ConcurrentLinkedDeque<>(),
            maxHistorySize
        );
    }
    
    /// Creates a new ChunkQueue with default history size of 1000.
    public ChunkQueue() {
        this(1000);
    }
    
    /// Offers a task to the pending task queue.
    /// 
    /// This is typically called by schedulers to add tasks for execution.
    /// 
    /// @param task The task to add to the queue
    /// @return true if the task was successfully added
    public boolean offerTask(ChunkScheduler.NodeDownloadTask task) {
        return taskQueue.offer(task);
    }
    
    /// Polls a task from the pending task queue.
    /// 
    /// This is typically called by task executors to get the next task to process.
    /// 
    /// @return The next task to process, or null if the queue is empty
    public ChunkScheduler.NodeDownloadTask pollTask() {
        return taskQueue.poll();
    }
    
    /// Gets or creates a future for the specified node index.
    /// 
    /// This ensures that multiple requests for the same node share the same future,
    /// preventing duplicate downloads.
    /// 
    /// @param nodeIndex The merkle tree node index
    /// @return The future for this node's download
    public CompletableFuture<Void> getOrCreateFuture(int nodeIndex) {
        return inFlightFutures.computeIfAbsent(nodeIndex, index -> new CompletableFuture<>());
    }
    
    /// Removes the future for a completed node.
    /// 
    /// This should be called when a download completes to clean up the in-flight map.
    /// 
    /// @param nodeIndex The node index that completed
    /// @return The future that was removed, or null if not present
    public CompletableFuture<Void> removeFuture(int nodeIndex) {
        return inFlightFutures.remove(nodeIndex);
    }
    
    /// Marks a task as completed and adds it to the history.
    /// 
    /// This should be called when a download task finishes successfully.
    /// The task is added to the completed history and the in-flight future is cleaned up.
    /// 
    /// @param task The task that completed
    /// @param completionTime When the task completed
    /// @param success Whether the task completed successfully
    /// @param bytesTransferred Number of bytes actually transferred
    public void markCompleted(ChunkScheduler.NodeDownloadTask task, Instant completionTime, 
                             boolean success, long bytesTransferred) {
        // Remove from in-flight map
        removeFuture(task.getNodeIndex());
        
        // Add to completed history
        CompletedTask completedTask = new CompletedTask(
            task.getNodeIndex(),
            task.getOffset(),
            task.getSize(),
            task.isLeafNode(),
            completionTime,
            success,
            bytesTransferred
        );
        
        // Add to history and maintain size limit
        completedTasksHistory.addLast(completedTask);
        while (completedTasksHistory.size() > maxHistorySize) {
            completedTasksHistory.removeFirst();
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
        return completedTasksHistory.size();
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
        completedTasksHistory.clear();
    }
    
    /// Clears all data structures.
    public void clearAll() {
        clearPending();
        clearHistory();
    }
    
    /// Executes a scheduling operation with synchronized access to return added task futures.
    /// 
    /// This method provides thread-safe access to the ChunkQueue during scheduling operations
    /// and returns the futures for tasks that were added during the scheduling call.
    /// 
    /// @param schedulingOperation The operation to execute that will add tasks to this queue
    /// @return List of futures for tasks that were added during this scheduling operation
    public synchronized List<CompletableFuture<Void>> executeScheduling(SchedulingOperation schedulingOperation) {
        SchedulingResult result = executeSchedulingWithTasks(schedulingOperation);
        return result.futures();
    }
    
    /// Executes a scheduling operation with synchronized access to return both tasks and futures.
    /// 
    /// This method provides thread-safe access to the ChunkQueue during scheduling operations
    /// and returns both the tasks and futures that were added during the scheduling call.
    /// This is more efficient for filtering relevant tasks by region.
    /// 
    /// @param schedulingOperation The operation to execute that will add tasks to this queue
    /// @return SchedulingResult containing both tasks and futures that were added
    public synchronized SchedulingResult executeSchedulingWithTasks(SchedulingOperation schedulingOperation) {
        // Track tasks added during this operation
        List<ChunkScheduler.NodeDownloadTask> addedTasks = new ArrayList<>();
        
        // Create a wrapper that tracks added tasks
        SchedulingWrapper wrapper = new SchedulingWrapper(this, addedTasks);
        
        // Execute the scheduling operation
        schedulingOperation.schedule(wrapper);
        
        // Return futures for all tasks that were added
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (ChunkScheduler.NodeDownloadTask task : addedTasks) {
            futures.add(task.getFuture());
        }
        
        return new SchedulingResult(addedTasks, futures);
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
    /// with synchronized access to the ChunkQueue.
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
        private final ChunkQueue chunkQueue;
        private final List<ChunkScheduler.NodeDownloadTask> addedTasks;
        
        private SchedulingWrapper(ChunkQueue chunkQueue, List<ChunkScheduler.NodeDownloadTask> addedTasks) {
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
        public ChunkQueue getChunkQueue() {
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