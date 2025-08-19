package io.nosqlbench.nbdatatools.api.concurrent;

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
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * A decorator interface for CompletableFuture that adds progress tracking capabilities.
 * 
 * This interface extends CompletableFuture and provides additional methods to track
 * the progress of long-running asynchronous operations. Progress is represented as
 * two double values: the total amount of work to be done and the current amount
 * of work completed.
 * 
 * @param <T> The result type returned by this future's get method
 */
public interface ProgressIndicator<T>  {
    
    /**
     * Gets the total amount of work to be done.
     * 
     * @return The total work as a double value. The units are implementation-specific
     *         (could be bytes, items, percentage as 0-100, etc.)
     */
    double getTotalWork();
    
    /**
     * Gets the current amount of work completed.
     * 
     * @return The current work completed as a double value. Should be between 0 and getTotalWork()
     */
    double getCurrentWork();
    
    /**
     * Gets the progress as a percentage.
     * 
     * @return The progress percentage (0.0 to 100.0). Returns 0.0 if total work is 0
     */
    default double getProgressPercentage() {
        double total = getTotalWork();
        if (total <= 0) {
            return 0.0;
        }
        return (getCurrentWork() / total) * 100.0;
    }
    
    /**
     * Gets the progress as a fraction.
     * 
     * @return The progress as a value between 0.0 and 1.0. Returns 0.0 if total work is 0
     */
    default double getProgressFraction() {
        double total = getTotalWork();
        if (total <= 0) {
            return 0.0;
        }
        return getCurrentWork() / total;
    }
    
    /**
     * Checks if the operation has made any progress.
     * 
     * @return true if current work is greater than 0, false otherwise
     */
    default boolean hasProgress() {
        return getCurrentWork() > 0;
    }
    
    /**
     * Checks if the operation is complete based on progress.
     * Note: This is different from CompletableFuture.isDone() as it checks
     * if the work is complete, not if the future has completed (which could
     * be due to cancellation or exception).
     * 
     * @return true if current work equals total work, false otherwise
     */
    default boolean isWorkComplete() {
        return getCurrentWork() >= getTotalWork() && getTotalWork() > 0;
    }
    
    /**
     * Gets the remaining work to be done.
     * 
     * @return The amount of work remaining (total - current)
     */
    default double getRemainingWork() {
        return Math.max(0, getTotalWork() - getCurrentWork());
    }
    
    /**
     * Registers a callback to be invoked when progress is updated.
     * The callback receives the current work and total work values.
     * 
     * @param progressListener A BiConsumer that accepts (currentWork, totalWork)
     */
    void addProgressListener(BiConsumer<Double, Double> progressListener);
    
    /**
     * Removes a previously registered progress listener.
     * 
     * @param progressListener The listener to remove
     * @return true if the listener was removed, false if it was not registered
     */
    boolean removeProgressListener(BiConsumer<Double, Double> progressListener);
    
    /**
     * Gets the underlying CompletableFuture.
     * This allows access to all standard CompletableFuture methods.
     * 
     * @return The underlying CompletableFuture
     */
    CompletableFuture<T> toCompletableFuture();
    
    /**
     * Waits if necessary for this future to complete, and then returns its result.
     * 
     * @return the result value
     * @throws java.util.concurrent.CancellationException if this future was cancelled
     * @throws java.util.concurrent.ExecutionException if this future completed exceptionally
     * @throws InterruptedException if the current thread was interrupted while waiting
     */
    default T get() throws InterruptedException, java.util.concurrent.ExecutionException {
        return toCompletableFuture().get();
    }
    
    /**
     * Waits if necessary for at most the given time for this future to complete,
     * and then returns its result, if available.
     * 
     * @param timeout the maximum time to wait
     * @param unit the time unit of the timeout argument
     * @return the result value
     * @throws java.util.concurrent.CancellationException if this future was cancelled
     * @throws java.util.concurrent.ExecutionException if this future completed exceptionally
     * @throws InterruptedException if the current thread was interrupted while waiting
     * @throws java.util.concurrent.TimeoutException if the wait timed out
     */
    default T get(long timeout, TimeUnit unit) throws InterruptedException, 
                                              java.util.concurrent.ExecutionException,
                                              java.util.concurrent.TimeoutException {
        return toCompletableFuture().get(timeout, unit);
    }
    
    /**
     * Creates a string representation of the current progress.
     * 
     * @return A formatted string showing progress (e.g., "45.5/100.0 (45.5%)")
     */
    default String getProgressString() {
        return String.format("%.1f/%.1f (%.1f%%)", 
            getCurrentWork(), 
            getTotalWork(), 
            getProgressPercentage());
    }
    
    /**
     * Estimates the remaining time based on the current progress rate.
     * This method should be overridden by implementations that track timing information.
     * 
     * @param unit The time unit for the result
     * @return The estimated remaining time, or -1 if it cannot be estimated
     */
    default long getEstimatedRemainingTime(TimeUnit unit) {
        return -1; // Default implementation returns -1 (unknown)
    }
    
    /**
     * Gets a snapshot of the current progress state.
     * This can be useful for atomically getting both values.
     * 
     * @return A ProgressSnapshot containing the current state
     */
    default ProgressSnapshot getProgressSnapshot() {
        return new ProgressSnapshot(getCurrentWork(), getTotalWork());
    }
    
    /**
     * A snapshot of progress at a point in time.
     */
    public static record ProgressSnapshot(double currentWork, double totalWork) {
        
        public double getPercentage() {
            if (totalWork <= 0) {
                return 0.0;
            }
            return (currentWork / totalWork) * 100.0;
        }
        
        public double getFraction() {
            if (totalWork <= 0) {
                return 0.0;
            }
            return currentWork / totalWork;
        }
        
        public boolean isComplete() {
            return currentWork >= totalWork && totalWork > 0;
        }
        
        public double getRemainingWork() {
            return Math.max(0, totalWork - currentWork);
        }
        
        @Override
        public String toString() {
            return String.format("Progress[%.1f/%.1f (%.1f%%)]", 
                currentWork, totalWork, getPercentage());
        }
    }
}