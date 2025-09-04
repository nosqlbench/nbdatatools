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

import java.io.PrintStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
     * Gets the number of bytes per work unit for contextual display purposes.
     * This allows progress indicators that work in abstract units (like chunks)
     * to display meaningful byte amounts with appropriate ISO units.
     * 
     * @return The number of bytes per work unit, or 1.0 if not applicable
     */
    default double getBytesPerUnit() {
        return 1.0;
    }
    
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
     * Creates a string representation of the current progress.
     * 
     * @return A formatted string showing progress (e.g., "45.5/100.0 (45.5%)")
     */
    default String getProgressString() {
        double bytesPerUnit = getBytesPerUnit();
        if (bytesPerUnit > 1.0) {
            // Show both unit count and byte amounts with appropriate ISO units
            double currentBytes = getCurrentWork() * bytesPerUnit;
            double totalBytes = getTotalWork() * bytesPerUnit;
            return String.format("%.1f/%.1f chunks (%s/%s, %.1f%%)", 
                getCurrentWork(), 
                getTotalWork(), 
                formatBytes(currentBytes),
                formatBytes(totalBytes),
                getProgressPercentage());
        } else {
            // Default format for non-byte units
            return String.format("%.1f/%.1f (%.1f%%)", 
                getCurrentWork(), 
                getTotalWork(), 
                getProgressPercentage());
        }
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
    public static class ProgressSnapshot {
        private final double currentWork;
        private final double totalWork;
        
        public ProgressSnapshot(double currentWork, double totalWork) {
            this.currentWork = currentWork;
            this.totalWork = totalWork;
        }
        
        public double currentWork() {
            return currentWork;
        }
        
        public double totalWork() {
            return totalWork;
        }
        
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
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            ProgressSnapshot that = (ProgressSnapshot) obj;
            return Double.compare(that.currentWork, currentWork) == 0 && Double.compare(that.totalWork, totalWork) == 0;
        }
        
        @Override
        public int hashCode() {
            return java.util.Objects.hash(currentWork, totalWork);
        }
        
        @Override
        public String toString() {
            return String.format("Progress[%.1f/%.1f (%.1f%%)]", 
                currentWork, totalWork, getPercentage());
        }
    }
    
    /**
     * Prints the progress status of this ProgressIndicator to the provided output stream
     * at regular intervals until completion.
     * 
     * If this instance is also a CompletableFuture, it will wait for the future to complete.
     * If not, it will poll the progress until it reaches 100%.
     * 
     * Includes rate estimation and remaining time calculation based on progress history.
     * Only prints updates when the progress string has changed.
     * Includes a busy indicator using ANSI escape codes.
     * 
     * @param outputStream The output stream to print progress to
     * @param intervalMs The interval in milliseconds between progress updates
     * @return A CompletableFuture that completes when monitoring stops
     */
    default CompletableFuture<Void> monitorProgress(PrintStream outputStream, long intervalMs) {
        return CompletableFuture.supplyAsync(() -> {
            // Set up shutdown hook for Ctrl-C (SIGINT) handling
            final java.util.concurrent.atomic.AtomicBoolean interrupted = new java.util.concurrent.atomic.AtomicBoolean(false);
            final Thread shutdownHook = new Thread(() -> {
                interrupted.set(true);
                outputStream.print("\r\033[K"); // Clear line
                outputStream.println("Interrupted by user (Ctrl-C)");
                outputStream.flush();
                
                // If this is a CompletableFuture, try to cancel it
                if (this instanceof CompletableFuture) {
                    ((CompletableFuture<?>) this).cancel(true);
                }
            });
            
            try {
                // Track progress history for rate calculation (sparse sampling)
                java.util.Deque<ProgressSample> progressHistory = new java.util.ArrayDeque<>();
                final int MAX_SAMPLES = 10; // Keep only recent samples
                
                // Busy indicator characters
                final String[] BUSY_CHARS = {"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"};
                int busyIndex = 0;
                
                String lastProgressString = null;
                long startTime = System.currentTimeMillis();
                long lastProgressUpdate = startTime;
                boolean hasOutput = false;
                
                // Add shutdown hook (will be called on Ctrl-C)
                Runtime.getRuntime().addShutdownHook(shutdownHook);
                
                // Print initial progress
                String currentProgressString = getEnhancedProgressString(progressHistory, startTime);
                outputStream.print(currentProgressString);
                outputStream.flush();
                lastProgressString = currentProgressString;
                hasOutput = true;
                
                // Add initial sample
                progressHistory.addLast(new ProgressSample(getCurrentWork(), System.currentTimeMillis()));
                
                // Check if this instance is also a CompletableFuture
                if (this instanceof CompletableFuture) {
                    CompletableFuture<?> future = (CompletableFuture<?>) this;
                    
                    // Loop until the future completes or is interrupted
                    while (!future.isDone() && !interrupted.get()) {
                        try {
                            // Wait for a shorter interval to update busy indicator
                            long busyInterval = Math.min(intervalMs / 4, 250); // Update busy indicator more frequently
                            future.get(busyInterval, TimeUnit.MILLISECONDS);
                            // If we get here, the future completed
                            break;
                        } catch (TimeoutException e) {
                            long currentTime = System.currentTimeMillis();
                            double currentWork = getCurrentWork();
                            
                            // Check if enough time has passed for a progress update
                            if (currentTime - lastProgressUpdate >= intervalMs) {
                                // Add sample to history (sparse sampling)
                                progressHistory.addLast(new ProgressSample(currentWork, currentTime));
                                if (progressHistory.size() > MAX_SAMPLES) {
                                    progressHistory.removeFirst();
                                }
                                
                                currentProgressString = getEnhancedProgressString(progressHistory, startTime);
                                if (!currentProgressString.equals(lastProgressString)) {
                                    // Clear current line and print new progress
                                    if (hasOutput) {
                                        outputStream.print("\r\033[K"); // Clear line
                                    }
                                    outputStream.print(currentProgressString);
                                    outputStream.flush();
                                    lastProgressString = currentProgressString;
                                    hasOutput = true;
                                }
                                lastProgressUpdate = currentTime;
                            } else {
                                // Just update the busy indicator
                                if (hasOutput) {
                                    outputStream.print("\r\033[K"); // Clear line
                                }
                                outputStream.print(lastProgressString + " " + BUSY_CHARS[busyIndex]);
                                outputStream.flush();
                                busyIndex = (busyIndex + 1) % BUSY_CHARS.length;
                                hasOutput = true;
                            }
                        }
                    }
                    
                    // Clear line and print final status
                    if (hasOutput) {
                        outputStream.print("\r\033[K"); // Clear line
                    }
                    if (future.isCompletedExceptionally()) {
                        outputStream.println("Task failed");
                    } else {
                        outputStream.println(getEnhancedProgressString(progressHistory, startTime) + " - Complete");
                    }
                } else {
                    // Not a CompletableFuture, poll until 100% complete or interrupted
                    while (!isWorkComplete() && !interrupted.get()) {
                        try {
                            // Sleep for a shorter interval to update busy indicator
                            long busyInterval = Math.min(intervalMs / 4, 250);
                            Thread.sleep(busyInterval);
                            
                            long currentTime = System.currentTimeMillis();
                            double currentWork = getCurrentWork();
                            
                            // Check if enough time has passed for a progress update
                            if (currentTime - lastProgressUpdate >= intervalMs) {
                                // Add sample to history (sparse sampling)
                                progressHistory.addLast(new ProgressSample(currentWork, currentTime));
                                if (progressHistory.size() > MAX_SAMPLES) {
                                    progressHistory.removeFirst();
                                }
                                
                                currentProgressString = getEnhancedProgressString(progressHistory, startTime);
                                if (!currentProgressString.equals(lastProgressString)) {
                                    // Clear current line and print new progress
                                    if (hasOutput) {
                                        outputStream.print("\r\033[K"); // Clear line
                                    }
                                    outputStream.print(currentProgressString);
                                    outputStream.flush();
                                    lastProgressString = currentProgressString;
                                    hasOutput = true;
                                }
                                lastProgressUpdate = currentTime;
                            } else {
                                // Just update the busy indicator
                                if (hasOutput) {
                                    outputStream.print("\r\033[K"); // Clear line
                                }
                                outputStream.print(lastProgressString + " " + BUSY_CHARS[busyIndex]);
                                outputStream.flush();
                                busyIndex = (busyIndex + 1) % BUSY_CHARS.length;
                                hasOutput = true;
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            if (hasOutput) {
                                outputStream.print("\r\033[K"); // Clear line
                            }
                            outputStream.println("Monitoring interrupted");
                            throw new RuntimeException(e);
                        }
                    }
                    
                    // Clear line and print final status
                    if (hasOutput) {
                        outputStream.print("\r\033[K"); // Clear line
                    }
                    if (interrupted.get()) {
                        outputStream.println("Monitoring cancelled by user");
                    } else {
                        outputStream.println(getEnhancedProgressString(progressHistory, startTime) + " - Complete");
                    }
                }
                
                // Check if we were interrupted
                if (interrupted.get()) {
                    throw new InterruptedException("Progress monitoring interrupted by user signal");
                }
                
                return null;
            } catch (Exception e) {
                outputStream.println("\nError monitoring progress: " + e.getMessage());
                throw new RuntimeException(e);
            } finally {
                // Remove shutdown hook if monitoring completes normally
                try {
                    Runtime.getRuntime().removeShutdownHook(shutdownHook);
                } catch (IllegalStateException e) {
                    // Shutdown in progress, hook already executing or removed
                }
            }
        });
    }
    
    /**
     * Gets an enhanced progress string with rate and time estimation.
     * 
     * @param progressHistory Historical progress samples for rate calculation
     * @param startTime The start time of monitoring
     * @return Enhanced progress string with rate and estimated time remaining
     */
    default String getEnhancedProgressString(java.util.Deque<ProgressSample> progressHistory, long startTime) {
        String baseProgress = getProgressString();
        
        if (progressHistory.size() < 2) {
            return baseProgress;
        }
        
        // Calculate rate based on recent samples
        ProgressSample oldest = progressHistory.peekFirst();
        ProgressSample newest = progressHistory.peekLast();
        
        long timeDiff = newest.timestamp - oldest.timestamp;
        double workDiff = newest.work - oldest.work;
        
        if (timeDiff <= 0 || workDiff <= 0) {
            return baseProgress;
        }
        
        // Calculate rate (work per second)
        double rate = (workDiff * 1000.0) / timeDiff;
        
        // Calculate estimated remaining time
        double remainingWork = getRemainingWork();
        String timeEstimate = "";
        if (rate > 0 && remainingWork > 0) {
            long estimatedSecondsRemaining = (long) (remainingWork / rate);
            timeEstimate = formatDuration(estimatedSecondsRemaining);
        }
        
        // Calculate elapsed time
        long elapsedMs = System.currentTimeMillis() - startTime;
        String elapsed = formatDuration(elapsedMs / 1000);
        
        StringBuilder enhanced = new StringBuilder(baseProgress);
        enhanced.append(" [").append(String.format("%.1f/s", rate));
        
        // Add MBit/s rate if we have byte context
        double bytesPerUnit = getBytesPerUnit();
        if (bytesPerUnit > 1.0 && rate > 0) {
            double bytesPerSecond = rate * bytesPerUnit;
            double mbitsPerSecond = (bytesPerSecond * 8.0) / (1024.0 * 1024.0); // Convert bytes/s to Mbit/s
            enhanced.append(String.format(", %.1f Mbit/s", mbitsPerSecond));
        }
        
        enhanced.append(", elapsed: ").append(elapsed);
        if (!timeEstimate.isEmpty()) {
            enhanced.append(", ETA: ").append(timeEstimate);
        }
        enhanced.append("]");
        
        return enhanced.toString();
    }
    
    /**
     * Formats a duration in seconds to a human-readable string.
     * 
     * @param seconds Duration in seconds
     * @return Formatted duration string (e.g., "2m 30s", "1h 15m", "45s")
     */
    default String formatDuration(long seconds) {
        if (seconds < 60) {
            return seconds + "s";
        } else if (seconds < 3600) {
            long minutes = seconds / 60;
            long remainingSeconds = seconds % 60;
            return minutes + "m" + (remainingSeconds > 0 ? " " + remainingSeconds + "s" : "");
        } else {
            long hours = seconds / 3600;
            long remainingMinutes = (seconds % 3600) / 60;
            return hours + "h" + (remainingMinutes > 0 ? " " + remainingMinutes + "m" : "");
        }
    }
    
    /**
     * Formats byte amounts using appropriate ISO units (KB, MB, GB, TB).
     * 
     * @param bytes Number of bytes to format
     * @return Formatted string with appropriate unit (e.g., "1.5 MB", "3.2 GB")
     */
    default String formatBytes(double bytes) {
        if (bytes < 1024) {
            return String.format("%.0f B", bytes);
        } else if (bytes < 1024 * 1024) {
            return String.format("%.1f KB", bytes / 1024);
        } else if (bytes < 1024 * 1024 * 1024) {
            return String.format("%.1f MB", bytes / (1024 * 1024));
        } else if (bytes < 1024L * 1024 * 1024 * 1024) {
            return String.format("%.1f GB", bytes / (1024.0 * 1024 * 1024));
        } else {
            return String.format("%.1f TB", bytes / (1024.0 * 1024 * 1024 * 1024));
        }
    }
    
    /**
     * A sample of progress at a specific point in time.
     */
    class ProgressSample {
        private final double work;
        private final long timestamp;
        
        public ProgressSample(double work, long timestamp) {
            this.work = work;
            this.timestamp = timestamp;
        }
        
        public double work() {
            return work;
        }
        
        public long timestamp() {
            return timestamp;
        }
        
        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            ProgressSample that = (ProgressSample) obj;
            return Double.compare(that.work, work) == 0 && timestamp == that.timestamp;
        }
        
        @Override
        public int hashCode() {
            return java.util.Objects.hash(work, timestamp);
        }
    }
    
    /**
     * Prints the progress status of this ProgressIndicator to System.out
     * at regular intervals until completion.
     * 
     * @param intervalMs The interval in milliseconds between progress updates
     * @return A CompletableFuture that completes when monitoring stops
     */
    default CompletableFuture<Void> monitorProgress(long intervalMs) {
        return monitorProgress(System.out, intervalMs);
    }
}