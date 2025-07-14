package io.nosqlbench.command.merkle.console;

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

import io.nosqlbench.vectordata.merkle.MerkleTreeBuildProgress;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simple progress reporter that logs progress at configurable intervals
 * instead of using the TUI interface.
 * 
 * This is useful for **headless environments**, CI/CD pipelines, or when
 * the TUI interface is not desired. Provides periodic logging of progress
 * metrics and performance statistics.
 */
public class SimpleProgressReporter implements AutoCloseable {
    private static final Logger logger = LogManager.getLogger(SimpleProgressReporter.class);
    
    private final Path filePath;
    private final int reportingIntervalSeconds;
    private final AtomicLong totalBlocksProcessed;
    private final long totalBlocks;
    private final int totalFileBlocks;
    private volatile boolean shutdown = false;
    private Thread reportingThread;
    private final long startTime;
    
    /**
     * Creates a new simple progress reporter.
     * 
     * @param filePath The path of the file being processed
     * @param reportingIntervalSeconds The interval in seconds between progress reports
     * @param totalBlocksProcessed Counter for session-wide progress tracking (optional)
     * @param totalBlocks Total number of leaf chunks across all files (for session-wide progress)
     * @param totalFileBlocks Total leaf chunks for the current file (only leaf chunks, excludes internal nodes)
     */
    public SimpleProgressReporter(Path filePath, int reportingIntervalSeconds, 
                                 AtomicLong totalBlocksProcessed, long totalBlocks, int totalFileBlocks) {
        this.filePath = filePath;
        this.reportingIntervalSeconds = Math.max(1, reportingIntervalSeconds); // Minimum 1 second
        this.totalBlocksProcessed = totalBlocksProcessed;
        this.totalBlocks = totalBlocks;
        this.totalFileBlocks = totalFileBlocks;
        this.startTime = System.currentTimeMillis();
        
        logger.info("Starting Merkle tree creation for: {}", filePath);
    }
    
    /**
     * Starts the progress reporting thread.
     * 
     * @param progress The progress object to monitor
     */
    public void startReporting(MerkleTreeBuildProgress progress) {
        if (reportingThread != null) {
            return; // Already started
        }
        
        reportingThread = new Thread(() -> {
            try {
                while (!shutdown && !Thread.currentThread().isInterrupted()) {
                    reportProgress(progress);
                    
                    // Check if the future is done
                    if (progress.getFuture().isDone()) {
                        break;
                    }
                    
                    // Wait for the next reporting interval
                    Thread.sleep(reportingIntervalSeconds * 1000L);
                }
                
                // Report final progress
                if (!shutdown) {
                    reportFinalProgress(progress);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                if (!shutdown) {
                    logger.debug("Progress reporting thread interrupted");
                }
            }
        });
        
        reportingThread.setName("MerkleProgressReporter-" + filePath.getFileName());
        reportingThread.setDaemon(true);
        reportingThread.start();
    }
    
    /**
     * Reports the current progress.
     * 
     * @param progress The progress object to report on
     */
    private void reportProgress(MerkleTreeBuildProgress progress) {
        int processedChunks = progress.getProcessedChunks();
        int totalChunks = progress.getTotalChunks();
        long totalBytes = progress.getTotalBytes();
        String currentPhase = progress.getPhase();
        
        // Calculate file progress percentage using leaf chunks only
        double fileProgress = totalFileBlocks > 0 ? (double) processedChunks / totalFileBlocks * 100.0 : 0.0;
        
        // Calculate processing rate
        long elapsedTime = System.currentTimeMillis() - startTime;
        double chunksPerSecond = elapsedTime > 0 ? (double) processedChunks / (elapsedTime / 1000.0) : 0.0;
        
        // Format file progress
        String fileProgressStr = String.format("%.1f%% (%d/%d leaf chunks)", fileProgress, processedChunks, totalFileBlocks);
        
        // Format rate
        String rateStr = String.format("%.1f chunks/sec", chunksPerSecond);
        
        // Session progress (if available)
        String sessionProgressStr = "";
        if (totalBlocksProcessed != null && totalBlocks > 0) {
            // Calculate current file progress in terms of leaf chunks only
            // Since we only track leaf chunks, use processed chunks directly
            int currentFileProgress = processedChunks;
            
            // Session progress = already completed + current file progress
            long currentSessionProgress = totalBlocksProcessed.get() + currentFileProgress;
            double sessionProgress = (double) currentSessionProgress / totalBlocks * 100.0;
            sessionProgressStr = String.format(" | Session: %.1f%% (%d/%d leaf chunks)", 
                                             sessionProgress, currentSessionProgress, totalBlocks);
        }
        
        logger.info("Merkle tree progress - File: {} | Phase: {} | Rate: {}{}",
                   fileProgressStr, currentPhase, rateStr, sessionProgressStr);
    }
    
    /// Reports the final progress when processing is complete.
    /// @param progress The progress object to report on
    private void reportFinalProgress(MerkleTreeBuildProgress progress) {
        long elapsedTime = System.currentTimeMillis() - startTime;
        double elapsedSeconds = elapsedTime / 1000.0;
        
        int totalChunks = progress.getTotalChunks();
        long totalBytes = progress.getTotalBytes();
        
        // Calculate final rates using leaf chunks only
        double chunksPerSecond = elapsedSeconds > 0 ? totalFileBlocks / elapsedSeconds : 0.0;
        double mbPerSecond = elapsedSeconds > 0 ? (totalBytes / 1024.0 / 1024.0) / elapsedSeconds : 0.0;
        
        // Get performance metrics if available
        String performanceMetrics = "";
        try {
            // Check if the method exists using reflection to avoid compilation issues
            Class<?> clazz = progress.getClass();
            java.lang.reflect.Method method = clazz.getDeclaredMethod("getPerformanceMetrics");
            String metrics = (String) method.invoke(progress);
            if (metrics != null && !metrics.isEmpty()) {
                performanceMetrics = "\n" + metrics;
            }
        } catch (Exception e) {
            // Ignore if performance metrics are not available
        }
        
        logger.info("Merkle tree creation completed for: {}", filePath);
        logger.info("Processing time: {}s | Total leaf chunks: {} | Rate: {} chunks/sec | Throughput: {} MB/s{}",
                   String.format("%.2f", elapsedSeconds), totalFileBlocks, String.format("%.1f", chunksPerSecond), 
                   String.format("%.1f", mbPerSecond), performanceMetrics);
    }
    
    /**
     * Logs a message during processing.
     * 
     * @param message The message to log
     * @param args Message arguments
     */
    public void log(String message, Object... args) {
        logger.info(message, args);
    }
    
    /**
     * Sets the current status/phase.
     * 
     * @param status The current status
     */
    public void setStatus(String status) {
        logger.debug("Status: {}", status);
    }
    
    /**
     * Updates the progress (no-op for simple reporter, as it polls the progress object).
     * 
     * @param bytesProcessed Bytes processed
     * @param totalBytes Total bytes
     * @param chunksProcessed Chunks processed
     * @param totalChunks Total chunks
     */
    public void updateProgress(long bytesProcessed, long totalBytes, int chunksProcessed, int totalChunks) {
        // No-op for simple reporter - it polls the progress object directly
    }
    
    /**
     * Updates the session progress (no-op for simple reporter).
     * 
     * @param blocksProcessed Leaf chunks processed
     * @param totalBlocks Total leaf chunks
     */
    public void updateSessionProgress(long blocksProcessed, long totalBlocks) {
        // No-op for simple reporter - it gets session progress from the counter
    }
    
    @Override
    public void close() {
        shutdown = true;
        if (reportingThread != null) {
            reportingThread.interrupt();
            try {
                reportingThread.join(1000); // Wait up to 1 second for thread to finish
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}