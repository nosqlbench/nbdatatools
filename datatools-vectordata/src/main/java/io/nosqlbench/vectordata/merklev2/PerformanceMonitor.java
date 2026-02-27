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
import io.micrometer.core.instrument.search.Search;

import java.util.List;
import java.util.concurrent.TimeUnit;

/// Performance monitoring and reporting utility for MAFileChannel and OptimizedChunkQueue.
/// 
/// This utility provides methods to collect, analyze, and report performance metrics
/// from the Micrometer registry. It can generate human-readable performance reports
/// and provide insights into system behavior.
/// 
/// Usage:
/// ```java
/// PerformanceMonitor monitor = new PerformanceMonitor(Metrics.globalRegistry);
/// 
/// // Get a performance snapshot
/// PerformanceSnapshot snapshot = monitor.getSnapshot();
/// System.out.println(snapshot.generateReport());
/// 
/// // Monitor performance over time
/// monitor.startPeriodicReporting(Duration.ofMinutes(1));
/// ```
public class PerformanceMonitor {
    
    private final MeterRegistry meterRegistry;
    
    /// Creates a new performance monitor.
    /// 
    /// @param meterRegistry The Micrometer registry to monitor
    public PerformanceMonitor(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }
    
    /// Gets a performance snapshot of the current state.
    /// 
    /// @return Current performance metrics snapshot
    public PerformanceSnapshot getSnapshot() {
        return new PerformanceSnapshot(
            getChunkQueueMetrics(),
            getMAFileChannelMetrics(),
            getSystemMetrics()
        );
    }
    
    /// Collects ChunkQueue performance metrics.
    private ChunkQueueMetrics getChunkQueueMetrics() {
        // Scheduling metrics
        Timer schedulingTimer = findTimer("chunk_queue.scheduling.duration");
        double avgSchedulingTime = schedulingTimer != null ? 
            schedulingTimer.mean(TimeUnit.MILLISECONDS) : 0.0;
        long totalSchedulingOps = schedulingTimer != null ? 
            schedulingTimer.count() : 0;
            
        // Task metrics
        Counter tasksAdded = findCounter("chunk_queue.tasks.added");
        Counter tasksCompleted = findCounter("chunk_queue.tasks.completed");
        long totalTasksAdded = tasksAdded != null ? (long) tasksAdded.count() : 0;
        long totalTasksCompleted = tasksCompleted != null ? (long) tasksCompleted.count() : 0;
        
        // Current state
        Gauge pendingTasks = findGauge("chunk_queue.tasks.pending");
        Gauge inFlightTasks = findGauge("chunk_queue.tasks.in_flight");
        int currentPending = pendingTasks != null ? (int) pendingTasks.value() : 0;
        int currentInFlight = inFlightTasks != null ? (int) inFlightTasks.value() : 0;
        
        return new ChunkQueueMetrics(
            avgSchedulingTime,
            totalSchedulingOps,
            totalTasksAdded,
            totalTasksCompleted,
            currentPending,
            currentInFlight
        );
    }
    
    /// Collects MAFileChannel performance metrics.
    private MAFileChannelMetrics getMAFileChannelMetrics() {
        // Read operation metrics
        Timer readTimer = findTimer("ma_file_channel.read.duration");
        double avgReadTime = readTimer != null ? 
            readTimer.mean(TimeUnit.MILLISECONDS) : 0.0;
        long totalReadOps = readTimer != null ? readTimer.count() : 0;
        
        // Read size distribution
        DistributionSummary readBytes = findDistributionSummary("ma_file_channel.read.bytes");
        double avgBytesPerRead = readBytes != null ? readBytes.mean() : 0.0;
        long totalBytesRead = readBytes != null ? (long) readBytes.totalAmount() : 0;
        
        // Prebuffer metrics
        Timer prebufferTimer = findTimer("ma_file_channel.prebuffer.duration");
        double avgPrebufferTime = prebufferTimer != null ? 
            prebufferTimer.mean(TimeUnit.MILLISECONDS) : 0.0;
        long totalPrebufferOps = prebufferTimer != null ? prebufferTimer.count() : 0;
        
        // File state
        Gauge fileSize = findGauge("ma_file_channel.file.size");
        long fileSizeBytes = fileSize != null ? (long) fileSize.value() : 0;
        
        Gauge downloadsInFlight = findGauge("ma_file_channel.downloads.in_flight");
        int currentDownloads = downloadsInFlight != null ? (int) downloadsInFlight.value() : 0;
        
        return new MAFileChannelMetrics(
            avgReadTime,
            totalReadOps,
            avgBytesPerRead,
            totalBytesRead,
            avgPrebufferTime,
            totalPrebufferOps,
            fileSizeBytes,
            currentDownloads
        );
    }
    
    /// Collects system-level metrics.
    private SystemMetrics getSystemMetrics() {
        // Get JVM metrics if available
        Gauge heapUsed = findGauge("jvm.memory.used");
        double heapUsedMB = heapUsed != null ? heapUsed.value() / (1024 * 1024) : 0.0;
        
        Gauge heapMax = findGauge("jvm.memory.max");
        double heapMaxMB = heapMax != null ? heapMax.value() / (1024 * 1024) : 0.0;
        
        return new SystemMetrics(heapUsedMB, heapMaxMB);
    }
    
    /// Utility methods for finding meters
    private Timer findTimer(String name) {
        return Search.in(meterRegistry).name(name).timer();
    }
    
    private Counter findCounter(String name) {
        return Search.in(meterRegistry).name(name).counter();
    }
    
    private Gauge findGauge(String name) {
        return Search.in(meterRegistry).name(name).gauge();
    }
    
    private DistributionSummary findDistributionSummary(String name) {
        return Search.in(meterRegistry).name(name).summary();
    }
    
    /// Comprehensive performance snapshot containing all metrics.
    public static class PerformanceSnapshot {
        private final ChunkQueueMetrics chunkQueue;
        private final MAFileChannelMetrics fileChannel;
        private final SystemMetrics system;
        
        PerformanceSnapshot(ChunkQueueMetrics chunkQueue, 
                           MAFileChannelMetrics fileChannel,
                           SystemMetrics system) {
            this.chunkQueue = chunkQueue;
            this.fileChannel = fileChannel;
            this.system = system;
        }
        
        /// Generates a human-readable performance report.
        /// 
        /// @return Formatted performance report
        public String generateReport() {
            StringBuilder report = new StringBuilder();
            
            report.append("=== MAFileChannel Performance Report ===\n\n");
            
            // ChunkQueue metrics
            report.append("ChunkQueue Performance:\n");
            report.append(String.format("  Scheduling Operations: %,d (avg: %.2f ms)\n", 
                chunkQueue.totalSchedulingOps(), chunkQueue.avgSchedulingTime()));
            report.append(String.format("  Tasks Added: %,d\n", chunkQueue.totalTasksAdded()));
            report.append(String.format("  Tasks Completed: %,d\n", chunkQueue.totalTasksCompleted()));
            report.append(String.format("  Currently Pending: %d\n", chunkQueue.currentPending()));
            report.append(String.format("  Currently In-Flight: %d\n", chunkQueue.currentInFlight()));
            
            if (chunkQueue.totalTasksAdded() > 0) {
                double completionRate = (double) chunkQueue.totalTasksCompleted() / chunkQueue.totalTasksAdded() * 100;
                report.append(String.format("  Completion Rate: %.1f%%\n", completionRate));
            }
            
            report.append("\n");
            
            // MAFileChannel metrics
            report.append("MAFileChannel Performance:\n");
            report.append(String.format("  Read Operations: %,d (avg: %.2f ms)\n", 
                fileChannel.totalReadOps(), fileChannel.avgReadTime()));
            report.append(String.format("  Total Bytes Read: %,d (%.2f MB)\n", 
                fileChannel.totalBytesRead(), fileChannel.totalBytesRead() / (1024.0 * 1024.0)));
            report.append(String.format("  Avg Bytes per Read: %.0f\n", fileChannel.avgBytesPerRead()));
            report.append(String.format("  Prebuffer Operations: %,d (avg: %.2f ms)\n", 
                fileChannel.totalPrebufferOps(), fileChannel.avgPrebufferTime()));
            report.append(String.format("  File Size: %.2f MB\n", 
                fileChannel.fileSizeBytes() / (1024.0 * 1024.0)));
            report.append(String.format("  Active Downloads: %d\n", fileChannel.currentDownloads()));
            
            if (fileChannel.totalReadOps() > 0 && fileChannel.avgReadTime() > 0) {
                double throughputMBps = (fileChannel.totalBytesRead() / (1024.0 * 1024.0)) / 
                    ((fileChannel.totalReadOps() * fileChannel.avgReadTime()) / 1000.0);
                report.append(String.format("  Read Throughput: %.2f MB/s\n", throughputMBps));
            }
            
            report.append("\n");
            
            // System metrics
            if (system.heapMaxMB() > 0) {
                report.append("System Performance:\n");
                report.append(String.format("  Heap Usage: %.1f MB / %.1f MB (%.1f%%)\n", 
                    system.heapUsedMB(), system.heapMaxMB(), 
                    system.heapUsedMB() / system.heapMaxMB() * 100));
                report.append("\n");
            }
            
            // Performance insights
            report.append("Performance Insights:\n");
            
            if (chunkQueue.avgSchedulingTime() > 10) {
                report.append("  ⚠️  High scheduling latency detected (>10ms)\n");
            }
            
            if (chunkQueue.currentPending() > 1000) {
                report.append("  ⚠️  High task queue backlog detected (>1000 tasks)\n");
            }
            
            if (fileChannel.avgReadTime() > 100) {
                report.append("  ⚠️  Slow read operations detected (>100ms)\n");
            }
            
            if (system.heapMaxMB() > 0 && (system.heapUsedMB() / system.heapMaxMB()) > 0.8) {
                report.append("  ⚠️  High memory usage detected (>80%)\n");
            }
            
            double taskBacklogRatio = chunkQueue.totalTasksAdded() > 0 ? 
                (double) chunkQueue.currentPending() / chunkQueue.totalTasksAdded() : 0;
            if (taskBacklogRatio > 0.1) {
                report.append("  ⚠️  Significant task backlog detected\n");
            }
            
            if (fileChannel.currentDownloads() == 0 && chunkQueue.currentPending() > 0) {
                report.append("  ⚠️  Tasks pending but no downloads active\n");
            }
            
            report.append("\n=== End Report ===");
            
            return report.toString();
        }
        
        /// Gets ChunkQueue metrics.
        /// @return ChunkQueue performance metrics
        public ChunkQueueMetrics getChunkQueueMetrics() { return chunkQueue; }
        
        /// Gets MAFileChannel metrics.
        /// @return MAFileChannel performance metrics
        public MAFileChannelMetrics getFileChannelMetrics() { return fileChannel; }
        
        /// Gets system metrics.
        /// @return System performance metrics
        public SystemMetrics getSystemMetrics() { return system; }
    }
    
    /// ChunkQueue performance metrics.
    public static class ChunkQueueMetrics {
        private final double avgSchedulingTime;
        private final long totalSchedulingOps;
        private final long totalTasksAdded;
        private final long totalTasksCompleted;
        private final int currentPending;
        private final int currentInFlight;
        
        /// Constructs ChunkQueueMetrics with the given values.
        ///
        /// @param avgSchedulingTime   average scheduling time in milliseconds
        /// @param totalSchedulingOps  total number of scheduling operations
        /// @param totalTasksAdded     total tasks added
        /// @param totalTasksCompleted total tasks completed
        /// @param currentPending      current number of pending tasks
        /// @param currentInFlight     current number of in-flight tasks
        public ChunkQueueMetrics(double avgSchedulingTime, long totalSchedulingOps, long totalTasksAdded,
                                long totalTasksCompleted, int currentPending, int currentInFlight) {
            this.avgSchedulingTime = avgSchedulingTime;
            this.totalSchedulingOps = totalSchedulingOps;
            this.totalTasksAdded = totalTasksAdded;
            this.totalTasksCompleted = totalTasksCompleted;
            this.currentPending = currentPending;
            this.currentInFlight = currentInFlight;
        }
        
        /// Returns the average scheduling time.
        /// @return the average scheduling time in milliseconds
        public double avgSchedulingTime() { return avgSchedulingTime; }
        /// Returns the total scheduling operations count.
        /// @return the total number of scheduling operations
        public long totalSchedulingOps() { return totalSchedulingOps; }
        /// Returns the total tasks added.
        /// @return the total number of tasks added
        public long totalTasksAdded() { return totalTasksAdded; }
        /// Returns the total tasks completed.
        /// @return the total number of tasks completed
        public long totalTasksCompleted() { return totalTasksCompleted; }
        /// Returns the current pending count.
        /// @return the number of currently pending tasks
        public int currentPending() { return currentPending; }
        /// Returns the current in-flight count.
        /// @return the number of currently in-flight tasks
        public int currentInFlight() { return currentInFlight; }
    }
    
    /// MAFileChannel performance metrics.
    public static class MAFileChannelMetrics {
        private final double avgReadTime;
        private final long totalReadOps;
        private final double avgBytesPerRead;
        private final long totalBytesRead;
        private final double avgPrebufferTime;
        private final long totalPrebufferOps;
        private final long fileSizeBytes;
        private final int currentDownloads;
        
        /// Constructs MAFileChannelMetrics with the given values.
        ///
        /// @param avgReadTime       average read time in milliseconds
        /// @param totalReadOps      total number of read operations
        /// @param avgBytesPerRead   average bytes per read
        /// @param totalBytesRead    total bytes read
        /// @param avgPrebufferTime  average prebuffer time in milliseconds
        /// @param totalPrebufferOps total number of prebuffer operations
        /// @param fileSizeBytes     the file size in bytes
        /// @param currentDownloads  current number of active downloads
        public MAFileChannelMetrics(double avgReadTime, long totalReadOps, double avgBytesPerRead,
                                   long totalBytesRead, double avgPrebufferTime, long totalPrebufferOps,
                                   long fileSizeBytes, int currentDownloads) {
            this.avgReadTime = avgReadTime;
            this.totalReadOps = totalReadOps;
            this.avgBytesPerRead = avgBytesPerRead;
            this.totalBytesRead = totalBytesRead;
            this.avgPrebufferTime = avgPrebufferTime;
            this.totalPrebufferOps = totalPrebufferOps;
            this.fileSizeBytes = fileSizeBytes;
            this.currentDownloads = currentDownloads;
        }
        
        /// Returns the average read time.
        /// @return the average read time in milliseconds
        public double avgReadTime() { return avgReadTime; }
        /// Returns the total read operations count.
        /// @return the total number of read operations
        public long totalReadOps() { return totalReadOps; }
        /// Returns the average bytes per read.
        /// @return the average bytes per read operation
        public double avgBytesPerRead() { return avgBytesPerRead; }
        /// Returns the total bytes read.
        /// @return the total number of bytes read
        public long totalBytesRead() { return totalBytesRead; }
        /// Returns the average prebuffer time.
        /// @return the average prebuffer time in milliseconds
        public double avgPrebufferTime() { return avgPrebufferTime; }
        /// Returns the total prebuffer operations count.
        /// @return the total number of prebuffer operations
        public long totalPrebufferOps() { return totalPrebufferOps; }
        /// Returns the file size.
        /// @return the file size in bytes
        public long fileSizeBytes() { return fileSizeBytes; }
        /// Returns the current download count.
        /// @return the number of active downloads
        public int currentDownloads() { return currentDownloads; }
    }
    
    /// System performance metrics.
    public static class SystemMetrics {
        private final double heapUsedMB;
        private final double heapMaxMB;
        
        /// Constructs SystemMetrics with the given heap values.
        ///
        /// @param heapUsedMB the used heap in megabytes
        /// @param heapMaxMB  the maximum heap in megabytes
        public SystemMetrics(double heapUsedMB, double heapMaxMB) {
            this.heapUsedMB = heapUsedMB;
            this.heapMaxMB = heapMaxMB;
        }
        
        /// Returns the used heap size.
        /// @return the used heap in megabytes
        public double heapUsedMB() { return heapUsedMB; }
        /// Returns the maximum heap size.
        /// @return the maximum heap in megabytes
        public double heapMaxMB() { return heapMaxMB; }
    }
}