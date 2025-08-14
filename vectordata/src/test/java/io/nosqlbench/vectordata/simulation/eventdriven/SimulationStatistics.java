package io.nosqlbench.vectordata.simulation.eventdriven;

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

import java.util.*;

/// Collects and aggregates statistics during event-driven simulation.
/// 
/// This class tracks various performance metrics including request rates,
/// download efficiency, bandwidth utilization, and timing characteristics.
/// All statistics are computed in simulation time rather than real time.
public class SimulationStatistics {
    
    // Request statistics
    private int totalRequests = 0;
    private long totalBytesRequested = 0;
    private final List<Double> requestTimes = new ArrayList<>();
    private final Map<Long, Double> requestStartTimes = new HashMap<>(); // requestId -> startTime
    private final List<Double> requestLatencies = new ArrayList<>(); // End-to-end request latencies
    
    // Download statistics
    private int totalDownloadsScheduled = 0;
    private int totalDownloadsCompleted = 0;
    private int totalDownloadsFailed = 0;
    private long totalBytesDownloaded = 0;
    private final List<Double> downloadDurations = new ArrayList<>();
    private final List<Double> downloadThroughputs = new ArrayList<>();
    
    // Timing statistics
    private double simulationStartTime = 0.0;
    private double simulationEndTime = 0.0;
    private double firstRequestTime = Double.MAX_VALUE;
    private double lastRequestTime = 0.0;
    
    // Chunk tracking
    private final Set<Integer> downloadedChunks = new HashSet<>();
    private final Set<Integer> requestedChunks = new HashSet<>();
    
    /// Records a read request.
    /// 
    /// @param offset Byte offset of the request
    /// @param length Number of bytes requested
    /// @param time Simulation time when request occurred
    public void recordReadRequest(long offset, int length, double time) {
        totalRequests++;
        totalBytesRequested += length;
        requestTimes.add(time);
        
        firstRequestTime = Math.min(firstRequestTime, time);
        lastRequestTime = Math.max(lastRequestTime, time);
        
        // Generate unique request ID and track start time for latency measurement
        long requestId = generateRequestId(offset, length, time);
        requestStartTimes.put(requestId, time);
        
        // Track which chunks were requested (approximate)
        // This is a simplified calculation - real implementation might be more precise
        int startChunk = (int) (offset / 65536); // Assume 64KB chunks
        int endChunk = (int) ((offset + length - 1) / 65536);
        for (int chunk = startChunk; chunk <= endChunk; chunk++) {
            requestedChunks.add(chunk);
        }
    }
    
    /// Records request completion for latency measurement.
    /// 
    /// @param offset Byte offset of the completed request
    /// @param length Number of bytes in the completed request
    /// @param startTime Original request start time
    /// @param completionTime Time when request was fully satisfied
    public void recordRequestCompletion(long offset, int length, double startTime, double completionTime) {
        double latency = completionTime - startTime;
        requestLatencies.add(latency);
        
        // Clean up tracking data
        long requestId = generateRequestId(offset, length, startTime);
        requestStartTimes.remove(requestId);
    }
    
    /// Generates a unique request ID for tracking.
    /// 
    /// @param offset Byte offset
    /// @param length Request length
    /// @param time Request time (used for uniqueness)
    /// @return Unique request identifier
    private long generateRequestId(long offset, int length, double time) {
        // Simple hash combining offset, length, and time
        long timeMillis = (long) (time * 1000000); // Convert to microseconds for uniqueness
        return offset ^ (((long) length) << 32) ^ timeMillis;
    }
    
    /// Records a scheduled download.
    /// 
    /// @param chunkIndex Index of the chunk being downloaded
    /// @param time Simulation time when download was scheduled
    public void recordDownloadScheduled(int chunkIndex, double time) {
        totalDownloadsScheduled++;
    }
    
    /// Records the start of a download.
    /// 
    /// @param chunkIndex Index of the chunk being downloaded
    /// @param startTime Simulation time when download started
    /// @param byteSize Size of the download in bytes
    public void recordDownloadStart(int chunkIndex, double startTime, long byteSize) {
        // Download start is tracked but main metrics come from completion
    }
    
    /// Records a completed download.
    /// 
    /// @param chunkIndex Index of the downloaded chunk
    /// @param completionTime Simulation time when download completed
    /// @param bytesTransferred Number of bytes transferred
    /// @param duration Duration of the download in seconds
    public void recordDownloadComplete(int chunkIndex, double completionTime, 
                                     long bytesTransferred, double duration) {
        totalDownloadsCompleted++;
        totalBytesDownloaded += bytesTransferred;
        downloadedChunks.add(chunkIndex);
        
        downloadDurations.add(duration);
        
        if (duration > 0) {
            double throughput = bytesTransferred / duration; // bytes per second
            downloadThroughputs.add(throughput);
        }
    }
    
    /// Records a failed download.
    /// 
    /// @param chunkIndex Index of the chunk that failed to download
    /// @param failureTime Simulation time when failure occurred
    /// @param reason Reason for the failure
    public void recordDownloadFailed(int chunkIndex, double failureTime, String reason) {
        totalDownloadsFailed++;
    }
    
    /// Records simulation start time.
    /// 
    /// @param startTime Simulation start time
    public void recordSimulationStart(double startTime) {
        this.simulationStartTime = startTime;
    }
    
    /// Records simulation end time.
    /// 
    /// @param endTime Simulation end time
    public void recordSimulationEnd(double endTime) {
        this.simulationEndTime = endTime;
    }
    
    /// Resets all statistics.
    public void reset() {
        totalRequests = 0;
        totalBytesRequested = 0;
        requestTimes.clear();
        requestStartTimes.clear();
        requestLatencies.clear();
        
        totalDownloadsScheduled = 0;
        totalDownloadsCompleted = 0;
        totalDownloadsFailed = 0;
        totalBytesDownloaded = 0;
        downloadDurations.clear();
        downloadThroughputs.clear();
        
        simulationStartTime = 0.0;
        simulationEndTime = 0.0;
        firstRequestTime = Double.MAX_VALUE;
        lastRequestTime = 0.0;
        
        downloadedChunks.clear();
        requestedChunks.clear();
    }
    
    /// Gets the total simulation duration.
    /// 
    /// @return Simulation duration in seconds
    public double getSimulationDuration() {
        return simulationEndTime - simulationStartTime;
    }
    
    /// Gets the request rate (requests per second).
    /// 
    /// @return Average request rate
    public double getRequestRate() {
        double duration = getSimulationDuration();
        return duration > 0 ? totalRequests / duration : 0.0;
    }
    
    /// Gets the download completion rate.
    /// 
    /// @return Completion rate as a fraction (0.0 to 1.0)
    public double getDownloadCompletionRate() {
        return totalDownloadsScheduled > 0 ? 
               (double) totalDownloadsCompleted / totalDownloadsScheduled : 0.0;
    }
    
    /// Gets the download failure rate.
    /// 
    /// @return Failure rate as a fraction (0.0 to 1.0)
    public double getDownloadFailureRate() {
        return totalDownloadsScheduled > 0 ? 
               (double) totalDownloadsFailed / totalDownloadsScheduled : 0.0;
    }
    
    /// Gets the average download throughput.
    /// 
    /// @return Average throughput in bytes per second
    public double getAverageThroughput() {
        return downloadThroughputs.isEmpty() ? 0.0 :
               downloadThroughputs.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
    }
    
    /// Gets the average download duration.
    /// 
    /// @return Average duration in seconds
    public double getAverageDownloadDuration() {
        return downloadDurations.isEmpty() ? 0.0 :
               downloadDurations.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
    }
    
    /// Gets the bandwidth utilization efficiency.
    /// 
    /// This compares actual bytes downloaded to theoretical maximum
    /// based on available bandwidth and simulation duration.
    /// 
    /// @param networkBandwidthBps Available network bandwidth in bytes per second
    /// @return Utilization efficiency as a fraction (0.0 to 1.0)
    public double getBandwidthUtilization(long networkBandwidthBps) {
        double duration = getSimulationDuration();
        if (duration <= 0 || networkBandwidthBps <= 0) {
            return 0.0;
        }
        
        long theoreticalMaxBytes = (long) (networkBandwidthBps * duration);
        return Math.min(1.0, (double) totalBytesDownloaded / theoreticalMaxBytes);
    }
    
    /// Gets the cache hit rate (chunks needed vs. chunks downloaded).
    /// 
    /// @return Hit rate as a fraction (0.0 to 1.0)
    public double getCacheHitRate() {
        if (downloadedChunks.isEmpty()) {
            return 0.0;
        }
        
        // Count how many downloaded chunks were actually requested
        Set<Integer> intersection = new HashSet<>(downloadedChunks);
        intersection.retainAll(requestedChunks);
        
        return (double) intersection.size() / downloadedChunks.size();
    }
    
    /// Gets the average request latency.
    /// 
    /// @return Average end-to-end request latency in seconds
    public double getAverageRequestLatency() {
        return requestLatencies.isEmpty() ? 0.0 :
               requestLatencies.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
    }
    
    /// Gets the median request latency.
    /// 
    /// @return Median request latency in seconds
    public double getMedianRequestLatency() {
        if (requestLatencies.isEmpty()) {
            return 0.0;
        }
        
        List<Double> sorted = new ArrayList<>(requestLatencies);
        sorted.sort(Double::compareTo);
        
        int size = sorted.size();
        if (size % 2 == 0) {
            return (sorted.get(size / 2 - 1) + sorted.get(size / 2)) / 2.0;
        } else {
            return sorted.get(size / 2);
        }
    }
    
    /// Gets the 95th percentile request latency.
    /// 
    /// @return 95th percentile request latency in seconds
    public double getP95RequestLatency() {
        if (requestLatencies.isEmpty()) {
            return 0.0;
        }
        
        List<Double> sorted = new ArrayList<>(requestLatencies);
        sorted.sort(Double::compareTo);
        
        int index = (int) Math.ceil(sorted.size() * 0.95) - 1;
        index = Math.max(0, Math.min(index, sorted.size() - 1));
        
        return sorted.get(index);
    }
    
    /// Gets the maximum request latency.
    /// 
    /// @return Maximum request latency in seconds
    public double getMaxRequestLatency() {
        return requestLatencies.isEmpty() ? 0.0 :
               requestLatencies.stream().mapToDouble(Double::doubleValue).max().orElse(0.0);
    }
    
    /// Gets a summary of key metrics.
    /// 
    /// @return Statistics summary
    public StatisticsSummary getSummary() {
        return new StatisticsSummary(
            totalRequests,
            totalDownloadsCompleted,
            totalDownloadsFailed,
            getDownloadCompletionRate(),
            getAverageThroughput(),
            getAverageDownloadDuration(),
            getCacheHitRate(),
            getAverageRequestLatency(),
            getMedianRequestLatency(),
            getP95RequestLatency(),
            getSimulationDuration()
        );
    }
    
    /// Summary of key simulation statistics.
    public static class StatisticsSummary {
        private final int totalRequests;
        private final int completedDownloads;
        private final int failedDownloads;
        private final double completionRate;
        private final double averageThroughput;
        private final double averageDuration;
        private final double cacheHitRate;
        private final double averageRequestLatency;
        private final double medianRequestLatency;
        private final double p95RequestLatency;
        private final double simulationDuration;
        
        public StatisticsSummary(int totalRequests, int completedDownloads, int failedDownloads,
                               double completionRate, double averageThroughput, double averageDuration,
                               double cacheHitRate, double averageRequestLatency, 
                               double medianRequestLatency, double p95RequestLatency,
                               double simulationDuration) {
            this.totalRequests = totalRequests;
            this.completedDownloads = completedDownloads;
            this.failedDownloads = failedDownloads;
            this.completionRate = completionRate;
            this.averageThroughput = averageThroughput;
            this.averageDuration = averageDuration;
            this.cacheHitRate = cacheHitRate;
            this.averageRequestLatency = averageRequestLatency;
            this.medianRequestLatency = medianRequestLatency;
            this.p95RequestLatency = p95RequestLatency;
            this.simulationDuration = simulationDuration;
        }
        
        public int getTotalRequests() { return totalRequests; }
        public int getCompletedDownloads() { return completedDownloads; }
        public int getFailedDownloads() { return failedDownloads; }
        public double getCompletionRate() { return completionRate; }
        public double getAverageThroughput() { return averageThroughput; }
        public double getAverageDuration() { return averageDuration; }
        public double getCacheHitRate() { return cacheHitRate; }
        public double getAverageRequestLatency() { return averageRequestLatency; }
        public double getMedianRequestLatency() { return medianRequestLatency; }
        public double getP95RequestLatency() { return p95RequestLatency; }
        public double getSimulationDuration() { return simulationDuration; }
        
        @Override
        public String toString() {
            return String.format(
                "Requests: %d, Downloads: %d/%d (%.1f%%), " +
                "Throughput: %.1f KB/s, Duration: %.3fs, Hit Rate: %.1f%%, " +
                "Latency: avg=%.3fs/med=%.3fs/p95=%.3fs",
                totalRequests, completedDownloads, completedDownloads + failedDownloads,
                completionRate * 100, averageThroughput / 1024, averageDuration, cacheHitRate * 100,
                averageRequestLatency, medianRequestLatency, p95RequestLatency
            );
        }
    }
}