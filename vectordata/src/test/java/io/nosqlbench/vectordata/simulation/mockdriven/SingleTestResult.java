package io.nosqlbench.vectordata.simulation.mockdriven;

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

import java.time.Duration;

/// Results from a single test iteration.
/// 
/// This class captures comprehensive performance metrics from running
/// a scheduler with a specific workload under specific network conditions
/// for one iteration of a performance test.
public class SingleTestResult {
    
    private final String schedulerName;
    private final String networkCondition;
    private final String workloadName;
    private final int iteration;
    private final Duration totalTime;
    private final WorkloadExecutionResult executionResult;
    private final TransportStatistics transportStats;
    private final int finalValidChunks;
    private final int totalChunks;
    
    /// Creates a single test result.
    /// 
    /// @param schedulerName Name of the scheduler that was tested
    /// @param networkCondition Description of network conditions
    /// @param workloadName Name of the workload pattern
    /// @param iteration Iteration number (0-based)
    /// @param totalTime Total wall-clock time for the test
    /// @param executionResult Results from workload execution
    /// @param transportStats Statistics from the transport client
    /// @param finalValidChunks Number of valid chunks at test end
    /// @param totalChunks Total number of chunks in the file
    public SingleTestResult(String schedulerName, String networkCondition, String workloadName,
                           int iteration, Duration totalTime, WorkloadExecutionResult executionResult,
                           TransportStatistics transportStats, int finalValidChunks, int totalChunks) {
        this.schedulerName = schedulerName;
        this.networkCondition = networkCondition;
        this.workloadName = workloadName;
        this.iteration = iteration;
        this.totalTime = totalTime;
        this.executionResult = executionResult;
        this.transportStats = transportStats;
        this.finalValidChunks = finalValidChunks;
        this.totalChunks = totalChunks;
    }
    
    /// Gets the name of the scheduler that was tested.
    public String getSchedulerName() { return schedulerName; }
    
    /// Gets the description of network conditions.
    public String getNetworkCondition() { return networkCondition; }
    
    /// Gets the name of the workload pattern.
    public String getWorkloadName() { return workloadName; }
    
    /// Gets the iteration number.
    public int getIteration() { return iteration; }
    
    /// Gets the total wall-clock time for the test.
    public Duration getTotalTime() { return totalTime; }
    
    /// Gets the workload execution results.
    public WorkloadExecutionResult getExecutionResult() { return executionResult; }
    
    /// Gets the transport statistics.
    public TransportStatistics getTransportStats() { return transportStats; }
    
    /// Gets the number of valid chunks at test end.
    public int getFinalValidChunks() { return finalValidChunks; }
    
    /// Gets the total number of chunks in the file.
    public int getTotalChunks() { return totalChunks; }
    
    /// Gets the completion percentage at test end.
    /// 
    /// @return Percentage of chunks that were valid (0.0 to 100.0)
    public double getCompletionPercentage() {
        return totalChunks > 0 ? (finalValidChunks * 100.0) / totalChunks : 100.0;
    }
    
    /// Gets the download efficiency (bytes transferred vs bytes requested).
    /// 
    /// Lower values indicate better efficiency (less redundant downloading).
    /// 
    /// @return Ratio of bytes transferred to bytes requested
    public double getDownloadEfficiency() {
        long requested = executionResult.getTotalBytesRequested();
        long transferred = transportStats.getTotalBytesTransferred();
        return requested > 0 ? (double) transferred / requested : 1.0;
    }
    
    /// Gets the overall throughput in bytes per second.
    /// 
    /// @return Throughput based on total time and bytes transferred
    public double getOverallThroughput() {
        long nanos = totalTime.toNanos();
        if (nanos == 0) return 0.0;
        return (transportStats.getTotalBytesTransferred() * 1_000_000_000.0) / nanos;
    }
    
    /// Checks if this test result indicates good performance.
    /// 
    /// This provides a simple assessment based on completion rate,
    /// efficiency, and transport performance.
    /// 
    /// @return true if performance is considered good
    public boolean isGoodPerformance() {
        return getCompletionPercentage() >= 95.0 && // Most chunks downloaded
               getDownloadEfficiency() <= 1.5 && // Not too much over-downloading
               transportStats.isPerformanceAcceptable(); // Transport performed well
    }
    
    @Override
    public String toString() {
        return String.format("%s on %s with %s (iteration %d): %.2f%% complete, %.2f efficiency, %s",
                           schedulerName, networkCondition, workloadName, iteration,
                           getCompletionPercentage(), getDownloadEfficiency(),
                           totalTime);
    }
}