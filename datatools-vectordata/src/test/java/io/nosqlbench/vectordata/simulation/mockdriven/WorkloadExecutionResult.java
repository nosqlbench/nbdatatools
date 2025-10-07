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
import java.util.List;

/// Results from executing a workload against a scheduler.
/// 
/// This class captures key metrics about how the scheduler performed
/// when executing a specific workload pattern, including download
/// efficiency and timing information.
public class WorkloadExecutionResult {
    
    private final long totalBytesRequested;
    private final int totalDownloads;
    private final List<Duration> downloadTimes;
    
    /// Creates a workload execution result.
    /// 
    /// @param totalBytesRequested Total bytes requested for download
    /// @param totalDownloads Number of individual download operations
    /// @param downloadTimes List of durations for each download
    public WorkloadExecutionResult(long totalBytesRequested, int totalDownloads, 
                                  List<Duration> downloadTimes) {
        this.totalBytesRequested = totalBytesRequested;
        this.totalDownloads = totalDownloads;
        this.downloadTimes = downloadTimes;
    }
    
    /// Gets the total bytes requested for download.
    /// 
    /// @return Total bytes requested
    public long getTotalBytesRequested() {
        return totalBytesRequested;
    }
    
    /// Gets the number of individual download operations.
    /// 
    /// @return Download operation count
    public int getTotalDownloads() {
        return totalDownloads;
    }
    
    /// Gets the list of download times.
    /// 
    /// @return List of durations for each download
    public List<Duration> getDownloadTimes() {
        return downloadTimes;
    }
    
    /// Gets the average download time.
    /// 
    /// @return Average duration per download
    public Duration getAverageDownloadTime() {
        if (downloadTimes.isEmpty()) {
            return Duration.ZERO;
        }
        
        long totalNanos = downloadTimes.stream()
            .mapToLong(Duration::toNanos)
            .sum();
        
        return Duration.ofNanos(totalNanos / downloadTimes.size());
    }
    
    /// Gets the total time spent on downloads.
    /// 
    /// @return Sum of all download times
    public Duration getTotalDownloadTime() {
        return downloadTimes.stream()
            .reduce(Duration.ZERO, Duration::plus);
    }
    
    /// Gets the average bytes per download.
    /// 
    /// @return Average bytes per download operation
    public double getAverageBytesPerDownload() {
        return totalDownloads > 0 ? (double) totalBytesRequested / totalDownloads : 0.0;
    }
}