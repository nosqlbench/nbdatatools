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

/// Performance statistics for transport client operations.
/// 
/// This class captures key performance metrics during simulation testing,
/// providing insights into how different scheduling strategies perform
/// under various network conditions.
/// 
/// Statistics include:
/// - Data transfer metrics (bytes transferred, throughput)
/// - Request metrics (total requests, failure rate)
/// - Timing information (total time, average request time)
/// - Network condition context
/// 
/// Example usage:
/// ```java
/// TransportStatistics stats = client.getStatistics();
/// System.out.printf("Transferred %d bytes in %s (%.2f MB/s)%n",
///     stats.getTotalBytesTransferred(),
///     stats.getTotalTime(),
///     stats.getThroughputMBps());
/// ```
public class TransportStatistics {
    
    private final long totalBytesTransferred;
    private final long totalRequests;
    private final long failedRequests;
    private final double throughputBps;
    private final Duration totalTime;
    private final NetworkConditions networkConditions;
    
    /// Creates transport statistics with the specified metrics.
    /// 
    /// @param totalBytesTransferred Total bytes successfully transferred
    /// @param totalRequests Total number of requests made
    /// @param failedRequests Number of requests that failed
    /// @param throughputBps Actual throughput in bytes per second
    /// @param totalTime Total time spent on operations
    /// @param networkConditions The network conditions during testing
    public TransportStatistics(long totalBytesTransferred, long totalRequests, long failedRequests,
                             double throughputBps, Duration totalTime, NetworkConditions networkConditions) {
        this.totalBytesTransferred = totalBytesTransferred;
        this.totalRequests = totalRequests;
        this.failedRequests = failedRequests;
        this.throughputBps = throughputBps;
        this.totalTime = totalTime;
        this.networkConditions = networkConditions;
    }
    
    /// Gets the total bytes successfully transferred.
    /// 
    /// @return Total bytes transferred
    public long getTotalBytesTransferred() {
        return totalBytesTransferred;
    }
    
    /// Gets the total number of requests made.
    /// 
    /// @return Total request count
    public long getTotalRequests() {
        return totalRequests;
    }
    
    /// Gets the number of requests that failed.
    /// 
    /// @return Failed request count
    public long getFailedRequests() {
        return failedRequests;
    }
    
    /// Gets the number of requests that succeeded.
    /// 
    /// @return Successful request count
    public long getSuccessfulRequests() {
        return totalRequests - failedRequests;
    }
    
    /// Gets the request success rate as a percentage.
    /// 
    /// @return Success rate (0.0 to 100.0)
    public double getSuccessRate() {
        return totalRequests > 0 ? (double) getSuccessfulRequests() * 100.0 / totalRequests : 100.0;
    }
    
    /// Gets the request failure rate as a percentage.
    /// 
    /// @return Failure rate (0.0 to 100.0)
    public double getFailureRate() {
        return 100.0 - getSuccessRate();
    }
    
    /// Gets the actual throughput in bytes per second.
    /// 
    /// @return Throughput in bytes per second
    public double getThroughputBps() {
        return throughputBps;
    }
    
    /// Gets the actual throughput in megabytes per second.
    /// 
    /// @return Throughput in MB/s
    public double getThroughputMBps() {
        return throughputBps / (1024 * 1024);
    }
    
    /// Gets the actual throughput in megabits per second.
    /// 
    /// @return Throughput in Mbps
    public double getThroughputMbps() {
        return (throughputBps * 8.0) / 1_000_000;
    }
    
    /// Gets the total time spent on operations.
    /// 
    /// @return Total time duration
    public Duration getTotalTime() {
        return totalTime;
    }
    
    /// Gets the average time per request.
    /// 
    /// @return Average request duration
    public Duration getAverageRequestTime() {
        if (totalRequests == 0) {
            return Duration.ZERO;
        }
        return Duration.ofNanos(totalTime.toNanos() / totalRequests);
    }
    
    /// Gets the average bytes per request.
    /// 
    /// @return Average bytes per request
    public double getAverageBytesPerRequest() {
        return totalRequests > 0 ? (double) totalBytesTransferred / totalRequests : 0.0;
    }
    
    /// Gets the network conditions that were simulated.
    /// 
    /// @return Network conditions
    public NetworkConditions getNetworkConditions() {
        return networkConditions;
    }
    
    /// Gets the bandwidth utilization as a percentage.
    /// 
    /// This compares the actual throughput to the available bandwidth.
    /// 
    /// @return Bandwidth utilization percentage (0.0 to 100.0+)
    public double getBandwidthUtilization() {
        long availableBandwidth = networkConditions.getBandwidthBps();
        if (availableBandwidth <= 0) {
            return 0.0; // Unlimited bandwidth
        }
        return (throughputBps * 100.0) / availableBandwidth;
    }
    
    /// Checks if the performance met expectations based on network conditions.
    /// 
    /// This provides a simple assessment of whether the transport performed
    /// reasonably well given the network constraints.
    /// 
    /// @return true if performance is considered acceptable
    public boolean isPerformanceAcceptable() {
        // Consider performance acceptable if:
        // 1. Success rate is above 90%
        // 2. Bandwidth utilization is above 70% (when bandwidth is limited)
        // 3. No major inefficiencies detected
        
        if (getSuccessRate() < 90.0) {
            return false; // Too many failures
        }
        
        long availableBandwidth = networkConditions.getBandwidthBps();
        if (availableBandwidth > 0 && getBandwidthUtilization() < 70.0) {
            return false; // Poor bandwidth utilization
        }
        
        return true;
    }
    
    /// Creates a formatted summary of the statistics.
    /// 
    /// @return Multi-line summary string
    public String getSummary() {
        StringBuilder sb = new StringBuilder();
        sb.append("Transport Performance Summary:\n");
        sb.append(String.format("  Network: %s\n", networkConditions.getDescription()));
        sb.append(String.format("  Data Transferred: %.2f MB (%d bytes)\n", 
                               totalBytesTransferred / (1024.0 * 1024.0), totalBytesTransferred));
        sb.append(String.format("  Requests: %d total, %d successful (%.1f%% success rate)\n",
                               totalRequests, getSuccessfulRequests(), getSuccessRate()));
        sb.append(String.format("  Time: %d ms total, %.1f ms average per request\n",
                               totalTime.toMillis(), getAverageRequestTime().toNanos() / 1_000_000.0));
        sb.append(String.format("  Throughput: %.2f MB/s (%.1f Mbps)\n",
                               getThroughputMBps(), getThroughputMbps()));
        
        if (networkConditions.getBandwidthBps() > 0) {
            sb.append(String.format("  Bandwidth Utilization: %.1f%%\n", getBandwidthUtilization()));
        }
        
        sb.append(String.format("  Performance Assessment: %s\n", 
                               isPerformanceAcceptable() ? "Acceptable" : "Needs Improvement"));
        
        return sb.toString();
    }
    
    @Override
    public String toString() {
        return String.format("TransportStatistics{%d bytes, %d requests, %.2f MB/s, %.1f%% success}",
                           totalBytesTransferred, totalRequests, getThroughputMBps(), getSuccessRate());
    }
}