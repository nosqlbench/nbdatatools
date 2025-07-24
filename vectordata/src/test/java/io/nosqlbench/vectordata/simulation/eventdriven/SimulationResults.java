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

/// Results from an event-driven simulation run.
/// 
/// This class encapsulates the outcomes of a simulation run, including
/// performance statistics, configuration details, and derived metrics.
/// It provides methods for analyzing and comparing simulation results.
public class SimulationResults {
    
    private final SimulationStatistics statistics;
    private final String schedulerName;
    private final String networkModelName;
    private final String workloadName;
    private final double compositeScore;
    
    /// Creates simulation results.
    /// 
    /// @param statistics The collected simulation statistics
    /// @param schedulerName Name of the scheduler that was tested
    /// @param networkModelName Name of the network model used
    /// @param workloadName Name of the workload pattern used
    public SimulationResults(SimulationStatistics statistics, String schedulerName,
                           String networkModelName, String workloadName) {
        this.statistics = statistics;
        this.schedulerName = schedulerName;
        this.networkModelName = networkModelName;
        this.workloadName = workloadName;
        this.compositeScore = calculateCompositeScore();
    }
    
    /// Gets the simulation statistics.
    /// 
    /// @return Simulation statistics
    public SimulationStatistics getStatistics() {
        return statistics;
    }
    
    /// Gets the scheduler name.
    /// 
    /// @return Scheduler name
    public String getSchedulerName() {
        return schedulerName;
    }
    
    /// Gets the network model name.
    /// 
    /// @return Network model name
    public String getNetworkModelName() {
        return networkModelName;
    }
    
    /// Gets the workload name.
    /// 
    /// @return Workload name
    public String getWorkloadName() {
        return workloadName;
    }
    
    /// Gets the composite performance score.
    /// 
    /// This score combines multiple metrics into a single value for
    /// easy comparison between different configurations.
    /// 
    /// @return Composite score (higher is better)
    public double getCompositeScore() {
        return compositeScore;
    }
    
    /// Calculates a composite performance score.
    /// 
    /// The score combines completion rate, throughput, cache efficiency, and 
    /// request latency with appropriate weightings to reflect user experience.
    /// 
    /// @return Composite score (0.0 to 100.0, higher is better)
    private double calculateCompositeScore() {
        SimulationStatistics.StatisticsSummary summary = statistics.getSummary();
        
        // Completion rate: 35% weight (most important)
        double completionScore = summary.getCompletionRate() * 35.0;
        
        // Request latency: 25% weight (critical for user experience)
        // Lower latency is better - normalize to 0-1 range
        double latencyScore = 25.0;
        if (summary.getAverageRequestLatency() > 0) {
            // Penalize high latency exponentially (latency > 1s becomes very bad)
            double latencyPenalty = Math.min(1.0, summary.getAverageRequestLatency() / 1.0);
            latencyScore *= Math.max(0.1, 1.0 - (latencyPenalty * latencyPenalty));
        }
        
        // P95 latency consideration: Additional penalty for poor tail latency
        if (summary.getP95RequestLatency() > 2.0 * summary.getAverageRequestLatency()) {
            latencyScore *= 0.8; // 20% penalty for high tail latency
        }
        
        // Throughput efficiency: 20% weight
        // Normalize throughput to 0-1 range (assume max useful throughput is 100 MB/s)
        double maxThroughput = 100.0 * 1024 * 1024; // 100 MB/s
        double throughputScore = Math.min(1.0, summary.getAverageThroughput() / maxThroughput) * 20.0;
        
        // Cache hit rate: 15% weight
        double cacheScore = summary.getCacheHitRate() * 15.0;
        
        // Speed bonus: 5% weight (faster downloads are better)
        // Penalize very slow downloads
        double speedScore = 5.0;
        if (summary.getAverageDuration() > 1.0) {
            speedScore *= Math.max(0.1, 1.0 / summary.getAverageDuration());
        }
        
        return completionScore + latencyScore + throughputScore + cacheScore + speedScore;
    }
    
    /// Prints detailed results to console.
    public void printDetailedResults() {
        SimulationStatistics.StatisticsSummary summary = statistics.getSummary();
        
        System.out.println("=".repeat(80));
        System.out.println("EVENT-DRIVEN SIMULATION RESULTS");
        System.out.println("=".repeat(80));
        System.out.println();
        
        System.out.println("Configuration:");
        System.out.printf("  Scheduler: %s%n", schedulerName);
        System.out.printf("  Network: %s%n", networkModelName);
        System.out.printf("  Workload: %s%n", workloadName);
        System.out.printf("  Duration: %.1f seconds%n", summary.getSimulationDuration());
        System.out.println();
        
        System.out.println("Performance Metrics:");
        System.out.printf("  Total Requests: %d%n", summary.getTotalRequests());
        System.out.printf("  Completed Downloads: %d%n", summary.getCompletedDownloads());
        System.out.printf("  Failed Downloads: %d%n", summary.getFailedDownloads());
        System.out.printf("  Completion Rate: %.1f%%%n", summary.getCompletionRate() * 100);
        System.out.printf("  Average Throughput: %.1f KB/s%n", summary.getAverageThroughput() / 1024);
        System.out.printf("  Average Duration: %.3f seconds%n", summary.getAverageDuration());
        System.out.printf("  Cache Hit Rate: %.1f%%%n", summary.getCacheHitRate() * 100);
        System.out.printf("  Composite Score: %.1f/100%n", compositeScore);
        System.out.println();
        
        System.out.println("=".repeat(80));
    }
    
    /// Prints results in tabular format.
    /// 
    /// @param includeHeaders Whether to include column headers
    public void printTabularResults(boolean includeHeaders) {
        if (includeHeaders) {
            System.out.printf("%-25s %-20s %-15s %-10s %-10s %-12s %-10s %-8s%n",
                "Scheduler", "Network", "Workload", "Requests", "Complete", "Throughput", "Duration", "Score");
            System.out.println("-".repeat(120));
        }
        
        SimulationStatistics.StatisticsSummary summary = statistics.getSummary();
        
        System.out.printf("%-25s %-20s %-15s %-10d %-10.1f%% %-12.1f %-10.3f %-8.1f%n",
            schedulerName, networkModelName, workloadName,
            summary.getTotalRequests(),
            summary.getCompletionRate() * 100,
            summary.getAverageThroughput() / 1024, // KB/s
            summary.getAverageDuration(),
            compositeScore);
    }
    
    /// Creates a CSV row for this result.
    /// 
    /// @return CSV-formatted string
    public String toCsvRow() {
        SimulationStatistics.StatisticsSummary summary = statistics.getSummary();
        
        return String.format("%s,%s,%s,%d,%d,%d,%.4f,%.2f,%.4f,%.4f,%.2f",
            schedulerName, networkModelName, workloadName,
            summary.getTotalRequests(), summary.getCompletedDownloads(), summary.getFailedDownloads(),
            summary.getCompletionRate(), summary.getAverageThroughput() / 1024,
            summary.getAverageDuration(), summary.getCacheHitRate(), compositeScore);
    }
    
    /// Gets CSV headers for results.
    /// 
    /// @return CSV header string
    public static String getCsvHeaders() {
        return "Scheduler,Network,Workload,Requests,Completed,Failed,CompletionRate,ThroughputKBps," +
               "AvgDuration,CacheHitRate,CompositeScore";
    }
    
    /// Compares this result to another based on composite score.
    /// 
    /// @param other The other result to compare to
    /// @return Comparison result (positive if this is better)
    public double compareScore(SimulationResults other) {
        return this.compositeScore - other.compositeScore;
    }
    
    @Override
    public String toString() {
        return String.format("%s on %s with %s: Score %.1f", 
                           schedulerName, networkModelName, workloadName, compositeScore);
    }
}