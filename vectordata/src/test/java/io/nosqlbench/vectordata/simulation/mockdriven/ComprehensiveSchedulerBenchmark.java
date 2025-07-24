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

import io.nosqlbench.vectordata.merklev2.ChunkScheduler;
import io.nosqlbench.vectordata.merklev2.schedulers.AdaptiveChunkScheduler;
import io.nosqlbench.vectordata.merklev2.schedulers.AggressiveChunkScheduler;
import io.nosqlbench.vectordata.merklev2.schedulers.ConservativeChunkScheduler;
import io.nosqlbench.vectordata.merklev2.schedulers.DefaultChunkScheduler;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutionException;

/// Comprehensive benchmark test that evaluates all scheduler combinations
/// and presents results in tabular format for easy comparison.
/// 
/// This test runs every combination of:
/// - Schedulers (Default, Aggressive, Conservative, Adaptive)
/// - Network conditions (Fiber, Broadband Fast/Standard, Mobile, Satellite)
/// - Workload patterns (Sequential, Random, Sparse, Clustered)
/// 
/// Results are presented in multiple tables showing:
/// - Performance metrics for each combination
/// - Best scheduler per network condition
/// - Best scheduler per workload pattern
/// - Overall rankings
public class ComprehensiveSchedulerBenchmark {
    
    /// Run comprehensive benchmark with all parameter combinations.
    /// 
    /// This test evaluates scheduler performance across all combinations
    /// and presents the results in easy-to-read tabular format.
    @Test
    public void runComprehensiveBenchmark() throws InterruptedException, ExecutionException {
        System.out.println("=== COMPREHENSIVE SCHEDULER BENCHMARK ===");
        System.out.println("Running all combinations of schedulers, network conditions, and workloads...");
        System.out.println();
        
        // Test configuration
        long fileSize = 20_000_000L; // 20MB for reasonable test time
        int chunkSize = 512 * 1024; // 512KB chunks
        int iterations = 2; // 2 iterations for averaging
        
        // All schedulers to test
        List<ChunkScheduler> schedulers = Arrays.asList(
            new DefaultChunkScheduler(),
            new AggressiveChunkScheduler(),
            new ConservativeChunkScheduler(),
            new AdaptiveChunkScheduler()
        );
        
        // Key network conditions to test
        List<NetworkConditions> networkConditions = Arrays.asList(
            NetworkConditions.Scenarios.FIBER,
            NetworkConditions.Scenarios.BROADBAND_FAST,
            NetworkConditions.Scenarios.BROADBAND_STANDARD,
            NetworkConditions.Scenarios.MOBILE_LTE,
            NetworkConditions.Scenarios.SATELLITE
        );
        
        // All workload patterns
        List<SchedulerWorkload> workloads = Arrays.asList(
            SchedulerWorkload.SEQUENTIAL_READ,
            SchedulerWorkload.RANDOM_READ,
            SchedulerWorkload.SPARSE_READ,
            SchedulerWorkload.CLUSTERED_READ
        );
        
        // Run the benchmark
        SchedulerPerformanceTest test = new SchedulerPerformanceTest()
            .withFileSize(fileSize)
            .withChunkSize(chunkSize)
            .withSchedulers(schedulers)
            .withNetworkConditions(networkConditions)
            .withWorkloads(workloads.toArray(new SchedulerWorkload[0]))
            .withIterations(iterations)
            .withTimeout(Duration.ofMinutes(2));
        
        PerformanceResults results = test.run();
        
        // Display results in various tabular formats
        System.out.println("\n" + "=".repeat(120));
        displayDetailedResultsTable(results);
        
        System.out.println("\n" + "=".repeat(120));
        displayNetworkConditionSummaryTable(results);
        
        System.out.println("\n" + "=".repeat(120));
        displayWorkloadPatternSummaryTable(results);
        
        System.out.println("\n" + "=".repeat(120));
        displayOverallRankingsTable(results);
        
        System.out.println("\n" + "=".repeat(120));
        displayPerformanceMatrixTable(results);
    }
    
    /// Display detailed results for all combinations in tabular format.
    private void displayDetailedResultsTable(PerformanceResults results) {
        System.out.println("DETAILED RESULTS TABLE");
        System.out.println("-".repeat(120));
        
        // Table header
        System.out.printf("%-20s | %-20s | %-15s | %8s | %10s | %10s | %12s | %10s%n",
            "Scheduler", "Network", "Workload", "Time(ms)", "Complete%", "Efficiency", "Throughput", "Score");
        System.out.println("-".repeat(120));
        
        // Sort combinations for better readability
        List<SchedulerPerformanceTest.TestCombination> combinations = 
            new ArrayList<>(results.getTestCombinations());
        combinations.sort(Comparator
            .comparing(SchedulerPerformanceTest.TestCombination::getSchedulerName)
            .thenComparing(SchedulerPerformanceTest.TestCombination::getNetworkCondition)
            .thenComparing(SchedulerPerformanceTest.TestCombination::getWorkloadName));
        
        String lastScheduler = "";
        for (SchedulerPerformanceTest.TestCombination combination : combinations) {
            PerformanceResults.AverageMetrics metrics = results.getAverageMetrics(combination);
            
            // Add separator between schedulers
            if (!combination.getSchedulerName().equals(lastScheduler)) {
                if (!lastScheduler.isEmpty()) {
                    System.out.println("-".repeat(120));
                }
                lastScheduler = combination.getSchedulerName();
            }
            
            System.out.printf("%-20s | %-20s | %-15s | %8d | %9.1f%% | %10.2f | %10.2f MB/s | %10.2f%n",
                combination.getSchedulerName(),
                truncate(combination.getNetworkCondition(), 20),
                combination.getWorkloadName(),
                metrics.getAverageTime().toMillis(),
                metrics.getAverageCompletionPercentage(),
                metrics.getAverageEfficiency(),
                metrics.getAverageThroughput() / (1024 * 1024),
                calculateScore(metrics));
        }
    }
    
    /// Display summary table showing best scheduler for each network condition.
    private void displayNetworkConditionSummaryTable(PerformanceResults results) {
        System.out.println("BEST SCHEDULER BY NETWORK CONDITION");
        System.out.println("-".repeat(80));
        
        System.out.printf("%-30s | %-20s | %15s | %10s%n",
            "Network Condition", "Best Scheduler", "Avg Score", "Workloads");
        System.out.println("-".repeat(80));
        
        for (String networkCondition : results.getNetworkConditions()) {
            String bestScheduler = results.getBestSchedulerForCondition(networkCondition);
            
            // Calculate average score for this combination
            double avgScore = calculateAverageScoreForCondition(results, networkCondition, bestScheduler);
            int workloadCount = results.getWorkloadNames().size();
            
            System.out.printf("%-30s | %-20s | %15.2f | %10d%n",
                truncate(networkCondition, 30),
                bestScheduler,
                avgScore,
                workloadCount);
        }
    }
    
    /// Display summary table showing best scheduler for each workload pattern.
    private void displayWorkloadPatternSummaryTable(PerformanceResults results) {
        System.out.println("BEST SCHEDULER BY WORKLOAD PATTERN");
        System.out.println("-".repeat(80));
        
        System.out.printf("%-20s | %-20s | %15s | %12s%n",
            "Workload Pattern", "Best Scheduler", "Avg Score", "Networks");
        System.out.println("-".repeat(80));
        
        for (String workload : results.getWorkloadNames()) {
            String bestScheduler = results.getBestSchedulerForWorkload(workload);
            
            // Calculate average score for this combination
            double avgScore = calculateAverageScoreForWorkload(results, workload, bestScheduler);
            int networkCount = results.getNetworkConditions().size();
            
            System.out.printf("%-20s | %-20s | %15.2f | %12d%n",
                workload,
                bestScheduler,
                avgScore,
                networkCount);
        }
    }
    
    /// Display overall scheduler rankings across all scenarios.
    private void displayOverallRankingsTable(PerformanceResults results) {
        System.out.println("OVERALL SCHEDULER RANKINGS");
        System.out.println("-".repeat(100));
        
        System.out.printf("%4s | %-20s | %12s | %12s | %12s | %10s | %15s%n",
            "Rank", "Scheduler", "Avg Score", "Min Score", "Max Score", "Scenarios", "Recommendation");
        System.out.println("-".repeat(100));
        
        List<PerformanceResults.SchedulerRanking> rankings = results.rankSchedulers();
        
        for (int i = 0; i < rankings.size(); i++) {
            PerformanceResults.SchedulerRanking ranking = rankings.get(i);
            String recommendation = getRecommendation(i, ranking);
            
            System.out.printf("%4d | %-20s | %12.2f | %12.2f | %12.2f | %10d | %15s%n",
                i + 1,
                ranking.getSchedulerName(),
                ranking.getAverageScore(),
                ranking.getMinScore(),
                ranking.getMaxScore(),
                ranking.getScenarioCount(),
                recommendation);
        }
    }
    
    /// Display performance matrix showing scores for each scheduler/network combination.
    private void displayPerformanceMatrixTable(PerformanceResults results) {
        System.out.println("PERFORMANCE MATRIX (Average Scores by Scheduler and Network)");
        System.out.println("-".repeat(120));
        
        // Get unique schedulers and networks
        List<String> schedulers = new ArrayList<>(results.getSchedulerNames());
        List<String> networks = new ArrayList<>(results.getNetworkConditions());
        
        // Print header
        System.out.printf("%-20s |", "Scheduler \\ Network");
        for (String network : networks) {
            System.out.printf(" %12s |", truncate(network, 12));
        }
        System.out.println();
        System.out.println("-".repeat(120));
        
        // Print matrix
        for (String scheduler : schedulers) {
            System.out.printf("%-20s |", scheduler);
            
            for (String network : networks) {
                double avgScore = calculateAverageScoreForSchedulerNetwork(results, scheduler, network);
                if (avgScore > 0) {
                    System.out.printf(" %12.2f |", avgScore);
                } else {
                    System.out.printf(" %12s |", "N/A");
                }
            }
            System.out.println();
        }
        
        System.out.println("-".repeat(120));
        System.out.println("Note: Higher scores indicate better performance (0-100 scale)");
    }
    
    /// Calculate composite score from metrics.
    private double calculateScore(PerformanceResults.AverageMetrics metrics) {
        // Same scoring algorithm as PerformanceResults
        double completionWeight = 0.4;
        double efficiencyWeight = 0.3;
        double throughputWeight = 0.2;
        double successWeight = 0.1;
        
        double completionScore = Math.min(100.0, metrics.getAverageCompletionPercentage());
        double efficiencyScore = Math.max(0.0, 100.0 - (metrics.getAverageEfficiency() - 1.0) * 50.0);
        double throughputScore = Math.min(100.0, (metrics.getAverageThroughput() / (100 * 1024 * 1024)) * 100.0);
        double successScore = metrics.getSuccessRate();
        
        return (completionScore * completionWeight) +
               (efficiencyScore * efficiencyWeight) +
               (throughputScore * throughputWeight) +
               (successScore * successWeight);
    }
    
    /// Calculate average score for a specific network condition and scheduler.
    private double calculateAverageScoreForCondition(PerformanceResults results, 
                                                    String networkCondition, 
                                                    String scheduler) {
        double totalScore = 0;
        int count = 0;
        
        for (SchedulerPerformanceTest.TestCombination combination : results.getTestCombinations()) {
            if (combination.getNetworkCondition().equals(networkCondition) &&
                combination.getSchedulerName().equals(scheduler)) {
                PerformanceResults.AverageMetrics metrics = results.getAverageMetrics(combination);
                totalScore += calculateScore(metrics);
                count++;
            }
        }
        
        return count > 0 ? totalScore / count : 0.0;
    }
    
    /// Calculate average score for a specific workload and scheduler.
    private double calculateAverageScoreForWorkload(PerformanceResults results,
                                                   String workload,
                                                   String scheduler) {
        double totalScore = 0;
        int count = 0;
        
        for (SchedulerPerformanceTest.TestCombination combination : results.getTestCombinations()) {
            if (combination.getWorkloadName().equals(workload) &&
                combination.getSchedulerName().equals(scheduler)) {
                PerformanceResults.AverageMetrics metrics = results.getAverageMetrics(combination);
                totalScore += calculateScore(metrics);
                count++;
            }
        }
        
        return count > 0 ? totalScore / count : 0.0;
    }
    
    /// Calculate average score for a specific scheduler and network combination.
    private double calculateAverageScoreForSchedulerNetwork(PerformanceResults results,
                                                          String scheduler,
                                                          String network) {
        double totalScore = 0;
        int count = 0;
        
        for (SchedulerPerformanceTest.TestCombination combination : results.getTestCombinations()) {
            if (combination.getSchedulerName().equals(scheduler) &&
                combination.getNetworkCondition().equals(network)) {
                PerformanceResults.AverageMetrics metrics = results.getAverageMetrics(combination);
                totalScore += calculateScore(metrics);
                count++;
            }
        }
        
        return count > 0 ? totalScore / count : 0.0;
    }
    
    /// Get recommendation based on ranking.
    private String getRecommendation(int rank, PerformanceResults.SchedulerRanking ranking) {
        if (rank == 0) {
            return "Best Overall";
        } else if (ranking.getAverageScore() > 80) {
            return "Excellent";
        } else if (ranking.getAverageScore() > 70) {
            return "Good";
        } else if (ranking.getAverageScore() > 60) {
            return "Acceptable";
        } else {
            return "Limited Use";
        }
    }
    
    /// Truncate string to specified length.
    private String truncate(String str, int maxLength) {
        if (str.length() <= maxLength) {
            return str;
        }
        return str.substring(0, maxLength - 3) + "...";
    }
    
    /// Quick test with reduced parameters for faster execution.
    @Test
    public void runQuickBenchmark() throws InterruptedException, ExecutionException {
        System.out.println("=== QUICK SCHEDULER BENCHMARK ===");
        System.out.println("Running reduced test for quick validation...");
        System.out.println();
        
        // Reduced configuration for quick test
        long fileSize = 5_000_000L; // 5MB
        int chunkSize = 256 * 1024; // 256KB chunks
        int iterations = 1;
        
        // Reduced parameter sets
        List<ChunkScheduler> schedulers = Arrays.asList(
            new DefaultChunkScheduler(),
            new ConservativeChunkScheduler()
        );
        
        List<NetworkConditions> networkConditions = Arrays.asList(
            NetworkConditions.Scenarios.BROADBAND_FAST,
            NetworkConditions.Scenarios.MOBILE_LTE
        );
        
        List<SchedulerWorkload> workloads = Arrays.asList(
            SchedulerWorkload.SEQUENTIAL_READ,
            SchedulerWorkload.SPARSE_READ
        );
        
        // Run the benchmark
        SchedulerPerformanceTest test = new SchedulerPerformanceTest()
            .withFileSize(fileSize)
            .withChunkSize(chunkSize)
            .withSchedulers(schedulers)
            .withNetworkConditions(networkConditions)
            .withWorkloads(workloads.toArray(new SchedulerWorkload[0]))
            .withIterations(iterations)
            .withTimeout(Duration.ofSeconds(30));
        
        PerformanceResults results = test.run();
        
        // Display simplified results
        displayDetailedResultsTable(results);
        System.out.println();
        displayOverallRankingsTable(results);
    }
}