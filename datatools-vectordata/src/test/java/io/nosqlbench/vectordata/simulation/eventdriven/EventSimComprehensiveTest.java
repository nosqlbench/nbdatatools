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

import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.stream.Collectors;

/// Comprehensive unit test that runs all scheduler/network/workload combinations
/// and provides detailed tabular analysis of the results.
/// 
/// This test harnesses the event-driven simulation framework to evaluate
/// performance across all possible configuration combinations, generating
/// multiple detailed tables for analysis.
public class EventSimComprehensiveTest {
    
    private static final long FILE_SIZE = 50_000_000L; // 50MB
    private static final long CHUNK_SIZE = 65536L; // 64KB chunks
    private static final double SIMULATION_DURATION = 60.0; // 60 seconds
    private static final long RANDOM_SEED = 42L;
    
    @Test
    public void testAllCombinationsWithTabularResults() {
        System.out.println("EVENT-DRIVEN SIMULATION COMPREHENSIVE TEST");
        System.out.println("==========================================");
        System.out.println();
        
        System.out.printf("Configuration: File=%.1fMB, Chunks=%d, Duration=%.0fs, Seed=%d%n",
                         FILE_SIZE / 1024.0 / 1024.0, 
                         (int) Math.ceil((double) FILE_SIZE / CHUNK_SIZE),
                         SIMULATION_DURATION, RANDOM_SEED);
        System.out.println();
        
        // Run all combinations and collect results
        long startTime = System.currentTimeMillis();
        List<SimulationResults> allResults = runAllCombinations();
        long endTime = System.currentTimeMillis();
        
        double executionTime = (endTime - startTime) / 1000.0;
        System.out.printf("Executed %d simulations in %.2f seconds (%.1f simulations/second)%n%n",
                         allResults.size(), executionTime, allResults.size() / executionTime);
        
        // Generate comprehensive tabular reports
        printFullResultsTable(allResults);
        printSchedulerPerformanceMatrix(allResults);
        printNetworkConditionsAnalysis(allResults);
        printWorkloadPatternAnalysis(allResults);
        printOptimalConfigurationMatrix(allResults);
        printPerformanceRankings(allResults);
        printStatisticalSummary(allResults);
        
        // Verify that we got expected number of results
        int expectedCombinations = getSchedulerCount() * getNetworkModelCount() * getWorkloadTypeCount();
        assert allResults.size() == expectedCombinations : 
            String.format("Expected %d combinations, got %d", expectedCombinations, allResults.size());
        
        System.out.println("✓ All combination tests completed successfully!");
    }
    
    /// Runs all possible combinations of schedulers, networks, and workloads.
    /// 
    /// @return List of all simulation results
    private List<SimulationResults> runAllCombinations() {
        List<SimulationResults> results = new ArrayList<>();
        
        String[] schedulerTypes = {"Default", "Aggressive", "Conservative", "Adaptive"};
        NetworkModel[] networks = NetworkModel.Presets.getAllPresets();
        WorkloadGeneratorFactory.WorkloadType[] workloadTypes = WorkloadGeneratorFactory.WorkloadType.values();
        
        int totalCombinations = schedulerTypes.length * networks.length * workloadTypes.length;
        int completed = 0;
        
        System.out.printf("Running %d combinations: %d schedulers × %d networks × %d workloads...%n",
                         totalCombinations, schedulerTypes.length, networks.length, workloadTypes.length);
        
        for (String schedulerType : schedulerTypes) {
            for (NetworkModel network : networks) {
                for (WorkloadGeneratorFactory.WorkloadType workloadType : workloadTypes) {
                    // CRITICAL: Create completely fresh instances for each test to avoid ANY state contamination
                    EventDrivenScheduler scheduler = createFreshScheduler(schedulerType);
                    WorkloadGenerator workload = WorkloadGeneratorFactory.create(
                        workloadType, FILE_SIZE, CHUNK_SIZE, RANDOM_SEED + completed); // Vary seed slightly
                    
                    SimulationResults result = runSingleSimulation(scheduler, network, workload);
                    results.add(result);
                    
                    completed++;
                    if (completed % 20 == 0 || completed == totalCombinations) {
                        System.out.printf("  Progress: %d/%d (%.1f%%)%n", 
                                        completed, totalCombinations, 
                                        100.0 * completed / totalCombinations);
                    }
                }
            }
        }
        
        return results;
    }
    
    /// Creates a completely fresh scheduler instance for testing.
    /// 
    /// @param schedulerType The type of scheduler to create
    /// @return A new scheduler instance with clean state
    private EventDrivenScheduler createFreshScheduler(String schedulerType) {
        // Ensure we create a completely new instance every time
        switch (schedulerType) {
            case "Default":
                return new DefaultEventDrivenScheduler();
            case "Aggressive":
                return new AggressiveEventDrivenScheduler();
            case "Conservative":
                return new ConservativeEventDrivenScheduler();
            case "Adaptive":
                return new AdaptiveEventDrivenScheduler();
            default:
                throw new IllegalArgumentException("Unknown scheduler type: " + schedulerType);
        }
    }
    
    /// Runs a single simulation with the given configuration.
    /// 
    /// @param scheduler The scheduler to test
    /// @param network The network model to use
    /// @param workload The workload generator to use
    /// @return Simulation results
    private SimulationResults runSingleSimulation(EventDrivenScheduler scheduler, 
                                                 NetworkModel network, 
                                                 WorkloadGenerator workload) {
        EventDrivenSimulation simulation = new EventDrivenSimulation(
            FILE_SIZE, CHUNK_SIZE, SIMULATION_DURATION)
            .withScheduler(scheduler)
            .withNetworkModel(network)
            .withWorkloadGenerator(workload);
        
        return simulation.run();
    }
    
    /// Prints the complete results table with all metrics.
    /// 
    /// @param results All simulation results
    private void printFullResultsTable(List<SimulationResults> results) {
        System.out.println("COMPLETE RESULTS TABLE");
        System.out.println("======================");
        
        // Print header
        System.out.printf("%-20s %-15s %-12s %-8s %-8s %-8s %-10s %-10s %-8s %-8s%n",
            "Scheduler", "Network", "Workload", "Requests", "Complete", "Failed", 
            "CompRate%", "Throughput", "Duration", "Score");
        System.out.println("-".repeat(120));
        
        // Sort by composite score descending
        results.stream()
            .sorted((a, b) -> Double.compare(b.getCompositeScore(), a.getCompositeScore()))
            .forEach(result -> {
                SimulationStatistics.StatisticsSummary summary = result.getStatistics().getSummary();
                System.out.printf("%-20s %-15s %-12s %-8d %-8d %-8d %-10.1f %-10.1f %-8.3f %-8.1f%n",
                    truncate(result.getSchedulerName(), 20),
                    truncate(result.getNetworkModelName(), 15),
                    truncate(result.getWorkloadName(), 12),
                    summary.getTotalRequests(),
                    summary.getCompletedDownloads(),
                    summary.getFailedDownloads(),
                    summary.getCompletionRate() * 100,
                    summary.getAverageThroughput() / 1024, // KB/s
                    summary.getAverageDuration(),
                    result.getCompositeScore()
                );
            });
        
        System.out.println();
    }
    
    /// Prints scheduler performance matrix across all conditions.
    /// 
    /// @param results All simulation results
    private void printSchedulerPerformanceMatrix(List<SimulationResults> results) {
        System.out.println("SCHEDULER PERFORMANCE MATRIX");
        System.out.println("============================");
        
        // Group results by scheduler
        Map<String, List<SimulationResults>> byScheduler = results.stream()
            .collect(Collectors.groupingBy(SimulationResults::getSchedulerName));
        
        // Calculate statistics for each scheduler
        System.out.printf("%-25s %-8s %-8s %-8s %-8s %-10s %-10s%n",
            "Scheduler", "Count", "AvgScore", "MinScore", "MaxScore", "StdDev", "WinRate%");
        System.out.println("-".repeat(85));
        
        List<SchedulerStats> schedulerStats = new ArrayList<>();
        
        for (Map.Entry<String, List<SimulationResults>> entry : byScheduler.entrySet()) {
            String schedulerName = entry.getKey();
            List<SimulationResults> schedulerResults = entry.getValue();
            
            DoubleSummaryStatistics scoreStats = schedulerResults.stream()
                .mapToDouble(SimulationResults::getCompositeScore)
                .summaryStatistics();
            
            double stdDev = calculateStandardDeviation(
                schedulerResults.stream()
                    .mapToDouble(SimulationResults::getCompositeScore)
                    .toArray());
            
            // Calculate win rate (how often this scheduler scored highest in its group)
            double winRate = calculateWinRate(schedulerName, results);
            
            SchedulerStats stats = new SchedulerStats(schedulerName, scoreStats, stdDev, winRate);
            schedulerStats.add(stats);
            
            System.out.printf("%-25s %-8d %-8.1f %-8.1f %-8.1f %-10.2f %-10.1f%n",
                truncate(schedulerName, 25),
                (int) scoreStats.getCount(),
                scoreStats.getAverage(),
                scoreStats.getMin(),
                scoreStats.getMax(),
                stdDev,
                winRate * 100);
        }
        
        System.out.println();
    }
    
    /// Prints network conditions analysis table.
    /// 
    /// @param results All simulation results
    private void printNetworkConditionsAnalysis(List<SimulationResults> results) {
        System.out.println("NETWORK CONDITIONS ANALYSIS");
        System.out.println("===========================");
        
        // Group by network model
        Map<String, List<SimulationResults>> byNetwork = results.stream()
            .collect(Collectors.groupingBy(SimulationResults::getNetworkModelName));
        
        System.out.printf("%-20s %-8s %-8s %-12s %-15s %-12s%n",
            "Network", "Count", "AvgScore", "BestScore", "BestScheduler", "WorstScore");
        System.out.println("-".repeat(85));
        
        byNetwork.entrySet().stream()
            .sorted((a, b) -> {
                double avgA = a.getValue().stream().mapToDouble(SimulationResults::getCompositeScore).average().orElse(0);
                double avgB = b.getValue().stream().mapToDouble(SimulationResults::getCompositeScore).average().orElse(0);
                return Double.compare(avgB, avgA);
            })
            .forEach(entry -> {
                String networkName = entry.getKey();
                List<SimulationResults> networkResults = entry.getValue();
                
                DoubleSummaryStatistics scoreStats = networkResults.stream()
                    .mapToDouble(SimulationResults::getCompositeScore)
                    .summaryStatistics();
                
                SimulationResults best = networkResults.stream()
                    .max(Comparator.comparing(SimulationResults::getCompositeScore))
                    .orElse(null);
                
                System.out.printf("%-20s %-8d %-8.1f %-12.1f %-15s %-12.1f%n",
                    truncate(networkName, 20),
                    networkResults.size(),
                    scoreStats.getAverage(),
                    scoreStats.getMax(),
                    best != null ? truncate(best.getSchedulerName(), 15) : "N/A",
                    scoreStats.getMin());
            });
        
        System.out.println();
    }
    
    /// Prints workload pattern analysis table.
    /// 
    /// @param results All simulation results
    private void printWorkloadPatternAnalysis(List<SimulationResults> results) {
        System.out.println("WORKLOAD PATTERN ANALYSIS");
        System.out.println("=========================");
        
        // Group by workload
        Map<String, List<SimulationResults>> byWorkload = results.stream()
            .collect(Collectors.groupingBy(SimulationResults::getWorkloadName));
        
        System.out.printf("%-15s %-8s %-8s %-12s %-15s %-12s %-12s%n",
            "Workload", "Count", "AvgScore", "BestScore", "BestScheduler", "AvgRequests", "AvgHitRate%");
        System.out.println("-".repeat(95));
        
        byWorkload.entrySet().stream()
            .sorted((a, b) -> {
                double avgA = a.getValue().stream().mapToDouble(SimulationResults::getCompositeScore).average().orElse(0);
                double avgB = b.getValue().stream().mapToDouble(SimulationResults::getCompositeScore).average().orElse(0);
                return Double.compare(avgB, avgA);
            })
            .forEach(entry -> {
                String workloadName = entry.getKey();
                List<SimulationResults> workloadResults = entry.getValue();
                
                DoubleSummaryStatistics scoreStats = workloadResults.stream()
                    .mapToDouble(SimulationResults::getCompositeScore)
                    .summaryStatistics();
                
                SimulationResults best = workloadResults.stream()
                    .max(Comparator.comparing(SimulationResults::getCompositeScore))
                    .orElse(null);
                
                double avgRequests = workloadResults.stream()
                    .mapToDouble(r -> r.getStatistics().getSummary().getTotalRequests())
                    .average().orElse(0);
                
                double avgHitRate = workloadResults.stream()
                    .mapToDouble(r -> r.getStatistics().getCacheHitRate())
                    .average().orElse(0);
                
                System.out.printf("%-15s %-8d %-8.1f %-12.1f %-15s %-12.1f %-12.1f%n",
                    truncate(workloadName, 15),
                    workloadResults.size(),
                    scoreStats.getAverage(),
                    scoreStats.getMax(),
                    best != null ? truncate(best.getSchedulerName(), 15) : "N/A",
                    avgRequests,
                    avgHitRate * 100);
            });
        
        System.out.println();
    }
    
    /// Prints optimal configuration matrix showing best scheduler for each network/workload combination.
    /// 
    /// @param results All simulation results
    private void printOptimalConfigurationMatrix(List<SimulationResults> results) {
        System.out.println("OPTIMAL SCHEDULER MATRIX (Network × Workload)");
        System.out.println("==============================================");
        
        // Get unique networks and workloads
        List<String> networks = results.stream()
            .map(SimulationResults::getNetworkModelName)
            .distinct()
            .sorted()
            .collect(Collectors.toList());
        
        List<String> workloads = results.stream()
            .map(SimulationResults::getWorkloadName)
            .distinct()
            .sorted()
            .collect(Collectors.toList());
        
        // Print header
        System.out.printf("%-20s", "Network \\ Workload");
        for (String workload : workloads) {
            System.out.printf(" %-12s", truncate(workload, 12));
        }
        System.out.println();
        System.out.println("-".repeat(20 + workloads.size() * 13));
        
        // Print matrix
        for (String network : networks) {
            System.out.printf("%-20s", truncate(network, 20));
            
            for (String workload : workloads) {
                SimulationResults best = results.stream()
                    .filter(r -> r.getNetworkModelName().equals(network) && 
                               r.getWorkloadName().equals(workload))
                    .max(Comparator.comparing(SimulationResults::getCompositeScore))
                    .orElse(null);
                
                String schedulerName = best != null ? 
                    abbreviateScheduler(best.getSchedulerName()) : "N/A";
                System.out.printf(" %-12s", schedulerName);
            }
            System.out.println();
        }
        
        System.out.println();
        System.out.println("Scheduler abbreviations: DEF=Default, AGG=Aggressive, CON=Conservative, ADA=Adaptive");
        System.out.println();
    }
    
    /// Prints performance rankings for top and bottom configurations.
    /// 
    /// @param results All simulation results
    private void printPerformanceRankings(List<SimulationResults> results) {
        System.out.println("PERFORMANCE RANKINGS");
        System.out.println("===================");
        
        // Top 15 configurations
        System.out.println("TOP 15 CONFIGURATIONS:");
        System.out.println("-".repeat(90));
        System.out.printf("%-4s %-20s %-15s %-12s %-8s %-10s %-10s%n",
            "Rank", "Scheduler", "Network", "Workload", "Score", "CompRate%", "Throughput");
        System.out.println("-".repeat(90));
        
        results.stream()
            .sorted((a, b) -> Double.compare(b.getCompositeScore(), a.getCompositeScore()))
            .limit(15)
            .forEach(new RankingPrinter()::printRanking);
        
        System.out.println();
        
        // Bottom 10 configurations
        System.out.println("BOTTOM 10 CONFIGURATIONS:");
        System.out.println("-".repeat(90));
        System.out.printf("%-4s %-20s %-15s %-12s %-8s %-10s %-10s%n",
            "Rank", "Scheduler", "Network", "Workload", "Score", "CompRate%", "Throughput");
        System.out.println("-".repeat(90));
        
        List<SimulationResults> sortedResults = results.stream()
            .sorted(Comparator.comparing(SimulationResults::getCompositeScore))
            .collect(Collectors.toList());
        
        for (int i = 0; i < Math.min(10, sortedResults.size()); i++) {
            SimulationResults result = sortedResults.get(i);
            SimulationStatistics.StatisticsSummary summary = result.getStatistics().getSummary();
            
            System.out.printf("%-4d %-20s %-15s %-12s %-8.1f %-10.1f %-10.1f%n",
                results.size() - i,
                truncate(result.getSchedulerName(), 20),
                truncate(result.getNetworkModelName(), 15),
                truncate(result.getWorkloadName(), 12),
                result.getCompositeScore(),
                summary.getCompletionRate() * 100,
                summary.getAverageThroughput() / 1024);
        }
        
        System.out.println();
    }
    
    /// Prints statistical summary of all results.
    /// 
    /// @param results All simulation results
    private void printStatisticalSummary(List<SimulationResults> results) {
        System.out.println("STATISTICAL SUMMARY");
        System.out.println("==================");
        
        DoubleSummaryStatistics scoreStats = results.stream()
            .mapToDouble(SimulationResults::getCompositeScore)
            .summaryStatistics();
        
        DoubleSummaryStatistics completionStats = results.stream()
            .mapToDouble(r -> r.getStatistics().getDownloadCompletionRate())
            .summaryStatistics();
        
        DoubleSummaryStatistics throughputStats = results.stream()
            .mapToDouble(r -> r.getStatistics().getAverageThroughput())
            .summaryStatistics();
        
        System.out.printf("Total Combinations: %d%n", results.size());
        System.out.println();
        
        System.out.printf("Composite Score:    Min=%.1f, Max=%.1f, Avg=%.1f, StdDev=%.2f%n",
            scoreStats.getMin(), scoreStats.getMax(), scoreStats.getAverage(),
            calculateStandardDeviation(results.stream().mapToDouble(SimulationResults::getCompositeScore).toArray()));
        
        System.out.printf("Completion Rate:    Min=%.1f%%, Max=%.1f%%, Avg=%.1f%%,StdDev=%.2f%%%n",
            completionStats.getMin() * 100, completionStats.getMax() * 100, 
            completionStats.getAverage() * 100,
            calculateStandardDeviation(results.stream().mapToDouble(r -> r.getStatistics().getDownloadCompletionRate()).toArray()) * 100);
        
        System.out.printf("Throughput (KB/s):  Min=%.1f, Max=%.1f, Avg=%.1f, StdDev=%.1f%n",
            throughputStats.getMin() / 1024, throughputStats.getMax() / 1024, 
            throughputStats.getAverage() / 1024,
            calculateStandardDeviation(results.stream().mapToDouble(r -> r.getStatistics().getAverageThroughput()).toArray()) / 1024);
        
        System.out.println();
    }
    
    // Helper methods
    
    private int getSchedulerCount() { return 4; }
    private int getNetworkModelCount() { return NetworkModel.Presets.getAllPresets().length; }
    private int getWorkloadTypeCount() { return WorkloadGeneratorFactory.WorkloadType.values().length; }
    
    private String truncate(String str, int maxLength) {
        return str.length() <= maxLength ? str : str.substring(0, maxLength - 3) + "...";
    }
    
    private String abbreviateScheduler(String schedulerName) {
        if (schedulerName.contains("Default")) return "DEF";
        if (schedulerName.contains("Aggressive")) return "AGG";
        if (schedulerName.contains("Conservative")) return "CON";
        if (schedulerName.contains("Adaptive")) return "ADA";
        return schedulerName.substring(0, Math.min(3, schedulerName.length())).toUpperCase();
    }
    
    private double calculateStandardDeviation(double[] values) {
        if (values.length <= 1) return 0.0;
        
        double mean = Arrays.stream(values).average().orElse(0.0);
        double sumSquaredDiffs = Arrays.stream(values)
            .map(x -> Math.pow(x - mean, 2))
            .sum();
        
        return Math.sqrt(sumSquaredDiffs / (values.length - 1));
    }
    
    private double calculateWinRate(String schedulerName, List<SimulationResults> allResults) {
        // Group by network and workload, then count wins
        Map<String, List<SimulationResults>> groups = allResults.stream()
            .collect(Collectors.groupingBy(r -> r.getNetworkModelName() + ":" + r.getWorkloadName()));
        
        int wins = 0;
        int totalGroups = groups.size();
        
        for (List<SimulationResults> group : groups.values()) {
            SimulationResults best = group.stream()
                .max(Comparator.comparing(SimulationResults::getCompositeScore))
                .orElse(null);
            
            if (best != null && best.getSchedulerName().equals(schedulerName)) {
                wins++;
            }
        }
        
        return totalGroups > 0 ? (double) wins / totalGroups : 0.0;
    }
    
    /// Helper class for storing scheduler statistics.
    private static class SchedulerStats {
        final String name;
        final DoubleSummaryStatistics scoreStats;
        final double stdDev;
        final double winRate;
        
        SchedulerStats(String name, DoubleSummaryStatistics scoreStats, double stdDev, double winRate) {
            this.name = name;
            this.scoreStats = scoreStats;
            this.stdDev = stdDev;
            this.winRate = winRate;
        }
    }
    
    /// Helper class for printing rankings.
    private class RankingPrinter {
        private int rank = 1;
        
        void printRanking(SimulationResults result) {
            SimulationStatistics.StatisticsSummary summary = result.getStatistics().getSummary();
            
            System.out.printf("%-4d %-20s %-15s %-12s %-8.1f %-10.1f %-10.1f%n",
                rank++,
                truncate(result.getSchedulerName(), 20),
                truncate(result.getNetworkModelName(), 15),
                truncate(result.getWorkloadName(), 12),
                result.getCompositeScore(),
                summary.getCompletionRate() * 100,
                summary.getAverageThroughput() / 1024);
        }
    }
}