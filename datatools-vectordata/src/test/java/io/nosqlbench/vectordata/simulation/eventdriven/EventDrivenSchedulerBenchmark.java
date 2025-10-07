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

/// Comprehensive benchmark for event-driven scheduler implementations.
/// 
/// This test evaluates all combinations of schedulers, network models,
/// and workload patterns, providing detailed performance comparisons.
/// The event-driven approach allows testing many scenarios quickly
/// without consuming significant resources.
public class EventDrivenSchedulerBenchmark {
    
    private static final long FILE_SIZE = 100_000_000L; // 100MB
    private static final long CHUNK_SIZE = 65536L; // 64KB
    private static final double SIMULATION_DURATION = 60.0; // 60 seconds
    private static final long RANDOM_SEED = 42L;
    
    @Test
    public void benchmarkAllSchedulerCombinations() {
        System.out.println("Event-Driven Scheduler Performance Benchmark");
        System.out.println("============================================");
        System.out.println();
        
        List<SimulationResults> allResults = runAllCombinations();
        
        // Print results in various formats
        printSummaryTable(allResults);
        printSchedulerComparison(allResults);
        printNetworkAnalysis(allResults);
        printWorkloadAnalysis(allResults);
        printTopPerformers(allResults);
    }
    
    /// Runs all combinations of schedulers, networks, and workloads.
    /// 
    /// @return List of all simulation results
    private List<SimulationResults> runAllCombinations() {
        List<SimulationResults> results = new ArrayList<>();
        
        // Define test configurations
        EventDrivenScheduler[] schedulers = {
            new DefaultEventDrivenScheduler(),
            new AggressiveEventDrivenScheduler(),
            new ConservativeEventDrivenScheduler(),
            new AdaptiveEventDrivenScheduler()
        };
        
        NetworkModel[] networks = NetworkModel.Presets.getAllPresets();
        
        WorkloadGeneratorFactory.WorkloadType[] workloads = {
            WorkloadGeneratorFactory.WorkloadType.SEQUENTIAL,
            WorkloadGeneratorFactory.WorkloadType.RANDOM,
            WorkloadGeneratorFactory.WorkloadType.SPARSE,
            WorkloadGeneratorFactory.WorkloadType.CLUSTERED,
            WorkloadGeneratorFactory.WorkloadType.BURSTY
        };
        
        int totalCombinations = schedulers.length * networks.length * workloads.length;
        int completed = 0;
        
        System.out.printf("Running %d test combinations...%n", totalCombinations);
        
        for (EventDrivenScheduler scheduler : schedulers) {
            for (NetworkModel network : networks) {
                for (WorkloadGeneratorFactory.WorkloadType workloadType : workloads) {
                    WorkloadGenerator workload = WorkloadGeneratorFactory.create(
                        workloadType, FILE_SIZE, CHUNK_SIZE, RANDOM_SEED);
                    
                    SimulationResults result = runSingleSimulation(scheduler, network, workload);
                    results.add(result);
                    
                    completed++;
                    if (completed % 10 == 0 || completed == totalCombinations) {
                        System.out.printf("Progress: %d/%d (%.1f%%)%n", 
                                        completed, totalCombinations, 
                                        100.0 * completed / totalCombinations);
                    }
                }
            }
        }
        
        System.out.println("All simulations completed!");
        System.out.println();
        
        return results;
    }
    
    /// Runs a single simulation configuration.
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
    
    /// Prints a comprehensive summary table.
    /// 
    /// @param results All simulation results
    private void printSummaryTable(List<SimulationResults> results) {
        System.out.println("COMPREHENSIVE RESULTS TABLE");
        System.out.println("===========================");
        
        results.get(0).printTabularResults(true);
        
        for (SimulationResults result : results) {
            result.printTabularResults(false);
        }
        
        System.out.println();
    }
    
    /// Prints scheduler performance comparison.
    /// 
    /// @param results All simulation results
    private void printSchedulerComparison(List<SimulationResults> results) {
        System.out.println("SCHEDULER PERFORMANCE COMPARISON");
        System.out.println("===============================");
        
        Map<String, List<Double>> schedulerScores = new HashMap<>();
        
        for (SimulationResults result : results) {
            schedulerScores.computeIfAbsent(result.getSchedulerName(), k -> new ArrayList<>())
                          .add(result.getCompositeScore());
        }
        
        System.out.printf("%-30s %-10s %-10s %-10s %-10s%n", 
                         "Scheduler", "Count", "Avg Score", "Min Score", "Max Score");
        System.out.println("-".repeat(80));
        
        schedulerScores.entrySet().stream()
            .sorted((a, b) -> Double.compare(
                b.getValue().stream().mapToDouble(Double::doubleValue).average().orElse(0.0),
                a.getValue().stream().mapToDouble(Double::doubleValue).average().orElse(0.0)))
            .forEach(entry -> {
                String name = entry.getKey();
                List<Double> scores = entry.getValue();
                DoubleSummaryStatistics stats = scores.stream().mapToDouble(Double::doubleValue).summaryStatistics();
                
                System.out.printf("%-30s %-10d %-10.1f %-10.1f %-10.1f%n",
                                name, stats.getCount(), stats.getAverage(), 
                                stats.getMin(), stats.getMax());
            });
        
        System.out.println();
    }
    
    /// Prints network condition analysis.
    /// 
    /// @param results All simulation results
    private void printNetworkAnalysis(List<SimulationResults> results) {
        System.out.println("NETWORK CONDITIONS ANALYSIS");
        System.out.println("===========================");
        
        Map<String, List<Double>> networkScores = new HashMap<>();
        
        for (SimulationResults result : results) {
            networkScores.computeIfAbsent(result.getNetworkModelName(), k -> new ArrayList<>())
                        .add(result.getCompositeScore());
        }
        
        System.out.printf("%-20s %-10s %-10s %-15s%n", 
                         "Network", "Count", "Avg Score", "Best Scheduler");
        System.out.println("-".repeat(70));
        
        for (String network : networkScores.keySet()) {
            List<Double> scores = networkScores.get(network);
            double avgScore = scores.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
            
            // Find best scheduler for this network
            String bestScheduler = results.stream()
                .filter(r -> r.getNetworkModelName().equals(network))
                .max(Comparator.comparing(SimulationResults::getCompositeScore))
                .map(SimulationResults::getSchedulerName)
                .orElse("Unknown");
            
            System.out.printf("%-20s %-10d %-10.1f %-15s%n",
                            network, scores.size(), avgScore, bestScheduler);
        }
        
        System.out.println();
    }
    
    /// Prints workload pattern analysis.
    /// 
    /// @param results All simulation results
    private void printWorkloadAnalysis(List<SimulationResults> results) {
        System.out.println("WORKLOAD PATTERNS ANALYSIS");
        System.out.println("==========================");
        
        Map<String, List<Double>> workloadScores = new HashMap<>();
        
        for (SimulationResults result : results) {
            workloadScores.computeIfAbsent(result.getWorkloadName(), k -> new ArrayList<>())
                         .add(result.getCompositeScore());
        }
        
        System.out.printf("%-15s %-10s %-10s %-15s%n", 
                         "Workload", "Count", "Avg Score", "Best Scheduler");
        System.out.println("-".repeat(65));
        
        workloadScores.entrySet().stream()
            .sorted((a, b) -> Double.compare(
                b.getValue().stream().mapToDouble(Double::doubleValue).average().orElse(0.0),
                a.getValue().stream().mapToDouble(Double::doubleValue).average().orElse(0.0)))
            .forEach(entry -> {
                String workload = entry.getKey();
                List<Double> scores = entry.getValue();
                double avgScore = scores.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
                
                String bestScheduler = results.stream()
                    .filter(r -> r.getWorkloadName().equals(workload))
                    .max(Comparator.comparing(SimulationResults::getCompositeScore))
                    .map(SimulationResults::getSchedulerName)
                    .orElse("Unknown");
                
                System.out.printf("%-15s %-10d %-10.1f %-15s%n",
                                workload, scores.size(), avgScore, bestScheduler);
            });
        
        System.out.println();
    }
    
    /// Prints top performing configurations.
    /// 
    /// @param results All simulation results
    private void printTopPerformers(List<SimulationResults> results) {
        System.out.println("TOP 10 PERFORMING CONFIGURATIONS");
        System.out.println("================================");
        
        results.stream()
            .sorted((a, b) -> Double.compare(b.getCompositeScore(), a.getCompositeScore()))
            .limit(10)
            .forEach(result -> {
                System.out.printf("Score: %6.1f - %s on %s with %s%n",
                                result.getCompositeScore(),
                                result.getSchedulerName(),
                                result.getNetworkModelName(),
                                result.getWorkloadName());
            });
        
        System.out.println();
        
        // Print worst 5 for comparison
        System.out.println("WORST 5 PERFORMING CONFIGURATIONS");
        System.out.println("=================================");
        
        results.stream()
            .sorted(Comparator.comparing(SimulationResults::getCompositeScore))
            .limit(5)
            .forEach(result -> {
                System.out.printf("Score: %6.1f - %s on %s with %s%n",
                                result.getCompositeScore(),
                                result.getSchedulerName(),
                                result.getNetworkModelName(),
                                result.getWorkloadName());
            });
        
        System.out.println();
    }
}