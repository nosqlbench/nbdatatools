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

/// Demonstration of enhanced simulation features including session concurrency limits,
/// minimum unloaded latency, and individual request latency measurement.
public class EnhancedSimulationDemo {
    
    private static final long FILE_SIZE = 10_000_000L; // 10MB
    private static final long CHUNK_SIZE = 65536L; // 64KB chunks
    private static final double SIMULATION_DURATION = 30.0; // 30 seconds
    
    @Test
    public void demonstrateEnhancedNetworkModel() {
        System.out.println("ENHANCED NETWORK MODEL DEMONSTRATION");
        System.out.println("====================================");
        System.out.println();
        
        NetworkModel[] networks = NetworkModel.Presets.getAllPresets();
        
        System.out.println("Network Models with Session Concurrency Limits and Unloaded Latency:");
        System.out.println("-".repeat(80));
        System.out.printf("%-20s %-10s %-10s %-8s %-8s %-10s %-10s%n",
                         "Network", "Bandwidth", "Latency", "MaxConn", "MaxSess", "UnloadedLat", "FailRate");
        System.out.println("-".repeat(80));
        
        for (NetworkModel network : networks) {
            System.out.printf("%-20s %-10.1f %-10.0f %-8d %-8d %-10.1f %-10.2f%%%n",
                             network.getName(),
                             (network.getBandwidthBps() * 8.0) / 1_000_000, // Mbps
                             network.getLatencySeconds() * 1000, // ms
                             network.getMaxConcurrentConnections(),
                             network.getMaxSessionConcurrency(),
                             network.getUnloadedLatencySeconds() * 1000, // ms
                             network.getFailureRate() * 100);
        }
        
        System.out.println();
        
        // Demonstrate transfer time calculation with session concurrency
        NetworkModel network = NetworkModel.Presets.BROADBAND_STANDARD;
        long chunkBytes = 65536L;
        
        System.out.printf("Transfer time calculation for %s with 64KB chunk:%n", network.getName());
        System.out.println("-".repeat(60));
        System.out.printf("%-15s %-15s %-15s %-15s%n", "Connections", "Session", "Transfer Time", "Difference");
        System.out.println("-".repeat(60));
        
        double baseTime = network.computeTransferTime(chunkBytes, 1, 1);
        
        for (int connections = 1; connections <= 4; connections++) {
            for (int session = 1; session <= Math.min(4, network.getMaxSessionConcurrency() + 1); session++) {
                double transferTime = network.computeTransferTime(chunkBytes, connections, session);
                double difference = transferTime / baseTime;
                
                System.out.printf("%-15d %-15d %-15.3f %-15.2fx%n",
                                 connections, session, transferTime, difference);
            }
        }
        
        System.out.println();
    }
    
    @Test
    public void demonstrateLatencyMeasurement() {
        System.out.println("REQUEST LATENCY MEASUREMENT DEMONSTRATION");
        System.out.println("=========================================");
        System.out.println();
        
        // Test with different scheduler/workload combinations
        String[] schedulerTypes = {"Default", "Aggressive", "Conservative", "Adaptive"};
        WorkloadGeneratorFactory.WorkloadType[] workloadTypes = {
            WorkloadGeneratorFactory.WorkloadType.SEQUENTIAL,
            WorkloadGeneratorFactory.WorkloadType.RANDOM,
            WorkloadGeneratorFactory.WorkloadType.BURSTY
        };
        
        NetworkModel network = NetworkModel.Presets.BROADBAND_FAST;
        
        System.out.printf("Testing latency with %s network:%n", network.getName());
        System.out.println("-".repeat(90));
        System.out.printf("%-20s %-12s %-8s %-10s %-10s %-10s %-10s%n",
                         "Scheduler", "Workload", "Requests", "AvgLatency", "MedianLat", "P95Lat", "Score");
        System.out.println("-".repeat(90));
        
        for (String schedulerType : schedulerTypes) {
            for (WorkloadGeneratorFactory.WorkloadType workloadType : workloadTypes) {
                EventDrivenScheduler scheduler = createScheduler(schedulerType);
                WorkloadGenerator workload = WorkloadGeneratorFactory.create(
                    workloadType, FILE_SIZE, CHUNK_SIZE, 42L);
                
                EventDrivenSimulation simulation = new EventDrivenSimulation(
                    FILE_SIZE, CHUNK_SIZE, SIMULATION_DURATION)
                    .withScheduler(scheduler)
                    .withNetworkModel(network)
                    .withWorkloadGenerator(workload);
                
                SimulationResults results = simulation.run();
                SimulationStatistics.StatisticsSummary summary = results.getStatistics().getSummary();
                
                System.out.printf("%-20s %-12s %-8d %-10.3f %-10.3f %-10.3f %-10.1f%n",
                                 truncate(schedulerType, 20),
                                 truncate(workload.getName(), 12),
                                 summary.getTotalRequests(),
                                 summary.getAverageRequestLatency(),
                                 summary.getMedianRequestLatency(),
                                 summary.getP95RequestLatency(),
                                 results.getCompositeScore());
            }
        }
        
        System.out.println();
    }
    
    @Test
    public void demonstrateSessionConcurrencyEffects() {
        System.out.println("SESSION CONCURRENCY EFFECTS DEMONSTRATION");
        System.out.println("==========================================");
        System.out.println();
        
        // Create networks with different session limits to show the effect
        NetworkModel[] testNetworks = {
            new NetworkModel("Low Session Limit", 10_000_000L, 0.020, 8, 2, 0.008, 0.0, 0.0, 42L),
            new NetworkModel("Medium Session Limit", 10_000_000L, 0.020, 8, 4, 0.008, 0.0, 0.0, 42L),
            new NetworkModel("High Session Limit", 10_000_000L, 0.020, 8, 8, 0.008, 0.0, 0.0, 42L)
        };
        
        EventDrivenScheduler scheduler = createScheduler("Aggressive");
        WorkloadGenerator workload = WorkloadGeneratorFactory.create(
            WorkloadGeneratorFactory.WorkloadType.BURSTY, FILE_SIZE, CHUNK_SIZE, 42L);
        
        System.out.println("Effect of session concurrency limits on performance:");
        System.out.println("-".repeat(75));
        System.out.printf("%-20s %-12s %-10s %-10s %-10s %-10s%n",
                         "Network", "SessionLimit", "Requests", "AvgLatency", "P95Lat", "Score");
        System.out.println("-".repeat(75));
        
        for (NetworkModel network : testNetworks) {
            EventDrivenSimulation simulation = new EventDrivenSimulation(
                FILE_SIZE, CHUNK_SIZE, SIMULATION_DURATION)
                .withScheduler(scheduler)
                .withNetworkModel(network)
                .withWorkloadGenerator(workload);
            
            SimulationResults results = simulation.run();
            SimulationStatistics.StatisticsSummary summary = results.getStatistics().getSummary();
            
            System.out.printf("%-20s %-12d %-10d %-10.3f %-10.3f %-10.1f%n",
                             truncate(network.getName(), 20),
                             network.getMaxSessionConcurrency(),
                             summary.getTotalRequests(),
                             summary.getAverageRequestLatency(),
                             summary.getP95RequestLatency(),
                             results.getCompositeScore());
        }
        
        System.out.println();
        System.out.println("Notice how lower session limits increase latency due to queueing delays!");
        System.out.println();
    }
    
    /// Creates a scheduler instance by type name.
    /// 
    /// @param schedulerType The type of scheduler to create
    /// @return A new scheduler instance
    private EventDrivenScheduler createScheduler(String schedulerType) {
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
    
    /// Truncates a string to a maximum length.
    /// 
    /// @param str The string to truncate
    /// @param maxLength Maximum length
    /// @return Truncated string
    private String truncate(String str, int maxLength) {
        return str.length() <= maxLength ? str : str.substring(0, maxLength - 3) + "...";
    }
}