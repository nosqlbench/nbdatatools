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

/// Demonstration of the event-driven simulation framework.
/// 
/// This test shows how to set up and run event-driven simulations
/// to evaluate scheduler performance under different conditions.
/// Unlike the real-time simulation, this approach is extremely fast
/// and can simulate hours of operation in milliseconds.
public class EventDrivenSimulationDemo {
    
    @Test
    public void demonstrateBasicSimulation() {
        System.out.println("Event-Driven Simulation Demonstration");
        System.out.println("=====================================");
        System.out.println();
        
        // Configuration
        long fileSize = 10_000_000L; // 10MB file
        long chunkSize = 65536L; // 64KB chunks
        double simulationDuration = 30.0; // 30 seconds
        
        // Create workload generator
        WorkloadGenerator workload = WorkloadGeneratorFactory.create(
            WorkloadGeneratorFactory.WorkloadType.SEQUENTIAL, 
            fileSize, chunkSize, 42L);
        
        // Test different schedulers on the same network and workload
        EventDrivenScheduler[] schedulers = {
            new DefaultEventDrivenScheduler(),
            new AggressiveEventDrivenScheduler(),
            new ConservativeEventDrivenScheduler(),
            new AdaptiveEventDrivenScheduler()
        };
        
        NetworkModel network = NetworkModel.Presets.BROADBAND_FAST;
        
        System.out.printf("Testing %d schedulers on %s with %s workload%n", 
                         schedulers.length, network.getName(), workload.getName());
        System.out.printf("File: %.1f MB, Chunks: %d, Duration: %.0f seconds%n%n",
                         fileSize / 1024.0 / 1024.0, (int) Math.ceil((double) fileSize / chunkSize), 
                         simulationDuration);
        
        // Run simulations
        for (EventDrivenScheduler scheduler : schedulers) {
            long startTime = System.nanoTime();
            
            EventDrivenSimulation simulation = new EventDrivenSimulation(fileSize, chunkSize, simulationDuration)
                .withScheduler(scheduler)
                .withNetworkModel(network)
                .withWorkloadGenerator(workload);
            
            SimulationResults results = simulation.run();
            
            long endTime = System.nanoTime();
            double executionTime = (endTime - startTime) / 1_000_000.0; // ms
            
            System.out.printf("%-25s: Score %5.1f (executed in %.1f ms)%n", 
                             scheduler.getName(), results.getCompositeScore(), executionTime);
        }
        
        System.out.println();
    }
    
    @Test
    public void demonstrateNetworkComparison() {
        System.out.println("Network Conditions Comparison");
        System.out.println("=============================");
        System.out.println();
        
        long fileSize = 50_000_000L; // 50MB file
        long chunkSize = 65536L; // 64KB chunks
        double simulationDuration = 60.0; // 1 minute
        
        EventDrivenScheduler scheduler = new AdaptiveEventDrivenScheduler();
        WorkloadGenerator workload = WorkloadGeneratorFactory.create(
            WorkloadGeneratorFactory.WorkloadType.RANDOM, 
            fileSize, chunkSize, 42L);
        
        NetworkModel[] networks = NetworkModel.Presets.getAllPresets();
        
        System.out.printf("Testing Adaptive Scheduler on %d network conditions%n", networks.length);
        System.out.printf("File: %.1f MB, Workload: %s, Duration: %.0f seconds%n%n",
                         fileSize / 1024.0 / 1024.0, workload.getName(), simulationDuration);
        
        for (NetworkModel network : networks) {
            EventDrivenSimulation simulation = new EventDrivenSimulation(fileSize, chunkSize, simulationDuration)
                .withScheduler(scheduler)
                .withNetworkModel(network)
                .withWorkloadGenerator(workload);
            
            SimulationResults results = simulation.run();
            
            System.out.printf("%-20s: Score %5.1f, Completion %5.1f%%, Throughput %6.1f KB/s%n",
                             network.getName(),
                             results.getCompositeScore(),
                             results.getStatistics().getDownloadCompletionRate() * 100,
                             results.getStatistics().getAverageThroughput() / 1024);
        }
        
        System.out.println();
    }
    
    @Test
    public void demonstrateWorkloadComparison() {
        System.out.println("Workload Patterns Comparison");
        System.out.println("============================");
        System.out.println();
        
        long fileSize = 20_000_000L; // 20MB file
        long chunkSize = 65536L; // 64KB chunks
        double simulationDuration = 45.0; // 45 seconds
        
        EventDrivenScheduler scheduler = new AggressiveEventDrivenScheduler();
        NetworkModel network = NetworkModel.Presets.BROADBAND_STANDARD;
        
        WorkloadGeneratorFactory.WorkloadType[] workloadTypes = {
            WorkloadGeneratorFactory.WorkloadType.SEQUENTIAL,
            WorkloadGeneratorFactory.WorkloadType.RANDOM,
            WorkloadGeneratorFactory.WorkloadType.SPARSE,
            WorkloadGeneratorFactory.WorkloadType.CLUSTERED,
            WorkloadGeneratorFactory.WorkloadType.BURSTY
        };
        
        System.out.printf("Testing %d workload patterns with Aggressive Scheduler%n", workloadTypes.length);
        System.out.printf("File: %.1f MB, Network: %s, Duration: %.0f seconds%n%n",
                         fileSize / 1024.0 / 1024.0, network.getName(), simulationDuration);
        
        for (WorkloadGeneratorFactory.WorkloadType workloadType : workloadTypes) {
            WorkloadGenerator workload = WorkloadGeneratorFactory.create(
                workloadType, fileSize, chunkSize, 42L);
            
            EventDrivenSimulation simulation = new EventDrivenSimulation(fileSize, chunkSize, simulationDuration)
                .withScheduler(scheduler)
                .withNetworkModel(network)
                .withWorkloadGenerator(workload);
            
            SimulationResults results = simulation.run();
            
            System.out.printf("%-15s: Score %5.1f, Requests %3d, Hit Rate %5.1f%%, Duration %6.3fs%n",
                             workload.getName(),
                             results.getCompositeScore(),
                             results.getStatistics().getSummary().getTotalRequests(),
                             results.getStatistics().getCacheHitRate() * 100,
                             results.getStatistics().getAverageDownloadDuration());
        }
        
        System.out.println();
    }
    
    @Test
    public void demonstrateSpeedComparison() {
        System.out.println("Event-Driven Simulation Speed Test");
        System.out.println("==================================");
        System.out.println();
        
        // Test with progressively larger simulations
        long[] fileSizes = {1_000_000L, 10_000_000L, 100_000_000L, 1_000_000_000L}; // 1MB to 1GB
        double[] durations = {10.0, 30.0, 60.0, 300.0}; // 10s to 5 minutes
        
        EventDrivenScheduler scheduler = new DefaultEventDrivenScheduler();
        NetworkModel network = NetworkModel.Presets.FIBER_GIGABIT;
        
        System.out.printf("Testing simulation speed with different problem sizes%n");
        System.out.printf("Scheduler: %s, Network: %s%n%n", scheduler.getName(), network.getName());
        
        for (int i = 0; i < fileSizes.length; i++) {
            long fileSize = fileSizes[i];
            double duration = durations[i];
            
            WorkloadGenerator workload = WorkloadGeneratorFactory.create(
                WorkloadGeneratorFactory.WorkloadType.RANDOM, 
                fileSize, 65536L, 42L);
            
            long startTime = System.nanoTime();
            
            EventDrivenSimulation simulation = new EventDrivenSimulation(fileSize, 65536L, duration)
                .withScheduler(scheduler)
                .withNetworkModel(network)
                .withWorkloadGenerator(workload);
            
            SimulationResults results = simulation.run();
            
            long endTime = System.nanoTime();
            double executionTime = (endTime - startTime) / 1_000_000.0; // ms
            
            int totalChunks = (int) Math.ceil((double) fileSize / 65536L);
            double speedRatio = (duration * 1000) / executionTime; // simulated time / real time
            
            System.out.printf("%-6s file, %3.0fs sim: %6.1f ms execution, %5dx speed, %4d chunks, Score %.1f%n",
                             formatFileSize(fileSize), duration, executionTime, speedRatio, 
                             totalChunks, results.getCompositeScore());
        }
        
        System.out.println();
        System.out.println("Event-driven simulation can process thousands of simulated seconds");
        System.out.println("in just milliseconds of real time, making it ideal for optimization!");
    }
    
    /// Formats file size for display.
    /// 
    /// @param bytes File size in bytes
    /// @return Formatted size string
    private String formatFileSize(long bytes) {
        if (bytes >= 1_000_000_000L) {
            return String.format("%.0fGB", bytes / 1_000_000_000.0);
        } else if (bytes >= 1_000_000L) {
            return String.format("%.0fMB", bytes / 1_000_000.0);
        } else if (bytes >= 1_000L) {
            return String.format("%.0fKB", bytes / 1_000.0);
        } else {
            return bytes + "B";
        }
    }
}