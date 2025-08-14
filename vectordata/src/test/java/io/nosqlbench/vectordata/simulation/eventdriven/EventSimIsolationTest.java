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
import java.util.List;

/// Test to isolate specific failing combination from comprehensive test.
public class EventSimIsolationTest {
    
    // Same parameters as comprehensive test
    private static final long FILE_SIZE = 50_000_000L; // 50MB
    private static final long CHUNK_SIZE = 65536L; // 64KB chunks
    private static final double SIMULATION_DURATION = 60.0; // 60 seconds
    private static final long RANDOM_SEED = 42L;
    
    @Test
    public void testSpecificFailingCombination() {
        System.out.println("ISOLATION TEST: Localhost + Random Access (Comprehensive Test Parameters)");
        System.out.println("=========================================================================");
        
        // Exact same as comprehensive test
        EventDrivenScheduler scheduler = new DefaultEventDrivenScheduler();
        NetworkModel network = NetworkModel.Presets.LOCALHOST;
        WorkloadGenerator workload = WorkloadGeneratorFactory.create(
            WorkloadGeneratorFactory.WorkloadType.RANDOM, FILE_SIZE, CHUNK_SIZE, RANDOM_SEED);
        
        System.out.printf("File: %.1fMB, Chunks: %d, Duration: %.0fs%n",
                         FILE_SIZE / 1024.0 / 1024.0, 
                         (int) Math.ceil((double) FILE_SIZE / CHUNK_SIZE),
                         SIMULATION_DURATION);
        
        // Check workload generation
        List<ReadRequestEvent> events = workload.generateEvents(SIMULATION_DURATION);
        System.out.printf("Workload generated %d events%n", events.size());
        
        if (events.size() > 0) {
            System.out.println("First 5 events:");
            for (int i = 0; i < Math.min(5, events.size()); i++) {
                ReadRequestEvent event = events.get(i);
                System.out.printf("  Event %d: time=%.3f, offset=%d, length=%d%n",
                                 i, event.getScheduledTime(), event.getOffset(), event.getLength());
            }
            
            ReadRequestEvent lastEvent = events.get(events.size() - 1);
            System.out.printf("Last event: time=%.3f (within simulation: %s)%n",
                             lastEvent.getScheduledTime(), 
                             lastEvent.getScheduledTime() < SIMULATION_DURATION ? "YES" : "NO");
        }
        
        // Test first event scheduling
        if (events.size() > 0) {
            ReadRequestEvent firstEvent = events.get(0);
            List<DownloadStartEvent> downloads = scheduler.scheduleDownloads(
                firstEvent.getScheduledTime(), firstEvent.getOffset(), firstEvent.getLength(),
                CHUNK_SIZE, FILE_SIZE, network.getMaxConcurrentConnections());
            
            System.out.printf("First event triggered %d downloads%n", downloads.size());
            
            if (downloads.size() > 0) {
                DownloadStartEvent firstDownload = downloads.get(0);
                double transferTime = network.computeTransferTime(firstDownload.getByteSize(), 1);
                double completionTime = firstEvent.getScheduledTime() + transferTime;
                
                System.out.printf("  First download: chunk=%d, transferTime=%.6fs, completion=%.3fs%n",
                                 firstDownload.getNodeIndex(), transferTime, completionTime);
                System.out.printf("  Completion within simulation: %s%n",
                                 completionTime < SIMULATION_DURATION ? "YES" : "NO");
            }
        }
        
        // Run simulation
        EventDrivenSimulation simulation = new EventDrivenSimulation(FILE_SIZE, CHUNK_SIZE, SIMULATION_DURATION)
            .withScheduler(scheduler)
            .withNetworkModel(network)
            .withWorkloadGenerator(workload);
        
        SimulationResults results = simulation.run();
        
        System.out.printf("RESULTS: requests=%d, completed=%d, failed=%d, completion=%.1f%%%n",
                         results.getStatistics().getSummary().getTotalRequests(),
                         results.getStatistics().getSummary().getCompletedDownloads(),
                         results.getStatistics().getSummary().getFailedDownloads(),
                         results.getStatistics().getDownloadCompletionRate() * 100);
        
        System.out.println();
    }
    
    @Test 
    public void testFiberSequentialWithLargeFile() {
        System.out.println("ISOLATION TEST: Fiber Gigabit + Sequential Access (Large File)");
        System.out.println("==============================================================");
        
        EventDrivenScheduler scheduler = new DefaultEventDrivenScheduler();
        NetworkModel network = NetworkModel.Presets.FIBER_GIGABIT;
        WorkloadGenerator workload = WorkloadGeneratorFactory.create(
            WorkloadGeneratorFactory.WorkloadType.SEQUENTIAL, FILE_SIZE, CHUNK_SIZE, RANDOM_SEED);
        
        System.out.printf("Network: %s (%.1f Mbps, %.0fms latency)%n",
                         network.getName(), 
                         (network.getBandwidthBps() * 8.0) / 1_000_000,
                         network.getLatencySeconds() * 1000);
        
        // Check if transfers can complete in time
        double transferTime = network.computeTransferTime(CHUNK_SIZE, 1);
        System.out.printf("Transfer time per chunk: %.6f seconds%n", transferTime);
        
        List<ReadRequestEvent> events = workload.generateEvents(SIMULATION_DURATION);
        System.out.printf("Generated %d events%n", events.size());
        
        if (events.size() > 0) {
            ReadRequestEvent firstEvent = events.get(0);
            ReadRequestEvent lastEvent = events.get(events.size() - 1);
            
            System.out.printf("Event timing: first=%.3fs, last=%.3fs%n",
                             firstEvent.getScheduledTime(), lastEvent.getScheduledTime());
            
            double latestCompletion = lastEvent.getScheduledTime() + transferTime;
            System.out.printf("Latest possible completion: %.3fs (within simulation: %s)%n",
                             latestCompletion, latestCompletion < SIMULATION_DURATION ? "YES" : "NO");
        }
        
        EventDrivenSimulation simulation = new EventDrivenSimulation(FILE_SIZE, CHUNK_SIZE, SIMULATION_DURATION)
            .withScheduler(scheduler)
            .withNetworkModel(network)
            .withWorkloadGenerator(workload);
        
        SimulationResults results = simulation.run();
        
        System.out.printf("RESULTS: requests=%d, completed=%d, failed=%d, completion=%.1f%%%n",
                         results.getStatistics().getSummary().getTotalRequests(),
                         results.getStatistics().getSummary().getCompletedDownloads(),
                         results.getStatistics().getSummary().getFailedDownloads(),
                         results.getStatistics().getDownloadCompletionRate() * 100);
        
        System.out.println();
    }
}