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

/// Debug test to isolate issues in the event-driven simulation.
public class EventSimDebugTest {
    
    private static final long FILE_SIZE = 1_000_000L; // 1MB
    private static final long CHUNK_SIZE = 65536L; // 64KB chunks (16 total)
    private static final double SIMULATION_DURATION = 10.0; // 10 seconds
    private static final long RANDOM_SEED = 42L;
    
    @Test
    public void debugLocalhostRandomAccess() {
        System.out.println("DEBUG: Localhost + Random Access");
        System.out.println("=================================");
        
        // Test configuration that should work but doesn't
        EventDrivenScheduler scheduler = new DefaultEventDrivenScheduler();
        NetworkModel network = NetworkModel.Presets.LOCALHOST;
        WorkloadGenerator workload = WorkloadGeneratorFactory.create(
            WorkloadGeneratorFactory.WorkloadType.RANDOM, FILE_SIZE, CHUNK_SIZE, RANDOM_SEED);
        
        System.out.printf("Network: %s (bandwidth=%d bps, latency=%.3fs, connections=%d)%n",
                         network.getName(), network.getBandwidthBps(), network.getLatencySeconds(),
                         network.getMaxConcurrentConnections());
        
        // Check workload generation
        List<ReadRequestEvent> events = workload.generateEvents(SIMULATION_DURATION);
        System.out.printf("Workload generated %d events%n", events.size());
        
        if (events.size() > 0) {
            ReadRequestEvent firstEvent = events.get(0);
            System.out.printf("First event: time=%.3f, offset=%d, length=%d%n",
                             firstEvent.getScheduledTime(), firstEvent.getOffset(), firstEvent.getLength());
            
            // Test scheduler manually
            List<DownloadStartEvent> downloads = scheduler.scheduleDownloads(
                firstEvent.getScheduledTime(), firstEvent.getOffset(), firstEvent.getLength(),
                CHUNK_SIZE, FILE_SIZE, network.getMaxConcurrentConnections());
            
            System.out.printf("Scheduler generated %d download events%n", downloads.size());
            
            for (int i = 0; i < Math.min(3, downloads.size()); i++) {
                DownloadStartEvent download = downloads.get(i);
                System.out.printf("  Download %d: chunk=%d, offset=%d, size=%d, connectionId=%d%n",
                                 i, download.getNodeIndex(), download.getByteOffset(), 
                                 download.getByteSize(), download.getConnectionId());
            }
        }
        
        // Run full simulation
        EventDrivenSimulation simulation = new EventDrivenSimulation(FILE_SIZE, CHUNK_SIZE, SIMULATION_DURATION)
            .withScheduler(scheduler)
            .withNetworkModel(network)
            .withWorkloadGenerator(workload);
        
        SimulationResults results = simulation.run();
        
        System.out.printf("Results: requests=%d, completed=%d, failed=%d, completion=%.1f%%,%n",
                         results.getStatistics().getSummary().getTotalRequests(),
                         results.getStatistics().getSummary().getCompletedDownloads(),
                         results.getStatistics().getSummary().getFailedDownloads(),
                         results.getStatistics().getDownloadCompletionRate() * 100);
        
        System.out.println();
    }
    
    @Test
    public void debugFiberSequentialAccess() {
        System.out.println("DEBUG: Fiber Gigabit + Sequential Access");
        System.out.println("========================================");
        
        // Test configuration that should work but doesn't
        EventDrivenScheduler scheduler = new DefaultEventDrivenScheduler();
        NetworkModel network = NetworkModel.Presets.FIBER_GIGABIT;
        WorkloadGenerator workload = WorkloadGeneratorFactory.create(
            WorkloadGeneratorFactory.WorkloadType.SEQUENTIAL, FILE_SIZE, CHUNK_SIZE, RANDOM_SEED);
        
        System.out.printf("Network: %s (bandwidth=%d bps, latency=%.3fs, connections=%d)%n",
                         network.getName(), network.getBandwidthBps(), network.getLatencySeconds(),
                         network.getMaxConcurrentConnections());
        
        // Test transfer time calculation
        double transferTime = network.computeTransferTime(CHUNK_SIZE, 1);
        System.out.printf("Transfer time for 64KB chunk: %.3f seconds%n", transferTime);
        
        // Check workload generation
        List<ReadRequestEvent> events = workload.generateEvents(SIMULATION_DURATION);
        System.out.printf("Workload generated %d events%n", events.size());
        
        if (events.size() > 0) {
            ReadRequestEvent firstEvent = events.get(0);
            System.out.printf("First event: time=%.3f, offset=%d, length=%d%n",
                             firstEvent.getScheduledTime(), firstEvent.getOffset(), firstEvent.getLength());
            
            // Calculate when download would complete
            double completionTime = firstEvent.getScheduledTime() + transferTime;
            System.out.printf("Expected completion time: %.3f seconds (within simulation: %s)%n",
                             completionTime, completionTime <= SIMULATION_DURATION ? "YES" : "NO");
        }
        
        // Run full simulation
        EventDrivenSimulation simulation = new EventDrivenSimulation(FILE_SIZE, CHUNK_SIZE, SIMULATION_DURATION)
            .withScheduler(scheduler)
            .withNetworkModel(network)
            .withWorkloadGenerator(workload);
        
        SimulationResults results = simulation.run();
        
        System.out.printf("Results: requests=%d, completed=%d, failed=%d, completion=%.1f%%%n",
                         results.getStatistics().getSummary().getTotalRequests(),
                         results.getStatistics().getSummary().getCompletedDownloads(),
                         results.getStatistics().getSummary().getFailedDownloads(),
                         results.getStatistics().getDownloadCompletionRate() * 100);
        
        System.out.println();
    }
    
    @Test
    public void debugConnectionHandling() {
        System.out.println("DEBUG: Connection Handling");
        System.out.println("==========================");
        
        EventDrivenSimulation simulation = new EventDrivenSimulation(FILE_SIZE, CHUNK_SIZE, SIMULATION_DURATION);
        NetworkModel network = NetworkModel.Presets.FIBER_GIGABIT;
        
        simulation.withNetworkModel(network);
        
        System.out.printf("Network max connections: %d%n", network.getMaxConcurrentConnections());
        // Create a scheduler and test download scheduling
        EventDrivenScheduler scheduler = new DefaultEventDrivenScheduler();
        List<DownloadStartEvent> downloads = scheduler.scheduleDownloads(
            0.1, 0, (int)CHUNK_SIZE, CHUNK_SIZE, FILE_SIZE, network.getMaxConcurrentConnections());
        
        System.out.printf("Scheduler wants to start %d downloads%n", downloads.size());
        
        for (int i = 0; i < downloads.size(); i++) {
            DownloadStartEvent download = downloads.get(i);
            System.out.printf("  Download %d: connectionId=%d, chunk=%d%n", 
                             i, download.getConnectionId(), download.getNodeIndex());
        }
        
        System.out.println();
    }
}