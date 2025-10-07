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

import java.util.*;

/// Base class for generating workload patterns in event-driven simulation.
/// 
/// Workload generators create sequences of read requests that occur over
/// simulation time. Each generator implements a different access pattern
/// that stresses schedulers in different ways.
/// 
/// Unlike the real-time simulation workloads, these generators produce
/// events with computed arrival times, allowing the simulation to process
/// them efficiently without waiting for real time to pass.
public abstract class WorkloadGenerator {
    
    protected final long fileSize;
    protected final long chunkSize;
    protected final int totalChunks;
    protected final Random random;
    protected final String name;
    
    /// Creates a workload generator.
    /// 
    /// @param name Human-readable name for this workload
    /// @param fileSize Total size of the file being accessed
    /// @param chunkSize Size of each chunk
    /// @param randomSeed Seed for reproducible random behavior
    protected WorkloadGenerator(String name, long fileSize, long chunkSize, long randomSeed) {
        this.name = name;
        this.fileSize = fileSize;
        this.chunkSize = chunkSize;
        this.totalChunks = (int) Math.ceil((double) fileSize / chunkSize);
        this.random = new Random(randomSeed);
    }
    
    /// Generates a sequence of read request events.
    /// 
    /// @param simulationDuration Total duration of simulation in seconds
    /// @return List of read request events ordered by time
    public abstract List<ReadRequestEvent> generateEvents(double simulationDuration);
    
    /// Gets the name of this workload generator.
    /// 
    /// @return Workload name
    public String getName() {
        return name;
    }
    
    /// Creates read request events with exponential inter-arrival times.
    /// 
    /// This simulates realistic request patterns where requests arrive
    /// at random intervals with a specified average rate.
    /// 
    /// @param requests List of request specifications
    /// @param requestRate Average requests per second
    /// @param startTime Time when requests should start arriving
    /// @return List of timed read request events
    protected List<ReadRequestEvent> createTimedEvents(List<RequestSpec> requests, 
                                                      double requestRate, double startTime) {
        List<ReadRequestEvent> events = new ArrayList<>();
        double currentTime = startTime;
        
        for (RequestSpec spec : requests) {
            // Exponential distribution for inter-arrival times
            double interArrivalTime = -Math.log(random.nextDouble()) / requestRate;
            currentTime += interArrivalTime;
            
            events.add(new ReadRequestEvent(currentTime, spec.offset, spec.length));
        }
        
        return events;
    }
    
    /// Helper class to specify a read request.
    protected static class RequestSpec {
        final long offset;
        final int length;
        
        public RequestSpec(long offset, int length) {
            this.offset = offset;
            this.length = length;
        }
    }
}

/// Sequential access workload generator.
/// 
/// Generates requests that read through the file sequentially from
/// beginning to end. This tests scheduler prefetching capabilities.
class SequentialWorkloadGenerator extends WorkloadGenerator {
    
    public SequentialWorkloadGenerator(long fileSize, long chunkSize, long randomSeed) {
        super("Sequential Access", fileSize, chunkSize, randomSeed);
    }
    
    @Override
    public List<ReadRequestEvent> generateEvents(double simulationDuration) {
        List<RequestSpec> requests = new ArrayList<>();
        
        // Generate sequential reads covering the entire file
        long requestSize = Math.min(chunkSize * 4, 64 * 1024); // 4 chunks or 64KB
        
        for (long offset = 0; offset < fileSize; offset += requestSize) {
            int length = (int) Math.min(requestSize, fileSize - offset);
            requests.add(new RequestSpec(offset, length));
        }
        
        // Calculate request rate to complete within simulation duration
        double requestRate = requests.size() / (simulationDuration * 0.8); // Use 80% of time
        
        return createTimedEvents(requests, requestRate, 0.1); // Start after 100ms
    }
}

/// Random access workload generator.
/// 
/// Generates randomly distributed read requests across the file.
/// This tests scheduler efficiency with unpredictable access patterns.
class RandomWorkloadGenerator extends WorkloadGenerator {
    
    public RandomWorkloadGenerator(long fileSize, long chunkSize, long randomSeed) {
        super("Random Access", fileSize, chunkSize, randomSeed);
    }
    
    @Override
    public List<ReadRequestEvent> generateEvents(double simulationDuration) {
        List<RequestSpec> requests = new ArrayList<>();
        
        long requestSize = Math.min(chunkSize, 32 * 1024); // 1 chunk or 32KB
        int numRequests = Math.min(200, totalChunks / 5); // Sample 20% of chunks
        
        for (int i = 0; i < numRequests; i++) {
            long maxOffset = fileSize - requestSize;
            long offset = maxOffset > 0 ? Math.abs(random.nextLong()) % maxOffset : 0;
            int length = (int) Math.min(requestSize, fileSize - offset);
            
            requests.add(new RequestSpec(offset, length));
        }
        
        double requestRate = requests.size() / (simulationDuration * 0.9);
        
        return createTimedEvents(requests, requestRate, 0.1);
    }
}

/// Sparse access workload generator.
/// 
/// Generates reads with large gaps between them, testing scheduler
/// efficiency when most of the file is not accessed.
class SparseWorkloadGenerator extends WorkloadGenerator {
    
    public SparseWorkloadGenerator(long fileSize, long chunkSize, long randomSeed) {
        super("Sparse Access", fileSize, chunkSize, randomSeed);
    }
    
    @Override
    public List<ReadRequestEvent> generateEvents(double simulationDuration) {
        List<RequestSpec> requests = new ArrayList<>();
        
        long requestSize = Math.min(chunkSize / 8, 4 * 1024); // Small requests
        long gapSize = chunkSize * 5; // Large gaps
        
        for (long offset = 0; offset < fileSize; offset += gapSize) {
            if (offset + requestSize <= fileSize) {
                requests.add(new RequestSpec(offset, (int) requestSize));
            }
        }
        
        double requestRate = requests.size() / (simulationDuration * 0.7);
        
        return createTimedEvents(requests, requestRate, 0.1);
    }
}

/// Clustered access workload generator.
/// 
/// Generates clusters of requests in specific regions with gaps between
/// clusters. This tests regional optimization strategies.
class ClusteredWorkloadGenerator extends WorkloadGenerator {
    
    public ClusteredWorkloadGenerator(long fileSize, long chunkSize, long randomSeed) {
        super("Clustered Access", fileSize, chunkSize, randomSeed);
    }
    
    @Override
    public List<ReadRequestEvent> generateEvents(double simulationDuration) {
        List<RequestSpec> requests = new ArrayList<>();
        
        long requestSize = Math.min(chunkSize / 4, 16 * 1024); // Small requests
        
        // Create 3-5 clusters
        int numClusters = 3 + random.nextInt(3);
        long clusterSize = fileSize / (numClusters * 2); // Leave gaps between clusters
        
        for (int cluster = 0; cluster < numClusters; cluster++) {
            long clusterStart = cluster * (clusterSize * 2);
            int requestsPerCluster = 10 + random.nextInt(20);
            
            for (int i = 0; i < requestsPerCluster; i++) {
                long offset = clusterStart + Math.abs(random.nextLong()) % clusterSize;
                if (offset + requestSize > fileSize) {
                    offset = fileSize - requestSize;
                }
                
                requests.add(new RequestSpec(offset, (int) requestSize));
            }
        }
        
        double requestRate = requests.size() / (simulationDuration * 0.8);
        
        return createTimedEvents(requests, requestRate, 0.1);
    }
}

/// Bursty workload generator.
/// 
/// Generates bursts of requests followed by quiet periods.
/// This tests scheduler behavior under variable load conditions.
class BurstyWorkloadGenerator extends WorkloadGenerator {
    
    public BurstyWorkloadGenerator(long fileSize, long chunkSize, long randomSeed) {
        super("Bursty Access", fileSize, chunkSize, randomSeed);
    }
    
    @Override
    public List<ReadRequestEvent> generateEvents(double simulationDuration) {
        List<ReadRequestEvent> events = new ArrayList<>();
        
        long requestSize = Math.min(chunkSize / 2, 32 * 1024);
        double currentTime = 0.1; // Start after 100ms
        
        // Generate bursts
        int numBursts = (int) (simulationDuration / 5.0); // One burst every 5 seconds
        
        for (int burst = 0; burst < numBursts; burst++) {
            // Burst period: many requests
            int burstSize = 20 + random.nextInt(30);
            double burstDuration = 0.5; // 500ms burst
            
            for (int i = 0; i < burstSize; i++) {
                long offset = Math.abs(random.nextLong()) % (fileSize - requestSize);
                double eventTime = currentTime + (i * burstDuration / burstSize);
                events.add(new ReadRequestEvent(eventTime, offset, (int) requestSize));
            }
            
            currentTime += burstDuration;
            
            // Quiet period: no requests
            double quietDuration = 3.0 + random.nextDouble() * 2.0; // 3-5 seconds
            currentTime += quietDuration;
        }
        
        return events;
    }
}

/// Factory for creating workload generators.
class WorkloadGeneratorFactory {
    
    /// Creates a workload generator of the specified type.
    /// 
    /// @param type The type of workload to create
    /// @param fileSize Size of the file being accessed
    /// @param chunkSize Size of each chunk
    /// @param randomSeed Seed for reproducible behavior
    /// @return A workload generator instance
    public static WorkloadGenerator create(WorkloadType type, long fileSize, 
                                         long chunkSize, long randomSeed) {
        switch (type) {
            case SEQUENTIAL:
                return new SequentialWorkloadGenerator(fileSize, chunkSize, randomSeed);
            case RANDOM:
                return new RandomWorkloadGenerator(fileSize, chunkSize, randomSeed);
            case SPARSE:
                return new SparseWorkloadGenerator(fileSize, chunkSize, randomSeed);
            case CLUSTERED:
                return new ClusteredWorkloadGenerator(fileSize, chunkSize, randomSeed);
            case BURSTY:
                return new BurstyWorkloadGenerator(fileSize, chunkSize, randomSeed);
            default:
                throw new IllegalArgumentException("Unknown workload type: " + type);
        }
    }
    
    /// Available workload types.
    public enum WorkloadType {
        SEQUENTIAL,
        RANDOM,
        SPARSE,
        CLUSTERED,
        BURSTY
    }
}