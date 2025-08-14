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

import io.nosqlbench.vectordata.merklev2.MerkleShape;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/// Defines different workload patterns for scheduler testing.
/// 
/// This enum provides various access patterns that represent real-world
/// usage scenarios. Each workload generates a series of read requests
/// that exercise different aspects of scheduler performance.
/// 
/// Workload types:
/// - Sequential: Reading data from start to finish
/// - Random: Randomly distributed reads across the file
/// - Clustered: Reads concentrated in specific regions
/// - Sparse: Reads with large gaps between them
/// - Mixed: Combination of different patterns
/// 
/// Each workload considers the scheduler's ability to:
/// - Optimize for sequential access patterns
/// - Handle random access efficiently
/// - Minimize unnecessary downloads
/// - Adapt to different request sizes
public enum SchedulerWorkload {
    
    /// Sequential read pattern - reads file from beginning to end.
    /// 
    /// This workload tests how well schedulers handle predictable sequential
    /// access patterns, which should allow for aggressive prefetching and
    /// batch downloading optimizations.
    SEQUENTIAL_READ("Sequential Read") {
        @Override
        public List<WorkloadRequest> generateRequests(MerkleShape shape) {
            List<WorkloadRequest> requests = new ArrayList<>();
            
            long totalSize = shape.getTotalContentSize();
            long chunkSize = shape.getChunkSize();
            int requestSize = (int) Math.min(chunkSize * 4, 64 * 1024); // 4 chunks or 64KB
            
            for (long offset = 0; offset < totalSize; offset += requestSize) {
                int length = (int) Math.min(requestSize, totalSize - offset);
                requests.add(new WorkloadRequest(offset, length));
            }
            
            return requests;
        }
    },
    
    /// Random read pattern - randomly distributed reads across the file.
    /// 
    /// This workload tests scheduler performance with unpredictable access
    /// patterns where prefetching strategies may be less effective.
    RANDOM_READ("Random Read") {
        @Override
        public List<WorkloadRequest> generateRequests(MerkleShape shape) {
            List<WorkloadRequest> requests = new ArrayList<>();
            Random random = new Random(42); // Deterministic for testing
            
            long totalSize = shape.getTotalContentSize();
            long chunkSize = shape.getChunkSize();
            int requestSize = (int) Math.min(chunkSize, 32 * 1024); // 1 chunk or 32KB
            int numRequests = Math.min(100, (int) (totalSize / requestSize / 10)); // Sample 10% of possible requests
            
            for (int i = 0; i < numRequests; i++) {
                long maxOffset = totalSize - requestSize;
                long offset = maxOffset > 0 ? Math.abs(random.nextLong()) % maxOffset : 0;
                int length = (int) Math.min(requestSize, totalSize - offset);
                requests.add(new WorkloadRequest(offset, length));
            }
            
            return requests;
        }
    },
    
    /// Clustered read pattern - reads concentrated in specific regions.
    /// 
    /// This workload tests how well schedulers can identify and optimize
    /// for locality of reference, downloading entire regions efficiently.
    CLUSTERED_READ("Clustered Read") {
        @Override
        public List<WorkloadRequest> generateRequests(MerkleShape shape) {
            List<WorkloadRequest> requests = new ArrayList<>();
            Random random = new Random(42);
            
            long totalSize = shape.getTotalContentSize();
            long chunkSize = shape.getChunkSize();
            int requestSize = (int) Math.min(chunkSize / 4, 16 * 1024); // Small requests
            
            // Create 3-5 clusters
            int numClusters = 3 + random.nextInt(3);
            long clusterSize = totalSize / (numClusters * 2); // Leave gaps between clusters
            
            for (int cluster = 0; cluster < numClusters; cluster++) {
                long clusterStart = cluster * (clusterSize * 2);
                int requestsPerCluster = 10 + random.nextInt(20);
                
                for (int i = 0; i < requestsPerCluster; i++) {
                    long offset = clusterStart + Math.abs(random.nextLong()) % clusterSize;
                    if (offset + requestSize > totalSize) {
                        offset = totalSize - requestSize;
                    }
                    
                    requests.add(new WorkloadRequest(offset, requestSize));
                }
            }
            
            return requests;
        }
    },
    
    /// Sparse read pattern - reads with large gaps between them.
    /// 
    /// This workload tests scheduler efficiency when most of the file
    /// is not accessed, requiring careful optimization to avoid
    /// downloading unnecessary data.
    SPARSE_READ("Sparse Read") {
        @Override
        public List<WorkloadRequest> generateRequests(MerkleShape shape) {
            List<WorkloadRequest> requests = new ArrayList<>();
            
            long totalSize = shape.getTotalContentSize();
            long chunkSize = shape.getChunkSize();
            int requestSize = (int) Math.min(chunkSize / 8, 4 * 1024); // Small requests
            long gapSize = chunkSize * 5; // Large gaps
            
            for (long offset = 0; offset < totalSize; offset += gapSize) {
                if (offset + requestSize <= totalSize) {
                    requests.add(new WorkloadRequest(offset, requestSize));
                }
            }
            
            return requests;
        }
    },
    
    /// Mixed pattern - combination of sequential and random reads.
    /// 
    /// This workload provides a realistic mix of access patterns that
    /// might occur in real applications, testing scheduler adaptability.
    MIXED_READ("Mixed Read") {
        @Override
        public List<WorkloadRequest> generateRequests(MerkleShape shape) {
            List<WorkloadRequest> requests = new ArrayList<>();
            
            // Add some sequential reads (first 30% of requests)
            List<WorkloadRequest> sequential = SEQUENTIAL_READ.generateRequests(shape);
            int seqCount = Math.min(sequential.size(), sequential.size() * 30 / 100);
            requests.addAll(sequential.subList(0, seqCount));
            
            // Add some random reads (remaining requests)
            List<WorkloadRequest> random = RANDOM_READ.generateRequests(shape);
            requests.addAll(random);
            
            // Shuffle to mix the patterns
            java.util.Collections.shuffle(requests, new Random(42));
            
            return requests;
        }
    },
    
    /// Large read pattern - reads entire chunks or multiple chunks at once.
    /// 
    /// This workload tests scheduler performance with large read requests
    /// that span multiple chunks, which should enable bulk download optimizations.
    LARGE_READ("Large Read") {
        @Override
        public List<WorkloadRequest> generateRequests(MerkleShape shape) {
            List<WorkloadRequest> requests = new ArrayList<>();
            
            long totalSize = shape.getTotalContentSize();
            long chunkSize = shape.getChunkSize();
            int requestSize = (int) Math.min(chunkSize * 8, 512 * 1024); // 8 chunks or 512KB
            
            for (long offset = 0; offset < totalSize; offset += requestSize) {
                int length = (int) Math.min(requestSize, totalSize - offset);
                requests.add(new WorkloadRequest(offset, length));
            }
            
            return requests;
        }
    },
    
    /// Small read pattern - very small reads that test overhead efficiency.
    /// 
    /// This workload tests how well schedulers handle many small requests
    /// without excessive overhead, potentially requiring request coalescing.
    SMALL_READ("Small Read") {
        @Override
        public List<WorkloadRequest> generateRequests(MerkleShape shape) {
            List<WorkloadRequest> requests = new ArrayList<>();
            
            long totalSize = shape.getTotalContentSize();
            int requestSize = 1024; // 1KB requests
            long step = requestSize * 16; // Sample every 16KB
            
            for (long offset = 0; offset < totalSize; offset += step) {
                if (offset + requestSize <= totalSize) {
                    requests.add(new WorkloadRequest(offset, requestSize));
                }
            }
            
            return requests;
        }
    };
    
    private final String name;
    
    SchedulerWorkload(String name) {
        this.name = name;
    }
    
    /// Gets the human-readable name of this workload.
    /// 
    /// @return Workload name
    public String getName() {
        return name;
    }
    
    /// Generates a list of read requests for this workload pattern.
    /// 
    /// @param shape The merkle shape defining file structure
    /// @return List of read requests to execute
    public abstract List<WorkloadRequest> generateRequests(MerkleShape shape);
    
    /// Gets all available workloads for comprehensive testing.
    /// 
    /// @return Array of all workload types
    public static SchedulerWorkload[] getAllWorkloads() {
        return values();
    }
    
    /// Gets basic workloads suitable for quick testing.
    /// 
    /// @return Array of essential workload types
    public static SchedulerWorkload[] getBasicWorkloads() {
        return new SchedulerWorkload[] {
            SEQUENTIAL_READ,
            RANDOM_READ,
            CLUSTERED_READ
        };
    }
}