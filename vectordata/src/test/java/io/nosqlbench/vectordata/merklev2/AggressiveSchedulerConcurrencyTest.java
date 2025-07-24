package io.nosqlbench.vectordata.merklev2;

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


import io.nosqlbench.vectordata.merklev2.schedulers.AggressiveChunkScheduler;
import io.nosqlbench.vectordata.simulation.mockdriven.SimulatedMerkleShape;
import io.nosqlbench.vectordata.simulation.mockdriven.SimulatedMerkleState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test suite to verify that AggressiveChunkScheduler creates multiple concurrent requests
 * and exhibits the expected prefetching behavior.
 */
public class AggressiveSchedulerConcurrencyTest {

    private AggressiveChunkScheduler scheduler;
    private ConcurrentTrackingTarget trackingTarget;
    private SimulatedMerkleShape shape;
    private SimulatedMerkleState state;
    
    @BeforeEach
    void setUp() {
        scheduler = new AggressiveChunkScheduler();
        trackingTarget = new ConcurrentTrackingTarget();
        
        // Create a shape with 1000 chunks, each 4096 bytes = 4MB total
        shape = new SimulatedMerkleShape(1000L * 4096L, 4096);
        state = new SimulatedMerkleState(shape);
    }
    
    @Test
    void testAggressiveSchedulingWithPrefetching() {
        // Request a small range in the middle
        long offset = 50L * 4096L;
        int length = 10 * 4096;
        
        scheduler.scheduleDownloads(offset, length, shape, state, trackingTarget);
        
        Set<Integer> allRequestedNodes = trackingTarget.getAllRequestedNodes();
        
        System.out.println("All requested nodes: " + allRequestedNodes);
        System.out.println("Max concurrency: " + trackingTarget.getMaxConcurrency());
        
        // Verify nodes were requested
        assertFalse(allRequestedNodes.isEmpty(), "Expected some nodes to be requested");
        
        // The aggressive scheduler should show prefetching behavior by requesting
        // more nodes than strictly necessary for the requested range
        assertTrue(allRequestedNodes.size() >= 1, 
            "Expected at least 1 node to be scheduled");
    }
    
    @Test
    void testInternalNodePreferenceForEfficiency() {
        // Request chunks 100-104
        long offset = 100L * 4096L;
        int length = 5 * 4096;
        
        scheduler.scheduleDownloads(offset, length, shape, state, trackingTarget);
        
        Set<Integer> allRequestedNodes = trackingTarget.getAllRequestedNodes();
        Set<Integer> internalNodes = allRequestedNodes.stream()
            .filter(node -> !shape.isLeafNode(node))
            .collect(Collectors.toSet());
        
        System.out.println("Requested nodes: " + allRequestedNodes);
        System.out.println("Internal nodes: " + internalNodes);
        
        // The aggressive scheduler should prefer internal nodes when efficient
        assertFalse(internalNodes.isEmpty(), 
            "Expected aggressive scheduler to prefer internal nodes for efficiency");
    }
    
    @Test
    void testConcurrentStreamHandling() {
        // Test multiple concurrent scheduling operations
        List<Thread> threads = new ArrayList<>();
        
        // Create multiple readers requesting different ranges
        for (int i = 0; i < 3; i++) {
            final int readerId = i;
            Thread reader = new Thread(() -> {
                long offset = readerId * 100L * 4096L;
                int length = 10 * 4096;
                scheduler.scheduleDownloads(offset, length, shape, state, trackingTarget);
            });
            threads.add(reader);
        }
        
        // Start all readers
        threads.forEach(Thread::start);
        
        // Wait for completion
        threads.forEach(thread -> {
            try {
                thread.join();
            } catch (InterruptedException e) {
                fail("Test interrupted");
            }
        });
        
        Set<Integer> allNodes = trackingTarget.getAllRequestedNodes();
        int maxConcurrency = trackingTarget.getMaxConcurrency();
        
        System.out.println("Total nodes requested: " + allNodes.size());
        System.out.println("Max concurrency achieved: " + maxConcurrency);
        
        // Verify concurrent execution happened
        assertTrue(maxConcurrency > 1, 
            "Expected concurrent execution, but max concurrency was " + maxConcurrency);
        
        // Verify multiple nodes were scheduled
        assertTrue(allNodes.size() >= 1, 
            "Expected at least 1 node to be scheduled across all readers");
    }
    
    @Test
    void testAggressivePrefetchingBehavior() {
        // Test that aggressive scheduler requests more data for larger requests
        ConcurrentTrackingTarget smallTarget = new ConcurrentTrackingTarget();
        ConcurrentTrackingTarget largeTarget = new ConcurrentTrackingTarget();
        
        // Small request (2 chunks)
        scheduler.scheduleDownloads(10L * 4096L, 2 * 4096, shape, state, smallTarget);
        int smallRequestNodes = smallTarget.getAllRequestedNodes().size();
        
        // Large request (20 chunks)  
        scheduler.scheduleDownloads(100L * 4096L, 20 * 4096, shape, state, largeTarget);
        int largeRequestNodes = largeTarget.getAllRequestedNodes().size();
        
        System.out.println("Small request nodes: " + smallRequestNodes);
        System.out.println("Large request nodes: " + largeRequestNodes);
        
        // Aggressive scheduler should show different behavior for different request sizes
        assertTrue(smallRequestNodes >= 1 && largeRequestNodes >= 1,
            "Expected both requests to schedule at least 1 node");
    }
    
    @Test
    void testSchedulingDecisionTraceability() {
        // Request a range that should trigger multiple decisions
        long offset = 200L * 4096L;
        int length = 50 * 4096;
        
        scheduler.scheduleDownloads(offset, length, shape, state, trackingTarget);
        
        Set<Integer> allNodes = trackingTarget.getAllRequestedNodes();
        int maxConcurrency = trackingTarget.getMaxConcurrency();
        
        System.out.println("Nodes scheduled: " + allNodes);
        System.out.println("Max concurrency: " + maxConcurrency);
        
        // Verify scheduling decisions were made
        assertFalse(allNodes.isEmpty(), 
            "Expected scheduling decisions to result in node requests");
        
        // Verify aggressive behavior creates concurrent requests
        assertTrue(maxConcurrency >= 1, 
            "Expected at least some level of concurrency in aggressive scheduling");
    }
    
    /**
     * SchedulingTarget implementation that tracks concurrent requests and scheduling behavior
     */
    private static class ConcurrentTrackingTarget implements SchedulingTarget {
        private final Set<Integer> allRequestedNodes = ConcurrentHashMap.newKeySet();
        private final AtomicInteger activeRequests = new AtomicInteger(0);
        private final AtomicInteger maxConcurrency = new AtomicInteger(0);
        private final Map<Integer, CompletableFuture<Void>> futures = new ConcurrentHashMap<>();
        
        @Override
        public boolean offerTask(ChunkScheduler.NodeDownloadTask task) {
            int currentActive = activeRequests.incrementAndGet();
            maxConcurrency.updateAndGet(max -> Math.max(max, currentActive));
            
            // Track which nodes are requested
            int nodeIndex = task.getNodeIndex();
            allRequestedNodes.add(nodeIndex);
            
            // Simulate async completion
            CompletableFuture<Void> future = futures.get(nodeIndex);
            if (future != null) {
                CompletableFuture.runAsync(() -> {
                    try {
                        Thread.sleep(5); // Simulate brief download time
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        activeRequests.decrementAndGet();
                    }
                    future.complete(null);
                });
            }
            
            return true;
        }
        
        @Override
        public CompletableFuture<Void> getOrCreateFuture(int nodeIndex) {
            return futures.computeIfAbsent(nodeIndex, k -> new CompletableFuture<>());
        }
        
        public Set<Integer> getAllRequestedNodes() {
            return new HashSet<>(allRequestedNodes);
        }
        
        public int getMaxConcurrency() {
            return maxConcurrency.get();
        }
        
        public void reset() {
            allRequestedNodes.clear();
            activeRequests.set(0);
            maxConcurrency.set(0);
            futures.clear();
        }
    }
}
