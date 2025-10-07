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

import io.nosqlbench.vectordata.merklev2.schedulers.AdaptiveChunkScheduler;
import io.nosqlbench.vectordata.merklev2.schedulers.AggressiveChunkScheduler;
import io.nosqlbench.vectordata.merklev2.schedulers.ConservativeChunkScheduler;
import io.nosqlbench.vectordata.merklev2.schedulers.DefaultChunkScheduler;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.ExecutionException;

/// Comprehensive example demonstrating the scheduler simulation framework.
/// 
/// This test class shows how to use all the simulation components together
/// to compare different scheduler implementations under various network
/// conditions and workload patterns.
/// 
/// The example includes:
/// - Multiple scheduler implementations (Default, Aggressive, Conservative, Adaptive)
/// - Various network conditions (from fiber to satellite)
/// - Different workload patterns (sequential, random, clustered, etc.)
/// - Performance analysis and comparison
/// 
/// Run this test to see a complete evaluation of scheduler performance
/// across different scenarios, helping to identify the optimal scheduler
/// for specific use cases.
@Tag("performance")
public class SchedulerSimulationExample {
    
    /// Comprehensive scheduler comparison test.
    /// 
    /// This test demonstrates the full capabilities of the simulation framework
    /// by running all schedulers against all network conditions and workloads.
    @Test
    public void testComprehensiveSchedulerComparison() throws InterruptedException, ExecutionException {
        System.out.println("=== Comprehensive Scheduler Simulation ===");
        System.out.println();
        
        // Create performance test with comprehensive configuration
        SchedulerPerformanceTest test = new SchedulerPerformanceTest()
            .withFileSize(50_000_000L) // 50MB file for reasonable test time
            .withChunkSize(1024 * 1024) // 1MB chunks
            .withSchedulers(
                new DefaultChunkScheduler(),
                new AggressiveChunkScheduler(),
                new ConservativeChunkScheduler(),
                new AdaptiveChunkScheduler()
            )
            .withNetworkConditions(
                NetworkConditions.Scenarios.FIBER,
                NetworkConditions.Scenarios.BROADBAND_FAST,
                NetworkConditions.Scenarios.BROADBAND_STANDARD,
                NetworkConditions.Scenarios.MOBILE_LTE
            )
            .withWorkloads(
                SchedulerWorkload.SEQUENTIAL_READ,
                SchedulerWorkload.RANDOM_READ,
                SchedulerWorkload.CLUSTERED_READ
            )
            .withIterations(3)
            .withTimeout(Duration.ofMinutes(2))
            .withRandomSeed(12345L);
        
        // Run the test
        PerformanceResults results = test.run();
        
        // Print comprehensive analysis
        results.printSummary();
    }
    
    /// Test focused on different network conditions.
    /// 
    /// This test evaluates how schedulers perform across the spectrum
    /// of network conditions, from high-speed fiber to slow satellite.
    @Test 
    public void testNetworkConditionComparison() throws InterruptedException, ExecutionException {
        System.out.println("=== Network Condition Comparison ===");
        System.out.println();
        
        // Test all common network scenarios
        SchedulerPerformanceTest test = new SchedulerPerformanceTest()
            .withFileSize(20_000_000L) // 20MB 
            .withChunkSize(512 * 1024) // 512KB chunks
            .withSchedulers(
                new DefaultChunkScheduler(),
                new AggressiveChunkScheduler(),
                new ConservativeChunkScheduler()
            )
            .withNetworkConditions(NetworkConditionSimulator.getCommonScenarios())
            .withWorkloads(SchedulerWorkload.SEQUENTIAL_READ)
            .withIterations(2);
        
        PerformanceResults results = test.run();
        
        System.out.println("Network Condition Analysis:");
        for (String condition : results.getNetworkConditions()) {
            String bestScheduler = results.getBestSchedulerForCondition(condition);
            System.out.printf("Best for %s: %s%n", condition, bestScheduler);
        }
        System.out.println();
    }
    
    /// Test focused on different workload patterns.
    /// 
    /// This test evaluates how schedulers handle different access patterns,
    /// from sequential reads to sparse random access.
    @Test
    public void testWorkloadPatternComparison() throws InterruptedException, ExecutionException {
        System.out.println("=== Workload Pattern Comparison ===");
        System.out.println();
        
        SchedulerPerformanceTest test = new SchedulerPerformanceTest()
            .withFileSize(30_000_000L) // 30MB
            .withChunkSize(1024 * 1024) // 1MB chunks
            .withSchedulers(
                new DefaultChunkScheduler(),
                new AggressiveChunkScheduler(),
                new ConservativeChunkScheduler(),
                new AdaptiveChunkScheduler()
            )
            .withNetworkConditions(NetworkConditions.Scenarios.BROADBAND_STANDARD)
            .withWorkloads(SchedulerWorkload.getAllWorkloads())
            .withIterations(2);
        
        PerformanceResults results = test.run();
        
        System.out.println("Workload Pattern Analysis:");
        for (String workload : results.getWorkloadNames()) {
            String bestScheduler = results.getBestSchedulerForWorkload(workload);
            System.out.printf("Best for %s: %s%n", workload, bestScheduler);
        }
        System.out.println();
    }
    
    /// Test simulating degrading network conditions.
    /// 
    /// This test shows how schedulers adapt to changing network conditions
    /// by simulating a network that starts fast and becomes progressively slower.
    @Test
    public void testDegradingNetworkConditions() throws InterruptedException, ExecutionException {
        System.out.println("=== Degrading Network Simulation ===");
        System.out.println();
        
        // Create degradation scenario from fast broadband to mobile
        var degradationScenario = NetworkConditionSimulator.createDegradationScenario(
            NetworkConditions.Scenarios.BROADBAND_FAST,
            NetworkConditions.Scenarios.MOBILE_LTE,
            5 // 5 degradation steps
        );
        
        SchedulerPerformanceTest test = new SchedulerPerformanceTest()
            .withFileSize(15_000_000L) // 15MB
            .withChunkSize(1024 * 1024) // 1MB chunks
            .withSchedulers(
                new DefaultChunkScheduler(),
                new AdaptiveChunkScheduler() // Should adapt better
            )
            .withNetworkConditions(degradationScenario)
            .withWorkloads(SchedulerWorkload.SEQUENTIAL_READ)
            .withIterations(1); // Single iteration for degradation test
        
        PerformanceResults results = test.run();
        
        System.out.println("Degradation Scenario Results:");
        System.out.println("Network conditions tested:");
        for (String condition : results.getNetworkConditions()) {
            System.out.printf("  - %s%n", condition);
        }
        
        // Show which scheduler handled degradation better
        var rankings = results.rankSchedulers();
        System.out.println("Scheduler rankings under degrading conditions:");
        for (int i = 0; i < rankings.size(); i++) {
            var ranking = rankings.get(i);
            System.out.printf("%d. %s (avg score: %.2f)%n", 
                            i + 1, ranking.getSchedulerName(), ranking.getAverageScore());
        }
        System.out.println();
    }
    
    /// Test with custom network conditions.
    /// 
    /// This test demonstrates creating custom network scenarios for
    /// specific testing requirements.
    @Test
    public void testCustomNetworkConditions() throws InterruptedException, ExecutionException {
        System.out.println("=== Custom Network Conditions Test ===");
        System.out.println();
        
        // Create custom network conditions for specific scenarios
        NetworkConditions highLatencyLowBandwidth = NetworkConditions.builder()
            .bandwidthMbps(2.0) // 2 Mbps
            .latencyMs(500) // 500ms latency (satellite-like)
            .maxConcurrentConnections(1)
            .successRate(0.9)
            .description("High Latency Low Bandwidth")
            .build();
        
        NetworkConditions highBandwidthHighLatency = NetworkConditions.builder()
            .bandwidthMbps(100.0) // 100 Mbps
            .latencyMs(200) // 200ms latency
            .maxConcurrentConnections(8)
            .successRate(0.95)
            .description("High Bandwidth High Latency")
            .build();
        
        NetworkConditions unreliableNetwork = NetworkConditions.builder()
            .bandwidthMbps(50.0) // 50 Mbps
            .latencyMs(50) // 50ms latency
            .maxConcurrentConnections(4)
            .successRate(0.8) // 20% failure rate
            .description("Unreliable Network")
            .build();
        
        SchedulerPerformanceTest test = new SchedulerPerformanceTest()
            .withFileSize(25_000_000L) // 25MB
            .withChunkSize(1024 * 1024) // 1MB chunks
            .withSchedulers(
                new DefaultChunkScheduler(),
                new AggressiveChunkScheduler(),
                new ConservativeChunkScheduler(),
                new AdaptiveChunkScheduler()
            )
            .withNetworkConditions(
                highLatencyLowBandwidth,
                highBandwidthHighLatency,
                unreliableNetwork
            )
            .withWorkloads(SchedulerWorkload.getBasicWorkloads())
            .withIterations(2);
        
        PerformanceResults results = test.run();
        
        // Show results summary
        System.out.println("Custom Network Conditions Results:");
        var rankings = results.rankSchedulers();
        for (var ranking : rankings) {
            System.out.printf("%s: %.2f average score%n", 
                            ranking.getSchedulerName(), ranking.getAverageScore());
        }
        System.out.println();
        
        // Show which scheduler works best for each custom condition
        for (String condition : results.getNetworkConditions()) {
            String best = results.getBestSchedulerForCondition(condition);
            System.out.printf("Best for %s: %s%n", condition, best);
        }
    }
    
    /// Simple focused test for quick validation.
    /// 
    /// This test provides a quick way to validate that the simulation
    /// framework is working correctly with minimal test time.
    @Test
    public void testQuickValidation() throws InterruptedException, ExecutionException {
        System.out.println("=== Quick Validation Test ===");
        System.out.println();
        
        SchedulerPerformanceTest test = new SchedulerPerformanceTest()
            .withFileSize(5_000_000L) // 5MB for fast test
            .withChunkSize(256 * 1024) // 256KB chunks
            .withSchedulers(
                new DefaultChunkScheduler(),
                new ConservativeChunkScheduler()
            )
            .withNetworkConditions(NetworkConditions.Scenarios.LOCALHOST)
            .withWorkloads(SchedulerWorkload.SEQUENTIAL_READ)
            .withIterations(1)
            .withTimeout(Duration.ofSeconds(30));
        
        PerformanceResults results = test.run();
        
        // Basic validation that test completed
        assert !results.getSchedulerNames().isEmpty();
        assert !results.getNetworkConditions().isEmpty();
        
        System.out.println("Quick validation completed successfully!");
        System.out.printf("Tested schedulers: %s%n", results.getSchedulerNames());
        
        // Show basic results
        var rankings = results.rankSchedulers();
        System.out.printf("Best performer: %s (score: %.2f)%n", 
                        rankings.get(0).getSchedulerName(), 
                        rankings.get(0).getAverageScore());
    }
}