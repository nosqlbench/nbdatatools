# Scheduler Simulation Framework

This package provides a comprehensive testing framework for evaluating and comparing different chunk download scheduling strategies under various network conditions and workload patterns.

## Overview

The simulation framework allows developers to:

1. **Test new scheduler algorithms** before deployment
2. **Compare performance** under different network conditions  
3. **Identify optimal scheduling strategies** for specific scenarios
4. **Validate scheduler behavior** under stress conditions
5. **Analyze performance trade-offs** between different approaches

## Key Components

### Core Simulation Components

- **`MockChunkedTransportClient`** - Configurable transport simulation with realistic network behavior
- **`NetworkConditions`** - Network condition parameters (bandwidth, latency, reliability)
- **`NetworkConditionSimulator`** - Factory for creating various network scenarios
- **`SimulatedMerkleShape`** - Lightweight merkle tree structure for testing
- **`SimulatedMerkleState`** - In-memory chunk validation state tracking

### Performance Testing Framework

- **`SchedulerPerformanceTest`** - Main test harness for running comparisons
- **`SchedulerWorkload`** - Different access patterns (sequential, random, clustered, etc.)
- **`PerformanceResults`** - Comprehensive analysis and ranking of results
- **`TransportStatistics`** - Detailed performance metrics

### Scheduler Implementations

- **`AggressiveChunkScheduler`** - Maximizes bandwidth utilization with larger downloads
- **`ConservativeChunkScheduler`** - Minimizes waste with precise chunk-only downloads  
- **`AdaptiveChunkScheduler`** - Dynamically adapts strategy based on performance

## Quick Start

### Basic Scheduler Comparison

```java
SchedulerPerformanceTest test = new SchedulerPerformanceTest()
    .withFileSize(100_000_000L) // 100MB file
    .withChunkSize(1024 * 1024) // 1MB chunks
    .withNetworkConditions(NetworkConditions.Scenarios.BROADBAND_FAST)
    .withSchedulers(
        new DefaultChunkScheduler(),
        new AggressiveChunkScheduler(),
        new ConservativeChunkScheduler()
    )
    .withWorkload(SchedulerWorkload.SEQUENTIAL_READ)
    .withIterations(5);

PerformanceResults results = test.run();
results.printSummary();
```

### Network Condition Testing

```java
// Test across multiple network scenarios
SchedulerPerformanceTest test = new SchedulerPerformanceTest()
    .withSchedulers(new DefaultChunkScheduler())
    .withNetworkConditions(
        NetworkConditions.Scenarios.FIBER,
        NetworkConditions.Scenarios.BROADBAND_FAST,
        NetworkConditions.Scenarios.MOBILE_LTE,
        NetworkConditions.Scenarios.SATELLITE
    )
    .withWorkloads(SchedulerWorkload.getBasicWorkloads());

PerformanceResults results = test.run();

// Find best scheduler for each condition
for (String condition : results.getNetworkConditions()) {
    String best = results.getBestSchedulerForCondition(condition);
    System.out.printf("Best for %s: %s%n", condition, best);
}
```

### Custom Network Conditions

```java
// Create custom network scenario
NetworkConditions customNetwork = NetworkConditions.builder()
    .bandwidthMbps(25.0) // 25 Mbps
    .latencyMs(100) // 100ms latency
    .maxConcurrentConnections(4)
    .successRate(0.95) // 5% failure rate
    .description("Custom DSL Connection")
    .build();

SchedulerPerformanceTest test = new SchedulerPerformanceTest()
    .withNetworkConditions(customNetwork)
    .withSchedulers(/* your schedulers */);
```

### Dynamic Network Scenarios

```java
// Test degrading network conditions
List<NetworkConditions> degradation = NetworkConditionSimulator
    .createDegradationScenario(
        NetworkConditions.Scenarios.BROADBAND_FAST,
        NetworkConditions.Scenarios.MOBILE_LTE,
        5 // 5 degradation steps
    );

SchedulerPerformanceTest test = new SchedulerPerformanceTest()
    .withNetworkConditions(degradation)
    .withSchedulers(new AdaptiveChunkScheduler()); // Should adapt well
```

## Workload Patterns

The framework includes several workload patterns that represent different real-world access scenarios:

- **`SEQUENTIAL_READ`** - Reading file from start to finish
- **`RANDOM_READ`** - Randomly distributed reads
- **`CLUSTERED_READ`** - Reads concentrated in specific regions  
- **`SPARSE_READ`** - Reads with large gaps between them
- **`MIXED_READ`** - Combination of sequential and random
- **`LARGE_READ`** - Large multi-chunk requests
- **`SMALL_READ`** - Many small requests

## Network Scenarios

Pre-defined network conditions for common scenarios:

- **`LOCALHOST`** - Unrestricted local access
- **`FIBER`** - High-speed fiber (1 Gbps, 5ms latency)
- **`BROADBAND_FAST`** - Fast broadband (100 Mbps, 20ms latency)
- **`BROADBAND_STANDARD`** - Standard broadband (20 Mbps, 50ms latency)
- **`MOBILE_LTE`** - Mobile LTE (5 Mbps, 150ms latency)
- **`SATELLITE`** - Satellite connection (1 Mbps, 600ms latency)

## Performance Metrics

The framework tracks comprehensive performance metrics:

### Transport Statistics
- Total bytes transferred vs requested
- Request success/failure rates  
- Actual throughput achieved
- Bandwidth utilization percentage

### Scheduler Efficiency
- Download efficiency (waste factor)
- Request consolidation effectiveness
- Completion percentage
- Average response times

### Composite Scoring
- Weighted performance score combining:
  - Completion rate (40%)
  - Efficiency (30%) 
  - Throughput (20%)
  - Reliability (10%)

## Extending the Framework

### Adding New Schedulers

Implement the `ChunkScheduler` interface:

```java
public class MyCustomScheduler implements ChunkScheduler {
    @Override
    public void scheduleDownloads(
            MerkleShape shape, MerkleState state, 
            long offset, int length,
            ChunkQueue chunkQueue) {
        // Your scheduling logic here
        // Create tasks and add them using chunkQueue.offerTask(task)
        // Use chunkQueue.getOrCreateFuture(nodeIndex) for deduplication
    }
}
```

### Creating Custom Workloads

Add new patterns to the `SchedulerWorkload` enum:

```java
CUSTOM_PATTERN("Custom Pattern") {
    @Override
    public List<WorkloadRequest> generateRequests(MerkleShape shape) {
        // Generate your custom access pattern
    }
}
```

### Custom Network Conditions

Use the builder pattern for specific scenarios:

```java
NetworkConditions myScenario = NetworkConditions.builder()
    .bandwidth(/* bytes per second */)
    .latency(Duration.ofMillis(/* latency */))
    .maxConcurrentConnections(/* count */)
    .successRate(/* 0.0 to 1.0 */)
    .description("My Custom Scenario")
    .build();
```

## Example Tests

See `SchedulerSimulationExample.java` for comprehensive examples including:

- **Comprehensive comparison** across all schedulers and conditions
- **Network condition focused** testing
- **Workload pattern analysis**
- **Degrading network simulation**
- **Custom scenario testing**
- **Quick validation** for development

## Best Practices

### Test Configuration
- Use multiple iterations (3-5) for statistical validity
- Choose appropriate file sizes (10-100MB for reasonable test times)
- Match chunk sizes to real-world usage (typically 256KB-4MB)
- Set reasonable timeouts based on network conditions

### Scheduler Design Guidelines
- **For high-bandwidth, low-latency**: Prefer aggressive strategies with larger downloads
- **For low-bandwidth, high-latency**: Use conservative strategies to minimize waste
- **For variable conditions**: Implement adaptive strategies that can adjust
- **For sequential access**: Enable prefetching and batch downloads
- **For random access**: Focus on minimal overhead and precise targeting

### Performance Analysis
- Look at composite scores for overall performance
- Examine efficiency ratios to identify waste
- Check bandwidth utilization to find bottlenecks  
- Consider success rates when reliability matters
- Analyze per-scenario results to understand trade-offs

## Integration with Real Schedulers

The simulation framework is designed to work with actual production scheduler implementations. Simply pass your real `ChunkScheduler` instances to the test framework alongside the simulation schedulers for direct comparison.

This allows you to:
- Validate that your production scheduler performs well across scenarios
- Compare against alternative implementations
- Identify performance regressions during development
- Optimize scheduling parameters based on target environments