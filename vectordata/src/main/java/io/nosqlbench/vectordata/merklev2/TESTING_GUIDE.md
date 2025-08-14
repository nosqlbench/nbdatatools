# ChunkScheduler Testing Framework Guide

This guide covers the comprehensive testing framework designed to validate ChunkScheduler implementations and ensure correct behavior under various scenarios.

## Overview

The testing framework provides structured, scenario-based testing for ChunkScheduler implementations with:

- **Scenario Builder**: Declarative test setup with realistic file and chunk configurations
- **Rich Assertions**: Comprehensive validation of scheduling decisions and behavior
- **Mock Implementations**: Lightweight mocks for isolated testing
- **Performance Benchmarks**: Performance measurement and regression detection
- **Traceability**: Full visibility into scheduling decisions and reasoning

## Core Components

### SchedulingTestFramework

The main entry point for all scheduler testing:

```java
SchedulingTestFramework framework = new SchedulingTestFramework();

// Create a test scenario
TestScenario scenario = framework.createScenario()
    .withFileSize(1_000_000)        // 1MB file
    .withChunkSize(64 * 1024)       // 64KB chunks  
    .withValidChunks(Set.of(0, 2, 4)) // Some chunks already valid
    .withReadRequest(100_000, 50_000); // Read 50KB at offset 100KB

// Test a scheduler
SchedulingTestResult result = framework.testScheduler(
    new AggressiveChunkScheduler(), scenario);

// Validate results
result.assertDecisionCount(3)
      .assertAllChunksCovered()
      .assertReasonUsed(SchedulingReason.PREFETCH)
      .assertMinimumEfficiency(0.5);
```

### TestScenario Builder

Declarative scenario configuration with fluent API:

```java
TestScenario scenario = framework.createScenario()
    // File configuration
    .withFileSize(10_000_000)           // 10MB file
    .withChunkSize(128 * 1024)          // 128KB chunks
    .withTotalChunks(78)                // Calculated: 10MB / 128KB
    
    // State configuration  
    .withValidChunks(Set.of(1, 3, 5))   // Specific valid chunks
    .withValidChunkRange(10, 20)        // Range of valid chunks
    .withRandomValidChunks(0.3)         // 30% randomly valid
    
    // Access patterns
    .withReadRequest(0, 128 * 1024)     // Single read
    .withSequentialReads(5, 64 * 1024)  // 5 sequential 64KB reads
    .withRandomReads(10, 32 * 1024)     // 10 random 32KB reads
    .withSparseReads(Arrays.asList(     // Custom sparse pattern
        new ReadRequest(0, 64 * 1024),
        new ReadRequest(5_000_000, 64 * 1024)
    ));
```

### SchedulingTestResult

Rich assertion API for validating scheduler behavior:

```java
SchedulingTestResult result = framework.testScheduler(scheduler, scenario);

// Decision count assertions
result.assertDecisionCount(5)                    // Exact count
      .assertDecisionCountRange(3, 7)            // Range
      .assertNoDecisions()                       // Empty result
      .assertHasDecisions();                     // Non-empty result

// Coverage assertions  
result.assertAllChunksCovered()                  // All required chunks covered
      .assertChunkCovered(5)                     // Specific chunk covered
      .assertChunksNotCovered(Set.of(1, 3))      // Specific chunks not covered
      .assertCoverageRatio(0.8);                 // 80% of chunks covered

// Efficiency assertions
result.assertMinimumEfficiency(0.5)             // All decisions >= 50% efficient
      .assertAverageEfficiency(0.7)              // Average >= 70% efficient
      .assertMaximumWaste(0.3);                  // Max 30% wasted bandwidth

// Reason assertions
result.assertReasonUsed(SchedulingReason.PREFETCH)     // Specific reason used  
      .assertReasonNotUsed(SchedulingReason.FALLBACK)  // Reason not used
      .assertReasonCount(SchedulingReason.EXACT_MATCH, 3); // Count of reason

// Priority assertions
result.assertPriorityOrder()                    // Decisions in priority order
      .assertHighestPriority(0)                 // Highest priority value
      .assertLowestPriority(4);                 // Lowest priority value

// Node type assertions
result.assertLeafNodesOnly()                    // Only leaf nodes selected
      .assertInternalNodesUsed()                // Internal nodes used
      .assertNodeTypeRatio(0.7);               // 70% leaf nodes

// Custom assertions
result.assertCustom(decisions -> {
    // Custom validation logic
    long totalBytes = decisions.stream()
        .mapToLong(SchedulingDecision::estimatedBytes)
        .sum();
    return totalBytes <= 1_000_000; // Max 1MB total
}, "Total estimated bytes should not exceed 1MB");
```

## Test Patterns

### Basic Functionality Tests

```java
@Test
void testBasicChunkSelection() {
    TestScenario scenario = framework.createScenario()
        .withFileSize(1_000_000)
        .withChunkSize(64 * 1024)
        .withValidChunks(Set.of())  // No valid chunks
        .withReadRequest(0, 64 * 1024); // Read first chunk
    
    SchedulingTestResult result = framework.testScheduler(
        new ConservativeChunkScheduler(), scenario);
    
    result.assertDecisionCount(1)
          .assertChunkCovered(0)
          .assertReasonUsed(SchedulingReason.MINIMAL_DOWNLOAD)
          .assertLeafNodesOnly();
}
```

### Edge Case Tests

```java
@Test  
void testBoundaryConditions() {
    // Test reading at file boundary
    TestScenario endOfFile = framework.createScenario()
        .withFileSize(1_000_000)
        .withChunkSize(64 * 1024)
        .withReadRequest(999_999, 1); // Read last byte
    
    SchedulingTestResult result = framework.testScheduler(
        new DefaultChunkScheduler(), endOfFile);
    
    result.assertDecisionCount(1)
          .assertChunkCovered(15) // Last chunk (0-based)
          .assertAllChunksCovered();
}

@Test
void testEmptyFileHandling() {
    TestScenario emptyFile = framework.createScenario()
        .withFileSize(0)
        .withChunkSize(64 * 1024)
        .withReadRequest(0, 0);
    
    SchedulingTestResult result = framework.testScheduler(
        new AggressiveChunkScheduler(), emptyFile);
    
    result.assertNoDecisions();
}
```

### Performance Characteristic Tests

```java
@Test
void testSchedulerEfficiency() {
    TestScenario sparseReads = framework.createScenario()
        .withFileSize(10_000_000)
        .withChunkSize(64 * 1024)
        .withSparseReads(Arrays.asList(
            new ReadRequest(0, 1024),           // First chunk
            new ReadRequest(5_000_000, 1024),   // Middle chunk  
            new ReadRequest(9_999_000, 1000)    // Last chunk
        ));
    
    // Conservative should be very efficient (minimal download)
    SchedulingTestResult conservative = framework.testScheduler(
        new ConservativeChunkScheduler(), sparseReads);
    
    conservative.assertMinimumEfficiency(0.95)  // Very efficient
               .assertDecisionCount(3)          // One per chunk
               .assertLeafNodesOnly();          // No consolidation
    
    // Aggressive may be less efficient but provide better caching
    SchedulingTestResult aggressive = framework.testScheduler(
        new AggressiveChunkScheduler(), sparseReads);
    
    aggressive.assertMinimumEfficiency(0.3)     // May download extra
              .assertDecisionCountRange(3, 10)  // Possibly more decisions
              .assertReasonUsed(SchedulingReason.PREFETCH);
}
```

### Concurrency and State Tests

```java
@Test
void testConcurrentStreamSafety() {
    TestScenario baseScenario = framework.createScenario()
        .withFileSize(1_000_000)
        .withChunkSize(64 * 1024)
        .withValidChunks(Set.of(1, 3, 5, 7))
        .withReadRequest(128 * 1024, 128 * 1024); // Chunks 2-3
    
    ChunkScheduler scheduler = new DefaultChunkScheduler();
    
    // Test multiple concurrent scheduling calls
    SchedulingTestResult result1 = framework.testScheduler(scheduler, baseScenario);
    SchedulingTestResult result2 = framework.testScheduler(scheduler, baseScenario);
    
    // Results should be identical (stateless scheduler)
    assertEquals(result1.getDecisions().size(), result2.getDecisions().size());
    assertEquals(result1.getTotalEstimatedBytes(), result2.getTotalEstimatedBytes());
}

@Test
void testStateAwareScheduling() {
    // Test with no valid chunks
    TestScenario emptyState = framework.createScenario()
        .withFileSize(1_000_000)
        .withChunkSize(64 * 1024)
        .withValidChunks(Set.of())
        .withReadRequest(0, 256 * 1024); // First 4 chunks
    
    SchedulingTestResult emptyResult = framework.testScheduler(
        new DefaultChunkScheduler(), emptyState);
    
    emptyResult.assertDecisionCountRange(1, 4)  // Should schedule downloads
               .assertAllChunksCovered();
    
    // Test with all chunks valid
    TestScenario allValid = framework.createScenario()
        .withFileSize(1_000_000)
        .withChunkSize(64 * 1024)
        .withValidChunks(Set.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15))
        .withReadRequest(0, 256 * 1024);
    
    SchedulingTestResult validResult = framework.testScheduler(
        new DefaultChunkScheduler(), allValid);
    
    validResult.assertNoDecisions(); // No downloads needed
}
```

### Comparative Analysis Tests

```java
@Test  
void testSchedulerComparison() {
    TestScenario scenario = framework.createScenario()
        .withFileSize(5_000_000)
        .withChunkSize(64 * 1024)
        .withSequentialReads(10, 64 * 1024);
    
    Map<String, SchedulingTestResult> results = new HashMap<>();
    
    // Test all schedulers with same scenario
    results.put("Aggressive", framework.testScheduler(new AggressiveChunkScheduler(), scenario));
    results.put("Default", framework.testScheduler(new DefaultChunkScheduler(), scenario));
    results.put("Conservative", framework.testScheduler(new ConservativeChunkScheduler(), scenario));
    results.put("Adaptive", framework.testScheduler(new AdaptiveChunkScheduler(), scenario));
    
    // Compare characteristics
    System.out.println("Scheduler Comparison:");
    for (Map.Entry<String, SchedulingTestResult> entry : results.entrySet()) {
        SchedulingTestResult result = entry.getValue();
        System.out.printf("%s: %d decisions, %.2f efficiency, %d bytes\n",
            entry.getKey(),
            result.getDecisions().size(),
            result.getAverageEfficiency(),
            result.getTotalEstimatedBytes());
    }
    
    // Validate expected characteristics
    assertTrue(results.get("Conservative").getDecisions().size() <= 
               results.get("Aggressive").getDecisions().size());
    assertTrue(results.get("Conservative").getAverageEfficiency() >= 
               results.get("Aggressive").getAverageEfficiency());
}
```

## Mock Implementations

The framework includes lightweight mock implementations for testing:

### MockMerkleShape
```java
MockMerkleShape shape = new MockMerkleShape(fileSize, chunkSize);
// Provides all MerkleShape operations for testing
assertEquals(16, shape.getTotalChunks()); // 1MB / 64KB = 16 chunks
```

### MockMerkleState  
```java
MockMerkleState state = new MockMerkleState(shape);
state.setValid(0, true);  // Mark chunk 0 as valid
state.setValid(2, true);  // Mark chunk 2 as valid
assertFalse(state.isValid(1)); // Chunk 1 is invalid
```

### MockSchedulingTarget
```java
MockSchedulingTarget target = new MockSchedulingTarget();
// Captures scheduled tasks for verification
scheduler.scheduleDownloads(0, 64*1024, shape, state, target);
assertEquals(1, target.getScheduledTasks().size());
```

## Performance Benchmarking

The framework includes performance benchmarking capabilities:

```java
@Test
void benchmarkSchedulerPerformance() {
    SchedulerPerformanceBenchmark benchmark = new SchedulerPerformanceBenchmark();
    
    // Measures decision generation time
    benchmark.benchmarkDecisionGeneration(Arrays.asList(
        new AggressiveChunkScheduler(),
        new ConservativeChunkScheduler()
    ));
    
    // Measures scaling behavior  
    benchmark.benchmarkScaling(schedulers);
    
    // Measures access pattern performance
    benchmark.benchmarkAccessPatterns(schedulers);
}
```

## Integration with JUnit 5

The framework integrates seamlessly with JUnit 5:

```java
@TestMethodOrder(OrderAnnotation.class)
class MySchedulerTest {
    
    private SchedulingTestFramework framework;
    
    @BeforeEach
    void setUp() {
        framework = new SchedulingTestFramework();
    }
    
    @Test
    @Order(1)
    void testBasicFunctionality() {
        // Basic tests first
    }
    
    @Test  
    @Order(2)
    void testEdgeCases() {
        // Edge cases after basic functionality
    }
    
    @Test
    @Order(3) 
    void testPerformanceCharacteristics() {
        // Performance tests last
    }
    
    @ParameterizedTest
    @ValueSource(classes = {
        AggressiveChunkScheduler.class,
        DefaultChunkScheduler.class,
        ConservativeChunkScheduler.class
    })
    void testAllSchedulers(Class<? extends ChunkScheduler> schedulerClass) 
        throws InstantiationException {
        
        ChunkScheduler scheduler = schedulerClass.getDeclaredConstructor().newInstance();
        TestScenario scenario = framework.createScenario()
            .withFileSize(1_000_000)
            .withChunkSize(64 * 1024)
            .withReadRequest(0, 64 * 1024);
        
        SchedulingTestResult result = framework.testScheduler(scheduler, scenario);
        result.assertAllChunksCovered()
              .assertHasDecisions();
    }
}
```

## Best Practices

### Test Organization
1. **Start Simple**: Begin with basic functionality tests
2. **Add Edge Cases**: Test boundary conditions and error cases  
3. **Test State Variations**: Validate behavior with different chunk validity states
4. **Performance Testing**: Measure and validate performance characteristics
5. **Comparative Analysis**: Compare schedulers under identical conditions

### Assertion Strategy
1. **Layer Assertions**: Start with basic checks, add specific validations
2. **Test Intentions**: Validate the scheduler's intended behavior, not implementation details
3. **Use Custom Assertions**: Create domain-specific validations for complex scenarios
4. **Document Expectations**: Clearly document what each test validates

### Scenario Design
1. **Realistic Scenarios**: Use file sizes and access patterns similar to production
2. **Edge Cases**: Test empty files, single chunks, very large files
3. **Access Patterns**: Test sequential, random, and sparse access patterns
4. **State Variations**: Test with different ratios of valid/invalid chunks

## Troubleshooting

### Common Issues

**Test Flakiness**: Use fixed random seeds for reproducible results
```java
TestScenario scenario = framework.createScenario()
    .withRandomSeed(42)  // Fixed seed
    .withRandomValidChunks(0.5);
```

**Performance Variability**: Use warmup runs and multiple iterations
```java
// Warmup before measuring
for (int i = 0; i < 100; i++) {
    scheduler.analyzeSchedulingDecisions(offset, length, shape, state);
}
// Then measure performance
```

**Mock State Issues**: Ensure mock state matches test expectations
```java
MockMerkleState state = new MockMerkleState(shape);
// Explicitly set expected state
state.setValid(0, true);
state.setValid(1, false);
// Verify state before test
assertTrue(state.isValid(0));
assertFalse(state.isValid(1));
```

### Debugging Failed Tests

1. **Use Debug Output**: Enable detailed decision logging
```java
for (SchedulingDecision decision : result.getDecisions()) {
    System.out.println(decision.getDebugInfo());
}
```

2. **Check Assumptions**: Validate test scenario setup
```java
assertEquals(expectedChunkCount, scenario.getShape().getTotalChunks());
assertEquals(expectedValidChunks, scenario.getValidChunks());
```

3. **Compare Expected vs Actual**: Use detailed assertion messages
```java
result.assertCustom(decisions -> decisions.size() == expectedCount,
    String.format("Expected %d decisions but got %d: %s", 
        expectedCount, decisions.size(), decisions));
```

## Examples

Complete working examples are available in:
- `SimpleChunkSelectionTest.java` - Basic functionality tests
- `SchedulerPerformanceBenchmark.java` - Performance measurement examples
- Test classes for each scheduler implementation

These examples demonstrate best practices and common testing patterns for the ChunkScheduler framework.