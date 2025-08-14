# Performance Tests

This package contains utilities for managing performance-oriented tests in the project.

## Running Performance Tests

Performance tests are disabled by default to prevent them from slowing down regular test runs. These tests typically involve large file operations, extensive computations, or other resource-intensive operations that are not suitable for regular CI/CD pipelines.

To run performance tests, you need to explicitly enable them using one of the following methods:

### Method 1: Using Maven Property

```bash
mvn test -DskipPerformanceTests=false
```

### Method 2: Running the Performance Test Suite Directly

```bash
mvn test -Dtest=PerformanceTestSuite
```

## Creating New Performance Tests

To create a new performance test, use the `@Tag("performance")` and `@Test` annotations:

```java
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

public class MyPerformanceTest {

    @Tag("performance")
    @Test
    void testPerformanceFeature() {
        // Performance test code
    }
}
```

Using these annotations:
1. `@Tag("performance")` tags the test for filtering by the PerformanceTestSuite
2. `@Test` marks it as a standard JUnit test
3. Tests with the "performance" tag are automatically included in the PerformanceTestSuite

## Existing Performance Tests

The following performance tests are currently implemented:

1. `MerkleTreeRealFileTest` - Tests loading a Merkle tree from a file
2. `MerkleTreeLoadingPerformanceTest` - Measures detailed performance metrics for loading Merkle trees
3. `MerkleTreePerformanceTest` - Measures performance of creating Merkle trees for large files

## Best Practices

When writing performance tests:

1. Include detailed metrics (time, memory usage, etc.)
2. Use warmup iterations to stabilize JVM performance
3. Run multiple iterations and report statistical measures (avg, min, max, std dev)
4. Consider using JFR (Java Flight Recorder) for detailed profiling
5. Document expected performance characteristics
