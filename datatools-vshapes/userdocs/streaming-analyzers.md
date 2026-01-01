# Streaming Analyzers

This guide covers the streaming analyzer framework for processing large vector datasets efficiently.

## Overview

The streaming analyzer framework allows multiple analyzers to process a dataset concurrently, where vectors are presented to each analyzer in chunks without reloading the dataset for each one. This is particularly useful for:

- Large datasets that benefit from single-pass processing
- Running multiple analyses simultaneously
- Memory-efficient processing via chunked iteration

## Core Components

| Component | Description |
|-----------|-------------|
| `StreamingAnalyzer<M>` | Interface for analyzers that produce a model of type `M` |
| `AnalyzerHarness` | Orchestrates multiple analyzers over a data source |
| `DataSource` | Abstraction for streaming vector data in chunks |
| `StreamingAnalyzerIO` | SPI factory for discovering and loading analyzers |

## Using the Analyzer Harness

### Basic Usage

```java
import io.nosqlbench.vshapes.stream.*;
import io.nosqlbench.vshapes.model.VectorSpaceModel;

// Create a data source
float[][] vectors = loadVectors();
DataSource source = new FloatArrayDataSource(vectors);

// Create harness and register analyzers
AnalyzerHarness harness = new AnalyzerHarness();
harness.register("dimension-distribution");  // Load by name via SPI

// Run analysis
AnalysisResults results = harness.run(source, 1000);  // 1000 vectors per chunk

// Get typed result
VectorSpaceModel model = results.getResult("dimension-distribution", VectorSpaceModel.class);

// Clean up
harness.shutdown();
```

### Loading Analyzers via SPI

The harness supports several ways to register analyzers:

```java
AnalyzerHarness harness = new AnalyzerHarness();

// Load by name (throws if not found)
harness.register("dimension-distribution");

// Load by name if available (silent if not found)
harness.registerIfAvailable("optional-analyzer");

// Load multiple by name
harness.registerAll("dimension-distribution", "stats-analyzer");

// Load all discovered analyzers
harness.registerAllAvailable();

// Direct instance registration
harness.register(new DimensionDistributionAnalyzer());
```

### Discovering Available Analyzers

```java
import io.nosqlbench.vshapes.stream.StreamingAnalyzerIO;

// List all available analyzer names
List<String> names = StreamingAnalyzerIO.getAvailableNames();

// Check if a specific analyzer is available
boolean available = StreamingAnalyzerIO.isAvailable("dimension-distribution");

// Get a specific analyzer instance
Optional<StreamingAnalyzer<?>> analyzer = StreamingAnalyzerIO.get("dimension-distribution");

// Get all analyzer instances
List<StreamingAnalyzer<?>> all = StreamingAnalyzerIO.getAll();
```

### Progress Reporting

```java
AnalysisResults results = harness.run(source, 1000, (progress, processed, total) -> {
    System.out.printf("Progress: %.1f%% (%d/%d vectors)%n",
        progress * 100, processed, total);
});
```

### Error Handling

By default, the harness uses fail-fast behavior. Configure it to continue on errors:

```java
AnalyzerHarness harness = new AnalyzerHarness()
    .failFast(false);  // Continue other analyzers if one fails

AnalysisResults results = harness.run(source, 1000);

if (results.hasErrors()) {
    results.getErrors().forEach((type, error) ->
        System.err.println(type + " failed: " + error.getMessage()));
}
```

## Creating Custom Analyzers

### Step 1: Implement StreamingAnalyzer

Create a class that implements `StreamingAnalyzer<M>` where `M` is your model type:

```java
package com.example.analyzers;

import io.nosqlbench.vshapes.stream.*;

@AnalyzerName("my-stats")
public class MyStatsAnalyzer implements StreamingAnalyzer<MyStats> {

    private DataspaceShape shape;
    private double sum = 0;
    private long count = 0;

    // Required: no-args constructor for SPI
    public MyStatsAnalyzer() {
    }

    @Override
    public String getAnalyzerType() {
        return "my-stats";
    }

    @Override
    public void initialize(DataspaceShape shape) {
        this.shape = shape;
        this.sum = 0;
        this.count = 0;
    }

    @Override
    public void accept(float[][] chunk, long startIndex) {
        for (float[] vector : chunk) {
            for (float v : vector) {
                sum += v;
                count++;
            }
        }
    }

    @Override
    public MyStats complete() {
        double mean = count > 0 ? sum / count : 0;
        return new MyStats(shape.cardinality(), shape.dimensionality(), mean);
    }

    @Override
    public String getDescription() {
        return "Computes basic statistics over vector components";
    }

    @Override
    public long estimatedMemoryBytes() {
        return 64;  // Minimal memory footprint
    }
}
```

### Step 2: Add the @AnalyzerName Annotation

The `@AnalyzerName` annotation marks your analyzer for SPI discovery:

```java
@AnalyzerName("my-stats")
public class MyStatsAnalyzer implements StreamingAnalyzer<MyStats> {
    // ...
}
```

The annotation value should match what `getAnalyzerType()` returns.

### Step 3: Provide a No-Args Constructor

SPI requires a public no-args constructor:

```java
public MyStatsAnalyzer() {
}
```

If your analyzer needs configuration, provide sensible defaults in the no-args constructor and optional configuration constructors:

```java
public MyStatsAnalyzer() {
    this(DEFAULT_SAMPLE_SIZE);
}

public MyStatsAnalyzer(int sampleSize) {
    this.sampleSize = sampleSize;
}
```

### Step 4: Register with ServiceLoader

Create the service provider configuration file:

```
src/main/resources/META-INF/services/io.nosqlbench.vshapes.stream.StreamingAnalyzer
```

Add your analyzer class (one per line):

```
com.example.analyzers.MyStatsAnalyzer
```

### Thread Safety

Analyzers must be thread-safe as the harness may call `accept()` from multiple threads concurrently. Use appropriate synchronization:

```java
@AnalyzerName("thread-safe-stats")
public class ThreadSafeStatsAnalyzer implements StreamingAnalyzer<Stats> {

    private final AtomicLong count = new AtomicLong();
    private final DoubleAdder sum = new DoubleAdder();

    @Override
    public void accept(float[][] chunk, long startIndex) {
        double localSum = 0;
        int localCount = 0;
        for (float[] vector : chunk) {
            for (float v : vector) {
                localSum += v;
                localCount++;
            }
        }
        sum.add(localSum);
        count.addAndGet(localCount);
    }

    // ...
}
```

## Built-in Analyzers

| Name | Description | Model Type |
|------|-------------|------------|
| `dimension-distribution` | Extracts per-dimension distribution models | `VectorSpaceModel` |

## DataSource Implementations

| Class | Description |
|-------|-------------|
| `FloatArrayDataSource` | Wraps an in-memory `float[][]` array |
| `VectorSpaceDataSource` | Wraps a `VectorSpace` implementation |

### Creating a Custom DataSource

```java
public class MyDataSource implements DataSource {

    @Override
    public DataspaceShape getShape() {
        return new DataspaceShape(vectorCount, dimensions, Map.of());
    }

    @Override
    public String getId() {
        return "my-data-source";
    }

    @Override
    public Iterable<float[][]> chunks(int chunkSize) {
        return () -> new Iterator<float[][]>() {
            // Implement chunked iteration
        };
    }
}
```

## DataspaceShape

The `DataspaceShape` record describes the vector space being analyzed:

```java
DataspaceShape shape = new DataspaceShape(
    1000000,              // cardinality (number of vectors)
    128,                  // dimensionality
    Map.of(               // additional parameters
        "source", "embeddings-v2",
        "normalized", true
    )
);

// Access shape properties
long count = shape.cardinality();
int dims = shape.dimensionality();
String source = shape.getStringParameter("source", "unknown");
boolean normalized = shape.getBooleanParameter("normalized", false);
```

## Best Practices

1. **Use online algorithms** for statistics to avoid storing all data in memory
2. **Minimize synchronization** by accumulating locally then merging
3. **Implement `estimatedMemoryBytes()`** to help the harness schedule work
4. **Validate in `initialize()`** to fail fast on incompatible data
5. **Keep `complete()` fast** as it runs after all chunks are processed
