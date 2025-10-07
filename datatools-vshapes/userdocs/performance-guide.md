# Performance Guide

This guide provides strategies for optimizing vector space analysis performance, especially for large datasets.

## Performance Characteristics

### Computational Complexity

| Measure | Time Complexity | Space Complexity | Notes |
|---------|----------------|------------------|-------|
| **LID** | O(n² log n) | O(n × k) | Distance computation dominates |
| **Margin** | O(n²) | O(1) | Pairwise distance checks |
| **Hubness** | O(n² log n) | O(n × k) | k-NN computation required |

Where:
- `n` = number of vectors
- `k` = number of nearest neighbors
- `d` = vector dimensionality

### Memory Usage

- **Vector storage**: O(n × d) 
- **Distance computations**: O(n²) if cached
- **k-NN results**: O(n × k)
- **Analysis results**: O(n) per measure

## Optimization Strategies

### 1. Caching System

The vshapes module includes intelligent caching to avoid recomputation:

```java
// Use persistent cache directory
Path cacheDir = Paths.get("/path/to/cache");
VectorSpaceAnalyzer analyzer = new VectorSpaceAnalyzer(cacheDir);

// First run: full computation
VectorSpaceAnalyzer.AnalysisReport report1 = analyzer.analyzeVectorSpace(vectorSpace);

// Subsequent runs: cached results (much faster)
VectorSpaceAnalyzer.AnalysisReport report2 = analyzer.analyzeVectorSpace(vectorSpace);
```

**Cache Benefits:**
- JSON-based artifact storage
- Automatic cache key generation from vector space ID
- Dependency-aware caching (only recompute what's needed)
- Cross-session persistence

### 2. Parameter Tuning

#### Optimize k-values

```java
// Smaller k values = faster computation
LIDMeasure fastLID = new LIDMeasure(10);    // Instead of default 20
HubnessMeasure fastHubness = new HubnessMeasure(5);  // Instead of default 10

VectorSpaceAnalyzer analyzer = new VectorSpaceAnalyzer();
analyzer.registerMeasure(fastLID);
analyzer.registerMeasure(fastHubness);
```

**k-value Guidelines:**
- **Minimum**: k ≥ 2 for meaningful statistics
- **Performance**: Lower k = faster computation
- **Accuracy**: Higher k = more stable estimates
- **Recommendation**: k = 10-20 for most datasets

### 3. Data Preprocessing

#### Dimensionality Reduction

```java
// Apply PCA before analysis for very high dimensions
float[][] reducedVectors = applyPCA(originalVectors, targetDimensions);
VectorSpace reducedSpace = new MyVectorSpace(reducedVectors, classLabels);

// Analyze reduced space (much faster)
VectorSpaceAnalyzer.AnalysisReport report = analyzer.analyzeVectorSpace(reducedSpace);
```

**When to reduce dimensions:**
- Original dimensionality > 1000
- LID analysis suggests low intrinsic dimensionality
- Speed is more important than exact precision

#### Data Sampling

For exploratory analysis of very large datasets:

```java
public VectorSpace createSample(VectorSpace original, int sampleSize) {
    Random random = new Random(42); // Fixed seed for reproducibility
    int n = original.getVectorCount();
    
    List<Integer> indices = random.ints(0, n)
                                 .distinct()
                                 .limit(sampleSize)
                                 .boxed()
                                 .collect(Collectors.toList());
    
    return new SampledVectorSpace(original, indices);
}

// Analyze sample first for quick insights
VectorSpace sample = createSample(largeDataset, 5000);
VectorSpaceAnalyzer.AnalysisReport sampleReport = analyzer.analyzeVectorSpace(sample);
```

### 4. Memory Management

#### JVM Heap Sizing

For large datasets, increase heap size:

```bash
# For datasets with 100k+ vectors
java -Xmx8g -XX:+UseG1GC MyAnalysisApp

# For very large datasets (1M+ vectors)
java -Xmx32g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 MyAnalysisApp
```

#### Streaming VectorSpace Implementation

For datasets too large to fit in memory:

```java
public class StreamingVectorSpace implements VectorSpace {
    private final Path dataFile;
    private final int vectorCount;
    private final int dimension;
    
    @Override
    public float[] getVector(int index) {
        // Read specific vector from file/database
        return readVectorFromStorage(index);
    }
    
    @Override
    public float[][] getAllVectors() {
        // Load in chunks to avoid OOM
        return loadVectorsInChunks();
    }
    
    private float[][] loadVectorsInChunks() {
        float[][] result = new float[vectorCount][];
        int chunkSize = 1000;
        
        for (int start = 0; start < vectorCount; start += chunkSize) {
            int end = Math.min(start + chunkSize, vectorCount);
            float[][] chunk = readVectorChunk(start, end);
            System.arraycopy(chunk, 0, result, start, chunk.length);
        }
        
        return result;
    }
}
```

### 5. Parallel Processing

While the current implementation is single-threaded, you can parallelize analysis of multiple datasets:

```java
public class ParallelAnalysis {
    private final ExecutorService executor = ForkJoinPool.commonPool();
    
    public List<CompletableFuture<AnalysisResult>> analyzeBatch(List<VectorSpace> datasets) {
        return datasets.stream()
                      .map(this::analyzeAsync)
                      .collect(Collectors.toList());
    }
    
    private CompletableFuture<AnalysisResult> analyzeAsync(VectorSpace vectorSpace) {
        return CompletableFuture.supplyAsync(() -> {
            VectorSpaceAnalyzer analyzer = new VectorSpaceAnalyzer();
            VectorSpaceAnalyzer.AnalysisReport report = analyzer.analyzeVectorSpace(vectorSpace);
            return new AnalysisResult(vectorSpace.getId(), report);
        }, executor);
    }
}
```

## Performance Benchmarks

### Typical Performance (Intel i7, 16GB RAM)

| Dataset Size | Dimensions | LID (k=20) | Margin | Hubness (k=10) | Total |
|-------------|------------|------------|---------|----------------|-------|
| 1,000 | 10 | 0.5s | 0.3s | 0.4s | 1.2s |
| 5,000 | 50 | 12s | 8s | 10s | 30s |
| 10,000 | 100 | 45s | 30s | 40s | 115s |
| 50,000 | 200 | 20m | 12m | 18m | 50m |

*Note: First run without caching. Cached runs are ~100x faster.*

### Memory Requirements

| Dataset Size | Vector Memory | Analysis Memory | Total Peak |
|-------------|---------------|-----------------|------------|
| 10,000 × 100 | 4MB | 50MB | 100MB |
| 50,000 × 200 | 40MB | 500MB | 1GB |
| 100,000 × 500 | 200MB | 2GB | 4GB |
| 1,000,000 × 100 | 400MB | 20GB | 32GB |

## Dataset-Specific Optimizations

### High-Dimensional Data (d > 1000)

```java
// Strategy 1: Dimensionality reduction
float[][] reducedVectors = applyPCA(vectors, 100); // Reduce to 100D
VectorSpace reducedSpace = new MyVectorSpace(reducedVectors, labels);

// Strategy 2: Feature selection
int[] importantDimensions = selectTopFeatures(vectors, labels, 200);
float[][] selectedVectors = extractFeatures(vectors, importantDimensions);

// Strategy 3: Random projection (faster than PCA)
float[][] projectedVectors = randomProjection(vectors, 150);
```

### Sparse Data

```java
// Use sparse representations if many dimensions are zero
public class SparseVectorSpace implements VectorSpace {
    private final Map<Integer, Map<Integer, Float>> sparseVectors;
    
    @Override
    public float[] getVector(int index) {
        Map<Integer, Float> sparse = sparseVectors.get(index);
        float[] dense = new float[dimension];
        sparse.forEach((dim, value) -> dense[dim] = value);
        return dense;
    }
}
```

### Time Series Data

For temporal vector data:

```java
// Analyze windows instead of full time series
public List<AnalysisReport> analyzeTimeWindows(List<VectorSpace> timeWindows) {
    return timeWindows.parallelStream()
                     .map(window -> {
                         VectorSpaceAnalyzer analyzer = new VectorSpaceAnalyzer();
                         return analyzer.analyzeVectorSpace(window);
                     })
                     .collect(Collectors.toList());
}
```

## Monitoring Performance

### Built-in Timing

```java
public class TimedAnalysis {
    public void analyzeWithTiming(VectorSpace vectorSpace) {
        long startTime = System.currentTimeMillis();
        
        VectorSpaceAnalyzer analyzer = new VectorSpaceAnalyzer();
        
        System.out.println("Starting analysis of " + vectorSpace.getVectorCount() + " vectors");
        
        VectorSpaceAnalyzer.AnalysisReport report = analyzer.analyzeVectorSpace(vectorSpace);
        
        long totalTime = System.currentTimeMillis() - startTime;
        System.out.printf("Analysis completed in %.2f seconds\n", totalTime / 1000.0);
        
        // Memory usage
        Runtime runtime = Runtime.getRuntime();
        long usedMemory = runtime.totalMemory() - runtime.freeMemory();
        System.out.printf("Memory used: %.1f MB\n", usedMemory / (1024.0 * 1024.0));
    }
}
```

### Progress Monitoring

For long-running analyses:

```java
public class ProgressMonitor {
    public void analyzeWithProgress(VectorSpace vectorSpace) {
        int totalVectors = vectorSpace.getVectorCount();
        System.out.println("Analyzing " + totalVectors + " vectors...");
        
        // Could be enhanced to show actual progress if measures support it
        VectorSpaceAnalyzer analyzer = new VectorSpaceAnalyzer();
        VectorSpaceAnalyzer.AnalysisReport report = analyzer.analyzeVectorSpace(vectorSpace);
        
        System.out.println("Analysis complete!");
    }
}
```

## Troubleshooting Performance Issues

### Common Bottlenecks

1. **Distance Computation**: O(n²) scaling
   - **Solution**: Use approximate nearest neighbors
   - **Solution**: Reduce dimensionality first
   - **Solution**: Use sampling for exploration

2. **Memory Allocation**: Large intermediate arrays
   - **Solution**: Increase heap size
   - **Solution**: Use streaming implementations
   - **Solution**: Process in batches

3. **Cache Misses**: Repeated computation
   - **Solution**: Use persistent cache directory
   - **Solution**: Ensure stable vector space IDs
   - **Solution**: Verify cache directory is writable

### Memory Issues

**OutOfMemoryError during analysis:**
```bash
# Increase heap size
java -Xmx16g MyAnalysisApp

# Use G1GC for better large heap performance
java -XX:+UseG1GC -Xmx16g MyAnalysisApp

# Monitor memory usage
java -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Xmx16g MyAnalysisApp
```

**Memory leak symptoms:**
- Increasing memory usage over multiple analyses
- Check for static caches that aren't being cleared
- Use `analyzer.clearCache()` between analyses if needed

### CPU Performance

**Slow distance computations:**
```java
// Profile with different k-values
for (int k : Arrays.asList(5, 10, 15, 20, 25)) {
    long start = System.currentTimeMillis();
    LIDMeasure measure = new LIDMeasure(k);
    // ... analyze
    long time = System.currentTimeMillis() - start;
    System.out.printf("k=%d: %.2fs\n", k, time / 1000.0);
}
```

## Best Practices Summary

1. **Always use caching** for repeated analysis
2. **Start with samples** for large datasets  
3. **Reduce dimensions** if d > 1000
4. **Tune k-values** based on accuracy needs
5. **Monitor memory usage** and adjust heap size
6. **Use appropriate data structures** (sparse for sparse data)
7. **Profile before optimizing** specific bottlenecks
8. **Consider approximations** for very large datasets

## Next Steps

- See [examples](examples/) for optimized implementation patterns
- Check [API reference](api-reference.md) for configuration options
- Learn about [visualization](visualization.md) performance considerations