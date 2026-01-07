# AVX-512 Optimization Plan for Pearson Model Fitting Pipeline

## Executive Summary

The current implementation processes dimensions sequentially, limiting parallelism.
The key insight is that **dimensions are independent** - we can batch process
multiple dimensions simultaneously using AVX-512's 8 double-precision lanes.

## Current Pipeline Analysis

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    CURRENT SEQUENTIAL PIPELINE                              │
└─────────────────────────────────────────────────────────────────────────────┘

For D dimensions, N vectors:

  Input Data                    Processing                      Output
  float[N][D]   ────►  transpose  ────►  float[D][N]
                           │
                           ▼
                For each d in [0, D):        ← SEQUENTIAL BOTTLENECK
                    ├── DimensionStatistics.compute(d, data[d])
                    ├── PearsonClassifier.classify(skewness, kurtosis)
                    ├── For each fitter:
                    │       └── fitter.fit(stats, data[d])
                    │           ├── computeAndersonDarling()  ← SORT + CDF
                    │           └── create model
                    └── BestFitSelector.selectBest()
                           │
                           ▼
                VectorSpaceModel(components)

Total Time: O(D × N × F) where F = number of fitters
```

## Bottleneck Analysis

| Operation | Time Complexity | Vectorizable? | Current State |
|-----------|----------------|---------------|---------------|
| Transpose | O(N × D) | Cache-blocking | Naive nested loop |
| Statistics (per dim) | O(N) | Yes (reductions) | Scalar |
| Anderson-Darling | O(N log N) | Sort: No, CDF: Yes | Scalar sort+CDF |
| Pearson Classify | O(1) | Yes (batch) | Scalar |
| Model fitting | O(1) | Yes (batch) | Scalar |

## Optimization Strategies

### Strategy 1: Multi-Dimension Batch Statistics

**Problem**: Computing statistics for 1 dimension at a time wastes SIMD width.

**Solution**: Process 8 dimensions simultaneously using AVX-512.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│              BATCHED STATISTICS (8 dimensions per iteration)                │
└─────────────────────────────────────────────────────────────────────────────┘

For batch of 8 dimensions [d, d+1, ..., d+7]:

  Vector v:     ┌───┬───┬───┬───┬───┬───┬───┬───┐
                │v[d]│v[d+1]│v[d+2]│v[d+3]│v[d+4]│v[d+5]│v[d+6]│v[d+7]│
                └───┴───┴───┴───┴───┴───┴───┴───┘
                           │
                           ▼ Parallel for all 8 dimensions
  min[8]:       vmin = vmin.min(v)
  max[8]:       vmax = vmax.max(v)
  sum[8]:       vsum = vsum.add(v)

  After all vectors processed:
  - Extract 8 means = vsum / n
  - Second pass: 8 variances, 8 skewnesses, 8 kurtoses in parallel

Speedup: ~8x for statistics computation
```

**Data Layout**: Interleaved across dimensions for coalesced access:
```
data[v * 8 + 0] = vector[v].dim[d+0]
data[v * 8 + 1] = vector[v].dim[d+1]
...
data[v * 8 + 7] = vector[v].dim[d+7]
```

### Strategy 2: Cache-Blocked Transpose

**Problem**: Naive transpose has poor cache behavior - O(D) cache misses per row.

**Solution**: Block transpose to fit in L2 cache (typically 256KB - 1MB).

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      CACHE-BLOCKED TRANSPOSE                                │
└─────────────────────────────────────────────────────────────────────────────┘

Block size B chosen so B² × 4 bytes fits in L2 cache
For B = 256: 256 × 256 × 4 = 256KB

  Input: float[N][D]             Output: float[D][N]

  For bi in [0, N, B):
      For bj in [0, D, B):
          // Transpose block [bi:bi+B, bj:bj+B]
          For i in [bi, min(bi+B, N)):
              For j in [bj, min(bj+B, D)):
                  output[j][i] = input[i][j]

Inner loops can be vectorized with scatter/gather on AVX-512.
```

### Strategy 3: Vectorized CDF Computation

**Problem**: Anderson-Darling test computes CDF for N values sequentially.

**Solution**: Batch CDF computation using polynomial approximation.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│              VECTORIZED NORMAL CDF (8 values per iteration)                 │
└─────────────────────────────────────────────────────────────────────────────┘

Standard normal CDF Φ(z) using Abramowitz-Stegun approximation:

  z[8]:       ┌───┬───┬───┬───┬───┬───┬───┬───┐
              │z₀ │z₁ │z₂ │z₃ │z₄ │z₅ │z₆ │z₇ │
              └───┴───┴───┴───┴───┴───┴───┴───┘
                         │
                         ▼
  t = 1 / (1 + p × |z|)       ← 8-wide division
  poly = ((((a₅t + a₄)t + a₃)t + a₂)t + a₁)t   ← FMA chain
  Φ = 1 - poly × exp(-z²/2)   ← 8-wide exp
                         │
                         ▼
  cdf[8]:     ┌───┬───┬───┬───┬───┬───┬───┬───┐
              │Φ₀ │Φ₁ │Φ₂ │Φ₃ │Φ₄ │Φ₅ │Φ₆ │Φ₇ │
              └───┴───┴───┴───┴───┴───┴───┴───┘

Note: exp() is challenging without libm. Use polynomial approximation or
Java's Math.exp() with SIMD loop (JIT may still vectorize).
```

### Strategy 4: Parallel Dimension Processing

**Problem**: Dimensions are processed sequentially despite being independent.

**Solution**: Fork-Join parallel processing with work-stealing.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│           PARALLEL DIMENSION PROCESSING (no synchronization)                │
└─────────────────────────────────────────────────────────────────────────────┘

                              ForkJoinPool
                                  │
                 ┌────────────────┼────────────────┐
                 ▼                ▼                ▼
            Worker 1         Worker 2         Worker 3
         dims [0, D/3)   dims [D/3, 2D/3)  dims [2D/3, D)
              │                │                │
              ▼                ▼                ▼
         ┌─────────┐     ┌─────────┐     ┌─────────┐
         │ stats   │     │ stats   │     │ stats   │
         │ classify│     │ classify│     │ classify│
         │ fit     │     │ fit     │     │ fit     │
         │ select  │     │ select  │     │ select  │
         └─────────┘     └─────────┘     └─────────┘
              │                │                │
              ▼                ▼                ▼
        models[0:D/3)   models[D/3:2D/3)  models[2D/3:D)

No synchronization needed - each worker writes to its own slice.
Combine with SIMD: each worker processes 8 dimensions at a time.
```

### Strategy 5: Hybrid SIMD + Thread Parallelism

**Optimal Configuration**:
```
Threads = min(num_cores, D / 64)  // At least 64 dims per thread
SIMD batch = 8 dimensions         // AVX-512 double lanes

For a 4096-dimension dataset on 8-core machine:
  - 8 threads, each processing 512 dimensions
  - Each thread uses 8-wide SIMD = 64 batches per thread
  - Total parallelism = 8 threads × 8 SIMD = 64-way
```

## Implementation Priorities

### Phase 1: Batched Statistics (Highest Impact)

Create `BatchDimensionStatistics` that computes stats for 8 dimensions at once:

```java
public class BatchDimensionStatistics {
    private static final VectorSpecies<Double> SPECIES = DoubleVector.SPECIES_512;

    /**
     * Computes statistics for 8 consecutive dimensions simultaneously.
     * Data must be in interleaved format: [d0v0, d1v0, ..., d7v0, d0v1, d1v1, ...]
     *
     * @param interleavedData data interleaved by dimension (8 dims × N vectors)
     * @param numVectors number of vectors
     * @param baseDimension starting dimension index
     * @return array of 8 DimensionStatistics
     */
    public static DimensionStatistics[] computeBatch(
            double[] interleavedData, int numVectors, int baseDimension) {
        // ...
    }
}
```

### Phase 2: Parallel Dimension Processing

Create `ParallelDatasetModelExtractor`:

```java
public class ParallelDatasetModelExtractor implements ModelExtractor {
    private final ForkJoinPool pool;
    private final int batchSize = 8;  // SIMD width

    @Override
    public VectorSpaceModel extractVectorModel(float[][] data) {
        // 1. Transpose to interleaved format
        // 2. Partition dimensions across workers
        // 3. Each worker uses BatchDimensionStatistics
        // 4. Combine results (no sync needed)
    }
}
```

### Phase 3: Vectorized CDF

Create Panama-optimized `NormalCDF`:

```java
public class VectorizedNormalCDF {
    /**
     * Computes 8 CDF values simultaneously.
     */
    public static DoubleVector cdf(DoubleVector z) {
        // Abramowitz-Stegun polynomial approximation
        // Pure arithmetic - no libm calls
    }
}
```

### Phase 4: Cache-Blocked Transpose

Optimize the transpose for large datasets:

```java
public class CacheOptimizedTranspose {
    private static final int BLOCK_SIZE = 256;  // Fits in L2

    public static float[][] transposeBlocked(float[][] input) {
        // Block-based transpose
    }
}
```

## Expected Performance Gains

| Optimization | Speedup Factor | Conditions |
|--------------|---------------|------------|
| Batched Stats (8-wide) | 4-6x | Compute-bound datasets |
| Parallel Processing (8 cores) | 6-7x | Many dimensions |
| Combined SIMD + Parallel | 20-40x | Large datasets |
| Cache-Blocked Transpose | 2-3x | Large matrices |
| Vectorized CDF | 3-5x | Anderson-Darling heavy |

**Total potential speedup: 20-50x** for large datasets (D > 1000, N > 10000).

## Memory Layout Considerations

### Current Layout (Array of Structures - AoS)
```
data[vector][dimension]  ← Poor for dimension-wise access
```

### Optimal Layout (Interleaved)
```
interleavedData[vector * 8 + dimOffset]  ← Coalesced SIMD loads
```

### Alternative (Structure of Arrays - SoA)
```
dimensionData[dimension][vector]  ← Good for single-dimension access
```

**Recommendation**: Use interleaved layout for SIMD batches, convert lazily.

## API Design

```java
// High-level API (unchanged)
DatasetModelExtractor extractor = DatasetModelExtractor.parallel();
VectorSpaceModel model = extractor.extractVectorModel(data);

// Configuration
DatasetModelExtractor extractor = DatasetModelExtractor.builder()
    .parallelism(8)           // Thread count
    .simdBatchSize(8)         // AVX-512 width
    .cacheBlockSize(256)      // Transpose block size
    .build();
```

## Testing Strategy

1. **Accuracy Tests**: Verify SIMD results match scalar within tolerance
2. **Performance Tests**: JMH benchmarks for each optimization
3. **Scaling Tests**: Measure speedup vs dimensions and cardinality
4. **Hardware Tests**: Test on AVX2 (4-wide) and AVX-512 (8-wide)

## Next Steps

1. [ ] Implement `BatchDimensionStatistics` with 8-wide SIMD
2. [ ] Create JMH benchmark comparing single vs batched stats
3. [ ] Implement `ParallelDatasetModelExtractor`
4. [ ] Add `VectorizedNormalCDF` for Anderson-Darling
5. [ ] Profile full pipeline with JFR to identify remaining bottlenecks
