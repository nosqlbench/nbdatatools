# Profile Command - Algorithmic Flow

This document describes the algorithmic flow of the `analyze profile` command,
which extracts statistical distribution models from vector datasets.

## Overview

The profile command analyzes a vector dataset and builds a `VectorSpaceModel`
configuration that captures the statistical distribution of each dimension.
The resulting model can be used to generate synthetic vectors that match the
original dataset's statistical properties.

## Entry Point

**Command Class:** `CMD_analyze_profile`
**Location:** `datatools-commands/.../subcommands/CMD_analyze_profile.java`

## High-Level Data Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                          INPUT VECTOR FILE                          │
│                      (fvec, dvec, ivec, bvec)                       │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      VECTOR LOADING & SAMPLING                      │
│  • Read metadata (vector count, dimensions)                         │
│  • Apply range constraints if specified                             │
│  • Sample vectors if requested                                      │
│  • Load data (standard I/O or memory-mapped)                        │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     CHOOSE EXTRACTION STRATEGY                      │
│  Based on --compute flag and file size                              │
│  Auto-select STREAMING for files > 1M vectors                       │
└─────────────────────────────────────────────────────────────────────┘
                              │
          ┌───────────────────┼───────────────────┐
          │                   │                   │
          ▼                   ▼                   ▼
    [FULL MODE]        [STREAMING MODE]    [FAST MODE]
    VectorSpace        StreamingModel      DimStatsOnly
    Model Extractor    Extractor           (Gaussian-only)
          │                   │                   │
          └───────────────────┼───────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│          PER-DIMENSION ANALYSIS & MODEL FITTING                     │
│  For each dimension d:                                              │
│    1. Compute DimensionStatistics (mean, variance, skewness, ...)   │
│    2. Fit candidate distribution models                             │
│    3. Select best-fit model based on goodness-of-fit score          │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    CONSTRUCT VECTOR SPACE MODEL                     │
│  Aggregate per-dimension ScalarModels into VectorSpaceModel         │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      OPTIONAL POST-PROCESSING                       │
│  • Apply truncation bounds (--truncated)                            │
│  • Override specific dimensions (--empirical-dimensions)            │
│  • Analyze model equivalence (--analyze-equivalence)                │
│  • Display fit quality table (--show-fit-table)                     │
│  • Perform round-trip verification (--verify)                       │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    SAVE TO JSON OUTPUT FILE                         │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Compute Strategies

| Strategy | Description | Best For |
|----------|-------------|----------|
| **RESIDENT** | Virtual threads + SIMD + microbatching | Small/medium files (default) |
| **SIMD** | Single-threaded with SIMD acceleration | Maximum compatibility |
| **PARALLEL** | Multi-threaded with ForkJoinPool | Large files, multi-core |
| **NUMA** | NUMA-aware for multi-socket systems | Multi-socket servers |
| **STREAMING** | Chunked processing | Files exceeding heap memory |
| **FAST** | Gaussian-only statistics | Quick profiling |

---

## Model Types

| Model Type | Description |
|------------|-------------|
| **auto** | Best-fit from bounded distributions (Normal, Uniform, Empirical) |
| **pearson** | Full Pearson family (Normal, Beta, Gamma, StudentT, PearsonIV, InverseGamma, BetaPrime, Uniform) |
| **normal** | Force Normal distribution only |
| **uniform** | Force Uniform distribution only |
| **empirical** | Use empirical histogram only |
| **parametric** | Parametric distributions only (no empirical) |

---

## Detailed Algorithmic Steps

### Phase 1: Initialization and Validation

1. **Parse inputs:**
   - Load base vector file path from CLI arguments
   - Parse inline range specification (e.g., `file.fvec:[1000,5000)`)
   - Detect file format (fvec, dvec, ivec, bvec)
   - Verify file contains float vectors

2. **Handle checkpointing:**
   - If `--resume` flag set, load latest checkpoint
   - Create checkpoint directory if specified

3. **Auto-selection of compute strategy:**
   - If RESIDENT mode and file > 1M vectors → auto-switch to STREAMING

### Phase 2: Vector Loading

**Full Mode Loading:**

1. **Read metadata:**
   - Open VectorFileArray for random access
   - Extract total vector count and dimensionality
   - Apply range constraints if specified

2. **Determine loading strategy:**
   - Check if memory-mapped I/O is available
   - Determine if data should be loaded in transposed format
   - Choose between standard I/O and memory-mapped I/O

3. **Load with progress reporting:**
   - Display progress bar showing percentage
   - Support sampling: if `--sample` specified, randomly select subset

**Data Formats:**
- Row-major: `float[numVectors][numDimensions]`
- Column-major (transposed): `float[numDimensions][numVectors]` (more efficient for per-dimension processing)

### Phase 3: Model Extractor Selection

```
RESIDENT → VirtualThreadModelExtractor
  └─ Virtual threads + SIMD microbatching

SIMD → DatasetModelExtractor (or variants)
  ├─ AdaptiveModelExtractor (if adaptive enabled)
  └─ ConvergentDatasetModelExtractor (if convergence enabled)

PARALLEL → ParallelDatasetModelExtractor
  └─ ForkJoinPool with configured parallelism

NUMA → NumaAwareDatasetModelExtractor
  └─ NUMA-aware thread binding

STREAMING → StreamingModelExtractor
  └─ Handles chunks via AnalyzerHarness
```

### Phase 4: Dimension Statistics Computation

**Class:** `DimensionStatistics` (SIMD-optimized for Java 25+)

For each dimension, compute:
- **count** - number of vectors
- **min/max** - minimum and maximum values
- **mean** - arithmetic average
- **variance** - population variance σ²
- **stdDev** - standard deviation (√variance)
- **skewness** - third moment (distribution asymmetry)
- **kurtosis** - fourth moment (distribution tail heaviness)

**Algorithm:**
1. First pass (SIMD): Compute min, max, sum using vectorized reductions
2. Second pass: Compute mean = sum / count
3. Third pass (SIMD): Compute centered moments using Welford's algorithm variant

### Phase 5: Distribution Model Fitting

For each dimension:

1. **Create selector:** `BestFitSelector` with candidate fitters
   - Default: NormalModelFitter, UniformModelFitter, EmpiricalModelFitter
   - Pearson: adds Beta, Gamma, StudentT, PearsonIV, InverseGamma, BetaPrime

2. **For each candidate fitter:**
   - Call `fitter.fit(DimensionStatistics, float[] values)`
   - Compute goodness-of-fit using Kolmogorov-Smirnov D-statistic

3. **Select best model:**
   - Choose model with lowest goodness-of-fit score
   - Store in `FitResult` containing model, score, and type

### Phase 6: Vector Space Model Construction

```java
VectorSpaceModel model = new VectorSpaceModel(uniqueVectors, components);
```

**Model Structure:**
```
VectorSpaceModel
├─ uniqueVectors: long
└─ scalarModels: ScalarModel[M]
   ├─ Dimension 0: NormalScalarModel(μ₀, σ₀)
   ├─ Dimension 1: UniformScalarModel(lower₁, upper₁)
   ├─ Dimension 2: EmpiricalScalarModel(histogram₂)
   └─ ...
```

### Phase 7: Post-Processing (Optional)

| Option | Description |
|--------|-------------|
| `--truncated` | Add explicit truncation bounds to Normal models |
| `--empirical-dimensions` | Override selected dimensions with empirical models |
| `--analyze-equivalence` | Check if simpler models suffice |
| `--show-fit-table` | Display per-dimension fit scores for all types |
| `--verify` | Round-trip verification with synthetic data |

---

## Key Classes

### Input/Output
| Class | Role |
|-------|------|
| `VectorFileIO` | Reads vectors from files |
| `VectorFileArray` | Random access to vectors |
| `VectorSpaceModelConfig` | Serializes models to JSON |

### Statistics
| Class | Role |
|-------|------|
| `DimensionStatistics` | Per-dimension statistics (SIMD optimized) |

### Model Fitting
| Class | Role |
|-------|------|
| `ComponentModelFitter` | Interface for fitting one distribution type |
| `AbstractParametricFitter` | Base class with KS D-statistic scoring |
| `NormalModelFitter` | Fits Normal/Gaussian distribution |
| `UniformModelFitter` | Fits Uniform distribution |
| `EmpiricalModelFitter` | Builds histogram-based distribution |
| `BetaModelFitter` | Fits Beta distribution |
| `GammaModelFitter` | Fits Gamma distribution |
| `StudentTModelFitter` | Fits Student's t distribution |
| `BestFitSelector` | Selects best model from candidates |

### Extraction
| Class | Role |
|-------|------|
| `ModelExtractor` | Interface for extracting VectorSpaceModel |
| `DatasetModelExtractor` | Standard batch-mode extractor |
| `VirtualThreadModelExtractor` | Virtual threads + microbatching |
| `ParallelDatasetModelExtractor` | ForkJoinPool parallelism |
| `StreamingModelExtractor` | Chunk-based streaming |
| `ConvergentDatasetModelExtractor` | Convergence-based early stopping |
| `AdaptiveModelExtractor` | Parametric → Composite → Empirical fallback |

### Streaming
| Class | Role |
|-------|------|
| `StreamingAnalyzer` | Interface for chunk-based analysis |
| `StreamingDimensionAccumulator` | Per-dimension Welford accumulator |
| `AnalyzerHarness` | Orchestrates streaming pipeline |
| `TransposedChunkDataSource` | Reads chunks in column-major format |
| `PrefetchingDataSource` | Double-buffering for prefetch |

---

## Tensor Hierarchy

The vshapes module uses a hierarchical tensor model structure:

```
Level 1: ScalarModel (First-Order Tensor)
  └─ Single dimension distribution
  └─ Types: Normal, Uniform, Empirical, Composite, Beta, Gamma, etc.

Level 2: VectorModel / VectorSpaceModel (Second-Order Tensor)
  └─ Aggregation of M ScalarModels (one per dimension)
  └─ Represents full vector space with per-dimension distributions

Level 3: MatrixModel (Third-Order Tensor)
  └─ Collection of K VectorModels
```

The profile command operates at Level 2, extracting one ScalarModel per
dimension and aggregating into the full VectorSpaceModel.

---

## Goodness-of-Fit Scoring

All parametric fitters use the **Kolmogorov-Smirnov D-statistic** for uniform
scoring:

```
D = max|F_n(x) - F(x)|
```

Where:
- F_n(x) is the empirical CDF of the observed data
- F(x) is the theoretical CDF of the fitted model
- Lower D values indicate better fit (0 = perfect, 1 = worst)

This enables fair comparison across distribution types and automatic model
selection based on fit quality.

---

## Algorithmic Differences Between Modes

The compute modes fall into two categories based on their algorithmic behavior:

### Category 1: Same Algorithm, Different Execution Strategy

These modes produce **mathematically identical results** - they differ only in
how work is parallelized:

| Mode | Parallelism Strategy | Use Case |
|------|---------------------|----------|
| **SIMD** | Single-threaded with SIMD vectors | Baseline, maximum compatibility |
| **RESIDENT** | Virtual threads + microbatching | Default, best for small/medium |
| **PARALLEL** | ForkJoinPool threads | Large files, multi-core |
| **NUMA** | NUMA-aware thread binding | Multi-socket servers |

All use:
- `DimensionStatistics.compute()` for batch SIMD statistics
- Same `BestFitSelector` for model selection
- Same KS D-statistic goodness-of-fit scoring

### Category 2: Different Algorithms

**CONVERGENT** (SIMD with `--convergence` enabled):
- **Early stopping**: Stops before processing all data when statistics stabilize
- Tracks changes in all 4 moments (mean, variance, skewness, kurtosis)
- Checks convergence at configurable checkpoint intervals
- **Results may differ** because only a subset of vectors may be used

**STREAMING**:
- **Welford's online algorithm** instead of batch statistics
- Processes chunks incrementally without loading all data into memory
- **Numerical precision may differ slightly** due to incremental vs batch computation
- Currently has basic convergence tracking (needs enhancement)

**ADAPTIVE** (if enabled):
- **Different model selection strategy entirely**
- Uses fallback chain: `Parametric → Composite (2-4 modes) → Empirical`
- Runs internal verification to detect parameter instability
- **May produce composite (mixture) models** instead of single distributions

### Comparison Matrix

| Aspect | SIMD/RESIDENT/PARALLEL/NUMA | CONVERGENT | STREAMING | ADAPTIVE |
|--------|----------------------------|------------|-----------|----------|
| Statistics | Batch SIMD | Batch SIMD | Welford's online | Batch SIMD |
| Memory | All data in heap | All data in heap | Chunks only | All data in heap |
| Early stopping | No | Yes (4-moment) | Basic | No |
| Model selection | Best fit | Best fit | Best fit | Fallback chain |
| Composite models | No | No | No | Yes |
| Results identical? | Yes (to each other) | May differ (subset) | May differ (precision) | May differ (composites) |

### Current Limitations (Planned Enhancements)

The STREAMING mode currently lacks:
1. **Full 4-moment convergence testing** like CONVERGENT mode
2. **Adaptive fallback chain** like ADAPTIVE mode
3. **Composite model fitting** for multimodal dimensions

Goal: STREAMING should be the universal mode that handles any dataset size
while providing all the algorithmic capabilities of the other modes.

---

## Entry Flow Summary

```java
// In CMD_analyze_profile.call():
1. validateAndParseInputs()
2. auto_selectComputeStrategy(file_size)
3. print_computeCapabilities()

4. switch (effectiveStrategy):
   FAST:
     → profileVectorsFast()         // Gaussian-only
   STREAMING:
     → profileVectorsStreaming()    // Chunk-based
   else (FULL modes):
     → profileVectorsFull()         // All data in memory

5. optional_postProcessing:
   - analyzeEquivalence()
   - applyTruncation()
   - displayFitTable()
   - runRoundTripVerification()

6. VectorSpaceModelConfig.saveToFile(model, outputFile)
7. return 0 (success) or 1 (error)
```
