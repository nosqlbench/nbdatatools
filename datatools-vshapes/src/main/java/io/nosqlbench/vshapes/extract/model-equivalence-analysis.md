# Model Equivalence Analysis

The Model Equivalence Analyzer compares Pearson distribution models against simpler
alternatives to determine if higher-order moment parameters (skewness, kurtosis)
provide meaningful improvement over simpler models.

## Purpose

When fitting distributions using the full Pearson system, higher-order models
(e.g., Pearson Type IV with 4 parameters) may not always be necessary. This analyzer
quantifies how much the additional moment parameters contribute to the distribution
shape, helping users decide whether to use a simpler model.

## Pearson Model Hierarchy

The Pearson system forms a hierarchy based on moment complexity:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        PEARSON MODEL HIERARCHY                              │
│                     (ordered by moment complexity)                          │
└─────────────────────────────────────────────────────────────────────────────┘

  Simplest                                                          Most Complex
  (2 params)              (3 params)               (4 params)
  ┌─────────┐            ┌──────────┐            ┌──────────────┐
  │ Uniform │            │  Gamma   │            │  Pearson IV  │
  │ (min,   │            │ (shape,  │            │ (m, ν, a, λ) │
  │  max)   │            │  scale,  │            │              │
  │         │            │  loc)    │            │ Skewness +   │
  │ β₁=0    │            │          │            │ Kurtosis     │
  │ β₂=1.8  │            │ Skewness │            │              │
  └─────────┘            └──────────┘            └──────────────┘
       ↑                      ↑                        ↑
  ┌─────────┐            ┌──────────┐            ┌──────────────┐
  │ Normal  │            │ Student-t│            │    Beta      │
  │ (μ, σ)  │            │  (ν,μ,σ) │            │  (α,β,a,b)   │
  │         │            │          │            │              │
  │ β₁=0    │            │ β₁=0     │            │ Bounded      │
  │ β₂=3    │            │ β₂>3     │            │ Support      │
  └─────────┘            └──────────┘            └──────────────┘
```

## Divergence Metrics

Three complementary metrics quantify model equivalence:

| Metric | Description | Interpretation |
|--------|-------------|----------------|
| **Max CDF Difference** | Maximum \|F₁(x) - F₂(x)\| | Values < 0.02 indicate practical equivalence |
| **Mean CDF Difference** | Average \|F₁(x) - F₂(x)\| over support | Less sensitive to tail behavior |
| **Moment Deviation** | Relative difference in skewness/kurtosis | Direct measure of parameter significance |

## CLI Usage

### Options

| Option | Description | Default |
|--------|-------------|---------|
| `--analyze-equivalence` | Run equivalence analysis after extraction | false |
| `--equivalence-threshold` | Max CDF difference threshold for equivalence | 0.02 |
| `--apply-simplifications` | Apply recommended simplifications to saved model | false |

### Examples

```bash
# Analyze model equivalence and find simplification opportunities
nbvectors analyze profile -b vectors.fvec -o model.json \
    --extractor full --analyze-equivalence

# Apply simplifications with stricter threshold
nbvectors analyze profile -b vectors.fvec -o model.json \
    --extractor parallel --threads 16 \
    --analyze-equivalence --equivalence-threshold 0.01 --apply-simplifications

# NUMA-aware extraction with equivalence analysis
nbvectors analyze profile -b vectors.fvec -o model.json \
    --extractor numa --analyze-equivalence

# Profile only the first 10000 vectors using inline range
nbvectors analyze profile -b vectors.fvec:10000 -o model.json \
    --extractor full --analyze-equivalence

# Profile a specific range of vectors
nbvectors analyze profile -b vectors.fvec:[5000,15000) -o model.json
```

## Example Run

### Basic Analysis (without simplification)

```
$ nbvectors analyze profile -b testxvec_base.fvec -o model.json --extractor full --analyze-equivalence

Profiling vector file: testxvec_base.fvec
  Extraction mode: full
  Model type: auto
  Vectors in file: 25000
  Dimensions: 100
  Sampling: 25000 vectors
  Using single-threaded full extractor
  Extracting model...
  Extraction completed in 409 ms

Model Summary:
  Dimensions: 100
  Unique vectors: 1000000
  Distribution types:
    empirical: 100 dimensions (100.0%)

Model Equivalence Analysis:
  Equivalence threshold: 0.0200
VectorSimplificationSummary:
  Total dimensions: 100
  Can simplify: 100 (100.0%)
  Normal-equivalent: 100
  Uniform-equivalent: 0
  Average max CDF diff: 0.0000
  Recommended types: {normal=100}

  Use --apply-simplifications to apply recommended changes.
VectorSpaceModel config saved to: model.json
```

### With Applied Simplifications

```
$ nbvectors analyze profile -b testxvec_base.fvec -o model.json \
    --extractor parallel --threads 8 --analyze-equivalence --apply-simplifications

Profiling vector file: testxvec_base.fvec
  Extraction mode: parallel
  Model type: auto
  Vectors in file: 25000
  Dimensions: 100
  Sampling: 25000 vectors
  Using parallel extractor with 8 threads
  Extracting model...
  Extraction completed in 405 ms

Model Summary:
  Dimensions: 100
  Unique vectors: 1000000
  Distribution types:
    empirical: 100 dimensions (100.0%)

Model Equivalence Analysis:
  Equivalence threshold: 0.0200
VectorSimplificationSummary:
  Total dimensions: 100
  Can simplify: 100 (100.0%)
  Normal-equivalent: 100
  Uniform-equivalent: 0
  Average max CDF diff: 0.0000
  Recommended types: {normal=100}

  Applying 100 simplifications...
  Applied 100 simplifications.
VectorSpaceModel config saved to: model.json
```

### NUMA-Aware Extraction

```
$ nbvectors analyze profile -b testxvec_base.fvec -o model.json --extractor numa --analyze-equivalence

Profiling vector file: testxvec_base.fvec
  Extraction mode: numa
  Model type: auto
  Vectors in file: 25000
  Dimensions: 100
  Sampling: 25000 vectors
  Using NUMA-aware extractor: 2 nodes × 59 threads
  Extracting model...
  Extraction completed in 622 ms

Model Summary:
  Dimensions: 100
  Unique vectors: 1000000
  Distribution types:
    empirical: 100 dimensions (100.0%)

Model Equivalence Analysis:
  Equivalence threshold: 0.0200
VectorSimplificationSummary:
  Total dimensions: 100
  Can simplify: 100 (100.0%)
  Normal-equivalent: 100
  Uniform-equivalent: 0
  Average max CDF diff: 0.0000
  Recommended types: {normal=100}

  Use --apply-simplifications to apply recommended changes.
VectorSpaceModel config saved to: model.json
```

## Output Model Comparison

### Before Simplification (Empirical per-dimension)

```json
{
  "unique_vectors": 25000,
  "components": [
    {
      "type": "empirical",
      "mean": -0.001895864,
      "std_dev": 0.575120815,
      "bins": [
        -0.999836087, -0.874854110, -0.749872133, ...
      ],
      "min": -0.999836087,
      "max": 0.999875545
    },
    // ... 99 more empirical components
  ]
}
```

### After Simplification (Uniform Normal)

When all dimensions simplify to identical Normal distributions:

```json
{
  "unique_vectors": 25000,
  "dimensions": 100,
  "mean": 0.0,
  "std_dev": 1.0,
  "components": null
}
```

This compact representation is much smaller and faster to use for vector generation.

## Programmatic Usage

```java
import io.nosqlbench.vshapes.extract.ModelEquivalenceAnalyzer;
import io.nosqlbench.vshapes.model.*;

// Analyze a single scalar model
PearsonIVScalarModel p4 = new PearsonIVScalarModel(3.0, 0.1, 1.0, 0.0);
ModelEquivalenceAnalyzer analyzer = new ModelEquivalenceAnalyzer();

EquivalenceReport report = analyzer.analyze(p4);
System.out.println(report);

if (report.canSimplify()) {
    ScalarModel simplified = report.getRecommendedSimplification();
    System.out.println("Simplified to: " + simplified.getModelType());
}

// Analyze a vector model
VectorSpaceModel vectorModel = ...;
VectorSimplificationSummary summary = analyzer.summarizeVector(vectorModel);
System.out.println(summary);
```

## Interpretation Guidelines

| Simplification Rate | Interpretation |
|---------------------|----------------|
| > 90% | Data is well-approximated by standard distributions |
| 50-90% | Mixed characteristics; some dimensions benefit from complex models |
| < 50% | Data has significant non-standard features; keep full models |

| Model Type | When to Use |
|------------|-------------|
| **Uniform** | Data uniformly distributed within bounds |
| **Normal** | Symmetric, mesokurtic (kurtosis ≈ 3) data |
| **Student-t** | Symmetric but heavy-tailed data |
| **Gamma** | Positive skewed, semi-bounded data |
| **Empirical** | Non-standard distributions; captures exact shape |

## Performance Considerations

- Equivalence analysis adds minimal overhead (< 100ms for 100 dimensions)
- The analysis runs after extraction, so it doesn't affect extraction time
- Simplified models are smaller and faster for downstream vector generation
- NUMA-aware extraction provides best performance on multi-socket systems

## See Also

- `ModelEquivalenceAnalyzer.java` - The core analyzer implementation
- `VectorSpaceModelConfig.java` - Model serialization/deserialization
- `PearsonType.java` - Pearson distribution classification
