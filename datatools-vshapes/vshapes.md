# vshapes Module

The **vshapes** module provides a type-algebraic framework for modeling, analyzing, and
extracting statistical distributions from vector datasets. It enables you to capture
the "shape" of a vector space as a composable model that can be serialized, analyzed,
and used to generate synthetic vectors with matching statistical properties.

## Core Concept: Tensor Model Hierarchy

The module is built around a three-level **tensor model hierarchy** that mirrors the
mathematical structure of vector spaces:

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           TENSOR MODEL HIERARCHY                                │
└─────────────────────────────────────────────────────────────────────────────────┘

     ORDER        INTERFACE          MODELS                    MEANING
    ───────      ───────────        ────────                  ─────────
       1st       ScalarModel    →   Distribution         One dimension's values
                      │              for dim d            follow some P(x)
                      │
       2nd       VectorModel    →   M ScalarModels       Vector of M dims, each
                      │              + N cardinality      with its own distribution
                      │
       3rd       MatrixModel    →   K VectorModels       Collection of K separate
                                     (clusters)           vector distributions

    Each level composes the level below it.
```

### Mathematical Interpretation

| Level | Tensor Order | Notation | Interpretation |
|-------|--------------|----------|----------------|
| ScalarModel | 1st-order | P_d(x) | Probability distribution for dimension d |
| VectorModel | 2nd-order | V = (P_0, P_1, ..., P_{M-1}), N | M distributions + N unique vectors |
| MatrixModel | 3rd-order | M = (V_0, V_1, ..., V_{K-1}) | K independent vector distributions |

## Type Hierarchy

The type hierarchy separates **model interfaces** (what you work with) from
**concrete implementations** (what holds the data):

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              TYPE HIERARCHY                                     │
└─────────────────────────────────────────────────────────────────────────────────┘

                              ScalarModel (interface)
                                    │
       ┌────────────┬───────────────┼───────────────┬────────────┬────────────┐
       │            │               │               │            │            │
       ▼            ▼               ▼               ▼            ▼            ▼
NormalScalarModel  UniformScalarModel  EmpiricalScalarModel  CompositeScalarModel  ...
                                                                              │
                                              ┌───────────────────────────────┘
                                              │ Pearson Distribution Types:
                                              │ BetaScalarModel, GammaScalarModel,
                                              │ StudentTScalarModel, InverseGammaScalarModel,
                                              │ BetaPrimeScalarModel, PearsonIVScalarModel


                              VectorModel (interface)
                                    │
                                    │ implements
                                    ▼
                              VectorSpaceModel
                                    │
                                    │ implements
                                    ▼
                          IsomorphicVectorModel (marker interface)


                              MatrixModel (interface)
                                    │
                                    ▼
                               (future implementations)
```

## ScalarModel: First-Order Tensors

A `ScalarModel` describes the probability distribution for a single dimension.
Each implementation captures the natural parameters of its distribution:

```
┌───────────────────────────────────────────────────────────────────────────────┐
│                           SCALAR MODEL TYPES                                  │
└───────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────┐   ┌─────────────────────────┐
│   NormalScalarModel     │   │   UniformScalarModel    │
├─────────────────────────┤   ├─────────────────────────┤
│  mean: double           │   │  lower: double          │
│  stdDev: double         │   │  upper: double          │
│  lower: double (opt)    │   │                         │
│  upper: double (opt)    │   │  P(x) = 1/(upper-lower) │
│                         │   │  for x ∈ [lower, upper] │
│  P(x) = N(mean, σ²)     │   │                         │
│  optionally truncated   │   │                         │
└─────────────────────────┘   └─────────────────────────┘

┌─────────────────────────┐   ┌─────────────────────────┐
│  EmpiricalScalarModel   │   │  CompositeScalarModel   │
├─────────────────────────┤   ├─────────────────────────┤
│  binEdges: double[]     │   │  models: ScalarModel[]  │
│  cdf: double[]          │   │  weights: double[]      │
│                         │   │                         │
│  Non-parametric         │   │  P(x) = Σ wᵢ Pᵢ(x)     │
│  histogram-based        │   │  Mixture model          │
│  distribution           │   │                         │
└─────────────────────────┘   └─────────────────────────┘

                    PEARSON DISTRIBUTION TYPES
┌─────────────────────────┐   ┌─────────────────────────┐
│   BetaScalarModel       │   │   GammaScalarModel      │
├─────────────────────────┤   ├─────────────────────────┤
│  alpha, beta: double    │   │  shape, scale: double   │
│  Bounded [0,1]          │   │  Right-skewed           │
└─────────────────────────┘   └─────────────────────────┘

┌─────────────────────────┐   ┌─────────────────────────┐
│   StudentTScalarModel   │   │  InverseGammaScalarModel│
├─────────────────────────┤   ├─────────────────────────┤
│  df: double             │   │  shape, scale: double   │
│  location, scale        │   │  Heavy-tailed positive  │
│  Heavy-tailed symmetric │   │                         │
└─────────────────────────┘   └─────────────────────────┘

┌─────────────────────────┐   ┌─────────────────────────┐
│  BetaPrimeScalarModel   │   │  PearsonIVScalarModel   │
├─────────────────────────┤   ├─────────────────────────┤
│  alpha, beta: double    │   │  m, nu, a, lambda       │
│  Positive unbounded     │   │  Skewed, heavy-tailed   │
└─────────────────────────┘   └─────────────────────────┘
```

### Pearson Distribution System

The module implements Karl Pearson's distribution classification system, which
categorizes continuous probability distributions based on their first four moments
(specifically skewness β₁ and kurtosis β₂):

```
┌───────────────────────────────────────────────────────────────────────────────┐
│                      PEARSON CLASSIFICATION SPACE                             │
└───────────────────────────────────────────────────────────────────────────────┘

  β₂ (kurtosis)
   │
   │           Type VII (Student's t)
   │              symmetric, heavy-tailed
 6 ┤                    ●
   │
   │           ╱ Type IV region
 5 ┤         ╱   (unbounded asymmetric)
   │       ╱
   │     ╱    Type VI (Beta Prime)
 4 ┤   ╱        (positive unbounded)
   │ ╱
   │╱─────────── Type V line (Inverse Gamma) ───────────
 3 ┼────●────────────────────────────────────────────────── β₁ (skewness²)
   │  Type 0     Type III line (Gamma)
   │  (Normal)     ╲
 2 ┤  Type II      ╲ Type I (Beta)
   │  (Symmetric    ╲  (bounded)
   │   Beta)         ╲
 1 ┤                  ╲
   │
   └────┬────┬────┬────┬────┬────┬────┬────
        0   0.5   1   1.5   2   2.5   3   → β₁
```

**Classification Criterion (κ):**

```
κ = β₁(β₂ + 3)² / [4(2β₂ - 3β₁ - 6)(4β₂ - 3β₁)]
```

| Region | Type | Distribution | Characteristics |
|--------|------|--------------|-----------------|
| β₁ ≈ 0, β₂ ≈ 3 | 0 | Normal | Symmetric, mesokurtic |
| β₁ ≈ 0, β₂ < 3 | II | Symmetric Beta | Symmetric, platykurtic |
| β₁ ≈ 0, β₂ > 3 | VII | Student's t | Symmetric, heavy-tailed |
| κ < 0 | I | Beta | Bounded, flexible shape |
| κ ≈ 0 | III | Gamma | Right-skewed, semi-bounded |
| 0 < κ < 1 | IV | Pearson IV | Skewed, unbounded |
| κ ≈ 1 | V | Inverse Gamma | Heavy right tail |
| κ > 1 | VI | Beta Prime | Positive, heavy tail |

**Automatic Model Selection:**

The `BestFitSelector` uses `PearsonClassifier` to automatically choose the
appropriate distribution type for each dimension based on observed moments:

```java
// Classifier determines distribution type from moments
PearsonType type = PearsonClassifier.classify(skewness, kurtosis);

// Selector fits the appropriate model
switch (type) {
    case TYPE_0_NORMAL -> new NormalModelFitter().fit(data);
    case TYPE_I_BETA -> new BetaModelFitter().fit(data);
    case TYPE_III_GAMMA -> new GammaModelFitter().fit(data);
    // ... etc
}
```

### Design Principle: Type-Specific Parameters

Each model type carries **only** its natural parameters. There are no forced
generic accessors like `getMean()` on all types - that would be meaningless
for an empirical distribution. This keeps the type algebra clean:

```java
// Type-safe access to distribution parameters
if (model instanceof NormalScalarModel normal) {
    double mean = normal.getMean();
    double stdDev = normal.getStdDev();
    if (normal.isTruncated()) {
        double lower = normal.lower();
        double upper = normal.upper();
    }
}
```

## VectorModel: Second-Order Tensors

A `VectorModel` composes M `ScalarModel` instances into a vector space definition:

```
┌───────────────────────────────────────────────────────────────────────────────┐
│                              VECTOR MODEL                                     │
└───────────────────────────────────────────────────────────────────────────────┘

  VectorModel v:
  ┌─────────────────────────────────────────────────────────────────────────┐
  │  uniqueVectors(): long     →  N = cardinality (e.g., 1,000,000)         │
  │  dimensions(): int         →  M = dimensionality (e.g., 128)            │
  │  scalarModel(d): ScalarModel → distribution for dimension d             │
  │  scalarModels(): ScalarModel[] → all M distributions                    │
  └─────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
  ┌─────────────────────────────────────────────────────────────────────────┐
  │  d=0: NormalScalarModel(μ₀=0.12, σ₀=0.05)                               │
  │  d=1: NormalScalarModel(μ₁=-0.03, σ₁=0.08)                              │
  │  d=2: UniformScalarModel(lower=-1.0, upper=1.0)     ← heterogeneous!    │
  │  ...                                                                    │
  │  d=M-1: NormalScalarModel(μₘ₋₁=0.00, σₘ₋₁=0.10)                         │
  └─────────────────────────────────────────────────────────────────────────┘
```

### VectorSpaceModel: The Primary Implementation

`VectorSpaceModel` is the concrete implementation of `VectorModel`:

```java
// Uniform Normal distribution across all 128 dimensions
VectorSpaceModel model1 = new VectorSpaceModel(1_000_000, 128, 0.0, 1.0);

// Per-dimension heterogeneous distributions
ScalarModel[] dims = {
    new NormalScalarModel(0.0, 1.0),
    new UniformScalarModel(-1.0, 1.0),
    new NormalScalarModel(5.0, 0.5)
};
VectorSpaceModel model2 = new VectorSpaceModel(1_000_000, dims);

// Truncated Normal (values bounded to [-1, 1])
VectorSpaceModel model3 = new VectorSpaceModel(1_000_000, 128, 0.0, 1.0, -1.0, 1.0);
```

### IsomorphicVectorModel: Optimization Marker

When all dimensions share the same `ScalarModel` type (though possibly with
different parameters), the model is **isomorphic**. This enables vectorized
sampling optimizations:

```
┌───────────────────────────────────────────────────────────────────────────────┐
│                    ISOMORPHIC vs HETEROGENEOUS                                │
└───────────────────────────────────────────────────────────────────────────────┘

   ISOMORPHIC (all same type):              HETEROGENEOUS (mixed types):
   ┌─────────────────────────┐              ┌─────────────────────────┐
   │ d=0: Normal(μ₀, σ₀)     │              │ d=0: Normal(μ₀, σ₀)     │
   │ d=1: Normal(μ₁, σ₁)     │              │ d=1: Uniform(a₁, b₁)    │  ← different!
   │ d=2: Normal(μ₂, σ₂)     │              │ d=2: Empirical(hist)    │  ← different!
   └─────────────────────────┘              └─────────────────────────┘
            │                                        │
            ▼                                        ▼
   isIsomorphic() = true                    isIsomorphic() = false
   → vectorized sampler                     → per-dimension dispatch
```

Usage in sampler resolution:

```java
if (model instanceof IsomorphicVectorModel ivm && ivm.isIsomorphic()) {
    Class<?> type = ivm.scalarModelClass();
    if (NormalScalarModel.class.isAssignableFrom(type)) {
        return new VectorizedNormalSampler(model);  // SIMD-optimized
    }
}
return new ComponentWiseSampler(model);  // fallback
```

## MatrixModel: Third-Order Tensors

`MatrixModel` composes K `VectorModel` instances for multi-cluster datasets:

```
┌───────────────────────────────────────────────────────────────────────────────┐
│                              MATRIX MODEL                                     │
└───────────────────────────────────────────────────────────────────────────────┘

  MatrixModel m:
  ┌─────────────────────────────────────────────────────────────────────────┐
  │  vectorModelCount(): int      →  K = number of clusters/vector models   │
  │  vectorModel(k): VectorModel  →  the k-th vector model                  │
  │  vectorModels(): VectorModel[] → all K vector models                    │
  └─────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
  ┌─────────────────────────────────────────────────────────────────────────┐
  │  k=0: VectorModel(N₀=100K,  M₀=128, [Gaussian distributions])           │
  │  k=1: VectorModel(N₁=500K,  M₁=128, [Gaussian distributions])           │
  │  k=2: VectorModel(N₂=50K,   M₂=128, [Uniform distributions])            │
  └─────────────────────────────────────────────────────────────────────────┘

  Use cases:
    • Multi-cluster synthetic datasets
    • Hierarchical embeddings
    • Multi-modal vector spaces
```

## Model Extraction Pipeline

The **extract** package provides a pipeline for fitting models to observed data:

```
┌───────────────────────────────────────────────────────────────────────────────┐
│                         MODEL EXTRACTION PIPELINE                             │
└───────────────────────────────────────────────────────────────────────────────┘

  float[][] data                   ModelExtractor               VectorSpaceModel
  [numVectors][M]      ──────────────────────────────────────►  (N, M, models[])
        │                                │
        │                                │
        ▼                                ▼
  ┌─────────────┐               ┌────────────────────┐
  │  transpose  │               │ For each dim d:    │
  │  to columns │               │                    │
  └──────┬──────┘               │  1. Compute stats  │
         │                      │  2. Fit candidates │
         ▼                      │  3. Select best    │
  float[M][numVectors]          └────────────────────┘
```

### Fitting Architecture

```
┌───────────────────────────────────────────────────────────────────────────────┐
│                           FITTING ARCHITECTURE                                │
└───────────────────────────────────────────────────────────────────────────────┘

                              ComponentModelFitter (interface)
                                      │
    ┌──────────────┬──────────────────┼──────────────────┬──────────────┐
    │              │                  │                  │              │
    ▼              ▼                  ▼                  ▼              ▼
NormalModel   UniformModel    EmpiricalModel      BetaModel       GammaModel
  Fitter         Fitter          Fitter            Fitter          Fitter
                                                     │
                                    ┌────────────────┼────────────────┐
                                    ▼                ▼                ▼
                             StudentTModel    InverseGamma     BetaPrimeModel
                               Fitter        ModelFitter          Fitter
                                                                     │
                                                                     ▼
                                                            PearsonIVModelFitter
    │                          │                          │
    └──────────────────────────┼──────────────────────────┘
                               │
                               ▼
                 ┌─────────────────────────┐
                 │     BestFitSelector     │
                 │  + PearsonClassifier    │  ← Uses moment-based classification
                 └─────────────────────────┘
                               │
                   ┌───────────┴───────────┐
                   ▼                       ▼
             FitResult                 ScalarModel
             (score, type)             (best-fit model)
```

### Fit Selection Strategies

```java
// Automatic best-fit selection (default)
DatasetModelExtractor extractor = new DatasetModelExtractor();
VectorSpaceModel model = extractor.extractVectorModel(data);

// Force Normal distribution models only
DatasetModelExtractor normal = DatasetModelExtractor.normalOnly();

// Parametric only (Normal or Uniform, no Empirical)
DatasetModelExtractor parametric = DatasetModelExtractor.parametricOnly();

// Custom selector with specific fitters
BestFitSelector selector = BestFitSelector.withFitters(
    new NormalModelFitter(),
    new UniformModelFitter(),
    new BetaModelFitter()
);
DatasetModelExtractor custom = new DatasetModelExtractor(selector, 1_000_000);
```

## Streaming Analysis Framework

The **stream** package provides infrastructure for streaming analysis of vector data:

```
┌───────────────────────────────────────────────────────────────────────────────┐
│                        STREAMING ANALYSIS FRAMEWORK                           │
└───────────────────────────────────────────────────────────────────────────────┘

    DataSource                  AnalyzerHarness              AnalysisResults
  ┌───────────┐              ┌─────────────────┐            ┌──────────────┐
  │ float[][] │──────────────│  Orchestrates   │───────────►│ name → value │
  │  vectors  │   streams    │  multiple       │  produces  │   mappings   │
  └───────────┘   vectors    │  analyzers      │            └──────────────┘
                             └────────┬────────┘
                                      │
                        ┌─────────────┼─────────────┐
                        ▼             ▼             ▼
               ┌────────────┐ ┌────────────┐ ┌────────────┐
               │ Dimension  │ │ Dimension  │ │  Custom    │
               │ Distrib.   │ │ Statistics │ │ Analyzer   │
               │ Analyzer   │ │ Analyzer   │ │            │
               └────────────┘ └────────────┘ └────────────┘
```

### StreamingAnalyzer Interface

Analyzers implement online algorithms that update incrementally:

```java
public interface StreamingAnalyzer {
    void accept(float[] vector);           // Process one vector
    void acceptBatch(float[][] vectors);   // Process batch
    AnalysisResults results();             // Get current results
    void reset();                          // Clear state
}
```

### Built-in Analyzers

| Analyzer | Output | Purpose |
|----------|--------|---------|
| `DimensionDistributionAnalyzer` | `VectorSpaceModel` | Fits per-dim distributions |
| `DimensionStatisticsAnalyzer` | `DimensionStatisticsModel` | Computes per-dim moments |

## Serialization

`VectorSpaceModelConfig` provides JSON serialization with automatic format optimization:

```
┌───────────────────────────────────────────────────────────────────────────────┐
│                           SERIALIZATION FORMATS                               │
└───────────────────────────────────────────────────────────────────────────────┘

  UNIFORM FORMAT (when all dims identical):
  ┌──────────────────────────────────┐
  │ {                                │
  │   "uniqueVectors": 1000000,      │
  │   "dimensions": 128,             │
  │   "mean": 0.0,                   │     Compact: ~100 bytes
  │   "stdDev": 1.0                  │
  │ }                                │
  └──────────────────────────────────┘

  PER-DIMENSION FORMAT (heterogeneous):
  ┌──────────────────────────────────┐
  │ {                                │
  │   "uniqueVectors": 1000000,      │
  │   "components": [                │
  │     {"type":"gaussian",          │     Detailed: ~50 bytes/dim
  │      "mean":0.1, "stdDev":0.05}, │
  │     {"type":"uniform",           │
  │      "lower":-1, "upper":1},     │
  │     ...                          │
  │   ]                              │
  │ }                                │
  └──────────────────────────────────┘
```

```java
// Save model
VectorSpaceModelConfig.save(model, Path.of("model.json"));

// Load model
VectorSpaceModel loaded = VectorSpaceModelConfig.loadFromFile(Path.of("model.json"));
```

## Package Structure

```
io.nosqlbench.vshapes
├── model/                    # Tensor model hierarchy
│   ├── ScalarModel.java         # 1st-order interface
│   ├── VectorModel.java         # 2nd-order interface
│   ├── MatrixModel.java         # 3rd-order interface
│   ├── IsomorphicVectorModel.java  # Optimization marker
│   ├── VectorSpaceModel.java    # Primary VectorModel impl
│   ├── VectorSpaceModelConfig.java  # Serialization
│   │
│   │   # Core Distribution Models
│   ├── NormalScalarModel.java   # Normal (Gaussian) distribution
│   ├── UniformScalarModel.java  # Uniform distribution
│   ├── EmpiricalScalarModel.java # Histogram-based
│   ├── CompositeScalarModel.java # Mixture models
│   │
│   │   # Pearson Distribution Family
│   ├── BetaScalarModel.java     # Beta distribution (bounded [0,1])
│   ├── GammaScalarModel.java    # Gamma distribution (right-skewed)
│   ├── StudentTScalarModel.java # Student's t (heavy-tailed symmetric)
│   ├── InverseGammaScalarModel.java  # Inverse Gamma
│   ├── BetaPrimeScalarModel.java     # Beta Prime (positive unbounded)
│   ├── PearsonIVScalarModel.java     # Pearson Type IV (skewed, heavy-tailed)
│   │
│   ├── NormalCDF.java           # CDF helper
│   ├── PearsonType.java         # Pearson classification enum
│   └── package-info.java
│
├── extract/                  # Model fitting pipeline
│   ├── ModelExtractor.java      # Top-level interface
│   ├── DatasetModelExtractor.java  # Main implementation
│   ├── ComponentModelFitter.java   # Per-dim fitter interface
│   ├── ScalarModelFitter.java   # Base fitter class
│   │
│   │   # Core Fitters
│   ├── NormalModelFitter.java   # Normal distribution fitting
│   ├── UniformModelFitter.java  # Uniform fitting
│   ├── EmpiricalModelFitter.java # Empirical fitting
│   │
│   │   # Pearson Family Fitters
│   ├── BetaModelFitter.java     # Beta distribution fitting
│   ├── GammaModelFitter.java    # Gamma fitting
│   ├── StudentTModelFitter.java # Student's t fitting
│   ├── InverseGammaModelFitter.java  # Inverse Gamma fitting
│   ├── BetaPrimeModelFitter.java     # Beta Prime fitting
│   ├── PearsonIVModelFitter.java     # Pearson Type IV fitting
│   │
│   ├── BestFitSelector.java     # Fit selection strategy
│   ├── PearsonClassifier.java   # Moment-based distribution classification
│   └── DimensionStatistics.java # Per-dim stats
│
├── stream/                   # Streaming analysis
│   ├── StreamingAnalyzer.java   # Analyzer interface
│   ├── StreamingAnalyzerIO.java # SPI discovery
│   ├── AnalyzerHarness.java     # Orchestration
│   ├── DataSource.java          # Data abstraction
│   ├── DataspaceShape.java      # Metadata
│   └── AnalysisResults.java     # Result container
│
└── analyzers/                # Built-in analyzers
    ├── dimensiondistribution/   # Distribution fitting
    │   └── DimensionDistributionAnalyzer.java
    └── dimensionstatistics/     # Statistical moments
        └── DimensionStatisticsAnalyzer.java
```

## Design Principles

1. **Type-Algebraic Composition**: Models compose hierarchically (Scalar → Vector → Matrix)

2. **Type-Specific Parameters**: Each distribution type carries only its natural parameters

3. **Interface/Implementation Separation**: Work with interfaces, store concrete types

4. **Optimization Through Types**: `IsomorphicVectorModel` enables compile-time optimization paths

5. **Streaming-First Analysis**: Online algorithms that don't require full dataset in memory

6. **Serialization Round-Trip**: Models can be saved/loaded with automatic format optimization

## Testing

Tests are organized into three categories that can be run independently or together using Maven profiles:

| Command                              | Unit | Performance | Accuracy |
|--------------------------------------|------|-------------|----------|
| `mvn test`                           |  ✓   |             |          |
| `mvn test -Paccuracy`                |  ✓   |             |    ✓     |
| `mvn test -Pperformance`             |  ✓   |      ✓      |          |
| `mvn test -Pperformance,accuracy`    |  ✓   |      ✓      |    ✓     |
| `mvn test -Palltests`                |  ✓   |      ✓      |    ✓     |

- **Unit tests**: Core functionality tests, run by default
- **Performance tests**: Benchmarks and performance regression tests, tagged with `@Tag("performance")`
- **Accuracy tests**: Numerical precision and statistical accuracy tests, tagged with `@Tag("accuracy")`

## Related Modules

| Module | Relationship |
|--------|--------------|
| `datatools-virtdata` | Provides samplers that consume vshapes models to generate vectors |
| `datatools-vectordata` | Uses vshapes for vector dataset analysis and profiling |
| `datatools-commands` | CLI commands for model extraction and analysis |
