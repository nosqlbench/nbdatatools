# virtdata - Vector Variate Generation

The **virtdata** module provides generators that consume statistical models from the
vshapes module and produce synthetic vectors with matching statistical properties.

## Architectural Boundary

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              vshapes module                                     │
│                         (understanding vector spaces)                           │
├─────────────────────────────────────────────────────────────────────────────────┤
│   Raw Data  ──────▶  Analyzers  ──────▶  Models (pure data)                     │
│   float[][]                               ScalarModel, VectorModel              │
└────────────────────────────────────────────────│────────────────────────────────┘
                                                 │
                                                 ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              virtdata module                                    │
│                           (generating variates)                                 │
├─────────────────────────────────────────────────────────────────────────────────┤
│   Models  ──────▶  Samplers/Generators  ──────▶  Variates                       │
│                    ComponentSampler               float[]                       │
│                    VectorGenerator                float[][]                     │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## Key Principle

**Models describe. Generators produce.**

- Models from vshapes are pure data structures (parameters, statistics)
- Generators in virtdata interpret models and produce samples
- Sampling algorithms (inverse CDF, etc.) live here, not in models

## Tensor Hierarchy Alignment

| vshapes Model | virtdata Sampler/Generator |
|---------------|----------------------------|
| ScalarModel   | ComponentSampler           |
| VectorModel   | VectorGenerator            |

## Component Samplers

Each ScalarModel type has a corresponding sampler implementation:

| ScalarModel | ComponentSampler | Algorithm |
|-------------|------------------|-----------|
| NormalScalarModel | NormalSampler | Inverse CDF via InverseNormalCDF |
| NormalScalarModel (truncated) | TruncatedNormalSampler | Inverse CDF with bounds |
| UniformScalarModel | UniformSampler | Linear interpolation |
| EmpiricalScalarModel | EmpiricalSampler | Piecewise linear CDF inversion |
| CompositeScalarModel | CompositeSampler | Weighted mixture sampling |
| BetaScalarModel | BetaSampler | Inverse incomplete Beta |
| GammaScalarModel | GammaSampler | Inverse incomplete Gamma |
| StudentTScalarModel | StudentTSampler | Inverse t-distribution CDF |
| InverseGammaScalarModel | InverseGammaSampler | Inverse Gamma transform |
| BetaPrimeScalarModel | BetaPrimeSampler | Beta Prime inverse CDF |
| PearsonIVScalarModel | PearsonIVSampler | Numerical inverse CDF |

### Sampler Factory

```java
// Automatic sampler resolution from model type
ScalarSamplerFactory factory = new ScalarSamplerFactory();
ComponentSampler sampler = factory.create(scalarModel);

// Sample a value
double u = 0.5;  // uniform input in [0,1)
double value = sampler.sample(u);
```

## Vector Generators

| Generator | Purpose |
|-----------|---------|
| DimensionDistributionGenerator | Deterministic vectors by ordinal |
| ScalarDimensionDistributionGenerator | Per-dimension streaming |
| ConfigurableDimensionDistributionGenerator | Runtime-configurable options |
| NormalizingVectorGenerator | Unit-normalized output vectors |

### Generator Factory

```java
// Load model from file
VectorSpaceModel model = VectorSpaceModelConfig.loadFromFile(Path.of("model.json"));

// Create generator
VectorGenerator generator = VectorGeneratorIO.get("dimension-distribution", model);

// Generate deterministic vectors by index
float[] v0 = generator.generate(0);
float[] v1 = generator.generate(1);
// v0 and v1 are always the same for the same indices
```

## Stratified Sampling

The `StratifiedSampler` ensures even coverage of the sample space:

```
┌───────────────────────────────────────────────────────────────────────────────┐
│                        STRATIFIED SAMPLING                                     │
└───────────────────────────────────────────────────────────────────────────────┘

  ordinal ∈ [0, N)  ────▶  StratifiedSampler  ────▶  u ∈ [0, 1)
                                │
                                ├── Divides [0,1] into N strata
                                ├── Maps ordinal to stratum center
                                └── Permutes to avoid correlation

  Result: N distinct, evenly-distributed samples
```

## Panama Vector API Support

The module uses multi-release JARs to leverage SIMD acceleration on JDK 25+:

```
META-INF/versions/25/
└── DimensionDistributionGenerator.java  # Panama-accelerated version

src/main/java/
└── DimensionDistributionGenerator.java  # Scalar fallback version
```

On compatible runtimes, vector operations are accelerated using `jdk.incubator.vector`.

## Package Structure

```
io.nosqlbench.datatools.virtdata
├── VectorGenerator.java              # Generator interface
├── VectorGeneratorIO.java            # SPI discovery and factory
├── DimensionDistributionGenerator.java    # Main generator impl
├── ScalarDimensionDistributionGenerator.java  # Streaming variant
├── ConfigurableDimensionDistributionGenerator.java  # Configurable
├── NormalizingVectorGenerator.java   # Unit normalization wrapper
├── StratifiedSampler.java            # Stratified uniform samples
├── GeneratorOptions.java             # Configuration options
├── VectorGenFactory.java             # Factory utilities
│
└── sampling/                         # ComponentSampler implementations
    ├── ComponentSampler.java             # Sampler interface
    ├── ComponentSamplerFactory.java      # Auto-resolution
    ├── ScalarSampler.java                # Base sampler class
    ├── ScalarSamplerFactory.java         # Factory utilities
    │
    │   # Core Samplers
    ├── NormalSampler.java                # Normal distribution
    ├── TruncatedNormalSampler.java       # Truncated normal
    ├── UniformSampler.java               # Uniform distribution
    ├── EmpiricalSampler.java             # Histogram-based
    ├── CompositeSampler.java             # Mixture sampling
    │
    │   # Pearson Family Samplers
    ├── BetaSampler.java                  # Beta distribution
    ├── GammaSampler.java                 # Gamma distribution
    ├── StudentTSampler.java              # Student's t
    ├── InverseGammaSampler.java          # Inverse Gamma
    ├── BetaPrimeSampler.java             # Beta Prime
    ├── PearsonIVSampler.java             # Pearson Type IV
    │
    │   # Utilities
    ├── InverseNormalCDF.java             # Fast inverse CDF
    ├── LerpSampler.java                  # Linear interpolation
    └── LerpSamplerFactory.java           # Lerp factory
```

## Usage Example

```java
// 1. Load or create a model
VectorSpaceModel model = VectorSpaceModelConfig.loadFromFile(Path.of("dataset_model.json"));

// 2. Create a generator
VectorGenerator generator = VectorGeneratorIO.get("dimension-distribution", model);

// 3. Generate vectors
for (long ordinal = 0; ordinal < 1_000_000; ordinal++) {
    float[] vector = generator.generate(ordinal);
    // Each ordinal produces a unique, deterministic vector
    // matching the statistical properties of the source dataset
}
```

## Design Principles

1. **O(1) Sampling**: Each vector generation is constant-time (no iteration/rejection)
2. **Deterministic**: Same ordinal always produces the same vector
3. **Stateless**: Generators are thread-safe and can be shared
4. **Model-Agnostic**: Same generator API works with any ScalarModel type
5. **SIMD-Ready**: Panama acceleration when available

## Testing

Tests are organized into categories using Maven profiles:

| Command                              | Unit | Performance | Accuracy |
|--------------------------------------|------|-------------|----------|
| `mvn test`                           |  ✓   |             |          |
| `mvn test -Paccuracy`                |  ✓   |             |    ✓     |
| `mvn test -Pperformance`             |  ✓   |      ✓      |          |
| `mvn test -Palltests`                |  ✓   |      ✓      |    ✓     |

- **Unit tests**: Core functionality
- **Performance tests**: JMH benchmarks for generation throughput
- **Accuracy tests**: Statistical distribution validation

## Related Modules

| Module | Relationship |
|--------|--------------|
| `datatools-vshapes` | Provides models consumed by generators |
| `datatools-vectordata` | Uses generators for synthetic data |
| `datatools-commands` | CLI for model analysis and generation |
