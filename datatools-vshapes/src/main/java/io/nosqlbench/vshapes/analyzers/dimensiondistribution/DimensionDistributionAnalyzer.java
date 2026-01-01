package io.nosqlbench.vshapes.analyzers.dimensiondistribution;

/*
 * Copyright (c) nosqlbench
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import io.nosqlbench.vshapes.extract.BestFitSelector;
import io.nosqlbench.vshapes.extract.ComponentModelFitter;
import io.nosqlbench.vshapes.extract.DimensionStatistics;
import io.nosqlbench.vshapes.model.ComponentModel;
import io.nosqlbench.vshapes.model.VectorSpaceModel;
import io.nosqlbench.vshapes.stream.AnalyzerName;
import io.nosqlbench.vshapes.stream.DataspaceShape;
import io.nosqlbench.vshapes.stream.StreamingAnalyzer;

/// Streaming analyzer that extracts per-dimension distributions from vector data.
///
/// # Overview
///
/// This analyzer processes vector data in a single streaming pass, fitting
/// statistical distributions to each dimension independently. The result is a
/// [VectorSpaceModel] that can generate new vectors with similar characteristics.
///
/// ```text
/// ┌─────────────────────────────────────────────────────────────────────────┐
/// │                    DIMENSION DISTRIBUTION ANALYSIS                      │
/// └─────────────────────────────────────────────────────────────────────────┘
///
///   Source Vectors                                         Generated Vectors
///  ┌─────────────────┐                                   ┌─────────────────┐
///  │ v₀ = [x₀,y₀,z₀] │                                   │ v'₀= [x',y',z'] │
///  │ v₁ = [x₁,y₁,z₁] │   ┌────────────────────────────┐  │ v'₁= [x',y',z'] │
///  │ v₂ = [x₂,y₂,z₂] │──▶│ DimensionDistributionAnalyz│─▶│ v'₂= [x',y',z'] │
///  │       ...       │   │                            │  │       ...       │
///  │ vₙ = [xₙ,yₙ,zₙ] │   │  VectorSpaceModel output   │  │ v'ₘ= [x',y',z'] │
///  └─────────────────┘   └────────────────────────────┘  └─────────────────┘
///         │                         │                            ▲
///         │                         ▼                            │
///         │              ┌──────────────────────┐                │
///         │              │   Per-Dimension      │                │
///         └──────────────│   ComponentModels    │────────────────┘
///                        │                      │
///                        │  dim[0]: Gaussian    │
///                        │  dim[1]: Uniform     │
///                        │  dim[2]: Gaussian    │
///                        └──────────────────────┘
/// ```
///
/// # Key Insight
///
/// Unlike whole-vector analysis, this analyzer treats each dimension as an
/// independent random variable. This approach:
///
/// - Captures per-dimension statistical properties (mean, variance, skew)
/// - Fits the best distribution model per dimension
/// - Enables generation of statistically similar vectors
///
/// # Processing Pipeline
///
/// ## Phase 1: Initialization
///
/// When [#initialize] is called, the analyzer creates one [OnlineAccumulator]
/// per dimension:
///
/// ```text
/// DataspaceShape(cardinality=1M, dimensions=128)
///                       │
///                       ▼
///              ┌────────────────────┐
///              │ OnlineAccumulator  │ × 128
///              │  per dimension     │
///              └────────────────────┘
/// ```
///
/// ## Phase 2: Chunk Processing
///
/// Each chunk of vectors is distributed to dimension accumulators:
///
/// ```text
/// Chunk: float[1000][128]
///
///   vector[0] ──┬── dim[0] ──▶ accumulator[0].accept(value)
///               ├── dim[1] ──▶ accumulator[1].accept(value)
///               ├── dim[2] ──▶ accumulator[2].accept(value)
///               │      ...
///               └── dim[127]─▶ accumulator[127].accept(value)
///
///   vector[1] ──┬── dim[0] ──▶ accumulator[0].accept(value)
///               │     ...
/// ```
///
/// ## Phase 3: Distribution Fitting
///
/// When [#complete] is called, each dimension's statistics are fitted
/// to the best distribution model:
///
/// ```text
/// ┌───────────────┐     ┌─────────────────┐     ┌───────────────┐
/// │ Accumulator   │────▶│ DimensionStats  │────▶│ BestFitSelect │
/// │  (per-dim)    │     │ mean,var,skew.. │     │  Gaussian?    │
/// └───────────────┘     └─────────────────┘     │  Uniform?     │
///                                               │  Empirical?   │
///                                               └───────┬───────┘
///                                                       │
///                                                       ▼
///                                               ┌───────────────┐
///                                               │ ComponentModel│
///                                               │  (best fit)   │
///                                               └───────────────┘
/// ```
///
/// # Algorithms
///
/// The analyzer uses several online algorithms:
///
/// | Algorithm | Purpose |
/// |-----------|---------||
/// | Welford's | Mean, variance (numerically stable) |
/// | Extended Welford's | Skewness, kurtosis |
/// | Reservoir Sampling | Representative samples for empirical fitting |
///
/// # Thread Safety
///
/// This analyzer is **thread-safe**. The `AnalyzerHarness` may deliver chunks
/// from multiple threads concurrently. Each `OnlineAccumulator` uses internal
/// locking to serialize updates.
///
/// ```text
/// ┌────────────────────────────────────────────────────────────────┐
/// │                    CONCURRENT CHUNK DELIVERY                   │
/// └────────────────────────────────────────────────────────────────┘
///
///   Thread 1                Thread 2                Thread 3
///   chunk[0..999]          chunk[1000..1999]       chunk[2000..2999]
///        │                      │                      │
///        ▼                      ▼                      ▼
///   ┌─────────────────────────────────────────────────────────────┐
///   │                 DimensionDistributionAnalyzer               │
///   │  ┌──────────┐  ┌──────────┐  ┌──────────┐       ┌────────┐ │
///   │  │ accum[0] │  │ accum[1] │  │ accum[2] │  ...  │acc[127]│ │
///   │  │  (lock)  │  │  (lock)  │  │  (lock)  │       │ (lock) │ │
///   │  └──────────┘  └──────────┘  └──────────┘       └────────┘ │
///   └─────────────────────────────────────────────────────────────┘
/// ```
///
/// # Configuration
///
/// | Parameter | Default | Description |
/// |-----------|---------|-------------|
/// | `selector` | Default fitters | [BestFitSelector] for model selection |
/// | `uniqueVectors` | 1,000,000 | Target unique vectors in output model |
/// | `reservoirSize` | 10,000 | Samples per dimension for empirical fitting |
///
/// # Memory Usage
///
/// Memory is bounded by `dimensions × reservoirSize × 4 bytes`:
///
/// | Dimensions | Reservoir | Memory |
/// |------------|-----------|--------|
/// | 128 | 10,000 | ~5 MB |
/// | 768 | 10,000 | ~30 MB |
/// | 1536 | 10,000 | ~60 MB |
///
/// # Usage Examples
///
/// ## Basic Usage with SPI
///
/// ```java
/// AnalyzerHarness harness = new AnalyzerHarness();
/// harness.register("dimension-distribution");  // SPI lookup by name
///
/// AnalysisResults results = harness.run(source, 1000);
/// VectorSpaceModel model = results.getResult("dimension-distribution", VectorSpaceModel.class);
/// ```
///
/// ## Direct Instantiation
///
/// ```java
/// DimensionDistributionAnalyzer analyzer = new DimensionDistributionAnalyzer();
///
/// AnalyzerHarness harness = new AnalyzerHarness();
/// harness.register(analyzer);
///
/// AnalysisResults results = harness.run(source, 1000);
/// VectorSpaceModel model = results.getResult("dimension-distribution", VectorSpaceModel.class);
/// ```
///
/// ## Custom Configuration
///
/// ```java
/// BestFitSelector selector = BestFitSelector.builder()
///     .addFitter(new GaussianModelFitter())
///     .addFitter(new UniformModelFitter())
///     .build();
///
/// DimensionDistributionAnalyzer analyzer = new DimensionDistributionAnalyzer(
///     selector,
///     5_000_000,   // target unique vectors
///     50_000       // reservoir size for better empirical fitting
/// );
/// ```
///
/// @see VectorSpaceModel
/// @see OnlineAccumulator
/// @see BestFitSelector
/// @see StreamingAnalyzer
@AnalyzerName(DimensionDistributionAnalyzer.ANALYZER_TYPE)
public final class DimensionDistributionAnalyzer implements StreamingAnalyzer<VectorSpaceModel> {

    /// The analyzer type identifier used for SPI lookup.
    public static final String ANALYZER_TYPE = "dimension-distribution";

    private static final int DEFAULT_RESERVOIR_SIZE = 10_000;

    private final BestFitSelector selector;
    private final long uniqueVectors;
    private final int reservoirSize;

    private DataspaceShape shape;
    private OnlineAccumulator[] accumulators;

    /// Creates a dimension distribution analyzer with default settings.
    ///
    /// Uses:
    /// - Default [BestFitSelector] with Gaussian, Uniform, and Empirical fitters
    /// - 1,000,000 target unique vectors
    /// - 10,000 reservoir samples per dimension
    public DimensionDistributionAnalyzer() {
        this(BestFitSelector.defaultSelector(), 1_000_000, DEFAULT_RESERVOIR_SIZE);
    }

    /// Creates a dimension distribution analyzer with custom settings.
    ///
    /// @param selector the best-fit selector for choosing distribution models
    /// @param uniqueVectors target number of unique vectors for the output model
    /// @param reservoirSize number of samples to retain per dimension for empirical fitting
    public DimensionDistributionAnalyzer(BestFitSelector selector, long uniqueVectors, int reservoirSize) {
        this.selector = selector;
        this.uniqueVectors = uniqueVectors;
        this.reservoirSize = reservoirSize;
    }

    @Override
    public String getAnalyzerType() {
        return ANALYZER_TYPE;
    }

    @Override
    public String getDescription() {
        return "Extracts per-dimension statistical distributions from vector data";
    }

    /// Initializes the analyzer with the dataspace shape.
    ///
    /// Creates one [OnlineAccumulator] per dimension to collect statistics.
    ///
    /// @param shape the shape of the vector space (cardinality, dimensionality)
    @Override
    public void initialize(DataspaceShape shape) {
        this.shape = shape;
        int dims = shape.dimensionality();
        this.accumulators = new OnlineAccumulator[dims];
        for (int d = 0; d < dims; d++) {
            accumulators[d] = new OnlineAccumulator(reservoirSize);
        }
    }

    /// Processes a chunk of vectors.
    ///
    /// Each vector's components are distributed to the corresponding
    /// dimension accumulators.
    ///
    /// @param chunk the vectors to process
    /// @param startIndex the global index of the first vector in the chunk
    @Override
    public void accept(float[][] chunk, long startIndex) {
        for (float[] vector : chunk) {
            for (int d = 0; d < vector.length; d++) {
                accumulators[d].accept(vector[d]);
            }
        }
    }

    /// Completes the analysis and returns the extracted model.
    ///
    /// For each dimension:
    /// 1. Converts accumulated statistics to [DimensionStatistics]
    /// 2. Retrieves reservoir samples
    /// 3. Uses [BestFitSelector] to choose the best [ComponentModel]
    ///
    /// @return the extracted [VectorSpaceModel]
    @Override
    public VectorSpaceModel complete() {
        int dims = shape.dimensionality();
        ComponentModel[] components = new ComponentModel[dims];

        for (int d = 0; d < dims; d++) {
            OnlineAccumulator acc = accumulators[d];
            DimensionStatistics stats = acc.toStatistics(d);
            float[] samples = acc.getSamples();

            ComponentModelFitter.FitResult result = selector.selectBestResult(stats, samples);
            components[d] = result.model();
        }

        return new VectorSpaceModel(uniqueVectors, components);
    }

    /// Estimates memory usage for this analyzer.
    ///
    /// @return estimated bytes: `dimensions × (64 + reservoirSize × 4)`
    @Override
    public long estimatedMemoryBytes() {
        if (shape == null) return 0;
        return (long) shape.dimensionality() * (64 + reservoirSize * 4L);
    }
}
