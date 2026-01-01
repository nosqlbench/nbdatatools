package io.nosqlbench.vshapes.analyzers.dimensionstatistics;

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

import io.nosqlbench.vshapes.extract.DimensionStatistics;
import io.nosqlbench.vshapes.stream.AnalyzerName;
import io.nosqlbench.vshapes.stream.DataspaceShape;
import io.nosqlbench.vshapes.stream.StreamingAnalyzer;

/// Streaming analyzer that computes per-dimension statistical moments.
///
/// # Overview
///
/// This analyzer processes vector data in a single streaming pass, computing
/// mean, variance, skewness, and kurtosis for each dimension independently.
/// Unlike the `DimensionDistributionAnalyzer`, this analyzer captures raw
/// statistics without fitting distribution models.
///
/// ```text
/// ┌─────────────────────────────────────────────────────────────────────────┐
/// │                    DIMENSION STATISTICS ANALYSIS                        │
/// └─────────────────────────────────────────────────────────────────────────┘
///
///   Source Vectors                                   Statistics Output
///  ┌─────────────────┐                              ┌──────────────────────┐
///  │ v₀ = [x₀,y₀,z₀] │                              │ dim[0]: μ, σ, γ, κ   │
///  │ v₁ = [x₁,y₁,z₁] │  ─────► Analyze ─────►       │ dim[1]: μ, σ, γ, κ   │
///  │ v₂ = [x₂,y₂,z₂] │                              │ dim[2]: μ, σ, γ, κ   │
///  │       ...       │                              │   ...                │
///  │ vₙ = [xₙ,yₙ,zₙ] │                              │ dim[M]: μ, σ, γ, κ   │
///  └─────────────────┘                              └──────────────────────┘
///
///   where: μ = mean, σ = stdDev, γ = skewness, κ = kurtosis
/// ```
///
/// # Statistics Computed
///
/// | Statistic | Symbol | Description |
/// |-----------|--------|-------------|
/// | Count | n | Number of observations |
/// | Min/Max | - | Observed range bounds |
/// | Mean | μ | Arithmetic mean |
/// | Variance | σ² | Population variance |
/// | Std Dev | σ | Population standard deviation |
/// | Skewness | γ | Asymmetry measure (0 = symmetric) |
/// | Kurtosis | κ | Tail heaviness (3 = normal-like) |
///
/// # Use Cases
///
/// - **Data exploration**: Understand statistical properties before modeling
/// - **Anomaly detection**: Identify dimensions with unusual distributions
/// - **Quality assurance**: Verify data meets expected characteristics
/// - **Feature selection**: Choose dimensions based on their properties
///
/// # Algorithm
///
/// Uses Welford's online algorithm for numerically stable computation:
///
/// ```text
///   For each value x:
///     n++
///     delta = x - mean
///     mean += delta / n
///     M2 += delta * (x - mean)    // For variance
///     M3, M4 updated similarly    // For skewness, kurtosis
/// ```
///
/// This approach:
/// - Requires only O(1) memory per dimension
/// - Is numerically stable for large datasets
/// - Processes data in a single pass
///
/// # Thread Safety
///
/// This analyzer is **thread-safe**. Each [OnlineMomentsAccumulator] uses
/// internal locking to serialize updates from concurrent chunk delivery.
///
/// # Memory Usage
///
/// Memory is minimal: `dimensions × ~80 bytes` (no sampling required)
///
/// | Dimensions | Memory |
/// |------------|--------|
/// | 128 | ~10 KB |
/// | 768 | ~60 KB |
/// | 1536 | ~120 KB |
///
/// # Usage Examples
///
/// ## Basic Usage
///
/// ```java
/// AnalyzerHarness harness = new AnalyzerHarness();
/// harness.register("dimension-statistics");
///
/// AnalysisResults results = harness.run(source, 1000);
/// DimensionStatisticsModel model = results.getResult("dimension-statistics",
///     DimensionStatisticsModel.class);
///
/// // Get statistics for dimension 0
/// DimensionStatistics stats = model.getStatistics(0);
/// System.out.printf("Dim 0: mean=%.2f, stdDev=%.2f%n", stats.mean(), stats.stdDev());
/// ```
///
/// ## Combined Analysis
///
/// ```java
/// // Run multiple analyzers together
/// AnalyzerHarness harness = new AnalyzerHarness();
/// harness.register("dimension-statistics");
/// harness.register("dimension-distribution");
///
/// AnalysisResults results = harness.run(source, 1000);
///
/// // Statistics for exploration
/// DimensionStatisticsModel stats = results.getResult("dimension-statistics",
///     DimensionStatisticsModel.class);
///
/// // Model for generation
/// VectorSpaceModel model = results.getResult("dimension-distribution",
///     VectorSpaceModel.class);
/// ```
///
/// @see DimensionStatisticsModel
/// @see DimensionStatistics
/// @see OnlineMomentsAccumulator
@AnalyzerName(DimensionStatisticsAnalyzer.ANALYZER_TYPE)
public final class DimensionStatisticsAnalyzer implements StreamingAnalyzer<DimensionStatisticsModel> {

    /// The analyzer type identifier used for SPI lookup.
    public static final String ANALYZER_TYPE = "dimension-statistics";

    private DataspaceShape shape;
    private OnlineMomentsAccumulator[] accumulators;

    /// Creates a dimension statistics analyzer with default settings.
    public DimensionStatisticsAnalyzer() {
    }

    @Override
    public String getAnalyzerType() {
        return ANALYZER_TYPE;
    }

    @Override
    public String getDescription() {
        return "Computes per-dimension statistical moments (mean, stdDev, skewness, kurtosis)";
    }

    /// Initializes the analyzer with the dataspace shape.
    ///
    /// Creates one [OnlineMomentsAccumulator] per dimension.
    ///
    /// @param shape the shape of the vector space (cardinality, dimensionality)
    @Override
    public void initialize(DataspaceShape shape) {
        this.shape = shape;
        int dims = shape.dimensionality();
        this.accumulators = new OnlineMomentsAccumulator[dims];
        for (int d = 0; d < dims; d++) {
            accumulators[d] = new OnlineMomentsAccumulator();
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

    /// Completes the analysis and returns the statistics model.
    ///
    /// Converts each accumulator's state to a [DimensionStatistics] record.
    ///
    /// @return the [DimensionStatisticsModel] containing per-dimension statistics
    @Override
    public DimensionStatisticsModel complete() {
        int dims = shape.dimensionality();
        DimensionStatistics[] statistics = new DimensionStatistics[dims];

        for (int d = 0; d < dims; d++) {
            statistics[d] = accumulators[d].toStatistics(d);
        }

        return new DimensionStatisticsModel(shape.cardinality(), statistics);
    }

    /// Estimates memory usage for this analyzer.
    ///
    /// @return estimated bytes: `dimensions × 80`
    @Override
    public long estimatedMemoryBytes() {
        if (shape == null) return 0;
        return (long) shape.dimensionality() * 80;
    }
}
