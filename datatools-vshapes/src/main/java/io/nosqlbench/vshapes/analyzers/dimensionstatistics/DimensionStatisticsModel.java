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

import java.util.Arrays;
import java.util.Objects;

/// Model containing per-dimension statistical moments.
///
/// # Overview
///
/// This model captures the statistical properties of each dimension in a vector
/// dataset without fitting any distribution models. It provides raw statistical
/// insight into the data's characteristics.
///
/// ```text
/// ┌─────────────────────────────────────────────────────────────────────────┐
/// │                    DIMENSION STATISTICS MODEL                           │
/// └─────────────────────────────────────────────────────────────────────────┘
///
///   dim[0]: count=1M, mean=0.12, stdDev=0.95, skew=-0.02, kurt=3.01
///   dim[1]: count=1M, mean=5.43, stdDev=2.10, skew=0.15, kurt=2.89
///   dim[2]: count=1M, mean=-1.2, stdDev=0.48, skew=0.01, kurt=3.12
///     ...
///   dim[N]: count=1M, mean=2.78, stdDev=1.33, skew=-0.08, kurt=2.95
/// ```
///
/// # Statistics Per Dimension
///
/// | Statistic | Description |
/// |-----------|-------------|
/// | count | Number of observations |
/// | min/max | Observed range |
/// | mean | Arithmetic mean |
/// | variance | Population variance |
/// | stdDev | Population standard deviation |
/// | skewness | Asymmetry measure (0 = symmetric) |
/// | kurtosis | Tail heaviness (3 = normal-like) |
///
/// # Use Cases
///
/// - **Data exploration**: Understand the statistical properties of each dimension
/// - **Anomaly detection**: Identify dimensions with unusual distributions
/// - **Feature engineering**: Select dimensions based on their characteristics
/// - **Quality assurance**: Verify data meets expected statistical properties
///
/// # Usage
///
/// ```java
/// // Analyze dataset
/// AnalyzerHarness harness = new AnalyzerHarness();
/// harness.register("dimension-statistics");
/// AnalysisResults results = harness.run(source, 1000);
///
/// // Get the model
/// DimensionStatisticsModel model = results.getResult("dimension-statistics",
///     DimensionStatisticsModel.class);
///
/// // Explore statistics
/// for (int d = 0; d < model.dimensions(); d++) {
///     DimensionStatistics stats = model.getStatistics(d);
///     System.out.printf("Dim %d: mean=%.2f, stdDev=%.2f, skew=%.2f%n",
///         d, stats.mean(), stats.stdDev(), stats.skewness());
/// }
///
/// // Find dimensions with high skewness
/// for (int d = 0; d < model.dimensions(); d++) {
///     if (Math.abs(model.getStatistics(d).skewness()) > 1.0) {
///         System.out.println("High skewness in dimension " + d);
///     }
/// }
/// ```
///
/// @see DimensionStatistics
/// @see DimensionStatisticsAnalyzer
public final class DimensionStatisticsModel {

    private final long vectorCount;
    private final DimensionStatistics[] statistics;

    /// Creates a dimension statistics model.
    ///
    /// @param vectorCount the total number of vectors analyzed
    /// @param statistics the per-dimension statistics array
    public DimensionStatisticsModel(long vectorCount, DimensionStatistics[] statistics) {
        Objects.requireNonNull(statistics, "statistics cannot be null");
        this.vectorCount = vectorCount;
        this.statistics = statistics.clone();
    }

    /// Returns the total number of vectors analyzed.
    public long vectorCount() {
        return vectorCount;
    }

    /// Returns the number of dimensions.
    public int dimensions() {
        return statistics.length;
    }

    /// Returns the statistics for a specific dimension.
    ///
    /// @param dimension the dimension index (0-based)
    /// @return the statistics for that dimension
    /// @throws IndexOutOfBoundsException if dimension is out of range
    public DimensionStatistics getStatistics(int dimension) {
        return statistics[dimension];
    }

    /// Returns a copy of all dimension statistics.
    public DimensionStatistics[] getAllStatistics() {
        return statistics.clone();
    }

    /// Returns the mean values across all dimensions.
    public double[] getMeans() {
        double[] means = new double[statistics.length];
        for (int d = 0; d < statistics.length; d++) {
            means[d] = statistics[d].mean();
        }
        return means;
    }

    /// Returns the standard deviations across all dimensions.
    public double[] getStdDevs() {
        double[] stdDevs = new double[statistics.length];
        for (int d = 0; d < statistics.length; d++) {
            stdDevs[d] = statistics[d].stdDev();
        }
        return stdDevs;
    }

    /// Returns the skewness values across all dimensions.
    public double[] getSkewnesses() {
        double[] skewnesses = new double[statistics.length];
        for (int d = 0; d < statistics.length; d++) {
            skewnesses[d] = statistics[d].skewness();
        }
        return skewnesses;
    }

    /// Returns the kurtosis values across all dimensions.
    public double[] getKurtoses() {
        double[] kurtoses = new double[statistics.length];
        for (int d = 0; d < statistics.length; d++) {
            kurtoses[d] = statistics[d].kurtosis();
        }
        return kurtoses;
    }

    /// Returns a summary of the model.
    public String getSummary() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("DimensionStatisticsModel: %d dimensions, %d vectors%n",
            dimensions(), vectorCount));

        // Compute summary stats across dimensions
        double minMean = Arrays.stream(statistics).mapToDouble(DimensionStatistics::mean).min().orElse(0);
        double maxMean = Arrays.stream(statistics).mapToDouble(DimensionStatistics::mean).max().orElse(0);
        double avgStdDev = Arrays.stream(statistics).mapToDouble(DimensionStatistics::stdDev).average().orElse(0);
        double maxSkew = Arrays.stream(statistics).mapToDouble(s -> Math.abs(s.skewness())).max().orElse(0);
        double avgKurt = Arrays.stream(statistics).mapToDouble(DimensionStatistics::kurtosis).average().orElse(3);

        sb.append(String.format("  Mean range: [%.4f, %.4f]%n", minMean, maxMean));
        sb.append(String.format("  Avg stdDev: %.4f%n", avgStdDev));
        sb.append(String.format("  Max |skewness|: %.4f%n", maxSkew));
        sb.append(String.format("  Avg kurtosis: %.4f%n", avgKurt));

        return sb.toString();
    }

    @Override
    public String toString() {
        return String.format("DimensionStatisticsModel[dims=%d, vectors=%d]",
            dimensions(), vectorCount);
    }
}
