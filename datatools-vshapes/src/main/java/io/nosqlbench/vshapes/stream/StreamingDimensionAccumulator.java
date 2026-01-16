package io.nosqlbench.vshapes.stream;

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

/// Per-dimension accumulator for streaming statistics computation.
///
/// ## Purpose
///
/// This class accumulates statistics for a single dimension across multiple
/// chunks of data. It uses Welford's online algorithm for numerically stable
/// incremental computation of mean, variance, skewness, and kurtosis.
///
/// ## Welford's Algorithm
///
/// The standard two-pass algorithm (compute mean, then variance) requires
/// storing all data. Welford's algorithm computes statistics incrementally:
///
/// ```
/// For each new value x:
///   n++
///   delta = x - mean
///   mean += delta / n
///   delta2 = x - mean
///   M2 += delta * delta2
///   M3 += delta * delta2 * (delta * (n-2)/n)
///   M4 += delta * delta2 * (delta2 * delta - 3 * M2 / n)
/// ```
///
/// This provides:
/// - O(1) memory per dimension
/// - Numerical stability (avoids catastrophic cancellation)
/// - Exact equivalence to batch computation
///
/// ## Thread Safety
///
/// This class is **not thread-safe**. For concurrent accumulation across
/// chunks, use one accumulator per thread and merge with [combine].
///
/// ## Usage
///
/// ```java
/// StreamingDimensionAccumulator acc = new StreamingDimensionAccumulator(0);
///
/// // Process chunks
/// for (float[][] chunk : dataSource.chunks(1000)) {
///     acc.addAll(chunk[0]);  // Add all values for dimension 0
/// }
///
/// // Get final statistics
/// DimensionStatistics stats = acc.toStatistics();
/// ```
///
/// @see DimensionStatistics
/// @see StreamingAnalyzer
public final class StreamingDimensionAccumulator {

    private final int dimension;
    private long count = 0;
    private double mean = 0.0;
    private double m2 = 0.0;   // For variance
    private double m3 = 0.0;   // For skewness
    private double m4 = 0.0;   // For kurtosis
    private double min = Double.POSITIVE_INFINITY;
    private double max = Double.NEGATIVE_INFINITY;

    /// Creates an accumulator for the specified dimension.
    ///
    /// @param dimension the dimension index this accumulator tracks
    public StreamingDimensionAccumulator(int dimension) {
        if (dimension < 0) {
            throw new IllegalArgumentException("dimension must be non-negative: " + dimension);
        }
        this.dimension = dimension;
    }

    /// Adds a batch of values to this accumulator.
    ///
    /// This is the preferred method for processing chunks, as it allows
    /// potential SIMD optimization in the loop.
    ///
    /// @param values contiguous array of values for this dimension
    public void addAll(float[] values) {
        if (values == null || values.length == 0) {
            return;
        }

        for (float v : values) {
            addValue(v);
        }
    }

    /// Adds a batch of double values to this accumulator.
    ///
    /// @param values contiguous array of values for this dimension
    public void addAll(double[] values) {
        if (values == null || values.length == 0) {
            return;
        }

        for (double v : values) {
            addValue(v);
        }
    }

    /// Adds a single value using Welford's online algorithm.
    ///
    /// @param value the value to add
    public void add(float value) {
        addValue(value);
    }

    /// Adds a single value using Welford's online algorithm.
    ///
    /// @param value the value to add
    public void add(double value) {
        addValue(value);
    }

    /// Internal method implementing Welford's algorithm for all four moments.
    private void addValue(double value) {
        // Update min/max
        if (value < min) min = value;
        if (value > max) max = value;

        // Welford's algorithm for mean and higher moments
        count++;
        double n = count;

        double delta = value - mean;
        double deltaN = delta / n;
        double deltaN2 = deltaN * deltaN;
        double term1 = delta * deltaN * (n - 1);

        // Update mean
        mean += deltaN;

        // Update M4 before M3 and M2 (uses old M2 and M3 values)
        m4 += term1 * deltaN2 * (n * n - 3 * n + 3)
            + 6 * deltaN2 * m2
            - 4 * deltaN * m3;

        // Update M3 before M2 (uses old M2 value)
        m3 += term1 * deltaN * (n - 2)
            - 3 * deltaN * m2;

        // Update M2 last
        m2 += term1;
    }

    /// Combines this accumulator with another using parallel Welford's algorithm.
    ///
    /// This is useful for merging results from parallel chunk processing.
    /// The operation is algebraically equivalent to processing all data in one stream.
    ///
    /// @param other the accumulator to merge in
    /// @throws IllegalArgumentException if dimensions don't match
    public void combine(StreamingDimensionAccumulator other) {
        if (this.dimension != other.dimension) {
            throw new IllegalArgumentException(
                "Cannot combine accumulators for different dimensions: " +
                this.dimension + " vs " + other.dimension);
        }

        if (other.count == 0) {
            return;
        }

        if (this.count == 0) {
            this.count = other.count;
            this.mean = other.mean;
            this.m2 = other.m2;
            this.m3 = other.m3;
            this.m4 = other.m4;
            this.min = other.min;
            this.max = other.max;
            return;
        }

        // Parallel combination using Chan's formulas
        long nA = this.count;
        long nB = other.count;
        long nAB = nA + nB;

        double delta = other.mean - this.mean;
        double delta2 = delta * delta;
        double delta3 = delta2 * delta;
        double delta4 = delta2 * delta2;

        double nA_d = (double) nA;
        double nB_d = (double) nB;
        double nAB_d = (double) nAB;

        // Combined M2
        double combinedM2 = this.m2 + other.m2
            + delta2 * nA_d * nB_d / nAB_d;

        // Combined M3
        double combinedM3 = this.m3 + other.m3
            + delta3 * nA_d * nB_d * (nA_d - nB_d) / (nAB_d * nAB_d)
            + 3.0 * delta * (nA_d * other.m2 - nB_d * this.m2) / nAB_d;

        // Combined M4
        double combinedM4 = this.m4 + other.m4
            + delta4 * nA_d * nB_d * (nA_d * nA_d - nA_d * nB_d + nB_d * nB_d) / (nAB_d * nAB_d * nAB_d)
            + 6.0 * delta2 * (nA_d * nA_d * other.m2 + nB_d * nB_d * this.m2) / (nAB_d * nAB_d)
            + 4.0 * delta * (nA_d * other.m3 - nB_d * this.m3) / nAB_d;

        // Combined mean
        double combinedMean = this.mean + delta * nB_d / nAB_d;

        // Update state
        this.count = nAB;
        this.mean = combinedMean;
        this.m2 = combinedM2;
        this.m3 = combinedM3;
        this.m4 = combinedM4;
        this.min = Math.min(this.min, other.min);
        this.max = Math.max(this.max, other.max);
    }

    /// Combines with a pre-computed DimensionStatistics.
    ///
    /// This allows integrating batch-computed statistics with streaming results.
    ///
    /// @param stats the statistics to combine with
    /// @throws IllegalArgumentException if dimensions don't match
    public void combine(DimensionStatistics stats) {
        if (this.dimension != stats.dimension()) {
            throw new IllegalArgumentException(
                "Cannot combine with statistics for different dimension: " +
                this.dimension + " vs " + stats.dimension());
        }

        // Create a temporary accumulator from the stats
        StreamingDimensionAccumulator temp = fromStatistics(stats);
        this.combine(temp);
    }

    /// Creates an accumulator initialized from existing statistics.
    ///
    /// @param stats the statistics to initialize from
    /// @return a new accumulator with the given state
    public static StreamingDimensionAccumulator fromStatistics(DimensionStatistics stats) {
        StreamingDimensionAccumulator acc = new StreamingDimensionAccumulator(stats.dimension());
        acc.count = stats.count();
        acc.mean = stats.mean();
        acc.min = stats.min();
        acc.max = stats.max();

        // Reconstruct M2, M3, M4 from variance, skewness, kurtosis
        double variance = stats.variance();
        double stdDev = stats.stdDev();
        double n = (double) stats.count();

        acc.m2 = variance * n;
        acc.m3 = stats.skewness() * stdDev * stdDev * stdDev * n;
        acc.m4 = stats.kurtosis() * variance * variance * n;

        return acc;
    }

    /// Converts this accumulator's state to a DimensionStatistics.
    ///
    /// @return computed statistics from accumulated data
    /// @throws IllegalStateException if no data has been added
    public DimensionStatistics toStatistics() {
        if (count == 0) {
            throw new IllegalStateException("Cannot compute statistics: no data added");
        }

        double variance = m2 / count;
        double stdDev = Math.sqrt(variance);

        double skewness = 0;
        double kurtosis = 3;  // Default for degenerate case

        if (stdDev > 0) {
            skewness = (m3 / count) / (stdDev * stdDev * stdDev);
            kurtosis = (m4 / count) / (variance * variance);
        }

        return new DimensionStatistics(
            dimension,
            count,
            min,
            max,
            mean,
            variance,
            skewness,
            kurtosis
        );
    }

    /// Returns the dimension index this accumulator tracks.
    ///
    /// @return the dimension index
    public int getDimension() {
        return dimension;
    }

    /// Returns the number of values accumulated so far.
    ///
    /// @return count of accumulated values
    public long getCount() {
        return count;
    }

    /// Returns the current mean estimate.
    ///
    /// @return running mean
    public double getMean() {
        return mean;
    }

    /// Returns the current variance estimate.
    ///
    /// @return running variance (population formula)
    public double getVariance() {
        return count > 0 ? m2 / count : 0;
    }

    /// Returns the current standard deviation estimate.
    ///
    /// @return running standard deviation
    public double getStdDev() {
        return Math.sqrt(getVariance());
    }

    /// Returns the current skewness estimate.
    ///
    /// @return running skewness (0 for symmetric distributions)
    public double getSkewness() {
        if (count < 3) return 0;
        double variance = getVariance();
        if (variance <= 0) return 0;
        double stdDev = Math.sqrt(variance);
        return (m3 / count) / (stdDev * stdDev * stdDev);
    }

    /// Returns the current kurtosis estimate.
    ///
    /// @return running excess kurtosis (0 for normal distribution)
    public double getKurtosis() {
        if (count < 4) return 0;
        double variance = getVariance();
        if (variance <= 0) return 0;
        return (m4 / count) / (variance * variance) - 3.0;
    }

    /// Returns the current minimum value.
    ///
    /// @return minimum value seen, or +Infinity if no data
    public double getMin() {
        return min;
    }

    /// Returns the current maximum value.
    ///
    /// @return maximum value seen, or -Infinity if no data
    public double getMax() {
        return max;
    }

    /// Resets this accumulator to its initial state.
    public void reset() {
        count = 0;
        mean = 0.0;
        m2 = 0.0;
        m3 = 0.0;
        m4 = 0.0;
        min = Double.POSITIVE_INFINITY;
        max = Double.NEGATIVE_INFINITY;
    }

    @Override
    public String toString() {
        return String.format(
            "StreamingDimensionAccumulator[dim=%d, n=%d, mean=%.4f, stdDev=%.4f, range=[%.4f, %.4f]]",
            dimension, count, mean, getStdDev(), min, max);
    }
}
