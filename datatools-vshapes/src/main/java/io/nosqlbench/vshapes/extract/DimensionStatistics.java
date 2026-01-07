package io.nosqlbench.vshapes.extract;

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

import java.util.Objects;

/**
 * Statistics computed for a single dimension of vector data.
 *
 * <h2>Purpose</h2>
 *
 * <p>This class holds descriptive statistics for one dimension, computed from
 * observed vector data. These statistics are used by model fitters to determine
 * appropriate distribution parameters and by the best-fit selector to choose
 * the optimal distribution type.
 *
 * <h2>Statistics Included</h2>
 *
 * <ul>
 *   <li><b>count</b> - number of observations</li>
 *   <li><b>min/max</b> - observed range</li>
 *   <li><b>mean</b> - arithmetic mean</li>
 *   <li><b>variance/stdDev</b> - spread measures</li>
 *   <li><b>skewness</b> - asymmetry measure (0 = symmetric)</li>
 *   <li><b>kurtosis</b> - tail heaviness (3 = normal)</li>
 * </ul>
 *
 * <h2>Usage</h2>
 *
 * <pre>{@code
 * // Compute statistics from data
 * float[] dimensionData = extractDimension(vectors, dimIndex);
 * DimensionStatistics stats = DimensionStatistics.compute(dimensionData);
 *
 * // Use for model fitting
 * NormalScalarModel model = new NormalScalarModel(stats.mean(), stats.stdDev());
 * }</pre>
 *
 * @see ComponentModelFitter
 * @see BestFitSelector
 */
public final class DimensionStatistics {

    private final int dimension;
    private final long count;
    private final double min;
    private final double max;
    private final double mean;
    private final double variance;
    /// Computed from variance - transient to avoid redundant JSON serialization.
    private transient Double stdDev;
    private final double skewness;
    private final double kurtosis;

    /**
     * Constructs dimension statistics with all values.
     */
    public DimensionStatistics(int dimension, long count, double min, double max,
                                double mean, double variance, double skewness, double kurtosis) {
        this.dimension = dimension;
        this.count = count;
        this.min = min;
        this.max = max;
        this.mean = mean;
        this.variance = variance;
        this.stdDev = Math.sqrt(variance);
        this.skewness = skewness;
        this.kurtosis = kurtosis;
    }

    /**
     * Computes statistics from an array of float values.
     *
     * @param dimension the dimension index
     * @param values the observed values for this dimension
     * @return computed statistics
     */
    public static DimensionStatistics compute(int dimension, float[] values) {
        Objects.requireNonNull(values, "values cannot be null");
        if (values.length == 0) {
            throw new IllegalArgumentException("values cannot be empty");
        }

        long count = values.length;

        // First pass: min, max, mean
        double min = values[0];
        double max = values[0];
        double sum = 0;

        for (float v : values) {
            if (v < min) min = v;
            if (v > max) max = v;
            sum += v;
        }

        double mean = sum / count;

        // Second pass: variance, skewness, kurtosis
        double m2 = 0;  // sum of squared deviations
        double m3 = 0;  // sum of cubed deviations
        double m4 = 0;  // sum of fourth power deviations

        for (float v : values) {
            double diff = v - mean;
            double diff2 = diff * diff;
            m2 += diff2;
            m3 += diff2 * diff;
            m4 += diff2 * diff2;
        }

        double variance = m2 / count;
        double stdDev = Math.sqrt(variance);

        // Skewness and kurtosis (using population formulas)
        double skewness = 0;
        double kurtosis = 3;  // Excess kurtosis baseline is 0, raw kurtosis baseline is 3

        if (stdDev > 0) {
            skewness = (m3 / count) / (stdDev * stdDev * stdDev);
            kurtosis = (m4 / count) / (variance * variance);  // Raw kurtosis
        }

        return new DimensionStatistics(dimension, count, min, max, mean, variance, skewness, kurtosis);
    }

    /**
     * Computes statistics from an array of double values.
     *
     * @param dimension the dimension index
     * @param values the observed values for this dimension
     * @return computed statistics
     */
    public static DimensionStatistics compute(int dimension, double[] values) {
        Objects.requireNonNull(values, "values cannot be null");
        if (values.length == 0) {
            throw new IllegalArgumentException("values cannot be empty");
        }

        long count = values.length;

        // First pass: min, max, mean
        double min = values[0];
        double max = values[0];
        double sum = 0;

        for (double v : values) {
            if (v < min) min = v;
            if (v > max) max = v;
            sum += v;
        }

        double mean = sum / count;

        // Second pass: variance, skewness, kurtosis
        double m2 = 0;
        double m3 = 0;
        double m4 = 0;

        for (double v : values) {
            double diff = v - mean;
            double diff2 = diff * diff;
            m2 += diff2;
            m3 += diff2 * diff;
            m4 += diff2 * diff2;
        }

        double variance = m2 / count;
        double stdDev = Math.sqrt(variance);

        double skewness = 0;
        double kurtosis = 3;

        if (stdDev > 0) {
            skewness = (m3 / count) / (stdDev * stdDev * stdDev);
            kurtosis = (m4 / count) / (variance * variance);
        }

        return new DimensionStatistics(dimension, count, min, max, mean, variance, skewness, kurtosis);
    }

    /**
     * Returns the dimension index.
     */
    public int dimension() {
        return dimension;
    }

    /**
     * Returns the number of observations.
     */
    public long count() {
        return count;
    }

    /**
     * Returns the minimum observed value.
     */
    public double min() {
        return min;
    }

    /**
     * Returns the maximum observed value.
     */
    public double max() {
        return max;
    }

    /**
     * Returns the range (max - min).
     */
    public double range() {
        return max - min;
    }

    /**
     * Returns the arithmetic mean.
     */
    public double mean() {
        return mean;
    }

    /**
     * Returns the population variance.
     */
    public double variance() {
        return variance;
    }

    /**
     * Returns the population standard deviation.
     * Computed lazily from variance if needed (e.g., after deserialization).
     */
    public double stdDev() {
        if (stdDev == null) {
            stdDev = Math.sqrt(variance);
        }
        return stdDev;
    }

    /**
     * Returns the skewness (asymmetry measure).
     * <ul>
     *   <li>skewness = 0: symmetric distribution</li>
     *   <li>skewness &gt; 0: right-skewed (tail extends right)</li>
     *   <li>skewness &lt; 0: left-skewed (tail extends left)</li>
     * </ul>
     */
    public double skewness() {
        return skewness;
    }

    /**
     * Returns the raw kurtosis (tail heaviness).
     * <ul>
     *   <li>kurtosis = 3: mesokurtic (normal-like tails)</li>
     *   <li>kurtosis &gt; 3: leptokurtic (heavier tails than normal)</li>
     *   <li>kurtosis &lt; 3: platykurtic (lighter tails than normal)</li>
     * </ul>
     */
    public double kurtosis() {
        return kurtosis;
    }

    /**
     * Returns the excess kurtosis (kurtosis - 3).
     * For a normal distribution, excess kurtosis is 0.
     */
    public double excessKurtosis() {
        return kurtosis - 3;
    }

    /**
     * Checks if the data appears to be bounded (all values within a finite range).
     * This is a heuristic based on comparing the range to the standard deviation.
     */
    public boolean appearsBounded() {
        // If range is less than 6 standard deviations, data is likely bounded
        // (normal would have 99.7% within 3 sigma on each side)
        return range() < 6 * stdDev;
    }

    /**
     * Checks if the data appears approximately normally distributed.
     * Uses skewness and kurtosis as indicators.
     */
    public boolean appearsNormal() {
        // Rough heuristic: |skewness| < 0.5 and |excess kurtosis| < 1
        return Math.abs(skewness) < 0.5 && Math.abs(excessKurtosis()) < 1.0;
    }

    /**
     * Checks if the data appears approximately uniformly distributed.
     * Uses kurtosis as an indicator (uniform has kurtosis = 1.8).
     */
    public boolean appearsUniform() {
        // Uniform distribution has kurtosis = 1.8 (excess kurtosis = -1.2)
        // and skewness = 0
        return Math.abs(skewness) < 0.3 && kurtosis < 2.5 && kurtosis > 1.2;
    }

    @Override
    public String toString() {
        return String.format(
            "DimensionStatistics[dim=%d, n=%d, range=[%.4f, %.4f], mean=%.4f, stdDev=%.4f, skew=%.4f, kurt=%.4f]",
            dimension, count, min, max, mean, stdDev, skewness, kurtosis);
    }

    /**
     * Combines two DimensionStatistics using parallel Welford's algorithm.
     *
     * <p>This operation is algebraically sound: combining statistics produces
     * numerically equivalent results to computing statistics over all values at once.
     *
     * <h2>Algorithm</h2>
     *
     * <p>For statistics A and B with counts nA and nB:
     * <pre>{@code
     * nAB = nA + nB
     * δ = meanB - meanA
     * meanAB = meanA + δ * nB / nAB
     *
     * varianceAB = (nA * varA + nB * varB + δ² * nA * nB / nAB) / nAB
     * }</pre>
     *
     * <h2>Algebraic Properties</h2>
     *
     * <ul>
     *   <li><b>Associativity:</b> combine(A, combine(B, C)) == combine(combine(A, B), C)</li>
     *   <li><b>Commutativity:</b> combine(A, B) ≈ combine(B, A) (up to floating-point precision)</li>
     * </ul>
     *
     * @param other the statistics to combine with
     * @return a new DimensionStatistics with combined values
     * @throws IllegalArgumentException if dimensions don't match
     */
    public DimensionStatistics combine(DimensionStatistics other) {
        if (this.dimension != other.dimension) {
            throw new IllegalArgumentException(
                "Cannot combine statistics for different dimensions: " +
                this.dimension + " vs " + other.dimension);
        }

        if (this.count == 0) {
            return other;
        }
        if (other.count == 0) {
            return this;
        }

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

        // Combined mean
        double combinedMean = this.mean + delta * nB_d / nAB_d;

        // Combined variance (using M2 = variance * n)
        double m2A = this.variance * nA_d;
        double m2B = other.variance * nB_d;
        double m2AB = m2A + m2B + delta2 * nA_d * nB_d / nAB_d;
        double combinedVariance = m2AB / nAB_d;

        // Combined skewness (using M3 = skewness * stdDev^3 * n)
        double stdDevA = this.stdDev;
        double stdDevB = other.stdDev;
        double m3A = this.skewness * stdDevA * stdDevA * stdDevA * nA_d;
        double m3B = other.skewness * stdDevB * stdDevB * stdDevB * nB_d;
        double m3AB = m3A + m3B
            + delta3 * nA_d * nB_d * (nA_d - nB_d) / (nAB_d * nAB_d)
            + 3.0 * delta * (nA_d * m2B - nB_d * m2A) / nAB_d;

        double combinedStdDev = Math.sqrt(combinedVariance);
        double combinedSkewness = combinedStdDev > 0
            ? (m3AB / nAB_d) / (combinedStdDev * combinedStdDev * combinedStdDev)
            : 0;

        // Combined kurtosis (using M4 = kurtosis * variance^2 * n)
        double m4A = this.kurtosis * this.variance * this.variance * nA_d;
        double m4B = other.kurtosis * other.variance * other.variance * nB_d;
        double m4AB = m4A + m4B
            + delta4 * nA_d * nB_d * (nA_d * nA_d - nA_d * nB_d + nB_d * nB_d) / (nAB_d * nAB_d * nAB_d)
            + 6.0 * delta2 * (nA_d * nA_d * m2B + nB_d * nB_d * m2A) / (nAB_d * nAB_d)
            + 4.0 * delta * (nA_d * m3B - nB_d * m3A) / nAB_d;

        double combinedKurtosis = combinedVariance > 0
            ? (m4AB / nAB_d) / (combinedVariance * combinedVariance)
            : 3;

        return new DimensionStatistics(
            this.dimension,
            nAB,
            Math.min(this.min, other.min),
            Math.max(this.max, other.max),
            combinedMean,
            combinedVariance,
            combinedSkewness,
            combinedKurtosis
        );
    }
}
