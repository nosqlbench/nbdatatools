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

import io.nosqlbench.vshapes.ComputeModeSpecies;
import jdk.incubator.vector.*;

import java.util.Objects;

/**
 * Panama Vector API optimized statistics computation for dimension data.
 *
 * <h2>Purpose</h2>
 *
 * <p>This Java 25+ implementation uses SIMD operations for computing descriptive
 * statistics, providing significant speedups on CPUs with AVX2 or AVX-512 support.
 *
 * <h2>SIMD Optimizations</h2>
 *
 * <ul>
 *   <li>Vectorized min/max/sum reductions in first pass</li>
 *   <li>SIMD computation of variance, skewness, kurtosis moments</li>
 *   <li>FMA (fused multiply-add) for precision and performance</li>
 *   <li>8-way loop unrolling for reduced reduceLanes() overhead</li>
 *   <li>Processes 8 doubles (AVX-512) or 4 doubles (AVX2) per cycle</li>
 * </ul>
 *
 * <h2>Performance Characteristics</h2>
 *
 * <p>On AVX-512 hardware, this implementation can achieve:
 * <ul>
 *   <li>~8x throughput improvement for min/max/sum</li>
 *   <li>~4-6x throughput improvement for moment calculations</li>
 *   <li>Better cache utilization through streaming access patterns</li>
 * </ul>
 *
 * <p>SIMD species selection is centralized via {@link ComputeModeSpecies}.
 *
 * @see ComponentModelFitter
 * @see BestFitSelector
 */
public final class DimensionStatistics {

    private static final VectorSpecies<Double> SPECIES = ComputeModeSpecies.doubleSpecies();
    private static final int LANES = SPECIES.length();

    private final int dimension;
    private final long count;
    private final double min;
    private final double max;
    private final double mean;
    private final double variance;
    private final double stdDev;
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
     * Computes statistics from an array of float values using SIMD operations.
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

        // Convert to double for higher precision in moment calculations
        double[] dvalues = new double[values.length];
        for (int i = 0; i < values.length; i++) {
            dvalues[i] = values[i];
        }

        return computeDouble(dimension, dvalues);
    }

    /**
     * Computes statistics from an array of double values using SIMD operations.
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

        return computeDouble(dimension, values);
    }

    /**
     * SIMD-optimized statistics computation for double arrays.
     * Uses 8-way loop unrolling for reduced reduceLanes() overhead.
     */
    private static DimensionStatistics computeDouble(int dimension, double[] values) {
        long count = values.length;
        int vectorizedLength = SPECIES.loopBound(values.length);

        // ═══════════════════════════════════════════════════════════════════
        // FIRST PASS: min, max, sum (for mean) with 8-way unrolling
        // ═══════════════════════════════════════════════════════════════════

        DoubleVector vMin = DoubleVector.broadcast(SPECIES, Double.POSITIVE_INFINITY);
        DoubleVector vMax = DoubleVector.broadcast(SPECIES, Double.NEGATIVE_INFINITY);
        DoubleVector vSum = DoubleVector.broadcast(SPECIES, 0.0);

        // 8-way unrolled vectorized loop
        int i = 0;
        int unrollBound = vectorizedLength - (7 * LANES);
        for (; i <= unrollBound; i += 8 * LANES) {
            DoubleVector v0 = DoubleVector.fromArray(SPECIES, values, i);
            DoubleVector v1 = DoubleVector.fromArray(SPECIES, values, i + LANES);
            DoubleVector v2 = DoubleVector.fromArray(SPECIES, values, i + 2 * LANES);
            DoubleVector v3 = DoubleVector.fromArray(SPECIES, values, i + 3 * LANES);
            DoubleVector v4 = DoubleVector.fromArray(SPECIES, values, i + 4 * LANES);
            DoubleVector v5 = DoubleVector.fromArray(SPECIES, values, i + 5 * LANES);
            DoubleVector v6 = DoubleVector.fromArray(SPECIES, values, i + 6 * LANES);
            DoubleVector v7 = DoubleVector.fromArray(SPECIES, values, i + 7 * LANES);

            vMin = vMin.min(v0).min(v1).min(v2).min(v3).min(v4).min(v5).min(v6).min(v7);
            vMax = vMax.max(v0).max(v1).max(v2).max(v3).max(v4).max(v5).max(v6).max(v7);
            vSum = vSum.add(v0).add(v1).add(v2).add(v3).add(v4).add(v5).add(v6).add(v7);
        }

        // Handle remaining vectorized iterations (0-7)
        for (; i < vectorizedLength; i += LANES) {
            DoubleVector v = DoubleVector.fromArray(SPECIES, values, i);
            vMin = vMin.min(v);
            vMax = vMax.max(v);
            vSum = vSum.add(v);
        }

        // Reduce vector accumulators (single reduce per accumulator)
        double min = vMin.reduceLanes(VectorOperators.MIN);
        double max = vMax.reduceLanes(VectorOperators.MAX);
        double sum = vSum.reduceLanes(VectorOperators.ADD);

        // Process tail elements
        for (; i < values.length; i++) {
            double v = values[i];
            if (v < min) min = v;
            if (v > max) max = v;
            sum += v;
        }

        double mean = sum / count;

        // ═══════════════════════════════════════════════════════════════════
        // SECOND PASS: moments (m2, m3, m4) with 8-way unrolling
        // ═══════════════════════════════════════════════════════════════════

        DoubleVector vMean = DoubleVector.broadcast(SPECIES, mean);
        DoubleVector vM2 = DoubleVector.broadcast(SPECIES, 0.0);  // sum of (x-μ)²
        DoubleVector vM3 = DoubleVector.broadcast(SPECIES, 0.0);  // sum of (x-μ)³
        DoubleVector vM4 = DoubleVector.broadcast(SPECIES, 0.0);  // sum of (x-μ)⁴

        // 8-way unrolled vectorized moment computation
        i = 0;
        for (; i <= unrollBound; i += 8 * LANES) {
            for (int j = 0; j < 8; j++) {
                int offset = i + j * LANES;
                DoubleVector v = DoubleVector.fromArray(SPECIES, values, offset);
                DoubleVector diff = v.sub(vMean);
                DoubleVector diff2 = diff.mul(diff);
                vM2 = vM2.add(diff2);
                vM3 = diff2.fma(diff, vM3);      // m3 += diff² * diff (FMA)
                vM4 = diff2.fma(diff2, vM4);     // m4 += diff² * diff² (FMA)
            }
        }

        // Handle remaining vectorized iterations
        for (; i < vectorizedLength; i += LANES) {
            DoubleVector v = DoubleVector.fromArray(SPECIES, values, i);
            DoubleVector diff = v.sub(vMean);
            DoubleVector diff2 = diff.mul(diff);
            vM2 = vM2.add(diff2);
            vM3 = diff2.fma(diff, vM3);
            vM4 = diff2.fma(diff2, vM4);
        }

        // Reduce vector accumulators to scalars (single reduce per accumulator)
        double m2 = vM2.reduceLanes(VectorOperators.ADD);
        double m3 = vM3.reduceLanes(VectorOperators.ADD);
        double m4 = vM4.reduceLanes(VectorOperators.ADD);

        // Process tail elements (scalar)
        for (; i < values.length; i++) {
            double diff = values[i] - mean;
            double diff2 = diff * diff;
            m2 += diff2;
            m3 += diff2 * diff;
            m4 += diff2 * diff2;
        }

        // ═══════════════════════════════════════════════════════════════════
        // COMPUTE FINAL STATISTICS
        // ═══════════════════════════════════════════════════════════════════

        double variance = m2 / count;
        double stdDev = Math.sqrt(variance);

        double skewness = 0;
        double kurtosis = 3;  // Default for degenerate case

        if (stdDev > 0) {
            double stdDev3 = stdDev * stdDev * stdDev;
            skewness = (m3 / count) / stdDev3;
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
     */
    public double stdDev() {
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
        return range() < 6 * stdDev;
    }

    /**
     * Checks if the data appears approximately normally distributed.
     * Uses skewness and kurtosis as indicators.
     */
    public boolean appearsNormal() {
        return Math.abs(skewness) < 0.5 && Math.abs(excessKurtosis()) < 1.0;
    }

    /**
     * Checks if the data appears approximately uniformly distributed.
     * Uses kurtosis as an indicator (uniform has kurtosis = 1.8).
     */
    public boolean appearsUniform() {
        return Math.abs(skewness) < 0.3 && kurtosis < 2.5 && kurtosis > 1.2;
    }

    @Override
    public String toString() {
        return String.format(
            "DimensionStatistics[dim=%d, n=%d, range=[%.4f, %.4f], mean=%.4f, stdDev=%.4f, skew=%.4f, kurt=%.4f]",
            dimension, count, min, max, mean, stdDev, skewness, kurtosis);
    }
}
