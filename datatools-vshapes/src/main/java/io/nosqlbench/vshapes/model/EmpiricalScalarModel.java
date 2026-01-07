package io.nosqlbench.vshapes.model;

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

import com.google.gson.annotations.SerializedName;

import java.util.Arrays;
import java.util.Objects;

/**
 * Empirical distribution scalar model based on observed data histogram.
 *
 * <h2>Purpose</h2>
 *
 * <p>This scalar model preserves the exact shape of an observed distribution
 * by building a histogram from the data and sampling from it. This is useful
 * when the underlying distribution is unknown or doesn't fit standard parametric
 * models.
 *
 * <h2>Tensor Hierarchy</h2>
 *
 * <p>EmpiricalScalarModel is a first-order tensor model (ScalarModel) that
 * represents a single-dimensional empirical (histogram-based) distribution:
 * <ul>
 *   <li>{@link ScalarModel} - First-order (single dimension) - this class</li>
 *   <li>{@link VectorModel} - Second-order (M dimensions)</li>
 *   <li>{@link MatrixModel} - Third-order (K vector models)</li>
 * </ul>
 *
 * <h2>Algorithm</h2>
 *
 * <p>The model builds a piecewise linear approximation of the CDF:
 * <ol>
 *   <li>Bin the observed data into a histogram</li>
 *   <li>Convert counts to a cumulative distribution</li>
 *   <li>For sampling: binary search to find the bin, then linear interpolation</li>
 * </ol>
 *
 * <h2>Usage</h2>
 *
 * <pre>{@code
 * // Build from observed data
 * float[] observedValues = ...;
 * EmpiricalScalarModel model = EmpiricalScalarModel.fromData(observedValues, 100);
 *
 * // Sample a value
 * double value = model.sample(0.5);  // Returns ~median of observed data
 * }</pre>
 *
 * @see ScalarModel
 * @see VectorModel
 * @see VectorSpaceModel
 */
@ModelType(EmpiricalScalarModel.MODEL_TYPE)
public class EmpiricalScalarModel implements ScalarModel {

    public static final String MODEL_TYPE = "empirical";

    @SerializedName("bins")
    private final double[] binEdges;      // binCount + 1 edges

    @SerializedName("cdf")
    private final double[] cdf;           // binCount + 1 cumulative probabilities

    /// Computed from binEdges.length - transient to avoid redundant JSON serialization.
    private transient Integer binCount;

    @SerializedName("min")
    private final double min;

    @SerializedName("max")
    private final double max;

    @SerializedName("mean")
    private final double mean;

    @SerializedName("std_dev")
    private final double stdDev;

    /**
     * Constructs an empirical scalar model from precomputed histogram data.
     *
     * @param binEdges the bin edges (length = binCount + 1)
     * @param cdf the cumulative distribution values at each edge (length = binCount + 1)
     * @param mean the mean of the distribution
     * @param stdDev the standard deviation of the distribution
     */
    public EmpiricalScalarModel(double[] binEdges, double[] cdf, double mean, double stdDev) {
        Objects.requireNonNull(binEdges, "binEdges cannot be null");
        Objects.requireNonNull(cdf, "cdf cannot be null");
        if (binEdges.length != cdf.length) {
            throw new IllegalArgumentException("binEdges and cdf must have same length");
        }
        if (binEdges.length < 2) {
            throw new IllegalArgumentException("Need at least 2 bin edges (1 bin)");
        }

        this.binEdges = Arrays.copyOf(binEdges, binEdges.length);
        this.cdf = Arrays.copyOf(cdf, cdf.length);
        this.binCount = binEdges.length - 1;
        this.min = binEdges[0];
        this.max = binEdges[binEdges.length - 1];
        this.mean = mean;
        this.stdDev = stdDev;
    }

    /**
     * Builds an empirical scalar model from observed data.
     *
     * @param values the observed values
     * @param binCount the number of histogram bins
     * @return an EmpiricalScalarModel fitted to the data
     */
    public static EmpiricalScalarModel fromData(float[] values, int binCount) {
        Objects.requireNonNull(values, "values cannot be null");
        if (values.length == 0) {
            throw new IllegalArgumentException("values cannot be empty");
        }
        if (binCount < 1) {
            throw new IllegalArgumentException("binCount must be at least 1");
        }

        // Find min/max
        double min = values[0];
        double max = values[0];
        double sum = 0;
        for (float v : values) {
            if (v < min) min = v;
            if (v > max) max = v;
            sum += v;
        }
        double mean = sum / values.length;

        // Compute stdDev
        double sumSq = 0;
        for (float v : values) {
            double diff = v - mean;
            sumSq += diff * diff;
        }
        double stdDev = Math.sqrt(sumSq / values.length);

        // Handle edge case where all values are the same
        if (max == min) {
            max = min + 1.0;
        }

        // Build histogram
        double[] binEdges = new double[binCount + 1];
        int[] counts = new int[binCount];
        double binWidth = (max - min) / binCount;

        for (int i = 0; i <= binCount; i++) {
            binEdges[i] = min + i * binWidth;
        }
        binEdges[binCount] = max; // Ensure exact max

        for (float v : values) {
            int bin = (int) ((v - min) / binWidth);
            if (bin >= binCount) bin = binCount - 1;
            if (bin < 0) bin = 0;
            counts[bin]++;
        }

        // Build CDF
        double[] cdf = new double[binCount + 1];
        cdf[0] = 0.0;
        int cumulative = 0;
        for (int i = 0; i < binCount; i++) {
            cumulative += counts[i];
            cdf[i + 1] = (double) cumulative / values.length;
        }
        cdf[binCount] = 1.0; // Ensure exact 1.0

        return new EmpiricalScalarModel(binEdges, cdf, mean, stdDev);
    }

    /**
     * Builds an empirical scalar model from observed data with default bin count.
     *
     * <p>Uses Sturges' rule for bin count selection, clamped to [10, 1000].
     *
     * @param values the observed values
     * @return an EmpiricalScalarModel fitted to the data
     */
    public static EmpiricalScalarModel fromData(float[] values) {
        // Sturges' rule for bin count
        int binCount = (int) Math.ceil(Math.log(values.length) / Math.log(2)) + 1;
        binCount = Math.max(10, Math.min(binCount, 1000)); // Clamp to [10, 1000]
        return fromData(values, binCount);
    }

    @Override
    public String getModelType() {
        return MODEL_TYPE;
    }

    /**
     * Returns the mean of the empirical distribution.
     * @return the sample mean
     */
    public double getMean() {
        return mean;
    }

    /**
     * Returns the standard deviation of the empirical distribution.
     * @return the sample standard deviation
     */
    public double getStdDev() {
        return stdDev;
    }

    /**
     * Computes the probability density function (PDF) at a given value.
     * @param x the value at which to evaluate the PDF
     * @return the probability density at x
     */
    public double pdf(double x) {
        if (x < min || x > max) {
            return 0.0;
        }

        // Find the bin
        int count = getBinCount();
        double binWidth = (max - min) / count;
        int bin = (int) ((x - min) / binWidth);
        if (bin >= count) bin = count - 1;
        if (bin < 0) bin = 0;

        // PDF is (cdf[bin+1] - cdf[bin]) / binWidth
        return (cdf[bin + 1] - cdf[bin]) / binWidth;
    }

    /**
     * Computes the cumulative distribution function (CDF) at a given value.
     * @param x the value at which to evaluate the CDF
     * @return the cumulative probability P(X ≤ x)
     */
    public double cdf(double x) {
        if (x <= min) return 0.0;
        if (x >= max) return 1.0;

        // Find the bin
        int count = getBinCount();
        double binWidth = (max - min) / count;
        int bin = (int) ((x - min) / binWidth);
        if (bin >= count) bin = count - 1;
        if (bin < 0) bin = 0;

        // Linear interpolation within the bin
        double edgeLo = binEdges[bin];
        double edgeHi = binEdges[bin + 1];
        double t = (x - edgeLo) / (edgeHi - edgeLo);

        return cdf[bin] + t * (cdf[bin + 1] - cdf[bin]);
    }

    /**
     * Returns the number of bins in the histogram.
     * Computed lazily from binEdges if needed (e.g., after deserialization).
     * @return the bin count
     */
    public int getBinCount() {
        if (binCount == null) {
            binCount = binEdges.length - 1;
        }
        return binCount;
    }

    /**
     * Returns the bin edges.
     * @return a copy of the bin edges array
     */
    public double[] getBinEdges() {
        return Arrays.copyOf(binEdges, binEdges.length);
    }

    /**
     * Returns the cumulative distribution values.
     * @return a copy of the CDF array
     */
    public double[] getCdf() {
        return Arrays.copyOf(cdf, cdf.length);
    }

    /**
     * Returns the minimum value (first bin edge).
     * @return the minimum value
     */
    public double getMin() {
        return min;
    }

    /**
     * Returns the maximum value (last bin edge).
     * @return the maximum value
     */
    public double getMax() {
        return max;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EmpiricalScalarModel)) return false;
        EmpiricalScalarModel that = (EmpiricalScalarModel) o;
        return Arrays.equals(binEdges, that.binEdges) &&
               Arrays.equals(cdf, that.cdf);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(binEdges);
        result = 31 * result + Arrays.hashCode(cdf);
        return result;
    }

    @Override
    public String toString() {
        return "EmpiricalScalarModel[bins=" + getBinCount() + ", range=[" + min + ", " + max + "]]";
    }
}
