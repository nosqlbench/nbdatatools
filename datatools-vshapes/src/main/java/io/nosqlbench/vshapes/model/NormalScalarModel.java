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

import java.util.Objects;

/**
 * Normal (Gaussian) distribution scalar model - Pearson Type 0.
 *
 * <h2>Purpose</h2>
 *
 * <p>Each dimension in a vector space can have its own normal distribution,
 * characterized by a mean (μ) and standard deviation (σ). This class holds
 * those parameters and is used during the inverse CDF transform stage.
 *
 * <h2>Pearson Type</h2>
 *
 * <p>The normal distribution is Pearson Type 0, characterized by:
 * <ul>
 *   <li>Skewness (β₁) = 0</li>
 *   <li>Kurtosis (β₂) = 3</li>
 *   <li>Support: (-∞, +∞)</li>
 * </ul>
 *
 * <h2>Tensor Hierarchy</h2>
 *
 * <p>NormalScalarModel is a first-order tensor model (ScalarModel) that
 * represents a single-dimensional normal distribution:
 * <ul>
 *   <li>{@link ScalarModel} - First-order (single dimension) - this class</li>
 *   <li>{@link VectorModel} - Second-order (M dimensions)</li>
 *   <li>{@link MatrixModel} - Third-order (K vector models)</li>
 * </ul>
 *
 * <h2>Truncation Support</h2>
 *
 * <p>Optionally, the distribution can be truncated to a bounded interval [lower, upper].
 * When truncated, the proper inverse transform method is used to ensure:
 * <ul>
 *   <li>All samples fall exactly within [lower, upper]</li>
 *   <li>Quantile spacing is preserved</li>
 *   <li>Zero probability mass outside bounds</li>
 * </ul>
 *
 * <h2>Usage</h2>
 *
 * <pre>{@code
 * // Standard normal N(0, 1) - unbounded
 * NormalScalarModel standard = NormalScalarModel.standardNormal();
 *
 * // Truncated to [-1, 1]
 * NormalScalarModel bounded = new NormalScalarModel(0.0, 1.0, -1.0, 1.0);
 *
 * // Create M identical truncated models
 * NormalScalarModel[] uniform = NormalScalarModel.uniformScalar(0.0, 1.0, -1.0, 1.0, 128);
 * }</pre>
 *
 * @see ScalarModel
 * @see VectorModel
 * @see VectorSpaceModel
 * @see PearsonType#TYPE_0_NORMAL
 */
@ModelType(NormalScalarModel.MODEL_TYPE)
public class NormalScalarModel implements ScalarModel {

    public static final String MODEL_TYPE = "normal";

    @SerializedName("mean")
    private final double mean;

    @SerializedName("std_dev")
    private final double stdDev;

    @SerializedName("lower_bound")
    private final double lower;

    @SerializedName("upper_bound")
    private final double upper;

    private final transient boolean truncated;

    /**
     * Constructs an unbounded normal scalar model.
     *
     * @param mean the mean (μ) of the normal distribution
     * @param stdDev the standard deviation (σ) of the normal distribution; must be positive
     * @throws IllegalArgumentException if stdDev is not positive
     */
    public NormalScalarModel(double mean, double stdDev) {
        if (stdDev <= 0) {
            throw new IllegalArgumentException("Standard deviation must be positive, got: " + stdDev);
        }
        this.mean = mean;
        this.stdDev = stdDev;
        this.lower = Double.NEGATIVE_INFINITY;
        this.upper = Double.POSITIVE_INFINITY;
        this.truncated = false;
    }

    /**
     * Constructs a truncated normal scalar model.
     *
     * <p>Outputs will be bounded to [lower, upper] using the proper truncated
     * normal inverse transform method.
     *
     * @param mean the mean (μ) of the underlying normal distribution
     * @param stdDev the standard deviation (σ); must be positive
     * @param lower the lower bound of the truncation interval
     * @param upper the upper bound of the truncation interval
     * @throws IllegalArgumentException if stdDev ≤ 0 or lower ≥ upper
     */
    public NormalScalarModel(double mean, double stdDev, double lower, double upper) {
        if (stdDev <= 0) {
            throw new IllegalArgumentException("Standard deviation must be positive, got: " + stdDev);
        }
        if (lower >= upper) {
            throw new IllegalArgumentException("Lower bound must be less than upper: " + lower + " >= " + upper);
        }
        this.mean = mean;
        this.stdDev = stdDev;
        this.lower = lower;
        this.upper = upper;
        this.truncated = true;
    }

    @Override
    public String getModelType() {
        return MODEL_TYPE;
    }

    /**
     * Returns the mean of this normal distribution.
     * @return the mean (μ)
     */
    public double getMean() {
        return mean;
    }

    /**
     * Returns the standard deviation of this normal distribution.
     * @return the standard deviation (σ)
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
        if (truncated) {
            if (x < lower || x > upper) {
                return 0.0;
            }
            // PDF of truncated normal: f(x) = φ((x-μ)/σ) / (σ * (Φ(b) - Φ(a)))
            double z = (x - mean) / stdDev;
            double phi = Math.exp(-0.5 * z * z) / Math.sqrt(2 * Math.PI);
            double cdfRange = NormalCDF.cdf(upper, mean, stdDev) - NormalCDF.cdf(lower, mean, stdDev);
            return phi / (stdDev * cdfRange);
        } else {
            // Standard normal PDF: φ(x) = (1/σ√(2π)) * e^(-(x-μ)²/(2σ²))
            double z = (x - mean) / stdDev;
            return Math.exp(-0.5 * z * z) / (stdDev * Math.sqrt(2 * Math.PI));
        }
    }

    /**
     * Computes the cumulative distribution function (CDF) at a given value.
     *
     * <p>For truncated normal, guards against division by near-zero when the
     * truncation range captures very little probability mass (e.g., truncating
     * far from the mean). The result is always clamped to [0, 1].
     *
     * @param x the value at which to evaluate the CDF
     * @return the cumulative probability P(X ≤ x), in range [0, 1]
     */
    public double cdf(double x) {
        if (truncated) {
            if (x <= lower) return 0.0;
            if (x >= upper) return 1.0;
            // CDF of truncated normal: F(x) = (Φ((x-μ)/σ) - Φ(a)) / (Φ(b) - Φ(a))
            double cdfX = NormalCDF.cdf(x, mean, stdDev);
            double cdfLower = NormalCDF.cdf(lower, mean, stdDev);
            double cdfUpper = NormalCDF.cdf(upper, mean, stdDev);
            double range = cdfUpper - cdfLower;
            // Guard against division by near-zero when truncation range has tiny probability mass
            if (range < 1e-15) {
                // Degenerate case: truncation range has essentially no probability mass
                // Return linear interpolation within bounds
                return (x - lower) / (upper - lower);
            }
            double result = (cdfX - cdfLower) / range;
            // Clamp to [0, 1] to guard against floating-point errors
            return Math.max(0.0, Math.min(1.0, result));
        } else {
            return NormalCDF.cdf(x, mean, stdDev);
        }
    }

    /**
     * Returns the lower truncation bound.
     * @return the lower bound, or {@link Double#NEGATIVE_INFINITY} if unbounded
     */
    public double lower() {
        return lower;
    }

    /**
     * Returns the upper truncation bound.
     * @return the upper bound, or {@link Double#POSITIVE_INFINITY} if unbounded
     */
    public double upper() {
        return upper;
    }

    /**
     * Returns whether this model has truncation bounds.
     * @return true if truncated, false if unbounded
     */
    public boolean isTruncated() {
        return truncated;
    }

    /**
     * Creates a standard normal scalar model N(0, 1) - unbounded.
     * @return a standard normal scalar model
     */
    public static NormalScalarModel standardNormal() {
        return new NormalScalarModel(0.0, 1.0);
    }

    /**
     * Creates a standard normal scalar model N(0, 1) truncated to [-1, 1].
     * @return a unit-bounded standard normal scalar model
     */
    public static NormalScalarModel standardNormalUnitBounded() {
        return new NormalScalarModel(0.0, 1.0, -1.0, 1.0);
    }

    /**
     * Creates an array of identical unbounded normal scalar models.
     *
     * @param mean the mean for all models
     * @param stdDev the standard deviation for all models
     * @param dimensions the number of models (M)
     * @return an array of M identical normal scalar models
     */
    public static NormalScalarModel[] uniformScalar(double mean, double stdDev, int dimensions) {
        NormalScalarModel[] models = new NormalScalarModel[dimensions];
        NormalScalarModel model = new NormalScalarModel(mean, stdDev);
        for (int i = 0; i < dimensions; i++) {
            models[i] = model;
        }
        return models;
    }

    /**
     * Creates an array of identical truncated normal scalar models.
     *
     * @param mean the mean for all models
     * @param stdDev the standard deviation for all models
     * @param lower the lower truncation bound for all models
     * @param upper the upper truncation bound for all models
     * @param dimensions the number of models (M)
     * @return an array of M identical truncated normal scalar models
     */
    public static NormalScalarModel[] uniformScalar(double mean, double stdDev,
                                                      double lower, double upper, int dimensions) {
        NormalScalarModel[] models = new NormalScalarModel[dimensions];
        NormalScalarModel model = new NormalScalarModel(mean, stdDev, lower, upper);
        for (int i = 0; i < dimensions; i++) {
            models[i] = model;
        }
        return models;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof NormalScalarModel)) return false;
        NormalScalarModel that = (NormalScalarModel) o;
        return Double.compare(that.mean, mean) == 0 &&
               Double.compare(that.stdDev, stdDev) == 0 &&
               Double.compare(that.lower, lower) == 0 &&
               Double.compare(that.upper, upper) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(mean, stdDev, lower, upper);
    }

    @Override
    public String toString() {
        if (truncated) {
            return "NormalScalarModel[mean=" + mean + ", stdDev=" + stdDev +
                   ", truncated=[" + lower + ", " + upper + "]]";
        } else {
            return "NormalScalarModel[mean=" + mean + ", stdDev=" + stdDev + "]";
        }
    }
}
