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
 * Gamma distribution scalar model - Pearson Type III.
 *
 * <h2>Purpose</h2>
 *
 * <p>Models positive-valued, right-skewed data commonly arising in:
 * <ul>
 *   <li>Waiting times and service durations</li>
 *   <li>Rainfall amounts and natural phenomena</li>
 *   <li>Insurance claims and financial data</li>
 *   <li>Any sum of exponential random variables</li>
 * </ul>
 *
 * <h2>Pearson Type</h2>
 *
 * <p>The gamma distribution is Pearson Type III, characterized by:
 * <ul>
 *   <li>Semi-bounded support: [location, +∞)</li>
 *   <li>Right-skewed (positive skewness)</li>
 *   <li>On the Pearson classification line where 2β₂ - 3β₁ - 6 = 0</li>
 * </ul>
 *
 * <h2>Parameters</h2>
 *
 * <ul>
 *   <li><b>shape (k)</b>: Shape parameter; k &gt; 0. Controls distribution shape.</li>
 *   <li><b>scale (θ)</b>: Scale parameter; θ &gt; 0. Stretches the distribution.</li>
 *   <li><b>location (γ)</b>: Location parameter; shifts the distribution. Default 0.</li>
 * </ul>
 *
 * <h2>Alternative Parameterizations</h2>
 *
 * <ul>
 *   <li><b>Shape-Scale (k, θ):</b> Used here. Mean = kθ.</li>
 *   <li><b>Shape-Rate (α, β):</b> α = k, β = 1/θ. Mean = α/β.</li>
 * </ul>
 *
 * <h2>Special Cases</h2>
 *
 * <ul>
 *   <li>k = 1: Exponential distribution</li>
 *   <li>k = n/2, θ = 2: Chi-squared with n degrees of freedom</li>
 *   <li>k integer: Erlang distribution</li>
 * </ul>
 *
 * <h2>Moments</h2>
 *
 * <pre>{@code
 * Mean = kθ + γ
 * Variance = kθ²
 * Skewness = 2/√k
 * Kurtosis (excess) = 6/k
 * }</pre>
 *
 * <h2>Usage</h2>
 *
 * <pre>{@code
 * // Standard gamma with shape=2, scale=1
 * GammaScalarModel gamma = new GammaScalarModel(2.0, 1.0);
 *
 * // Exponential distribution (shape=1)
 * GammaScalarModel exponential = GammaScalarModel.exponential(2.0);
 *
 * // Chi-squared with 4 degrees of freedom
 * GammaScalarModel chiSquared = GammaScalarModel.chiSquared(4);
 *
 * // Shifted gamma
 * GammaScalarModel shifted = new GammaScalarModel(2.0, 1.0, 5.0);
 * }</pre>
 *
 * @see ScalarModel
 * @see PearsonType#TYPE_III_GAMMA
 */
@ModelType(GammaScalarModel.MODEL_TYPE)
public class GammaScalarModel implements ScalarModel {

    public static final String MODEL_TYPE = "gamma";

    @SerializedName("shape")
    private final double shape;     // k

    @SerializedName("scale")
    private final double scale;     // θ

    @SerializedName("location")
    private final double location;  // γ (shift)

    /**
     * Constructs a gamma scalar model with default location 0.
     *
     * @param shape the shape parameter (k); must be positive
     * @param scale the scale parameter (θ); must be positive
     * @throws IllegalArgumentException if shape or scale is not positive
     */
    public GammaScalarModel(double shape, double scale) {
        this(shape, scale, 0.0);
    }

    /**
     * Constructs a gamma scalar model with location shift.
     *
     * @param shape the shape parameter (k); must be positive
     * @param scale the scale parameter (θ); must be positive
     * @param location the location parameter (γ); shifts support to [γ, +∞)
     * @throws IllegalArgumentException if shape or scale is not positive
     */
    public GammaScalarModel(double shape, double scale, double location) {
        if (shape <= 0) {
            throw new IllegalArgumentException("Shape must be positive, got: " + shape);
        }
        if (scale <= 0) {
            throw new IllegalArgumentException("Scale must be positive, got: " + scale);
        }
        this.shape = shape;
        this.scale = scale;
        this.location = location;
    }

    @Override
    public String getModelType() {
        return MODEL_TYPE;
    }

    /**
     * Returns the shape parameter k.
     * @return the shape
     */
    public double getShape() {
        return shape;
    }

    /**
     * Returns the scale parameter θ.
     * @return the scale
     */
    public double getScale() {
        return scale;
    }

    /**
     * Returns the rate parameter β = 1/θ.
     * @return the rate
     */
    public double getRate() {
        return 1.0 / scale;
    }

    /**
     * Returns the location parameter γ.
     * @return the location (lower bound of support)
     */
    public double getLocation() {
        return location;
    }

    /**
     * Returns the lower bound of the support.
     * @return the lower bound (same as location)
     */
    public double getLower() {
        return location;
    }

    /**
     * Computes the mean of this gamma distribution.
     * <p>Mean = kθ + γ
     *
     * @return the mean
     */
    public double getMean() {
        return shape * scale + location;
    }

    /**
     * Computes the variance of this gamma distribution.
     * <p>Variance = kθ²
     *
     * @return the variance
     */
    public double getVariance() {
        return shape * scale * scale;
    }

    /**
     * Computes the standard deviation of this gamma distribution.
     * @return the standard deviation
     */
    public double getStdDev() {
        return Math.sqrt(getVariance());
    }

    /**
     * Computes the skewness of this gamma distribution.
     * <p>Skewness = 2/√k (always positive, right-skewed)
     *
     * @return the skewness
     */
    public double getSkewness() {
        return 2.0 / Math.sqrt(shape);
    }

    /**
     * Computes the excess kurtosis of this gamma distribution.
     * <p>Excess kurtosis = 6/k
     *
     * @return the excess kurtosis
     */
    public double getExcessKurtosis() {
        return 6.0 / shape;
    }

    /**
     * Computes the standard kurtosis (β₂) of this gamma distribution.
     * <p>Kurtosis = 3 + 6/k
     *
     * @return the standard kurtosis
     */
    public double getKurtosis() {
        return 3.0 + getExcessKurtosis();
    }

    /**
     * Computes the mode of this gamma distribution.
     * <p>Mode = (k - 1)θ + γ for k ≥ 1
     *
     * @return the mode, or location if k &lt; 1
     */
    public double getMode() {
        if (shape < 1.0) {
            return location;  // Mode at lower bound
        }
        return (shape - 1) * scale + location;
    }

    /**
     * Returns whether this is an exponential distribution (k = 1).
     * @return true if shape = 1
     */
    public boolean isExponential() {
        return Math.abs(shape - 1.0) < 1e-10;
    }

    /**
     * Creates an exponential distribution (Gamma with shape = 1).
     *
     * @param scale the scale parameter (mean of exponential)
     * @return exponential distribution model
     */
    public static GammaScalarModel exponential(double scale) {
        return new GammaScalarModel(1.0, scale);
    }

    /**
     * Creates a chi-squared distribution.
     *
     * <p>Chi-squared with n degrees of freedom is Gamma(n/2, 2).
     *
     * @param degreesOfFreedom the degrees of freedom; must be positive
     * @return chi-squared distribution model
     */
    public static GammaScalarModel chiSquared(int degreesOfFreedom) {
        if (degreesOfFreedom <= 0) {
            throw new IllegalArgumentException("Degrees of freedom must be positive");
        }
        return new GammaScalarModel(degreesOfFreedom / 2.0, 2.0);
    }

    /**
     * Creates a gamma distribution from mean and variance.
     *
     * <p>Method of moments estimation: k = mean²/variance, θ = variance/mean
     *
     * @param mean the target mean
     * @param variance the target variance
     * @return gamma model with specified moments
     * @throws IllegalArgumentException if mean or variance is not positive
     */
    public static GammaScalarModel fromMeanVariance(double mean, double variance) {
        if (mean <= 0) {
            throw new IllegalArgumentException("Mean must be positive for gamma: " + mean);
        }
        if (variance <= 0) {
            throw new IllegalArgumentException("Variance must be positive: " + variance);
        }
        double shape = (mean * mean) / variance;
        double scale = variance / mean;
        return new GammaScalarModel(shape, scale);
    }

    /**
     * Creates an array of identical gamma scalar models.
     *
     * @param shape the shape parameter
     * @param scale the scale parameter
     * @param dimensions the number of models
     * @return an array of identical gamma models
     */
    public static GammaScalarModel[] uniformScalar(double shape, double scale, int dimensions) {
        GammaScalarModel[] models = new GammaScalarModel[dimensions];
        GammaScalarModel model = new GammaScalarModel(shape, scale);
        for (int i = 0; i < dimensions; i++) {
            models[i] = model;
        }
        return models;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof GammaScalarModel)) return false;
        GammaScalarModel that = (GammaScalarModel) o;
        return Double.compare(that.shape, shape) == 0 &&
               Double.compare(that.scale, scale) == 0 &&
               Double.compare(that.location, location) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(shape, scale, location);
    }

    @Override
    public String toString() {
        if (location == 0.0) {
            return "GammaScalarModel[shape=" + shape + ", scale=" + scale + "]";
        } else {
            return "GammaScalarModel[shape=" + shape + ", scale=" + scale +
                   ", location=" + location + "]";
        }
    }
}
