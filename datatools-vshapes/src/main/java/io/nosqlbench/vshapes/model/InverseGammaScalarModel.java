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
 * Inverse gamma distribution scalar model - Pearson Type V.
 *
 * <h2>Purpose</h2>
 *
 * <p>Models positive-valued data that is the reciprocal of gamma-distributed data:
 * <ul>
 *   <li>Bayesian statistics: conjugate prior for normal variance</li>
 *   <li>Scaled inverse chi-squared distributions</li>
 *   <li>Reliability engineering</li>
 *   <li>Economic modeling</li>
 * </ul>
 *
 * <h2>Pearson Type</h2>
 *
 * <p>The inverse gamma distribution is Pearson Type V, characterized by:
 * <ul>
 *   <li>Semi-bounded support: (0, +∞)</li>
 *   <li>Right-skewed (positive skewness)</li>
 *   <li>Pearson criterion κ = 1</li>
 * </ul>
 *
 * <h2>Parameters</h2>
 *
 * <ul>
 *   <li><b>shape (α)</b>: Shape parameter; α &gt; 0</li>
 *   <li><b>scale (β)</b>: Scale parameter; β &gt; 0</li>
 * </ul>
 *
 * <h2>Relationship to Gamma</h2>
 *
 * <p>If X ~ Gamma(α, β), then 1/X ~ InverseGamma(α, 1/β).
 *
 * <h2>Moments (when defined)</h2>
 *
 * <pre>{@code
 * Mean = β / (α - 1) (for α > 1)
 * Variance = β² / [(α - 1)²(α - 2)] (for α > 2)
 * Mode = β / (α + 1)
 * }</pre>
 *
 * <h2>Usage</h2>
 *
 * <pre>{@code
 * // Standard inverse gamma
 * InverseGammaScalarModel ig = new InverseGammaScalarModel(3.0, 2.0);
 *
 * // Scaled inverse chi-squared
 * InverseGammaScalarModel scaledInvChiSq = InverseGammaScalarModel.scaledInverseChiSquared(5, 2.0);
 * }</pre>
 *
 * @see ScalarModel
 * @see GammaScalarModel
 * @see PearsonType#TYPE_V_INVERSE_GAMMA
 */
@ModelType(InverseGammaScalarModel.MODEL_TYPE)
public class InverseGammaScalarModel implements ScalarModel {

    public static final String MODEL_TYPE = "inverse_gamma";

    @SerializedName("shape")
    private final double shape;  // α

    @SerializedName("scale")
    private final double scale;  // β

    /**
     * Constructs an inverse gamma scalar model.
     *
     * @param shape the shape parameter (α); must be positive
     * @param scale the scale parameter (β); must be positive
     * @throws IllegalArgumentException if shape or scale is not positive
     */
    public InverseGammaScalarModel(double shape, double scale) {
        if (shape <= 0) {
            throw new IllegalArgumentException("Shape must be positive, got: " + shape);
        }
        if (scale <= 0) {
            throw new IllegalArgumentException("Scale must be positive, got: " + scale);
        }
        this.shape = shape;
        this.scale = scale;
    }

    @Override
    public String getModelType() {
        return MODEL_TYPE;
    }

    /**
     * Returns the shape parameter α.
     * @return the shape
     */
    public double getShape() {
        return shape;
    }

    /**
     * Returns the scale parameter β.
     * @return the scale
     */
    public double getScale() {
        return scale;
    }

    /**
     * Computes the mean of this inverse gamma distribution.
     * <p>Mean = β / (α - 1) for α &gt; 1
     *
     * @return the mean, or +∞ if α ≤ 1
     */
    public double getMean() {
        if (shape <= 1) {
            return Double.POSITIVE_INFINITY;
        }
        return scale / (shape - 1);
    }

    /**
     * Computes the variance of this inverse gamma distribution.
     * <p>Variance = β² / [(α - 1)²(α - 2)] for α &gt; 2
     *
     * @return the variance, or +∞ if 1 &lt; α ≤ 2, or NaN if α ≤ 1
     */
    public double getVariance() {
        if (shape <= 1) {
            return Double.NaN;
        } else if (shape <= 2) {
            return Double.POSITIVE_INFINITY;
        }
        double am1 = shape - 1;
        return (scale * scale) / (am1 * am1 * (shape - 2));
    }

    /**
     * Computes the standard deviation of this inverse gamma distribution.
     * @return the standard deviation
     */
    public double getStdDev() {
        return Math.sqrt(getVariance());
    }

    /**
     * Computes the mode of this inverse gamma distribution.
     * <p>Mode = β / (α + 1)
     *
     * @return the mode
     */
    public double getMode() {
        return scale / (shape + 1);
    }

    /**
     * Computes the skewness of this inverse gamma distribution.
     * <p>Skewness = 4√(α - 2) / (α - 3) for α &gt; 3
     *
     * @return the skewness, or NaN if α ≤ 3
     */
    public double getSkewness() {
        if (shape <= 3) {
            return Double.NaN;
        }
        return 4 * Math.sqrt(shape - 2) / (shape - 3);
    }

    /**
     * Computes the excess kurtosis of this inverse gamma distribution.
     * <p>Excess kurtosis = (30α - 66) / [(α - 3)(α - 4)] for α &gt; 4
     *
     * @return the excess kurtosis, or NaN if α ≤ 4
     */
    public double getExcessKurtosis() {
        if (shape <= 4) {
            return Double.NaN;
        }
        return (30 * shape - 66) / ((shape - 3) * (shape - 4));
    }

    /**
     * Returns whether this distribution has a defined mean (α &gt; 1).
     * @return true if mean is finite
     */
    public boolean hasFiniteMean() {
        return shape > 1;
    }

    /**
     * Returns whether this distribution has a finite variance (α &gt; 2).
     * @return true if variance is finite
     */
    public boolean hasFiniteVariance() {
        return shape > 2;
    }

    /**
     * Creates an inverse chi-squared distribution.
     *
     * <p>InverseChiSquared(ν) = InverseGamma(ν/2, 1/2)
     *
     * @param degreesOfFreedom the degrees of freedom; must be positive
     * @return inverse chi-squared distribution model
     */
    public static InverseGammaScalarModel inverseChiSquared(int degreesOfFreedom) {
        if (degreesOfFreedom <= 0) {
            throw new IllegalArgumentException("Degrees of freedom must be positive");
        }
        return new InverseGammaScalarModel(degreesOfFreedom / 2.0, 0.5);
    }

    /**
     * Creates a scaled inverse chi-squared distribution.
     *
     * <p>ScaledInverseChiSquared(ν, σ²) = InverseGamma(ν/2, νσ²/2)
     *
     * @param degreesOfFreedom the degrees of freedom; must be positive
     * @param scaleSquared the scale² parameter (σ²)
     * @return scaled inverse chi-squared distribution model
     */
    public static InverseGammaScalarModel scaledInverseChiSquared(int degreesOfFreedom, double scaleSquared) {
        if (degreesOfFreedom <= 0) {
            throw new IllegalArgumentException("Degrees of freedom must be positive");
        }
        if (scaleSquared <= 0) {
            throw new IllegalArgumentException("Scale squared must be positive");
        }
        return new InverseGammaScalarModel(degreesOfFreedom / 2.0, degreesOfFreedom * scaleSquared / 2.0);
    }

    /**
     * Creates an inverse gamma distribution from mean and variance.
     *
     * <p>Method of moments requires α &gt; 2.
     *
     * @param mean the target mean
     * @param variance the target variance
     * @return inverse gamma model with specified moments
     * @throws IllegalArgumentException if moments are not achievable
     */
    public static InverseGammaScalarModel fromMeanVariance(double mean, double variance) {
        if (mean <= 0) {
            throw new IllegalArgumentException("Mean must be positive: " + mean);
        }
        if (variance <= 0) {
            throw new IllegalArgumentException("Variance must be positive: " + variance);
        }
        // From mean = β/(α-1) and variance = β²/[(α-1)²(α-2)]
        // we get: α = 2 + mean²/variance and β = mean(α-1)
        double shape = 2 + (mean * mean) / variance;
        double scale = mean * (shape - 1);
        return new InverseGammaScalarModel(shape, scale);
    }

    /**
     * Creates an array of identical inverse gamma scalar models.
     *
     * @param shape the shape parameter
     * @param scale the scale parameter
     * @param dimensions the number of models
     * @return an array of identical inverse gamma models
     */
    public static InverseGammaScalarModel[] uniformScalar(double shape, double scale, int dimensions) {
        InverseGammaScalarModel[] models = new InverseGammaScalarModel[dimensions];
        InverseGammaScalarModel model = new InverseGammaScalarModel(shape, scale);
        for (int i = 0; i < dimensions; i++) {
            models[i] = model;
        }
        return models;
    }

    /**
     * Computes the cumulative distribution function (CDF) at a given value.
     *
     * <p>The CDF of inverse gamma is: F(x) = Q(α, β/x) where Q is the
     * regularized upper incomplete gamma function.
     *
     * @param x the value at which to evaluate the CDF
     * @return the cumulative probability P(X ≤ x), in range [0, 1]
     */
    @Override
    public double cdf(double x) {
        if (x <= 0) return 0.0;
        // F(x) = Q(α, β/x) = 1 - P(α, β/x)
        double z = scale / x;
        return 1.0 - regularizedGammaP(shape, z);
    }

    private static double regularizedGammaP(double a, double x) {
        if (x <= 0) return 0;
        if (x > a + 100) return 1.0;
        if (x < a + 1) {
            return gammaSeries(a, x);
        } else {
            return 1.0 - gammaContinuedFraction(a, x);
        }
    }

    private static double gammaSeries(double a, double x) {
        double sum = 1.0 / a;
        double term = sum;
        for (int n = 1; n <= 100; n++) {
            term *= x / (a + n);
            sum += term;
            if (Math.abs(term) < Math.abs(sum) * 1e-10) break;
        }
        return sum * Math.exp(-x + a * Math.log(x) - logGamma(a));
    }

    private static double gammaContinuedFraction(double a, double x) {
        double b = x + 1 - a;
        double c = 1.0 / 1e-30;
        double d = 1.0 / b;
        double h = d;
        for (int i = 1; i <= 100; i++) {
            double an = -i * (i - a);
            b += 2;
            d = an * d + b;
            if (Math.abs(d) < 1e-30) d = 1e-30;
            c = b + an / c;
            if (Math.abs(c) < 1e-30) c = 1e-30;
            d = 1.0 / d;
            double del = d * c;
            h *= del;
            if (Math.abs(del - 1) < 1e-10) break;
        }
        return Math.exp(-x + a * Math.log(x) - logGamma(a)) * h;
    }

    private static double logGamma(double x) {
        double[] cof = {76.18009172947146, -86.50532032941677, 24.01409824083091,
                        -1.231739572450155, 0.1208650973866179e-2, -0.5395239384953e-5};
        double y = x;
        double tmp = x + 5.5;
        tmp -= (x + 0.5) * Math.log(tmp);
        double ser = 1.000000000190015;
        for (int j = 0; j < 6; j++) {
            ser += cof[j] / ++y;
        }
        return -tmp + Math.log(2.5066282746310005 * ser / x);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof InverseGammaScalarModel)) return false;
        InverseGammaScalarModel that = (InverseGammaScalarModel) o;
        return Double.compare(that.shape, shape) == 0 &&
               Double.compare(that.scale, scale) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(shape, scale);
    }

    @Override
    public String toString() {
        return "InverseGammaScalarModel[α=" + shape + ", β=" + scale + "]";
    }
}
