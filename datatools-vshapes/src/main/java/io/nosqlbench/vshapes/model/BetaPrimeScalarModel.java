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
 * Beta prime (beta distribution of the second kind) scalar model - Pearson Type VI.
 *
 * <h2>Purpose</h2>
 *
 * <p>Models positive-valued, right-skewed data commonly arising in:
 * <ul>
 *   <li>F-distribution family (variance ratios)</li>
 *   <li>Income and wealth distributions</li>
 *   <li>Bayesian analysis</li>
 *   <li>Survival analysis</li>
 * </ul>
 *
 * <h2>Pearson Type</h2>
 *
 * <p>The beta prime distribution is Pearson Type VI, characterized by:
 * <ul>
 *   <li>Semi-bounded support: (0, +∞)</li>
 *   <li>Right-skewed (positive skewness)</li>
 *   <li>Pearson criterion κ &gt; 1</li>
 * </ul>
 *
 * <h2>Parameters</h2>
 *
 * <ul>
 *   <li><b>alpha (α)</b>: First shape parameter; α &gt; 0</li>
 *   <li><b>beta (β)</b>: Second shape parameter; β &gt; 0</li>
 *   <li><b>scale (σ)</b>: Scale parameter; σ &gt; 0 (default 1)</li>
 * </ul>
 *
 * <h2>Relationship to Other Distributions</h2>
 *
 * <ul>
 *   <li>If X ~ Beta(α, β), then X/(1-X) ~ BetaPrime(α, β)</li>
 *   <li>F(d₁, d₂) = (d₂/d₁) * BetaPrime(d₁/2, d₂/2)</li>
 *   <li>BetaPrime(α, β) = InverseGamma(β, 1) / Gamma(α, 1)</li>
 * </ul>
 *
 * <h2>Moments (when defined)</h2>
 *
 * <pre>{@code
 * Mean = α / (β - 1) (for β > 1)
 * Variance = α(α + β - 1) / [(β - 1)²(β - 2)] (for β > 2)
 * Mode = (α - 1) / (β + 1) (for α ≥ 1)
 * }</pre>
 *
 * <h2>Usage</h2>
 *
 * <pre>{@code
 * // Standard beta prime
 * BetaPrimeScalarModel bp = new BetaPrimeScalarModel(2.0, 4.0);
 *
 * // F-distribution
 * BetaPrimeScalarModel f = BetaPrimeScalarModel.fDistribution(5, 10);
 * }</pre>
 *
 * @see ScalarModel
 * @see BetaScalarModel
 * @see PearsonType#TYPE_VI_BETA_PRIME
 */
@ModelType(BetaPrimeScalarModel.MODEL_TYPE)
public class BetaPrimeScalarModel implements ScalarModel {

    public static final String MODEL_TYPE = "beta_prime";

    @SerializedName("alpha")
    private final double alpha;  // α (first shape)

    @SerializedName("beta")
    private final double beta;   // β (second shape)

    @SerializedName("scale")
    private final double scale;  // σ

    /**
     * Constructs a beta prime scalar model with default scale 1.
     *
     * @param alpha the first shape parameter (α); must be positive
     * @param beta the second shape parameter (β); must be positive
     * @throws IllegalArgumentException if alpha or beta is not positive
     */
    public BetaPrimeScalarModel(double alpha, double beta) {
        this(alpha, beta, 1.0);
    }

    /**
     * Constructs a beta prime scalar model.
     *
     * @param alpha the first shape parameter (α); must be positive
     * @param beta the second shape parameter (β); must be positive
     * @param scale the scale parameter (σ); must be positive
     * @throws IllegalArgumentException if any parameter is not positive
     */
    public BetaPrimeScalarModel(double alpha, double beta, double scale) {
        if (alpha <= 0) {
            throw new IllegalArgumentException("Alpha must be positive, got: " + alpha);
        }
        if (beta <= 0) {
            throw new IllegalArgumentException("Beta must be positive, got: " + beta);
        }
        if (scale <= 0) {
            throw new IllegalArgumentException("Scale must be positive, got: " + scale);
        }
        this.alpha = alpha;
        this.beta = beta;
        this.scale = scale;
    }

    @Override
    public String getModelType() {
        return MODEL_TYPE;
    }

    /**
     * Returns the first shape parameter α.
     * @return alpha
     */
    public double getAlpha() {
        return alpha;
    }

    /**
     * Returns the second shape parameter β.
     * @return beta
     */
    public double getBeta() {
        return beta;
    }

    /**
     * Returns the scale parameter σ.
     * @return the scale
     */
    public double getScale() {
        return scale;
    }

    /**
     * Computes the mean of this beta prime distribution.
     * <p>Mean = σα / (β - 1) for β &gt; 1
     *
     * @return the mean, or +∞ if β ≤ 1
     */
    public double getMean() {
        if (beta <= 1) {
            return Double.POSITIVE_INFINITY;
        }
        return scale * alpha / (beta - 1);
    }

    /**
     * Computes the variance of this beta prime distribution.
     * <p>Variance = σ²α(α + β - 1) / [(β - 1)²(β - 2)] for β &gt; 2
     *
     * @return the variance, or +∞ if 1 &lt; β ≤ 2, or NaN if β ≤ 1
     */
    public double getVariance() {
        if (beta <= 1) {
            return Double.NaN;
        } else if (beta <= 2) {
            return Double.POSITIVE_INFINITY;
        }
        double bm1 = beta - 1;
        return scale * scale * alpha * (alpha + beta - 1) / (bm1 * bm1 * (beta - 2));
    }

    /**
     * Computes the standard deviation of this beta prime distribution.
     * @return the standard deviation
     */
    public double getStdDev() {
        return Math.sqrt(getVariance());
    }

    /**
     * Computes the mode of this beta prime distribution.
     * <p>Mode = σ(α - 1) / (β + 1) for α ≥ 1
     *
     * @return the mode, or 0 if α &lt; 1
     */
    public double getMode() {
        if (alpha < 1) {
            return 0.0;  // Mode at lower bound
        }
        return scale * (alpha - 1) / (beta + 1);
    }

    /**
     * Computes the skewness of this beta prime distribution.
     * <p>Defined for β &gt; 3.
     *
     * @return the skewness, or NaN if β ≤ 3
     */
    public double getSkewness() {
        if (beta <= 3) {
            return Double.NaN;
        }
        double sumAB = alpha + beta;
        double num = 2 * (2 * alpha + beta - 1) * Math.sqrt(beta - 2);
        double denom = (beta - 3) * Math.sqrt(alpha * (sumAB - 1));
        return num / denom;
    }

    /**
     * Returns whether this distribution has a finite mean (β &gt; 1).
     * @return true if mean is finite
     */
    public boolean hasFiniteMean() {
        return beta > 1;
    }

    /**
     * Returns whether this distribution has a finite variance (β &gt; 2).
     * @return true if variance is finite
     */
    public boolean hasFiniteVariance() {
        return beta > 2;
    }

    /**
     * Creates an F-distribution.
     *
     * <p>F(d₁, d₂) is related to BetaPrime as: F = (d₂/d₁) * BetaPrime(d₁/2, d₂/2)
     *
     * @param d1 numerator degrees of freedom
     * @param d2 denominator degrees of freedom
     * @return F-distribution model
     */
    public static BetaPrimeScalarModel fDistribution(int d1, int d2) {
        if (d1 <= 0 || d2 <= 0) {
            throw new IllegalArgumentException("Degrees of freedom must be positive");
        }
        double alpha = d1 / 2.0;
        double beta = d2 / 2.0;
        double scale = (double) d2 / d1;
        return new BetaPrimeScalarModel(alpha, beta, scale);
    }

    /**
     * Creates an array of identical beta prime scalar models.
     *
     * @param alpha the first shape parameter
     * @param beta the second shape parameter
     * @param dimensions the number of models
     * @return an array of identical beta prime models
     */
    public static BetaPrimeScalarModel[] uniformScalar(double alpha, double beta, int dimensions) {
        BetaPrimeScalarModel[] models = new BetaPrimeScalarModel[dimensions];
        BetaPrimeScalarModel model = new BetaPrimeScalarModel(alpha, beta);
        for (int i = 0; i < dimensions; i++) {
            models[i] = model;
        }
        return models;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BetaPrimeScalarModel)) return false;
        BetaPrimeScalarModel that = (BetaPrimeScalarModel) o;
        return Double.compare(that.alpha, alpha) == 0 &&
               Double.compare(that.beta, beta) == 0 &&
               Double.compare(that.scale, scale) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(alpha, beta, scale);
    }

    @Override
    public String toString() {
        if (scale == 1.0) {
            return "BetaPrimeScalarModel[α=" + alpha + ", β=" + beta + "]";
        } else {
            return "BetaPrimeScalarModel[α=" + alpha + ", β=" + beta + ", σ=" + scale + "]";
        }
    }
}
