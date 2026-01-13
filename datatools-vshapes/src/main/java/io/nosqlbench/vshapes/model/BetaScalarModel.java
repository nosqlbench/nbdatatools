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
 * Beta distribution scalar model - Pearson Type I (asymmetric) and Type II (symmetric).
 *
 * <h2>Purpose</h2>
 *
 * <p>Models bounded distributions with flexible shape, commonly used for
 * proportions, percentages, and any data naturally constrained to an interval.
 *
 * <h2>Pearson Types</h2>
 *
 * <ul>
 *   <li><b>Type I</b>: Asymmetric beta (α ≠ β)</li>
 *   <li><b>Type II</b>: Symmetric beta (α = β)</li>
 * </ul>
 *
 * <p>Classification criteria:
 * <ul>
 *   <li>Bounded support: [lower, upper]</li>
 *   <li>Kurtosis (β₂) &lt; 3 (platykurtic)</li>
 *   <li>Skewness depends on α and β relationship</li>
 * </ul>
 *
 * <h2>Parameters</h2>
 *
 * <ul>
 *   <li><b>alpha (α)</b>: First shape parameter; α &gt; 0</li>
 *   <li><b>beta (β)</b>: Second shape parameter; β &gt; 0</li>
 *   <li><b>lower</b>: Lower bound of support (default 0)</li>
 *   <li><b>upper</b>: Upper bound of support (default 1)</li>
 * </ul>
 *
 * <h2>Special Cases</h2>
 *
 * <ul>
 *   <li>α = β = 1: Uniform distribution</li>
 *   <li>α = β: Symmetric about (lower + upper) / 2</li>
 *   <li>α &lt; 1, β &lt; 1: U-shaped (bimodal at endpoints)</li>
 *   <li>α &gt; 1, β &gt; 1: Unimodal</li>
 *   <li>α &lt; β: Left-skewed</li>
 *   <li>α &gt; β: Right-skewed</li>
 * </ul>
 *
 * <h2>Moments</h2>
 *
 * <pre>{@code
 * Mean = α / (α + β)
 * Variance = αβ / [(α + β)² (α + β + 1)]
 * Skewness = 2(β - α)√(α + β + 1) / [(α + β + 2)√(αβ)]
 * }</pre>
 *
 * <h2>Usage</h2>
 *
 * <pre>{@code
 * // Standard beta on [0, 1]
 * BetaScalarModel beta = new BetaScalarModel(2.0, 5.0);
 *
 * // Beta on [-1, 1]
 * BetaScalarModel scaled = new BetaScalarModel(2.0, 5.0, -1.0, 1.0);
 *
 * // Symmetric beta (Pearson Type II)
 * BetaScalarModel symmetric = new BetaScalarModel(3.0, 3.0);
 * }</pre>
 *
 * @see ScalarModel
 * @see PearsonType#TYPE_I_BETA
 * @see PearsonType#TYPE_II_SYMMETRIC_BETA
 */
@ModelType(BetaScalarModel.MODEL_TYPE)
public class BetaScalarModel implements ScalarModel {

    public static final String MODEL_TYPE = "beta";

    @SerializedName("alpha")
    private final double alpha;

    @SerializedName("beta")
    private final double beta;

    @SerializedName("lower")
    private final double lower;

    @SerializedName("upper")
    private final double upper;

    /**
     * Constructs a beta scalar model on the standard interval [0, 1].
     *
     * @param alpha the first shape parameter (α); must be positive
     * @param beta the second shape parameter (β); must be positive
     * @throws IllegalArgumentException if alpha or beta is not positive
     */
    public BetaScalarModel(double alpha, double beta) {
        this(alpha, beta, 0.0, 1.0);
    }

    /**
     * Constructs a beta scalar model on a custom interval [lower, upper].
     *
     * <p>The distribution is scaled and shifted from the standard [0, 1] beta.
     *
     * @param alpha the first shape parameter (α); must be positive
     * @param beta the second shape parameter (β); must be positive
     * @param lower the lower bound of the support
     * @param upper the upper bound of the support
     * @throws IllegalArgumentException if alpha ≤ 0, beta ≤ 0, or lower ≥ upper
     */
    public BetaScalarModel(double alpha, double beta, double lower, double upper) {
        if (alpha <= 0) {
            throw new IllegalArgumentException("Alpha must be positive, got: " + alpha);
        }
        if (beta <= 0) {
            throw new IllegalArgumentException("Beta must be positive, got: " + beta);
        }
        if (lower >= upper) {
            throw new IllegalArgumentException("Lower must be less than upper: " + lower + " >= " + upper);
        }
        this.alpha = alpha;
        this.beta = beta;
        this.lower = lower;
        this.upper = upper;
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
     * Returns the lower bound of the support.
     * @return the lower bound
     */
    public double getLower() {
        return lower;
    }

    /**
     * Returns the upper bound of the support.
     * @return the upper bound
     */
    public double getUpper() {
        return upper;
    }

    /**
     * Returns the range (upper - lower) of the support.
     * @return the range
     */
    public double getRange() {
        return upper - lower;
    }

    /**
     * Returns whether this distribution is symmetric (α = β).
     * @return true if symmetric
     */
    public boolean isSymmetric() {
        return Math.abs(alpha - beta) < 1e-10;
    }

    /**
     * Returns whether this Beta distribution is effectively uniform.
     *
     * <p>Beta(1, 1) is mathematically identical to a uniform distribution.
     * This method returns true when both α and β are within a tolerance of 1.0,
     * indicating that the distribution behaves like a uniform distribution
     * and could be replaced with a simpler Uniform model.
     *
     * @return true if α ≈ 1 and β ≈ 1 (within 0.1 tolerance)
     */
    public boolean isEffectivelyUniform() {
        return Math.abs(alpha - 1.0) < 0.1 && Math.abs(beta - 1.0) < 0.1;
    }

    /**
     * Returns the Pearson type for this beta distribution.
     * @return TYPE_II_SYMMETRIC_BETA if α = β, otherwise TYPE_I_BETA
     */
    public PearsonType getPearsonType() {
        return isSymmetric() ? PearsonType.TYPE_II_SYMMETRIC_BETA : PearsonType.TYPE_I_BETA;
    }

    /**
     * Computes the mean of this beta distribution.
     * <p>For standard beta [0,1]: E[X] = α / (α + β)
     * <p>Scaled: E[X] = lower + range * α / (α + β)
     *
     * @return the mean
     */
    public double getMean() {
        double standardMean = alpha / (alpha + beta);
        return lower + getRange() * standardMean;
    }

    /**
     * Computes the variance of this beta distribution.
     * <p>For standard beta [0,1]: Var[X] = αβ / [(α + β)² (α + β + 1)]
     *
     * @return the variance
     */
    public double getVariance() {
        double sumAB = alpha + beta;
        double standardVariance = (alpha * beta) / (sumAB * sumAB * (sumAB + 1));
        double range = getRange();
        return standardVariance * range * range;
    }

    /**
     * Computes the standard deviation of this beta distribution.
     * @return the standard deviation
     */
    public double getStdDev() {
        return Math.sqrt(getVariance());
    }

    /**
     * Computes the skewness of this beta distribution.
     * <p>Skewness = 2(β - α)√(α + β + 1) / [(α + β + 2)√(αβ)]
     *
     * @return the skewness
     */
    public double getSkewness() {
        double sumAB = alpha + beta;
        return 2.0 * (beta - alpha) * Math.sqrt(sumAB + 1) /
               ((sumAB + 2) * Math.sqrt(alpha * beta));
    }

    /**
     * Computes the kurtosis of this beta distribution (standard, not excess).
     * <p>For beta: β₂ = 3 * (α + β + 1) * [2(α + β)² + αβ(α + β - 6)] / [αβ(α + β + 2)(α + β + 3)]
     *
     * @return the kurtosis (standard)
     */
    public double getKurtosis() {
        double sumAB = alpha + beta;
        double numerator = 3.0 * (sumAB + 1) * (2 * sumAB * sumAB + alpha * beta * (sumAB - 6));
        double denominator = alpha * beta * (sumAB + 2) * (sumAB + 3);
        return numerator / denominator;
    }

    /**
     * Computes the mode of this beta distribution.
     * <p>Mode = (α - 1) / (α + β - 2) for α, β &gt; 1
     *
     * @return the mode, or NaN if α ≤ 1 or β ≤ 1 (undefined or at boundary)
     */
    public double getMode() {
        if (alpha <= 1 || beta <= 1) {
            return Double.NaN;  // Mode at boundary or undefined
        }
        double standardMode = (alpha - 1) / (alpha + beta - 2);
        return lower + getRange() * standardMode;
    }

    /**
     * Computes the cumulative distribution function (CDF) at a given value.
     *
     * <p>The CDF is computed using the regularized incomplete beta function I_x(α, β).
     *
     * @param x the value at which to evaluate the CDF
     * @return the cumulative probability P(X ≤ x), in range [0, 1]
     */
    @Override
    public double cdf(double x) {
        if (x <= lower) return 0.0;
        if (x >= upper) return 1.0;

        // Standardize to [0, 1]
        double t = (x - lower) / (upper - lower);
        return regularizedIncompleteBeta(t, alpha, beta);
    }

    /**
     * Computes the regularized incomplete beta function I_x(a, b).
     */
    private static double regularizedIncompleteBeta(double x, double a, double b) {
        if (x <= 0) return 0;
        if (x >= 1) return 1;

        // Use continued fraction approximation
        double bt = Math.exp(logGamma(a + b) - logGamma(a) - logGamma(b) +
                             a * Math.log(x) + b * Math.log(1 - x));

        if (x < (a + 1) / (a + b + 2)) {
            return bt * betaContinuedFraction(x, a, b) / a;
        } else {
            return 1.0 - bt * betaContinuedFraction(1 - x, b, a) / b;
        }
    }

    private static double betaContinuedFraction(double x, double a, double b) {
        double qab = a + b;
        double qap = a + 1;
        double qam = a - 1;
        double c = 1;
        double d = 1 - qab * x / qap;
        if (Math.abs(d) < 1e-30) d = 1e-30;
        d = 1 / d;
        double h = d;

        for (int m = 1; m <= 100; m++) {
            int m2 = 2 * m;
            double aa = m * (b - m) * x / ((qam + m2) * (a + m2));
            d = 1 + aa * d;
            if (Math.abs(d) < 1e-30) d = 1e-30;
            c = 1 + aa / c;
            if (Math.abs(c) < 1e-30) c = 1e-30;
            d = 1 / d;
            h *= d * c;

            aa = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2));
            d = 1 + aa * d;
            if (Math.abs(d) < 1e-30) d = 1e-30;
            c = 1 + aa / c;
            if (Math.abs(c) < 1e-30) c = 1e-30;
            d = 1 / d;
            double del = d * c;
            h *= del;
            if (Math.abs(del - 1) < 1e-10) break;
        }
        return h;
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

    /**
     * Creates a standard uniform distribution (Beta(1, 1) on [0, 1]).
     * @return uniform distribution model
     */
    public static BetaScalarModel uniform() {
        return new BetaScalarModel(1.0, 1.0);
    }

    /**
     * Creates a uniform distribution on a custom interval.
     * @param lower the lower bound
     * @param upper the upper bound
     * @return uniform distribution model
     */
    public static BetaScalarModel uniform(double lower, double upper) {
        return new BetaScalarModel(1.0, 1.0, lower, upper);
    }

    /**
     * Creates a symmetric beta distribution.
     * @param shape the common shape parameter (α = β)
     * @return symmetric beta model
     */
    public static BetaScalarModel symmetric(double shape) {
        return new BetaScalarModel(shape, shape);
    }

    /**
     * Creates a symmetric beta distribution on a custom interval.
     * @param shape the common shape parameter (α = β)
     * @param lower the lower bound
     * @param upper the upper bound
     * @return symmetric beta model
     */
    public static BetaScalarModel symmetric(double shape, double lower, double upper) {
        return new BetaScalarModel(shape, shape, lower, upper);
    }

    /**
     * Creates an array of identical beta scalar models.
     *
     * @param alpha the first shape parameter
     * @param beta the second shape parameter
     * @param dimensions the number of models
     * @return an array of identical beta models
     */
    public static BetaScalarModel[] uniformScalar(double alpha, double beta, int dimensions) {
        BetaScalarModel[] models = new BetaScalarModel[dimensions];
        BetaScalarModel model = new BetaScalarModel(alpha, beta);
        for (int i = 0; i < dimensions; i++) {
            models[i] = model;
        }
        return models;
    }

    /**
     * Creates an array of identical beta scalar models on a custom interval.
     *
     * @param alpha the first shape parameter
     * @param beta the second shape parameter
     * @param lower the lower bound
     * @param upper the upper bound
     * @param dimensions the number of models
     * @return an array of identical beta models
     */
    public static BetaScalarModel[] uniformScalar(double alpha, double beta,
                                                   double lower, double upper, int dimensions) {
        BetaScalarModel[] models = new BetaScalarModel[dimensions];
        BetaScalarModel model = new BetaScalarModel(alpha, beta, lower, upper);
        for (int i = 0; i < dimensions; i++) {
            models[i] = model;
        }
        return models;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BetaScalarModel)) return false;
        BetaScalarModel that = (BetaScalarModel) o;
        return Double.compare(that.alpha, alpha) == 0 &&
               Double.compare(that.beta, beta) == 0 &&
               Double.compare(that.lower, lower) == 0 &&
               Double.compare(that.upper, upper) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(alpha, beta, lower, upper);
    }

    @Override
    public String toString() {
        if (lower == 0.0 && upper == 1.0) {
            return "BetaScalarModel[α=" + alpha + ", β=" + beta + "]";
        } else {
            return "BetaScalarModel[α=" + alpha + ", β=" + beta +
                   ", support=[" + lower + ", " + upper + "]]";
        }
    }
}
