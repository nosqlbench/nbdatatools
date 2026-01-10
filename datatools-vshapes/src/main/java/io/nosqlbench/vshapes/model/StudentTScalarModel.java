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
 * Student's t-distribution scalar model - Pearson Type VII.
 *
 * <h2>Purpose</h2>
 *
 * <p>Models symmetric, heavy-tailed data commonly arising in:
 * <ul>
 *   <li>Small sample statistics (confidence intervals, t-tests)</li>
 *   <li>Financial returns (fat tails)</li>
 *   <li>Natural phenomena with occasional extreme values</li>
 *   <li>Robust regression and outlier-tolerant estimation</li>
 * </ul>
 *
 * <h2>Pearson Type</h2>
 *
 * <p>The Student's t-distribution is Pearson Type VII, characterized by:
 * <ul>
 *   <li>Symmetric: skewness (β₁) = 0</li>
 *   <li>Leptokurtic: kurtosis (β₂) &gt; 3</li>
 *   <li>Unbounded support: (-∞, +∞)</li>
 *   <li>Heavier tails than normal distribution</li>
 * </ul>
 *
 * <h2>Parameters</h2>
 *
 * <ul>
 *   <li><b>degrees of freedom (ν)</b>: Controls tail heaviness; ν &gt; 0</li>
 *   <li><b>location (μ)</b>: Center of the distribution; default 0</li>
 *   <li><b>scale (σ)</b>: Spread parameter; default 1</li>
 * </ul>
 *
 * <h2>Degrees of Freedom Effects</h2>
 *
 * <ul>
 *   <li>ν = 1: Cauchy distribution (no mean or variance)</li>
 *   <li>ν = 2: Mean exists, but infinite variance</li>
 *   <li>ν &gt; 2: Finite variance</li>
 *   <li>ν &gt; 4: Finite kurtosis</li>
 *   <li>ν → ∞: Converges to normal distribution</li>
 * </ul>
 *
 * <h2>Moments (when defined)</h2>
 *
 * <pre>{@code
 * Mean = μ (for ν > 1)
 * Variance = σ² * ν/(ν-2) (for ν > 2)
 * Skewness = 0 (for ν > 3)
 * Excess Kurtosis = 6/(ν-4) (for ν > 4)
 * }</pre>
 *
 * <h2>Usage</h2>
 *
 * <pre>{@code
 * // Standard t with 5 degrees of freedom
 * StudentTScalarModel t5 = new StudentTScalarModel(5);
 *
 * // Location-scale t-distribution
 * StudentTScalarModel scaled = new StudentTScalarModel(10, 5.0, 2.0);
 *
 * // Cauchy distribution (t with ν=1)
 * StudentTScalarModel cauchy = StudentTScalarModel.cauchy();
 *
 * // Heavy-tailed model
 * StudentTScalarModel heavyTailed = StudentTScalarModel.withExcessKurtosis(3.0);
 * }</pre>
 *
 * @see ScalarModel
 * @see PearsonType#TYPE_VII_STUDENT_T
 */
@ModelType(StudentTScalarModel.MODEL_TYPE)
public class StudentTScalarModel implements ScalarModel {

    public static final String MODEL_TYPE = "student_t";

    @SerializedName("nu")
    private final double degreesOfFreedom;  // ν

    @SerializedName("mu")
    private final double location;          // μ

    @SerializedName("sigma")
    private final double scale;             // σ

    /**
     * Constructs a standard Student's t-distribution (μ=0, σ=1).
     *
     * @param degreesOfFreedom the degrees of freedom (ν); must be positive
     * @throws IllegalArgumentException if degreesOfFreedom is not positive
     */
    public StudentTScalarModel(double degreesOfFreedom) {
        this(degreesOfFreedom, 0.0, 1.0);
    }

    /**
     * Constructs a location-scale Student's t-distribution.
     *
     * @param degreesOfFreedom the degrees of freedom (ν); must be positive
     * @param location the location parameter (μ)
     * @param scale the scale parameter (σ); must be positive
     * @throws IllegalArgumentException if degreesOfFreedom or scale is not positive
     */
    public StudentTScalarModel(double degreesOfFreedom, double location, double scale) {
        if (degreesOfFreedom <= 0) {
            throw new IllegalArgumentException("Degrees of freedom must be positive, got: " + degreesOfFreedom);
        }
        if (scale <= 0) {
            throw new IllegalArgumentException("Scale must be positive, got: " + scale);
        }
        this.degreesOfFreedom = degreesOfFreedom;
        this.location = location;
        this.scale = scale;
    }

    @Override
    public String getModelType() {
        return MODEL_TYPE;
    }

    /**
     * Returns the degrees of freedom (ν).
     * @return the degrees of freedom
     */
    public double getDegreesOfFreedom() {
        return degreesOfFreedom;
    }

    /**
     * Returns the location parameter (μ).
     * @return the location
     */
    public double getLocation() {
        return location;
    }

    /**
     * Returns the scale parameter (σ).
     * @return the scale
     */
    public double getScale() {
        return scale;
    }

    /**
     * Computes the mean of this t-distribution.
     * <p>Mean = μ for ν &gt; 1, undefined otherwise.
     *
     * @return the mean, or NaN if ν ≤ 1
     */
    public double getMean() {
        if (degreesOfFreedom <= 1) {
            return Double.NaN;
        }
        return location;
    }

    /**
     * Computes the variance of this t-distribution.
     * <p>Variance = σ² * ν/(ν-2) for ν &gt; 2
     *
     * @return the variance, or +∞ if 1 &lt; ν ≤ 2, or NaN if ν ≤ 1
     */
    public double getVariance() {
        if (degreesOfFreedom <= 1) {
            return Double.NaN;
        } else if (degreesOfFreedom <= 2) {
            return Double.POSITIVE_INFINITY;
        }
        return scale * scale * degreesOfFreedom / (degreesOfFreedom - 2);
    }

    /**
     * Computes the standard deviation of this t-distribution.
     * @return the standard deviation
     */
    public double getStdDev() {
        return Math.sqrt(getVariance());
    }

    /**
     * Computes the skewness of this t-distribution.
     * <p>Skewness = 0 for ν &gt; 3 (symmetric distribution).
     *
     * @return 0 if ν &gt; 3, NaN otherwise
     */
    public double getSkewness() {
        if (degreesOfFreedom <= 3) {
            return Double.NaN;
        }
        return 0.0;  // Symmetric
    }

    /**
     * Computes the excess kurtosis of this t-distribution.
     * <p>Excess kurtosis = 6/(ν-4) for ν &gt; 4
     *
     * @return the excess kurtosis, or +∞ if 2 &lt; ν ≤ 4, or NaN if ν ≤ 2
     */
    public double getExcessKurtosis() {
        if (degreesOfFreedom <= 2) {
            return Double.NaN;
        } else if (degreesOfFreedom <= 4) {
            return Double.POSITIVE_INFINITY;
        }
        return 6.0 / (degreesOfFreedom - 4);
    }

    /**
     * Computes the standard kurtosis (β₂) of this t-distribution.
     * <p>Kurtosis = 3 + 6/(ν-4) for ν &gt; 4
     *
     * @return the standard kurtosis
     */
    public double getKurtosis() {
        double excessKurt = getExcessKurtosis();
        if (Double.isNaN(excessKurt) || Double.isInfinite(excessKurt)) {
            return excessKurt;
        }
        return 3.0 + excessKurt;
    }

    /**
     * Returns whether this is a Cauchy distribution (ν = 1).
     * @return true if degrees of freedom = 1
     */
    public boolean isCauchy() {
        return Math.abs(degreesOfFreedom - 1.0) < 1e-10;
    }

    /**
     * Returns whether this distribution has a defined mean (ν &gt; 1).
     * @return true if mean exists
     */
    public boolean hasMean() {
        return degreesOfFreedom > 1;
    }

    /**
     * Returns whether this distribution has a finite variance (ν &gt; 2).
     * @return true if variance is finite
     */
    public boolean hasFiniteVariance() {
        return degreesOfFreedom > 2;
    }

    /**
     * Returns whether this distribution has a finite kurtosis (ν &gt; 4).
     * @return true if kurtosis is finite
     */
    public boolean hasFiniteKurtosis() {
        return degreesOfFreedom > 4;
    }

    /**
     * Creates a standard Cauchy distribution (t with ν = 1).
     * @return Cauchy distribution model
     */
    public static StudentTScalarModel cauchy() {
        return new StudentTScalarModel(1.0);
    }

    /**
     * Creates a Cauchy distribution with location and scale.
     * @param location the location parameter
     * @param scale the scale parameter
     * @return Cauchy distribution model
     */
    public static StudentTScalarModel cauchy(double location, double scale) {
        return new StudentTScalarModel(1.0, location, scale);
    }

    /**
     * Creates a t-distribution with a target excess kurtosis.
     *
     * <p>Solves 6/(ν-4) = excessKurtosis for ν.
     *
     * @param excessKurtosis the target excess kurtosis; must be positive
     * @return t-distribution with specified kurtosis
     * @throws IllegalArgumentException if excessKurtosis ≤ 0
     */
    public static StudentTScalarModel withExcessKurtosis(double excessKurtosis) {
        if (excessKurtosis <= 0) {
            throw new IllegalArgumentException(
                "Excess kurtosis must be positive for t-distribution, got: " + excessKurtosis);
        }
        double degreesOfFreedom = 4 + 6.0 / excessKurtosis;
        return new StudentTScalarModel(degreesOfFreedom);
    }

    /**
     * Estimates degrees of freedom from observed excess kurtosis.
     *
     * @param excessKurtosis observed excess kurtosis
     * @return estimated degrees of freedom, or NaN if not consistent with t-distribution
     */
    public static double estimateDegreesOfFreedom(double excessKurtosis) {
        if (excessKurtosis <= 0) {
            return Double.NaN;  // t-distribution always has excess kurtosis > 0
        }
        double df = 4 + 6.0 / excessKurtosis;
        if (df <= 4) {
            return Double.NaN;  // Would give infinite kurtosis
        }
        return df;
    }

    /**
     * Creates an array of identical Student's t scalar models.
     *
     * @param degreesOfFreedom the degrees of freedom
     * @param dimensions the number of models
     * @return an array of identical t-distribution models
     */
    public static StudentTScalarModel[] uniformScalar(double degreesOfFreedom, int dimensions) {
        StudentTScalarModel[] models = new StudentTScalarModel[dimensions];
        StudentTScalarModel model = new StudentTScalarModel(degreesOfFreedom);
        for (int i = 0; i < dimensions; i++) {
            models[i] = model;
        }
        return models;
    }

    /**
     * Creates an array of identical location-scale t scalar models.
     *
     * @param degreesOfFreedom the degrees of freedom
     * @param location the location parameter
     * @param scale the scale parameter
     * @param dimensions the number of models
     * @return an array of identical t-distribution models
     */
    public static StudentTScalarModel[] uniformScalar(double degreesOfFreedom,
                                                       double location, double scale, int dimensions) {
        StudentTScalarModel[] models = new StudentTScalarModel[dimensions];
        StudentTScalarModel model = new StudentTScalarModel(degreesOfFreedom, location, scale);
        for (int i = 0; i < dimensions; i++) {
            models[i] = model;
        }
        return models;
    }

    /**
     * Computes the cumulative distribution function (CDF) at a given value.
     *
     * <p>The CDF is computed using the relationship to the incomplete beta function.
     *
     * @param x the value at which to evaluate the CDF
     * @return the cumulative probability P(X ≤ x), in range [0, 1]
     */
    @Override
    public double cdf(double x) {
        double t = (x - location) / scale;
        double df = degreesOfFreedom;
        double z = df / (df + t * t);
        double beta = regularizedIncompleteBeta(z, df / 2, 0.5);

        if (t > 0) {
            return 1.0 - beta / 2;
        } else {
            return beta / 2;
        }
    }

    private static double regularizedIncompleteBeta(double x, double a, double b) {
        if (x <= 0) return 0;
        if (x >= 1) return 1;

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof StudentTScalarModel)) return false;
        StudentTScalarModel that = (StudentTScalarModel) o;
        return Double.compare(that.degreesOfFreedom, degreesOfFreedom) == 0 &&
               Double.compare(that.location, location) == 0 &&
               Double.compare(that.scale, scale) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(degreesOfFreedom, location, scale);
    }

    @Override
    public String toString() {
        if (location == 0.0 && scale == 1.0) {
            return "StudentTScalarModel[ν=" + degreesOfFreedom + "]";
        } else {
            return "StudentTScalarModel[ν=" + degreesOfFreedom +
                   ", μ=" + location + ", σ=" + scale + "]";
        }
    }
}
