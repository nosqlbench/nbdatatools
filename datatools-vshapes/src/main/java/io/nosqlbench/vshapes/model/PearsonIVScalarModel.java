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
 * Pearson Type IV distribution scalar model.
 *
 * <h2>Purpose</h2>
 *
 * <p>The Pearson Type IV distribution is a flexible continuous distribution that can model:
 * <ul>
 *   <li>Asymmetric distributions with unbounded support</li>
 *   <li>Data that doesn't fit standard parametric families</li>
 *   <li>Skewed data with either light or heavy tails</li>
 * </ul>
 *
 * <h2>Pearson Type</h2>
 *
 * <p>Pearson Type IV is characterized by:
 * <ul>
 *   <li>Unbounded support: (-∞, +∞)</li>
 *   <li>Asymmetric: skewness ≠ 0</li>
 *   <li>Pearson criterion: 0 &lt; κ &lt; 1</li>
 * </ul>
 *
 * <h2>Parameters</h2>
 *
 * <ul>
 *   <li><b>m</b>: Shape parameter; m &gt; 1/2. Controls tail heaviness.</li>
 *   <li><b>nu (ν)</b>: Skewness parameter. ν = 0 gives symmetric distribution.</li>
 *   <li><b>a</b>: Scale parameter; a &gt; 0</li>
 *   <li><b>lambda (λ)</b>: Location parameter</li>
 * </ul>
 *
 * <h2>PDF</h2>
 *
 * <pre>{@code
 * f(x) = k * [1 + ((x - λ)/a)²]^(-m) * exp(-ν * arctan((x - λ)/a))
 * }</pre>
 *
 * <p>where k is the normalization constant.
 *
 * <h2>Moments</h2>
 *
 * <pre>{@code
 * Mean = λ + aν / (2m - 2) (for m > 1)
 * }</pre>
 *
 * <h2>Special Cases</h2>
 *
 * <ul>
 *   <li>ν = 0: Symmetric Pearson VII (Student's t-like)</li>
 *   <li>As m → ∞ with a = σ√(2m-1): converges to normal</li>
 * </ul>
 *
 * <h2>Usage</h2>
 *
 * <pre>{@code
 * // Pearson IV with skewness
 * PearsonIVScalarModel p4 = new PearsonIVScalarModel(2.0, 1.0, 1.0, 0.0);
 *
 * // From moments (skewness and kurtosis)
 * PearsonIVScalarModel fitted = PearsonIVScalarModel.fromMoments(0.0, 1.0, 0.5, 4.5);
 * }</pre>
 *
 * @see ScalarModel
 * @see PearsonType#TYPE_IV
 */
@ModelType(PearsonIVScalarModel.MODEL_TYPE)
public class PearsonIVScalarModel implements ScalarModel {

    public static final String MODEL_TYPE = "pearson_iv";

    @SerializedName("m")
    private final double m;       // Shape (controls kurtosis)

    @SerializedName("nu")
    private final double nu;      // Skewness parameter (ν)

    @SerializedName("a")
    private final double a;       // Scale

    @SerializedName("lambda")
    private final double lambda;  // Location (λ)

    /**
     * Constructs a Pearson Type IV scalar model.
     *
     * @param m the shape parameter; must be &gt; 0.5
     * @param nu the skewness parameter (ν); can be any real value
     * @param a the scale parameter; must be positive
     * @param lambda the location parameter (λ)
     * @throws IllegalArgumentException if m ≤ 0.5 or a ≤ 0
     */
    public PearsonIVScalarModel(double m, double nu, double a, double lambda) {
        if (m <= 0.5) {
            throw new IllegalArgumentException("Shape m must be > 0.5, got: " + m);
        }
        if (a <= 0) {
            throw new IllegalArgumentException("Scale a must be positive, got: " + a);
        }
        this.m = m;
        this.nu = nu;
        this.a = a;
        this.lambda = lambda;
    }

    @Override
    public String getModelType() {
        return MODEL_TYPE;
    }

    /**
     * Returns the shape parameter m.
     * @return the shape parameter
     */
    public double getM() {
        return m;
    }

    /**
     * Returns the skewness parameter ν.
     * @return nu
     */
    public double getNu() {
        return nu;
    }

    /**
     * Returns the scale parameter a.
     * @return the scale
     */
    public double getA() {
        return a;
    }

    /**
     * Returns the location parameter λ.
     * @return lambda
     */
    public double getLambda() {
        return lambda;
    }

    /**
     * Computes the mean of this Pearson IV distribution.
     * <p>Mean = λ + aν / (2m - 2) for m &gt; 1
     *
     * @return the mean, or NaN if m ≤ 1
     */
    public double getMean() {
        if (m <= 1) {
            return Double.NaN;
        }
        return lambda + a * nu / (2 * m - 2);
    }

    /**
     * Returns whether this distribution is symmetric (ν = 0).
     * @return true if symmetric
     */
    public boolean isSymmetric() {
        return Math.abs(nu) < 1e-10;
    }

    /**
     * Returns whether this distribution has a defined mean (m &gt; 1).
     * @return true if mean exists
     */
    public boolean hasMean() {
        return m > 1;
    }

    /**
     * Creates a Pearson IV distribution from sample moments.
     *
     * <p>This method estimates the Pearson IV parameters from the first four
     * standardized moments (mean, standard deviation, skewness, kurtosis).
     *
     * <p>The parameter estimation uses the Pearson criterion and moment matching.
     *
     * @param mean the sample mean
     * @param stdDev the sample standard deviation
     * @param skewness the sample skewness
     * @param kurtosis the sample kurtosis (standard, not excess)
     * @return Pearson IV model matching the moments
     * @throws IllegalArgumentException if moments don't correspond to Type IV
     */
    public static PearsonIVScalarModel fromMoments(double mean, double stdDev,
                                                    double skewness, double kurtosis) {
        if (stdDev <= 0) {
            throw new IllegalArgumentException("Standard deviation must be positive");
        }

        double beta1 = skewness * skewness;  // β₁
        double beta2 = kurtosis;              // β₂

        // Check if this corresponds to Type IV (0 < κ < 1)
        double denom1 = 2 * beta2 - 3 * beta1 - 6;
        double denom2 = 4 * beta2 - 3 * beta1;

        if (Math.abs(denom1 * denom2) < 1e-10) {
            throw new IllegalArgumentException("Moments do not correspond to Pearson Type IV");
        }

        double kappa = beta1 * Math.pow(beta2 + 3, 2) / (4 * denom1 * denom2);

        if (kappa <= 0 || kappa >= 1) {
            throw new IllegalArgumentException(
                "Moments give κ = " + kappa + ", which is outside (0, 1) for Type IV");
        }

        // Compute Pearson IV parameters from moments
        // r = 6 * (β₂ - β₁ - 1) / (2β₂ - 3β₁ - 6)
        double r = 6 * (beta2 - beta1 - 1) / denom1;

        // m = r / 2
        double m = r / 2;

        // ν can be computed from skewness
        // For simplicity, we use an approximation based on sign of skewness
        double nu = -skewness * Math.sqrt(m);

        // Scale parameter from variance relationship
        double a = stdDev * Math.sqrt((2 * m - 2) * (2 * m - 2) /
                                       ((2 * m - 1) * (2 * m - 2) - nu * nu));

        // Location from mean
        double lambda = mean - a * nu / (2 * m - 2);

        return new PearsonIVScalarModel(m, nu, a, lambda);
    }

    /**
     * Creates a symmetric Pearson IV (equivalent to Pearson VII / scaled t).
     *
     * @param m the shape parameter; controls kurtosis
     * @param a the scale parameter
     * @param lambda the location parameter
     * @return symmetric Pearson IV model
     */
    public static PearsonIVScalarModel symmetric(double m, double a, double lambda) {
        return new PearsonIVScalarModel(m, 0.0, a, lambda);
    }

    /**
     * Creates a standard Pearson IV centered at 0 with unit scale.
     *
     * @param m the shape parameter
     * @param nu the skewness parameter
     * @return standard Pearson IV model
     */
    public static PearsonIVScalarModel standard(double m, double nu) {
        return new PearsonIVScalarModel(m, nu, 1.0, 0.0);
    }

    /**
     * Creates an array of identical Pearson IV scalar models.
     *
     * @param m the shape parameter
     * @param nu the skewness parameter
     * @param a the scale parameter
     * @param lambda the location parameter
     * @param dimensions the number of models
     * @return an array of identical Pearson IV models
     */
    public static PearsonIVScalarModel[] uniformScalar(double m, double nu, double a,
                                                        double lambda, int dimensions) {
        PearsonIVScalarModel[] models = new PearsonIVScalarModel[dimensions];
        PearsonIVScalarModel model = new PearsonIVScalarModel(m, nu, a, lambda);
        for (int i = 0; i < dimensions; i++) {
            models[i] = model;
        }
        return models;
    }

    /**
     * Computes the cumulative distribution function (CDF) at a given value.
     *
     * <p>The Pearson IV CDF is computed via numerical integration of the PDF.
     *
     * @param x the value at which to evaluate the CDF
     * @return the cumulative probability P(X ≤ x), in range [0, 1]
     */
    @Override
    public double cdf(double x) {
        // Numerical integration using adaptive Simpson's rule
        // Integrate from -inf to x, but use practical bounds
        double lowerBound = lambda - 10 * a;  // Practical lower bound
        double upperBound = x;

        if (upperBound <= lowerBound) return 0.0;

        // Use simple trapezoidal integration
        int steps = 1000;
        double h = (upperBound - lowerBound) / steps;
        double sum = 0.5 * (pdf(lowerBound) + pdf(upperBound));
        for (int i = 1; i < steps; i++) {
            sum += pdf(lowerBound + i * h);
        }
        double result = sum * h;

        // Clamp to [0, 1]
        return Math.max(0, Math.min(1, result));
    }

    /**
     * Computes the probability density function (PDF) at a given value.
     */
    private double pdf(double x) {
        double z = (x - lambda) / a;
        double base = 1 + z * z;
        double exponent = -m;
        double expTerm = -nu * Math.atan(z);

        // Compute normalization constant (approximate)
        double k = 1.0 / (a * Math.sqrt(Math.PI) * gammaRatio(m, 0.5));

        return k * Math.pow(base, exponent) * Math.exp(expTerm);
    }

    private static double gammaRatio(double a, double b) {
        // Approximate Gamma(a) / Gamma(a - b) using Stirling
        return Math.exp(logGamma(a) - logGamma(a - b));
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
        if (!(o instanceof PearsonIVScalarModel)) return false;
        PearsonIVScalarModel that = (PearsonIVScalarModel) o;
        return Double.compare(that.m, m) == 0 &&
               Double.compare(that.nu, nu) == 0 &&
               Double.compare(that.a, a) == 0 &&
               Double.compare(that.lambda, lambda) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(m, nu, a, lambda);
    }

    @Override
    public String toString() {
        return "PearsonIVScalarModel[m=" + m + ", ν=" + nu +
               ", a=" + a + ", λ=" + lambda + "]";
    }
}
