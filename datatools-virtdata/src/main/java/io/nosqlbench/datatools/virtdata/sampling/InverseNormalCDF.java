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

package io.nosqlbench.datatools.virtdata.sampling;

import io.nosqlbench.vshapes.model.NormalScalarModel;

/**
 * Inverse Normal CDF (quantile function) using Abramowitz and Stegun approximation.
 *
 * <h2>Purpose</h2>
 *
 * <p>This class implements the inverse CDF for normal distributions, transforming
 * a uniform random value in (0, 1) into a normally-distributed value. This is the
 * classic "inverse transform sampling" technique.
 *
 * <h2>The Inverse Transform Method</h2>
 *
 * <pre>{@code
 *                    INVERSE CDF TRANSFORM
 *
 *   Uniform input u ∈ (0,1)              Normal output x
 *         │                                    │
 *         │  u=0.95 ─────────────────────►     │ x=1.645
 *         │                                    │
 *         │  u=0.50 ────────────────►          │ x=0.000 (median)
 *         │                                    │
 *         │  u=0.05 ──────►                    │ x=-1.645
 *         │                                    │
 *      0 ─┼─ 1                            -∞ ──┼── +∞
 *
 *   The inverse CDF Φ⁻¹(u) maps uniform [0,1] to Normal (-∞,+∞)
 * }</pre>
 *
 * <h2>Algorithm</h2>
 *
 * <p>Uses the Abramowitz and Stegun rational approximation (formula 26.2.23).
 *
 * <h2>Accuracy</h2>
 *
 * <p>The approximation provides accuracy to approximately 4.5 × 10⁻⁴ relative error
 * for the standard normal distribution. This is sufficient for most sampling
 * applications where the statistical properties matter more than exact precision.
 *
 * @see TruncatedNormalSampler
 */
public final class InverseNormalCDF {

    // Coefficients for rational approximation (Abramowitz and Stegun 26.2.23)
    private static final double C0 = 2.515517;
    private static final double C1 = 0.802853;
    private static final double C2 = 0.010328;
    private static final double D1 = 1.432788;
    private static final double D2 = 0.189269;
    private static final double D3 = 0.001308;

    private InverseNormalCDF() {
        // Utility class, no instantiation
    }

    /**
     * Computes the inverse CDF (quantile function) of the standard normal distribution.
     *
     * <p>Given a probability p in (0, 1), returns the value x such that P(X ≤ x) = p
     * where X follows a standard normal distribution N(0, 1).
     *
     * <pre>{@code
     * Standard quantiles:
     *   p=0.001 → x≈-3.09   (0.1th percentile)
     *   p=0.025 → x≈-1.96   (2.5th percentile, 95% CI lower)
     *   p=0.50  → x=0.00    (median)
     *   p=0.975 → x≈+1.96   (97.5th percentile, 95% CI upper)
     *   p=0.999 → x≈+3.09   (99.9th percentile)
     * }</pre>
     *
     * @param p the probability value in the open interval (0, 1)
     * @return the quantile value for the standard normal distribution
     * @throws IllegalArgumentException if p is not in (0, 1)
     */
    public static double standardNormalQuantile(double p) {
        if (p <= 0.0 || p >= 1.0) {
            throw new IllegalArgumentException("Probability must be in (0, 1), got: " + p);
        }

        // Use symmetry to handle both tails
        if (p < 0.5) {
            return -rationalApproximation(Math.sqrt(-2.0 * Math.log(p)));
        } else {
            return rationalApproximation(Math.sqrt(-2.0 * Math.log(1.0 - p)));
        }
    }

    /**
     * Computes the inverse CDF for a normal distribution with specified mean and stdDev.
     *
     * <p>This is the main method used in the vector generation pipeline.
     * Given a probability p in (0, 1), returns the value x such that P(X ≤ x) = p
     * where X follows N(mean, stdDev²).
     *
     * @param p the probability value in the open interval (0, 1)
     * @param mean the mean (μ) of the normal distribution
     * @param stdDev the standard deviation (σ) of the normal distribution
     * @return the quantile value for the specified normal distribution
     */
    public static double quantile(double p, double mean, double stdDev) {
        return mean + stdDev * standardNormalQuantile(p);
    }

    /**
     * Computes the inverse CDF for a normal distribution with specified model.
     *
     * @param p the probability value in the open interval (0, 1)
     * @param model the normal component model specifying mean and stdDev
     * @return the quantile value for the specified normal distribution
     */
    public static double quantile(double p, NormalScalarModel model) {
        return model.getMean() + model.getStdDev() * standardNormalQuantile(p);
    }

    /**
     * Rational approximation function (Abramowitz and Stegun formula 26.2.23).
     */
    private static double rationalApproximation(double t) {
        return t - (C0 + C1 * t + C2 * t * t) / (1.0 + D1 * t + D2 * t * t + D3 * t * t * t);
    }
}
