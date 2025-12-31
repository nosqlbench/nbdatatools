package io.nosqlbench.datatools.virtdata;

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

/**
 * Inverse Gaussian CDF (quantile function) using Abramowitz and Stegun approximation.
 *
 * <h2>Purpose</h2>
 *
 * <p>This class implements Stage 3 of the vector generation pipeline: transforming
 * a uniform random value in (0, 1) into a Gaussian-distributed value. This is the
 * classic "inverse transform sampling" technique.
 *
 * <h2>The Inverse Transform Method</h2>
 *
 * <pre>{@code
 *                    INVERSE CDF TRANSFORM
 *
 *   Uniform input u ∈ (0,1)              Gaussian output x
 *         │                                    │
 *         │  u=0.95 ─────────────────────►     │ x=1.645
 *         │                                    │
 *         │  u=0.50 ────────────────►          │ x=0.000 (median)
 *         │                                    │
 *         │  u=0.05 ──────►                    │ x=-1.645
 *         │                                    │
 *      0 ─┼─ 1                            -∞ ──┼── +∞
 *
 *   The inverse CDF Φ⁻¹(u) maps uniform [0,1] to Gaussian (-∞,+∞)
 * }</pre>
 *
 * <h2>Algorithm</h2>
 *
 * <p>Uses the Abramowitz and Stegun rational approximation (formula 26.2.23):
 *
 * <pre>{@code
 * ┌────────────────────────────────────────────────────────────────────┐
 * │                 INVERSE GAUSSIAN CDF ALGORITHM                     │
 * └────────────────────────────────────────────────────────────────────┘
 *
 *   INPUT: p ∈ (0, 1), mean μ, stdDev σ
 *
 *   ┌──────────────────────────────────────────────────────────────────┐
 *   │ STEP 1: DETERMINE TAIL                                          │
 *   │   if p < 0.5:  lower tail, use p                                │
 *   │   else:        upper tail, use (1-p)                            │
 *   └────────────────────────────────────────────────────────┬─────────┘
 *                                                            │
 *                                                            ▼
 *   ┌──────────────────────────────────────────────────────────────────┐
 *   │ STEP 2: COMPUTE INTERMEDIATE t                                  │
 *   │   t = √(-2 · ln(p_tail))                                        │
 *   │                                                                 │
 *   │   where p_tail = p (lower) or 1-p (upper)                       │
 *   └────────────────────────────────────────────────────────┬─────────┘
 *                                                            │
 *                                                            ▼
 *   ┌──────────────────────────────────────────────────────────────────┐
 *   │ STEP 3: RATIONAL APPROXIMATION                                  │
 *   │                                                                 │
 *   │         c₀ + c₁·t + c₂·t²                                       │
 *   │   x ≈ t - ─────────────────────                                 │
 *   │         1 + d₁·t + d₂·t² + d₃·t³                                │
 *   │                                                                 │
 *   │   Coefficients (A&S 26.2.23):                                   │
 *   │   c₀=2.515517  c₁=0.802853  c₂=0.010328                         │
 *   │   d₁=1.432788  d₂=0.189269  d₃=0.001308                         │
 *   └────────────────────────────────────────────────────────┬─────────┘
 *                                                            │
 *                                                            ▼
 *   ┌──────────────────────────────────────────────────────────────────┐
 *   │ STEP 4: APPLY SIGN AND SCALE                                    │
 *   │   z = -x (if lower tail) or +x (if upper tail)                  │
 *   │   result = μ + σ · z                                            │
 *   └────────────────────────────────────────────────────────┬─────────┘
 *                                                            │
 *                                                            ▼
 *   OUTPUT: Gaussian quantile value
 * }</pre>
 *
 * <h2>Accuracy</h2>
 *
 * <p>The approximation provides accuracy to approximately 4.5 × 10⁻⁴ relative error
 * for the standard normal distribution. This is sufficient for most sampling
 * applications where the statistical properties matter more than exact precision.
 *
 * <h2>Performance</h2>
 *
 * <p>O(1) computation cost - the rational approximation requires only:
 * <ul>
 *   <li>1 logarithm</li>
 *   <li>1 square root</li>
 *   <li>~10 multiply/add operations</li>
 * </ul>
 *
 * @see VectorGen
 * @see StratifiedSampler
 */
public final class InverseGaussianCDF {

    // Coefficients for rational approximation (Abramowitz and Stegun 26.2.23)
    private static final double C0 = 2.515517;
    private static final double C1 = 0.802853;
    private static final double C2 = 0.010328;
    private static final double D1 = 1.432788;
    private static final double D2 = 0.189269;
    private static final double D3 = 0.001308;

    private InverseGaussianCDF() {
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
     * Computes the inverse CDF for a Gaussian distribution with specified mean and stdDev.
     *
     * <p>This is the main method used in the vector generation pipeline.
     * Given a probability p in (0, 1), returns the value x such that P(X ≤ x) = p
     * where X follows N(mean, stdDev²).
     *
     * <pre>{@code
     * Transform: z = Φ⁻¹(p)           // standard normal quantile
     *            x = μ + σ·z          // scale to target distribution
     *
     * Example for N(5, 2):
     *   p=0.50 → z=0.0 → x = 5 + 2·0 = 5.0
     *   p=0.84 → z≈1.0 → x = 5 + 2·1 = 7.0
     *   p=0.16 → z≈-1.0 → x = 5 + 2·(-1) = 3.0
     * }</pre>
     *
     * @param p the probability value in the open interval (0, 1)
     * @param mean the mean (μ) of the Gaussian distribution
     * @param stdDev the standard deviation (σ) of the Gaussian distribution
     * @return the quantile value for the specified Gaussian distribution
     */
    public static double quantile(double p, double mean, double stdDev) {
        return mean + stdDev * standardNormalQuantile(p);
    }

    /**
     * Computes the inverse CDF for a Gaussian distribution with specified model.
     *
     * @param p the probability value in the open interval (0, 1)
     * @param model the Gaussian component model specifying mean and stdDev
     * @return the quantile value for the specified Gaussian distribution
     */
    public static double quantile(double p, GaussianComponentModel model) {
        return model.mean() + model.stdDev() * standardNormalQuantile(p);
    }

    /**
     * Rational approximation function (Abramowitz and Stegun formula 26.2.23).
     *
     * <pre>{@code
     *              c₀ + c₁·t + c₂·t²
     * x(t) ≈ t - ─────────────────────
     *            1 + d₁·t + d₂·t² + d₃·t³
     * }</pre>
     */
    private static double rationalApproximation(double t) {
        return t - (C0 + C1 * t + C2 * t * t) / (1.0 + D1 * t + D2 * t * t + D3 * t * t * t);
    }
}
