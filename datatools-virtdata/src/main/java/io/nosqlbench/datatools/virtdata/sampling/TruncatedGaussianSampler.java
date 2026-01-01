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

import io.nosqlbench.vshapes.model.GaussianCDF;

/**
 * Truncated Gaussian sampler using proper inverse transform method.
 *
 * <h2>Purpose</h2>
 *
 * <p>This class provides truncated normal sampling where outputs are guaranteed
 * to fall within specified bounds [lower, upper]. Unlike simple clamping, this
 * method preserves correct quantile spacing and has zero probability mass
 * outside the bounds.
 *
 * <h2>Algorithm</h2>
 *
 * <pre>{@code
 *   SETUP (precomputed once per sampler):
 *     Step 1: Standardize bounds
 *       a = (lower - μ) / σ        (lower bound in standard units)
 *       b = (upper - μ) / σ        (upper bound in standard units)
 *
 *     Step 2: Compute CDF at bounds
 *       Φ(a) = P(Z ≤ a)            (probability below lower)
 *       Φ(b) = P(Z ≤ b)            (probability below upper)
 *
 *     Step 3: Compute probability mass
 *       Z = Φ(b) - Φ(a)            (mass within [lower, upper])
 *
 *   SAMPLING (per value):
 *     Step 4: Rescale unit interval
 *       u' = Φ(a) + u · Z          (maps u∈(0,1) to u'∈(Φ(a),Φ(b)))
 *
 *     Step 5: Invert to get sample
 *       x = μ + σ · Φ⁻¹(u')        (gives x ∈ [lower, upper])
 * }</pre>
 *
 * @see InverseGaussianCDF
 */
public final class TruncatedGaussianSampler {

    private final double mean;
    private final double stdDev;
    private final double lower;
    private final double upper;

    // Precomputed values
    private final double cdfAtLower;  // Φ(a)
    private final double cdfRange;    // Z = Φ(b) - Φ(a)

    /**
     * Creates a truncated Gaussian sampler with specified bounds.
     *
     * @param mean the mean (μ) of the underlying Gaussian
     * @param stdDev the standard deviation (σ) of the underlying Gaussian
     * @param lower the lower bound of the truncation interval
     * @param upper the upper bound of the truncation interval
     * @throws IllegalArgumentException if stdDev ≤ 0 or lower ≥ upper
     */
    public TruncatedGaussianSampler(double mean, double stdDev, double lower, double upper) {
        if (stdDev <= 0) {
            throw new IllegalArgumentException("Standard deviation must be positive, got: " + stdDev);
        }
        if (lower >= upper) {
            throw new IllegalArgumentException("Lower bound must be less than upper bound: " + lower + " >= " + upper);
        }

        this.mean = mean;
        this.stdDev = stdDev;
        this.lower = lower;
        this.upper = upper;

        // Precompute CDF bounds
        // Step 1: Standardize bounds
        double a = (lower - mean) / stdDev;
        double b = (upper - mean) / stdDev;

        // Step 2 & 3: Compute CDF values and range
        this.cdfAtLower = GaussianCDF.standardNormalCDF(a);
        double cdfAtUpper = GaussianCDF.standardNormalCDF(b);
        this.cdfRange = cdfAtUpper - cdfAtLower;

        if (cdfRange <= 0) {
            throw new IllegalArgumentException(
                "Truncation bounds [" + lower + ", " + upper + "] contain negligible probability mass " +
                "for N(" + mean + ", " + stdDev + "²). Try wider bounds or different distribution parameters.");
        }
    }

    /**
     * Samples from the truncated Gaussian distribution.
     *
     * <p>Maps a unit interval value u ∈ (0, 1) to a value x ∈ [lower, upper]
     * following the truncated normal distribution.
     *
     * @param u a value in the open interval (0, 1)
     * @return a sample from the truncated Gaussian, guaranteed to be in [lower, upper]
     */
    public double sample(double u) {
        // Step 4: Rescale unit interval
        // u' = Φ(a) + u · Z
        double uPrime = cdfAtLower + u * cdfRange;

        // Step 5: Invert to get sample
        // x = μ + σ · Φ⁻¹(u')
        return mean + stdDev * InverseGaussianCDF.standardNormalQuantile(uPrime);
    }

    /**
     * Returns the mean of the underlying (non-truncated) Gaussian.
     * @return the mean (μ)
     */
    public double mean() {
        return mean;
    }

    /**
     * Returns the standard deviation of the underlying (non-truncated) Gaussian.
     * @return the standard deviation (σ)
     */
    public double stdDev() {
        return stdDev;
    }

    /**
     * Returns the lower truncation bound.
     * @return the lower bound
     */
    public double lower() {
        return lower;
    }

    /**
     * Returns the upper truncation bound.
     * @return the upper bound
     */
    public double upper() {
        return upper;
    }

    /**
     * Returns the probability mass within the truncation bounds.
     * @return Z = Φ(b) - Φ(a), the fraction of the original distribution retained
     */
    public double probabilityMass() {
        return cdfRange;
    }

    /**
     * Creates a sampler for the common case of truncation to [-1, 1].
     *
     * @param mean the mean of the underlying Gaussian
     * @param stdDev the standard deviation of the underlying Gaussian
     * @return a sampler truncated to [-1, 1]
     */
    public static TruncatedGaussianSampler unitBounded(double mean, double stdDev) {
        return new TruncatedGaussianSampler(mean, stdDev, -1.0, 1.0);
    }

    /**
     * Creates a sampler for truncation to [0, 1] (useful for normalized embeddings).
     *
     * @param mean the mean of the underlying Gaussian
     * @param stdDev the standard deviation of the underlying Gaussian
     * @return a sampler truncated to [0, 1]
     */
    public static TruncatedGaussianSampler zeroOneBounded(double mean, double stdDev) {
        return new TruncatedGaussianSampler(mean, stdDev, 0.0, 1.0);
    }
}
