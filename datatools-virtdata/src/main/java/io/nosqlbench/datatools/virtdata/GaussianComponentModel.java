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

import java.util.Objects;

/**
 * Immutable model for a single dimension's Gaussian distribution parameters.
 *
 * <h2>Purpose</h2>
 *
 * <p>Each dimension in a vector space can have its own Gaussian distribution,
 * characterized by a mean (μ) and standard deviation (σ). This class holds
 * those parameters and is used during the inverse CDF transform stage.
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
 * <pre>{@code
 *   Unbounded N(μ, σ²)                Truncated N(μ, σ²) on [lower, upper]
 *
 *         ╭───╮                              ╭───╮
 *        ╱     ╲                            ╱     ╲
 *       ╱       ╲                          │       │
 *      ╱         ╲                         │       │
 *   ──┴───────────┴──  -∞ to +∞        ───┴───────┴───  [lower, upper]
 * }</pre>
 *
 * <h2>Usage</h2>
 *
 * <pre>{@code
 * // Standard normal N(0, 1) - unbounded
 * GaussianComponentModel standard = GaussianComponentModel.standardNormal();
 *
 * // Truncated to [-1, 1]
 * GaussianComponentModel bounded = new GaussianComponentModel(0.0, 1.0, -1.0, 1.0);
 *
 * // Create M identical truncated models
 * GaussianComponentModel[] uniform = GaussianComponentModel.uniformTruncated(0.0, 1.0, -1.0, 1.0, 128);
 * }</pre>
 *
 * @see VectorSpaceModel
 * @see InverseGaussianCDF
 * @see TruncatedGaussianSampler
 */
public final class GaussianComponentModel {

    private final double mean;
    private final double stdDev;
    private final double lower;
    private final double upper;
    private final boolean truncated;

    // Lazily initialized truncated sampler
    private transient TruncatedGaussianSampler truncatedSampler;

    /**
     * Constructs an unbounded Gaussian component model.
     *
     * @param mean the mean (μ) of the Gaussian distribution
     * @param stdDev the standard deviation (σ) of the Gaussian distribution; must be positive
     * @throws IllegalArgumentException if stdDev is not positive
     */
    public GaussianComponentModel(double mean, double stdDev) {
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
     * Constructs a truncated Gaussian component model.
     *
     * <p>Outputs will be bounded to [lower, upper] using the proper truncated
     * normal inverse transform method.
     *
     * @param mean the mean (μ) of the underlying Gaussian distribution
     * @param stdDev the standard deviation (σ); must be positive
     * @param lower the lower bound of the truncation interval
     * @param upper the upper bound of the truncation interval
     * @throws IllegalArgumentException if stdDev ≤ 0 or lower ≥ upper
     */
    public GaussianComponentModel(double mean, double stdDev, double lower, double upper) {
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

    /**
     * Returns the mean of this Gaussian distribution.
     *
     * @return the mean (μ)
     */
    public double mean() {
        return mean;
    }

    /**
     * Returns the standard deviation of this Gaussian distribution.
     *
     * @return the standard deviation (σ)
     */
    public double stdDev() {
        return stdDev;
    }

    /**
     * Returns the lower truncation bound.
     *
     * @return the lower bound, or {@link Double#NEGATIVE_INFINITY} if unbounded
     */
    public double lower() {
        return lower;
    }

    /**
     * Returns the upper truncation bound.
     *
     * @return the upper bound, or {@link Double#POSITIVE_INFINITY} if unbounded
     */
    public double upper() {
        return upper;
    }

    /**
     * Returns whether this model has truncation bounds.
     *
     * @return true if truncated, false if unbounded
     */
    public boolean isTruncated() {
        return truncated;
    }

    /**
     * Samples a value from this distribution given a unit interval input.
     *
     * <p>This is the main sampling method that handles both truncated and
     * unbounded cases correctly.
     *
     * @param u a value in the open interval (0, 1)
     * @return a sample from the (possibly truncated) Gaussian distribution
     */
    public double sample(double u) {
        if (truncated) {
            return getTruncatedSampler().sample(u);
        } else {
            return InverseGaussianCDF.quantile(u, mean, stdDev);
        }
    }

    /**
     * Returns the truncated sampler, creating it lazily if needed.
     */
    private TruncatedGaussianSampler getTruncatedSampler() {
        if (truncatedSampler == null) {
            truncatedSampler = new TruncatedGaussianSampler(mean, stdDev, lower, upper);
        }
        return truncatedSampler;
    }

    /**
     * Creates a standard normal component model N(0, 1) - unbounded.
     *
     * @return a standard normal Gaussian component model
     */
    public static GaussianComponentModel standardNormal() {
        return new GaussianComponentModel(0.0, 1.0);
    }

    /**
     * Creates a standard normal component model N(0, 1) truncated to [-1, 1].
     *
     * @return a unit-bounded standard normal Gaussian component model
     */
    public static GaussianComponentModel standardNormalUnitBounded() {
        return new GaussianComponentModel(0.0, 1.0, -1.0, 1.0);
    }

    /**
     * Creates an array of identical unbounded Gaussian component models.
     *
     * @param mean the mean for all components
     * @param stdDev the standard deviation for all components
     * @param dimensions the number of components (M)
     * @return an array of M identical Gaussian component models
     */
    public static GaussianComponentModel[] uniform(double mean, double stdDev, int dimensions) {
        GaussianComponentModel[] models = new GaussianComponentModel[dimensions];
        GaussianComponentModel model = new GaussianComponentModel(mean, stdDev);
        for (int i = 0; i < dimensions; i++) {
            models[i] = model;
        }
        return models;
    }

    /**
     * Creates an array of identical truncated Gaussian component models.
     *
     * @param mean the mean for all components
     * @param stdDev the standard deviation for all components
     * @param lower the lower truncation bound for all components
     * @param upper the upper truncation bound for all components
     * @param dimensions the number of components (M)
     * @return an array of M identical truncated Gaussian component models
     */
    public static GaussianComponentModel[] uniformTruncated(double mean, double stdDev,
                                                            double lower, double upper, int dimensions) {
        GaussianComponentModel[] models = new GaussianComponentModel[dimensions];
        GaussianComponentModel model = new GaussianComponentModel(mean, stdDev, lower, upper);
        for (int i = 0; i < dimensions; i++) {
            models[i] = model;
        }
        return models;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof GaussianComponentModel)) return false;
        GaussianComponentModel that = (GaussianComponentModel) o;
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
            return "GaussianComponentModel[mean=" + mean + ", stdDev=" + stdDev +
                   ", truncated=[" + lower + ", " + upper + "]]";
        } else {
            return "GaussianComponentModel[mean=" + mean + ", stdDev=" + stdDev + "]";
        }
    }
}
