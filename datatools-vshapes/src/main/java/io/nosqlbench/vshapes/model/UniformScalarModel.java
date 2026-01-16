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
 * Uniform distribution scalar model over a bounded range [lower, upper].
 *
 * <h2>Purpose</h2>
 *
 * <p>This scalar model generates values uniformly distributed over the
 * interval [lower, upper]. Each value in this range has equal probability
 * of being sampled.
 *
 * <h2>Tensor Hierarchy</h2>
 *
 * <p>UniformScalarModel is a first-order tensor model (ScalarModel) that
 * represents a single-dimensional uniform distribution:
 * <ul>
 *   <li>{@link ScalarModel} - First-order (single dimension) - this class</li>
 *   <li>{@link VectorModel} - Second-order (M dimensions)</li>
 *   <li>{@link MatrixModel} - Third-order (K vector models)</li>
 * </ul>
 *
 * <h2>Properties</h2>
 *
 * <ul>
 *   <li><b>Mean</b>: (lower + upper) / 2</li>
 *   <li><b>Standard Deviation</b>: (upper - lower) / sqrt(12)</li>
 *   <li><b>PDF</b>: 1 / (upper - lower) for x in [lower, upper], 0 otherwise</li>
 *   <li><b>CDF</b>: (x - lower) / (upper - lower) for x in [lower, upper]</li>
 * </ul>
 *
 * <h2>Sampling</h2>
 *
 * <p>Uses simple linear interpolation: sample(u) = lower + u * (upper - lower)
 *
 * <h2>Usage</h2>
 *
 * <pre>{@code
 * // Uniform over [0, 1]
 * UniformScalarModel model = new UniformScalarModel(0.0, 1.0);
 *
 * // Uniform over [-1, 1]
 * UniformScalarModel unitBounded = UniformScalarModel.unitBounded();
 *
 * // Sample a value
 * double value = model.sample(0.5);  // Returns 0.5 (midpoint)
 * }</pre>
 *
 * @see ScalarModel
 * @see VectorModel
 * @see VectorSpaceModel
 */
@ModelType(UniformScalarModel.MODEL_TYPE)
public class UniformScalarModel implements ScalarModel {

    public static final String MODEL_TYPE = "uniform";

    @SerializedName("lower")
    private final double lower;

    @SerializedName("upper")
    private final double upper;

    private final transient double range;
    private final transient double mean;
    private final transient double stdDev;

    /**
     * Constructs a uniform scalar model over [lower, upper].
     *
     * @param lower the lower bound of the interval
     * @param upper the upper bound of the interval
     * @throws IllegalArgumentException if lower >= upper
     */
    public UniformScalarModel(double lower, double upper) {
        if (lower >= upper) {
            throw new IllegalArgumentException("Lower bound must be less than upper: " + lower + " >= " + upper);
        }
        this.lower = lower;
        this.upper = upper;
        this.range = upper - lower;
        this.mean = (lower + upper) / 2.0;
        this.stdDev = range / Math.sqrt(12.0);
    }

    @Override
    public String getModelType() {
        return MODEL_TYPE;
    }

    /**
     * Returns the mean of this uniform distribution.
     * @return (lower + upper) / 2
     */
    public double getMean() {
        return mean;
    }

    /**
     * Returns the standard deviation of this uniform distribution.
     * @return (upper - lower) / sqrt(12)
     */
    public double getStdDev() {
        return stdDev;
    }

    /**
     * Computes the probability density function (PDF) at a given value.
     * @param x the value at which to evaluate the PDF
     * @return 1/range if x is in [lower, upper], 0 otherwise
     */
    public double pdf(double x) {
        if (x < lower || x > upper) {
            return 0.0;
        }
        return 1.0 / range;
    }

    /**
     * Computes the cumulative distribution function (CDF) at a given value.
     * @param x the value at which to evaluate the CDF
     * @return the cumulative probability P(X ≤ x)
     */
    public double cdf(double x) {
        if (x < lower) return 0.0;
        if (x > upper) return 1.0;
        return (x - lower) / range;
    }

    /**
     * Returns the lower bound of this uniform distribution.
     * @return the lower bound
     */
    public double getLower() {
        return lower;
    }

    /**
     * Returns the upper bound of this uniform distribution.
     * @return the upper bound
     */
    public double getUpper() {
        return upper;
    }

    /**
     * Returns the range (upper - lower) of this uniform distribution.
     * @return the range
     */
    public double getRange() {
        return range;
    }

    /**
     * Creates a uniform model over [0, 1].
     * @return a UniformScalarModel for the unit interval
     */
    public static UniformScalarModel zeroOne() {
        return new UniformScalarModel(0.0, 1.0);
    }

    /**
     * Creates a uniform model over [-1, 1].
     * @return a UniformScalarModel for the symmetric unit interval
     */
    public static UniformScalarModel unitBounded() {
        return new UniformScalarModel(-1.0, 1.0);
    }

    /**
     * Creates an array of identical uniform scalar models.
     *
     * @param lower the lower bound for all models
     * @param upper the upper bound for all models
     * @param dimensions the number of models (M)
     * @return an array of M identical uniform scalar models
     */
    public static UniformScalarModel[] uniformScalar(double lower, double upper, int dimensions) {
        UniformScalarModel[] models = new UniformScalarModel[dimensions];
        UniformScalarModel model = new UniformScalarModel(lower, upper);
        for (int i = 0; i < dimensions; i++) {
            models[i] = model;
        }
        return models;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof UniformScalarModel)) return false;
        UniformScalarModel that = (UniformScalarModel) o;
        return Double.compare(that.lower, lower) == 0 &&
               Double.compare(that.upper, upper) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(lower, upper);
    }

    @Override
    public String toString() {
        return "UniformScalarModel[lower=" + lower + ", upper=" + upper + "]";
    }
}
