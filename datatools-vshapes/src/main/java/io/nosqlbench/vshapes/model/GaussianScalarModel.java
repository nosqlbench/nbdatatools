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

/**
 * Gaussian (normal) distribution scalar model.
 *
 * <p>This class is functionally identical to {@link GaussianComponentModel}.
 * It provides the preferred naming convention for the tensor model hierarchy.
 *
 * <h2>Tensor Hierarchy</h2>
 *
 * <p>GaussianScalarModel is a first-order tensor model (ScalarModel) that
 * represents a single-dimensional Gaussian (normal) distribution:
 * <ul>
 *   <li>{@link ScalarModel} - First-order (single dimension) - this class</li>
 *   <li>{@link VectorModel} - Second-order (M dimensions)</li>
 *   <li>{@link MatrixModel} - Third-order (K vector models)</li>
 * </ul>
 *
 * <h2>Usage</h2>
 *
 * <pre>{@code
 * // Standard normal N(0, 1) - unbounded
 * GaussianScalarModel standard = GaussianScalarModel.standardNormal();
 *
 * // Truncated to [-1, 1]
 * GaussianScalarModel bounded = new GaussianScalarModel(0.0, 1.0, -1.0, 1.0);
 *
 * // Create M identical truncated models
 * GaussianScalarModel[] uniform = GaussianScalarModel.uniformScalar(0.0, 1.0, -1.0, 1.0, 128);
 * }</pre>
 *
 * @see ScalarModel
 * @see VectorModel
 * @see GaussianComponentModel
 */
public final class GaussianScalarModel extends GaussianComponentModel {

    /**
     * Constructs an unbounded Gaussian scalar model.
     *
     * @param mean the mean (μ) of the Gaussian distribution
     * @param stdDev the standard deviation (σ) of the Gaussian distribution; must be positive
     * @throws IllegalArgumentException if stdDev is not positive
     */
    public GaussianScalarModel(double mean, double stdDev) {
        super(mean, stdDev);
    }

    /**
     * Constructs a truncated Gaussian scalar model.
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
    public GaussianScalarModel(double mean, double stdDev, double lower, double upper) {
        super(mean, stdDev, lower, upper);
    }

    /**
     * Creates a standard normal scalar model N(0, 1) - unbounded.
     *
     * @return a standard normal Gaussian scalar model
     */
    public static GaussianScalarModel standardNormal() {
        return new GaussianScalarModel(0.0, 1.0);
    }

    /**
     * Creates a standard normal scalar model N(0, 1) truncated to [-1, 1].
     *
     * @return a unit-bounded standard normal Gaussian scalar model
     */
    public static GaussianScalarModel standardNormalUnitBounded() {
        return new GaussianScalarModel(0.0, 1.0, -1.0, 1.0);
    }

    /**
     * Creates an array of identical unbounded Gaussian scalar models.
     *
     * @param mean the mean for all models
     * @param stdDev the standard deviation for all models
     * @param dimensions the number of models (M)
     * @return an array of M identical Gaussian scalar models
     */
    public static GaussianScalarModel[] uniformScalar(double mean, double stdDev, int dimensions) {
        GaussianScalarModel[] models = new GaussianScalarModel[dimensions];
        GaussianScalarModel model = new GaussianScalarModel(mean, stdDev);
        for (int i = 0; i < dimensions; i++) {
            models[i] = model;
        }
        return models;
    }

    /**
     * Creates an array of identical truncated Gaussian scalar models.
     *
     * @param mean the mean for all models
     * @param stdDev the standard deviation for all models
     * @param lower the lower truncation bound for all models
     * @param upper the upper truncation bound for all models
     * @param dimensions the number of models (M)
     * @return an array of M identical truncated Gaussian scalar models
     */
    public static GaussianScalarModel[] uniformScalar(double mean, double stdDev,
                                                       double lower, double upper, int dimensions) {
        GaussianScalarModel[] models = new GaussianScalarModel[dimensions];
        GaussianScalarModel model = new GaussianScalarModel(mean, stdDev, lower, upper);
        for (int i = 0; i < dimensions; i++) {
            models[i] = model;
        }
        return models;
    }

    @Override
    public String toString() {
        if (isTruncated()) {
            return "GaussianScalarModel[mean=" + getMean() + ", stdDev=" + getStdDev() +
                   ", truncated=[" + lower() + ", " + upper() + "]]";
        } else {
            return "GaussianScalarModel[mean=" + getMean() + ", stdDev=" + getStdDev() + "]";
        }
    }
}
