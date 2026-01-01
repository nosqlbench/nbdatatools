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
 * Uniform distribution scalar model over a bounded range [lower, upper].
 *
 * <p>This class is functionally identical to {@link UniformComponentModel}.
 * It provides the preferred naming convention for the tensor model hierarchy.
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
 * <h2>Usage</h2>
 *
 * <pre>{@code
 * // Uniform over [0, 1]
 * UniformScalarModel model = new UniformScalarModel(0.0, 1.0);
 *
 * // Uniform over [-1, 1]
 * UniformScalarModel unitBounded = UniformScalarModel.unitBounded();
 * }</pre>
 *
 * @see ScalarModel
 * @see VectorModel
 * @see UniformComponentModel
 */
public final class UniformScalarModel extends UniformComponentModel {

    /**
     * Constructs a uniform scalar model over [lower, upper].
     *
     * @param lower the lower bound of the interval
     * @param upper the upper bound of the interval
     * @throws IllegalArgumentException if lower >= upper
     */
    public UniformScalarModel(double lower, double upper) {
        super(lower, upper);
    }

    /**
     * Creates a uniform model over [0, 1].
     *
     * @return a UniformScalarModel for the unit interval
     */
    public static UniformScalarModel zeroOne() {
        return new UniformScalarModel(0.0, 1.0);
    }

    /**
     * Creates a uniform model over [-1, 1].
     *
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
    public String toString() {
        return "UniformScalarModel[lower=" + getLower() + ", upper=" + getUpper() + "]";
    }
}
