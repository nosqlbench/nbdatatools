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

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Composite (mixture) scalar model combining multiple distributions with weights.
 *
 * <h2>Purpose</h2>
 *
 * <p>This scalar model combines multiple scalar models into a mixture
 * distribution. Each component has an associated weight that determines the
 * probability of sampling from that component. This is useful for modeling
 * multi-modal distributions.
 *
 * <h2>Tensor Hierarchy</h2>
 *
 * <p>CompositeScalarModel is a first-order tensor model (ScalarModel) that
 * represents a mixture of multiple scalar distributions:
 * <ul>
 *   <li>{@link ScalarModel} - First-order (single dimension) - this class</li>
 *   <li>{@link VectorModel} - Second-order (M dimensions)</li>
 *   <li>{@link MatrixModel} - Third-order (K vector models)</li>
 * </ul>
 *
 * <h2>Design</h2>
 *
 * <p>This model is a pure data container holding the component models and
 * their weights. It does not compute derived statistics (mean, stdDev, bounds)
 * because that would require assuming all component types support those methods.
 * Instead, the CompositeSampler in virtdata handles sampling logic.
 *
 * <h2>Usage</h2>
 *
 * <pre>{@code
 * // Create a bimodal distribution (two Gaussians)
 * CompositeScalarModel bimodal = CompositeScalarModel.of(
 *     new NormalScalarModel(-2.0, 0.5),  // left mode
 *     new NormalScalarModel(2.0, 0.5)    // right mode
 * );  // equal weights
 *
 * // Create with custom weights
 * CompositeScalarModel weighted = new CompositeScalarModel(
 *     List.of(
 *         new NormalScalarModel(0.0, 1.0),
 *         new UniformScalarModel(-1.0, 1.0)
 *     ),
 *     new double[]{0.7, 0.3}  // 70% Gaussian, 30% Uniform
 * );
 * }</pre>
 *
 * @see ScalarModel
 * @see VectorModel
 * @see VectorSpaceModel
 */
@ModelType(CompositeScalarModel.MODEL_TYPE)
public class CompositeScalarModel implements ScalarModel {

    public static final String MODEL_TYPE = "composite";

    @SerializedName("components")
    private final ScalarModel[] components;

    @SerializedName("weights")
    private final double[] weights;

    /**
     * Constructs a composite model with specified components and weights.
     *
     * @param components the scalar models
     * @param weights the weights for each component (will be normalized to sum to 1.0)
     * @throws IllegalArgumentException if arrays have different lengths or weights are negative
     */
    public CompositeScalarModel(List<? extends ScalarModel> components, double[] weights) {
        Objects.requireNonNull(components, "components cannot be null");
        Objects.requireNonNull(weights, "weights cannot be null");
        if (components.isEmpty()) {
            throw new IllegalArgumentException("components cannot be empty");
        }
        if (components.size() != weights.length) {
            throw new IllegalArgumentException("components and weights must have same length");
        }

        // Validate and normalize weights
        double sum = 0;
        for (double w : weights) {
            if (w < 0) {
                throw new IllegalArgumentException("weights must be non-negative");
            }
            sum += w;
        }
        if (sum <= 0) {
            throw new IllegalArgumentException("weights must sum to a positive value");
        }

        this.components = components.toArray(new ScalarModel[0]);
        this.weights = new double[weights.length];
        for (int i = 0; i < weights.length; i++) {
            this.weights[i] = weights[i] / sum;  // Normalize
        }
    }

    /**
     * Creates a composite model with equal weights.
     *
     * @param scalars the scalar models
     * @return a CompositeScalarModel with equal weights
     */
    public static CompositeScalarModel of(ScalarModel... scalars) {
        double[] weights = new double[scalars.length];
        Arrays.fill(weights, 1.0 / scalars.length);
        return new CompositeScalarModel(Arrays.asList(scalars), weights);
    }

    @Override
    public String getModelType() {
        return MODEL_TYPE;
    }

    /**
     * Returns the scalar models.
     * @return a copy of the scalar models array
     */
    public ScalarModel[] getScalarModels() {
        return Arrays.copyOf(components, components.length);
    }

    /**
     * Returns the normalized weights.
     * @return a copy of the weights array (sums to 1.0)
     */
    public double[] getWeights() {
        return Arrays.copyOf(weights, weights.length);
    }

    /**
     * Returns the number of components.
     * @return the component count
     */
    public int getComponentCount() {
        return components.length;
    }

    /**
     * Computes the cumulative distribution function (CDF) at a given value.
     *
     * <p>For a mixture model, the CDF is the weighted sum of component CDFs:
     * F(x) = Σ wᵢ Fᵢ(x)
     *
     * @param x the value at which to evaluate the CDF
     * @return the cumulative probability P(X ≤ x), in range [0, 1]
     */
    @Override
    public double cdf(double x) {
        double sum = 0;
        for (int i = 0; i < components.length; i++) {
            sum += weights[i] * components[i].cdf(x);
        }
        return sum;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CompositeScalarModel)) return false;
        CompositeScalarModel that = (CompositeScalarModel) o;
        return Arrays.equals(components, that.components) &&
               Arrays.equals(weights, that.weights);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(components);
        result = 31 * result + Arrays.hashCode(weights);
        return result;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("CompositeScalarModel[");
        for (int i = 0; i < components.length; i++) {
            if (i > 0) sb.append(", ");
            sb.append(String.format("%.2f*%s", weights[i], components[i].getModelType()));
        }
        sb.append("]");
        return sb.toString();
    }
}
