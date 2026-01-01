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

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Composite (mixture) component model combining multiple distributions with weights.
 *
 * <h2>Purpose</h2>
 *
 * <p>This component model combines multiple component models into a mixture
 * distribution. Each component has an associated weight that determines the
 * probability of sampling from that component. This is useful for modeling
 * multi-modal distributions.
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
 * CompositeComponentModel bimodal = CompositeComponentModel.of(
 *     new GaussianComponentModel(-2.0, 0.5),  // left mode
 *     new GaussianComponentModel(2.0, 0.5)    // right mode
 * );  // equal weights
 *
 * // Create with custom weights
 * CompositeComponentModel weighted = new CompositeComponentModel(
 *     List.of(
 *         new GaussianComponentModel(0.0, 1.0),
 *         new UniformComponentModel(-1.0, 1.0)
 *     ),
 *     new double[]{0.7, 0.3}  // 70% Gaussian, 30% Uniform
 * );
 * }</pre>
 *
 * @see ComponentModel
 * @see VectorSpaceModel
 */
public class CompositeComponentModel implements ComponentModel {

    public static final String MODEL_TYPE = "composite";

    private final ComponentModel[] components;
    private final double[] weights;

    /**
     * Constructs a composite model with specified components and weights.
     *
     * @param components the component models
     * @param weights the weights for each component (will be normalized to sum to 1.0)
     * @throws IllegalArgumentException if arrays have different lengths or weights are negative
     */
    public CompositeComponentModel(List<ComponentModel> components, double[] weights) {
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

        this.components = components.toArray(new ComponentModel[0]);
        this.weights = new double[weights.length];
        for (int i = 0; i < weights.length; i++) {
            this.weights[i] = weights[i] / sum;  // Normalize
        }
    }

    /**
     * Creates a composite model with equal weights.
     *
     * @param components the component models
     * @return a CompositeComponentModel with equal weights
     */
    public static CompositeComponentModel of(ComponentModel... components) {
        double[] weights = new double[components.length];
        Arrays.fill(weights, 1.0 / components.length);
        return new CompositeComponentModel(Arrays.asList(components), weights);
    }

    @Override
    public String getModelType() {
        return MODEL_TYPE;
    }

    /**
     * Returns the component models.
     * @return a copy of the component models array
     */
    public ComponentModel[] getComponents() {
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CompositeComponentModel)) return false;
        CompositeComponentModel that = (CompositeComponentModel) o;
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
        StringBuilder sb = new StringBuilder("CompositeComponentModel[");
        for (int i = 0; i < components.length; i++) {
            if (i > 0) sb.append(", ");
            sb.append(String.format("%.2f×%s", weights[i], components[i].getModelType()));
        }
        sb.append("]");
        return sb.toString();
    }
}
