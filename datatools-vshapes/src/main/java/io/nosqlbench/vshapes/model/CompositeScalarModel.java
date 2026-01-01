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

/**
 * Composite (mixture) scalar model combining multiple distributions with weights.
 *
 * <p>This class is functionally identical to {@link CompositeComponentModel}.
 * It provides the preferred naming convention for the tensor model hierarchy.
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
 * <h2>Usage</h2>
 *
 * <pre>{@code
 * // Create a bimodal distribution (two Gaussians)
 * CompositeScalarModel bimodal = CompositeScalarModel.of(
 *     new GaussianScalarModel(-2.0, 0.5),  // left mode
 *     new GaussianScalarModel(2.0, 0.5)    // right mode
 * );  // equal weights
 *
 * // Create with custom weights
 * CompositeScalarModel weighted = new CompositeScalarModel(
 *     List.of(
 *         new GaussianScalarModel(0.0, 1.0),
 *         new UniformScalarModel(-1.0, 1.0)
 *     ),
 *     new double[]{0.7, 0.3}  // 70% Gaussian, 30% Uniform
 * );
 * }</pre>
 *
 * @see ScalarModel
 * @see VectorModel
 * @see CompositeComponentModel
 */
public final class CompositeScalarModel extends CompositeComponentModel {

    /**
     * Constructs a composite scalar model with specified components and weights.
     *
     * @param components the scalar models (can be any ScalarModel implementations)
     * @param weights the weights for each component (will be normalized to sum to 1.0)
     * @throws IllegalArgumentException if arrays have different lengths or weights are negative
     */
    public CompositeScalarModel(List<? extends ScalarModel> components, double[] weights) {
        super(toComponentModels(components), weights);
    }

    @SuppressWarnings("deprecation")
    private static List<ComponentModel> toComponentModels(List<? extends ScalarModel> scalars) {
        // Since ComponentModel extends ScalarModel, we can cast any ComponentModel implementation
        // For ScalarModels that aren't ComponentModels, we'd need an adapter (not needed for current impls)
        return scalars.stream()
            .map(s -> {
                if (s instanceof ComponentModel) {
                    return (ComponentModel) s;
                }
                throw new IllegalArgumentException("ScalarModel must be a ComponentModel: " + s.getClass());
            })
            .toList();
    }

    /**
     * Creates a composite scalar model with equal weights.
     *
     * @param scalars the scalar models
     * @return a CompositeScalarModel with equal weights
     */
    public static CompositeScalarModel of(ScalarModel... scalars) {
        double[] weights = new double[scalars.length];
        Arrays.fill(weights, 1.0 / scalars.length);
        return new CompositeScalarModel(Arrays.asList(scalars), weights);
    }

    /**
     * Returns the scalar models.
     *
     * @return a copy of the scalar models array
     */
    public ScalarModel[] getScalarModels() {
        ComponentModel[] components = getComponents();
        ScalarModel[] result = new ScalarModel[components.length];
        for (int i = 0; i < components.length; i++) {
            result[i] = components[i];
        }
        return result;
    }

    @Override
    public String toString() {
        ScalarModel[] scalars = getScalarModels();
        double[] weights = getWeights();
        StringBuilder sb = new StringBuilder("CompositeScalarModel[");
        for (int i = 0; i < scalars.length; i++) {
            if (i > 0) sb.append(", ");
            sb.append(String.format("%.2f*%s", weights[i], scalars[i].getModelType()));
        }
        sb.append("]");
        return sb.toString();
    }
}
