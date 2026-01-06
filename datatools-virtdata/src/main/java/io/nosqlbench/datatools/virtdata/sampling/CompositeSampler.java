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

import io.nosqlbench.vshapes.model.ScalarModel;
import io.nosqlbench.vshapes.model.CompositeScalarModel;

/// Sampler for Composite (mixture) distributions, bound at construction.
///
/// Selects a component based on weights and delegates sampling to
/// the pre-bound component sampler.
public final class CompositeSampler implements ComponentSampler {

    private final ComponentSampler[] componentSamplers;
    private final double[] cumulativeWeights;

    /// Creates a sampler bound to the given Composite model.
    ///
    /// @param model the Composite component model
    public CompositeSampler(CompositeScalarModel model) {
        ScalarModel[] components = model.getScalarModels();
        double[] weights = model.getWeights();

        // Create bound samplers for each component
        this.componentSamplers = new ComponentSampler[components.length];
        for (int i = 0; i < components.length; i++) {
            this.componentSamplers[i] = ComponentSamplerFactory.forModel(components[i]);
        }

        // Build cumulative weights
        this.cumulativeWeights = new double[weights.length];
        double cumulative = 0;
        for (int i = 0; i < weights.length; i++) {
            cumulative += weights[i];
            this.cumulativeWeights[i] = cumulative;
        }
        this.cumulativeWeights[weights.length - 1] = 1.0; // Ensure exact 1.0
    }

    @Override
    public double sample(double u) {
        // Find which component to sample from
        int componentIndex = 0;
        for (int i = 0; i < cumulativeWeights.length; i++) {
            if (u < cumulativeWeights[i]) {
                componentIndex = i;
                break;
            }
        }

        // Rescale u to [0,1] within the component's weight range
        double lower = componentIndex == 0 ? 0.0 : cumulativeWeights[componentIndex - 1];
        double upper = cumulativeWeights[componentIndex];
        double rescaledU = (u - lower) / (upper - lower);

        // Clamp to valid range
        rescaledU = Math.max(1e-10, Math.min(1.0 - 1e-10, rescaledU));

        return componentSamplers[componentIndex].sample(rescaledU);
    }
}
