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

import io.nosqlbench.vshapes.model.ComponentModel;
import io.nosqlbench.vshapes.model.ScalarModel;
import io.nosqlbench.vshapes.model.VectorModel;

/// Factory for creating bound ScalarSampler instances from ScalarModel types.
///
/// # Tensor Hierarchy
///
/// ScalarSamplerFactory corresponds to the first-order tensor level (ScalarModel):
/// - ScalarSamplerFactory - Creates samplers for ScalarModels
/// - VectorGeneratorIO - Creates generators for VectorModels
///
/// # Overview
///
/// This factory creates samplers that are bound to their model at construction
/// time. The type dispatch happens once here, not on every sample call.
///
/// # Usage
///
/// ```java
/// ScalarModel model = ...; // from VectorModel
/// ScalarSampler sampler = ScalarSamplerFactory.forModel(model);
///
/// // Hot path - no type checks, no casting
/// double value = sampler.sample(u);
/// ```
///
/// @see ScalarSampler
/// @see io.nosqlbench.vshapes.model.ScalarModel
public final class ScalarSamplerFactory {

    private ScalarSamplerFactory() {
        // Factory class, no instantiation
    }

    /// Creates a sampler bound to the given scalar model.
    ///
    /// Type dispatch happens here at construction time, not at sample time.
    ///
    /// @param model the scalar model
    /// @return a ScalarSampler bound to the model's parameters
    /// @throws IllegalArgumentException if the model type is not supported
    @SuppressWarnings("deprecation")
    public static ScalarSampler forModel(ScalarModel model) {
        // Delegate to ComponentSamplerFactory for now
        // All current ScalarModel implementations are ComponentModel subclasses
        if (model instanceof ComponentModel) {
            ComponentModel componentModel = (ComponentModel) model;
            ComponentSampler sampler = ComponentSamplerFactory.forModel(componentModel);
            // Wrap the ComponentSampler as a ScalarSampler
            return sampler::sample;
        }
        throw new IllegalArgumentException(
            "No sampler for model type: " + model.getClass().getName());
    }

    /// Creates an array of samplers for the given scalar models.
    ///
    /// Convenience method for creating samplers for all dimensions at once.
    ///
    /// @param models the scalar models
    /// @return an array of bound samplers
    public static ScalarSampler[] forModels(ScalarModel[] models) {
        ScalarSampler[] samplers = new ScalarSampler[models.length];
        for (int i = 0; i < models.length; i++) {
            samplers[i] = forModel(models[i]);
        }
        return samplers;
    }

    /// Creates an array of samplers for all dimensions of a VectorModel.
    ///
    /// Convenience method for creating samplers from a VectorModel.
    ///
    /// @param model the vector model
    /// @return an array of bound samplers, one per dimension
    public static ScalarSampler[] forVectorModel(VectorModel model) {
        return forModels(model.scalarModels());
    }
}
