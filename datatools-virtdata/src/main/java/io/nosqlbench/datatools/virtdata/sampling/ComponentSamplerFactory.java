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
import io.nosqlbench.vshapes.model.CompositeComponentModel;
import io.nosqlbench.vshapes.model.EmpiricalComponentModel;
import io.nosqlbench.vshapes.model.GaussianComponentModel;
import io.nosqlbench.vshapes.model.UniformComponentModel;

/// Factory for creating bound ComponentSampler instances.
///
/// # Overview
///
/// This factory creates samplers that are bound to their model at construction
/// time. The type dispatch happens once here, not on every sample call.
///
/// # Usage
///
/// ```java
/// ComponentModel model = ...; // from VectorSpaceModel
/// ComponentSampler sampler = ComponentSamplerFactory.forModel(model);
///
/// // Hot path - no type checks, no casting
/// double value = sampler.sample(u);
/// ```
///
/// @see ComponentSampler
public final class ComponentSamplerFactory {

    private ComponentSamplerFactory() {
        // Factory class, no instantiation
    }

    /// Creates a sampler bound to the given model.
    ///
    /// Type dispatch happens here at construction time, not at sample time.
    ///
    /// @param model the component model
    /// @return a ComponentSampler bound to the model's parameters
    /// @throws IllegalArgumentException if the model type is not supported
    public static ComponentSampler forModel(ComponentModel model) {
        if (model instanceof GaussianComponentModel) {
            return new GaussianSampler((GaussianComponentModel) model);
        } else if (model instanceof UniformComponentModel) {
            return new UniformSampler((UniformComponentModel) model);
        } else if (model instanceof EmpiricalComponentModel) {
            return new EmpiricalSampler((EmpiricalComponentModel) model);
        } else if (model instanceof CompositeComponentModel) {
            return new CompositeSampler((CompositeComponentModel) model);
        } else {
            throw new IllegalArgumentException(
                "No sampler for model type: " + model.getClass().getName());
        }
    }

    /// Creates an array of samplers for the given models.
    ///
    /// Convenience method for creating samplers for all dimensions at once.
    ///
    /// @param models the component models
    /// @return an array of bound samplers
    public static ComponentSampler[] forModels(ComponentModel[] models) {
        ComponentSampler[] samplers = new ComponentSampler[models.length];
        for (int i = 0; i < models.length; i++) {
            samplers[i] = forModel(models[i]);
        }
        return samplers;
    }
}
