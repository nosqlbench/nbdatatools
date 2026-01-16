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

/// Factory for creating LERP-wrapped samplers.
///
/// # Overview
///
/// This factory provides convenience methods for creating samplers that use
/// pre-computed lookup tables for O(1) sampling performance.
///
/// # Usage
///
/// ```java
/// // Wrap an existing sampler
/// ComponentSampler direct = new NormalSampler(model);
/// ComponentSampler lerp = LerpSamplerFactory.wrap(direct);
///
/// // Create directly from model
/// ComponentSampler lerp = LerpSamplerFactory.forModel(model);
///
/// // Create for all dimensions at once
/// ComponentSampler[] samplers = LerpSamplerFactory.forModels(models);
/// ```
///
/// @see LerpSampler
/// @see ComponentSamplerFactory
public final class LerpSamplerFactory {

    private LerpSamplerFactory() {
        // Factory class, no instantiation
    }

    /// Wraps a sampler with LERP optimization using default table size.
    ///
    /// @param sampler the sampler to wrap
    /// @return a LERP-wrapped sampler
    public static ComponentSampler wrap(ComponentSampler sampler) {
        return new LerpSampler(sampler);
    }

    /// Wraps a sampler with LERP optimization using custom table size.
    ///
    /// @param sampler the sampler to wrap
    /// @param tableSize number of pre-computed points
    /// @return a LERP-wrapped sampler
    public static ComponentSampler wrap(ComponentSampler sampler, int tableSize) {
        return new LerpSampler(sampler, tableSize);
    }

    /// Creates a LERP-optimized sampler directly from a model.
    ///
    /// @param model the scalar model
    /// @return a LERP-wrapped sampler bound to the model
    public static ComponentSampler forModel(ScalarModel model) {
        return new LerpSampler(ComponentSamplerFactory.forModel(model));
    }

    /// Creates a LERP-optimized sampler with custom table size from a model.
    ///
    /// @param model the scalar model
    /// @param tableSize number of pre-computed points
    /// @return a LERP-wrapped sampler bound to the model
    public static ComponentSampler forModel(ScalarModel model, int tableSize) {
        return new LerpSampler(ComponentSamplerFactory.forModel(model), tableSize);
    }

    /// Creates LERP-optimized samplers for all models using default table size.
    ///
    /// @param models the scalar models
    /// @return an array of LERP-wrapped samplers
    public static ComponentSampler[] forModels(ScalarModel[] models) {
        return forModels(models, LerpSampler.DEFAULT_TABLE_SIZE);
    }

    /// Creates LERP-optimized samplers for all models with custom table size.
    ///
    /// @param models the scalar models
    /// @param tableSize number of pre-computed points per sampler
    /// @return an array of LERP-wrapped samplers
    public static ComponentSampler[] forModels(ScalarModel[] models, int tableSize) {
        ComponentSampler[] samplers = new ComponentSampler[models.length];
        for (int i = 0; i < models.length; i++) {
            samplers[i] = forModel(models[i], tableSize);
        }
        return samplers;
    }
}
