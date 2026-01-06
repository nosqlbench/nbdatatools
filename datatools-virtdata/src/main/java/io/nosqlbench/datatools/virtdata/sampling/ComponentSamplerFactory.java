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

import io.nosqlbench.vshapes.model.BetaPrimeScalarModel;
import io.nosqlbench.vshapes.model.BetaScalarModel;
import io.nosqlbench.vshapes.model.CompositeScalarModel;
import io.nosqlbench.vshapes.model.EmpiricalScalarModel;
import io.nosqlbench.vshapes.model.GammaScalarModel;
import io.nosqlbench.vshapes.model.InverseGammaScalarModel;
import io.nosqlbench.vshapes.model.NormalScalarModel;
import io.nosqlbench.vshapes.model.PearsonIVScalarModel;
import io.nosqlbench.vshapes.model.ScalarModel;
import io.nosqlbench.vshapes.model.StudentTScalarModel;
import io.nosqlbench.vshapes.model.UniformScalarModel;

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
/// ScalarModel model = ...; // from VectorSpaceModel.scalarModel(d)
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
    /// @param model the scalar model
    /// @return a ComponentSampler bound to the model's parameters
    /// @throws IllegalArgumentException if the model type is not supported
    public static ComponentSampler forModel(ScalarModel model) {
        if (model instanceof NormalScalarModel) {
            return new NormalSampler((NormalScalarModel) model);
        } else if (model instanceof UniformScalarModel) {
            return new UniformSampler((UniformScalarModel) model);
        } else if (model instanceof BetaScalarModel) {
            return new BetaSampler((BetaScalarModel) model);
        } else if (model instanceof GammaScalarModel) {
            return new GammaSampler((GammaScalarModel) model);
        } else if (model instanceof StudentTScalarModel) {
            return new StudentTSampler((StudentTScalarModel) model);
        } else if (model instanceof InverseGammaScalarModel) {
            return new InverseGammaSampler((InverseGammaScalarModel) model);
        } else if (model instanceof BetaPrimeScalarModel) {
            return new BetaPrimeSampler((BetaPrimeScalarModel) model);
        } else if (model instanceof PearsonIVScalarModel) {
            return new PearsonIVSampler((PearsonIVScalarModel) model);
        } else if (model instanceof EmpiricalScalarModel) {
            return new EmpiricalSampler((EmpiricalScalarModel) model);
        } else if (model instanceof CompositeScalarModel) {
            return new CompositeSampler((CompositeScalarModel) model);
        } else {
            throw new IllegalArgumentException(
                "No sampler for model type: " + model.getClass().getName());
        }
    }

    /// Creates an array of samplers for the given models.
    ///
    /// Convenience method for creating samplers for all dimensions at once.
    ///
    /// @param models the scalar models
    /// @return an array of bound samplers
    public static ComponentSampler[] forModels(ScalarModel[] models) {
        ComponentSampler[] samplers = new ComponentSampler[models.length];
        for (int i = 0; i < models.length; i++) {
            samplers[i] = forModel(models[i]);
        }
        return samplers;
    }
}
