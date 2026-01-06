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

import io.nosqlbench.vshapes.model.UniformScalarModel;

/// Sampler for Uniform distributions, bound at construction.
///
/// Uses simple linear interpolation to map uniform samples to the
/// bounded interval [lower, upper].
public final class UniformSampler implements ComponentSampler {

    private final double lower;
    private final double range;

    /// Creates a sampler bound to the given Uniform model.
    ///
    /// @param model the Uniform component model
    public UniformSampler(UniformScalarModel model) {
        this.lower = model.getLower();
        this.range = model.getRange();
    }

    @Override
    public double sample(double u) {
        return lower + u * range;
    }
}
