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

/// Sampler that produces variates from a bound distribution.
///
/// # Overview
///
/// ComponentSampler is bound to a specific distribution at construction time.
/// All distribution parameters are extracted and stored, so sampling requires
/// only the uniform input value - no model reference, no type checks, no casting.
///
/// ```text
/// ┌─────────────────────────────────────────────────────────────────────────┐
/// │                         SAMPLING ARCHITECTURE                           │
/// └─────────────────────────────────────────────────────────────────────────┘
///
///   Construction (once)                    Sampling (per value)
///  ┌─────────────────┐                   ┌─────────────────┐
///  │ ComponentModel  │                   │   double u      │
///  │ mean, stdDev,   │ ──► Sampler ◄──── │   ∈ (0,1)       │
///  │ bounds, type    │     (bound)       └────────┬────────┘
///  └─────────────────┘                            │
///                                                 ▼
///                                        ┌─────────────────┐
///                                        │  double value   │
///                                        │  (variate)      │
///                                        └─────────────────┘
/// ```
///
/// # Usage
///
/// ```java
/// // Create sampler bound to model (extracts parameters)
/// ComponentSampler sampler = ComponentSamplerFactory.forModel(model);
///
/// // Sample with just u - no model reference needed
/// double value = sampler.sample(0.5);
/// ```
///
/// @see ComponentSamplerFactory
@FunctionalInterface
public interface ComponentSampler {

    /// Samples a value from this distribution.
    ///
    /// Uses inverse CDF transform sampling: given a uniform value
    /// u ∈ (0,1), returns the corresponding quantile.
    ///
    /// @param u a value in the open interval (0, 1)
    /// @return a sample from the distribution
    double sample(double u);
}
