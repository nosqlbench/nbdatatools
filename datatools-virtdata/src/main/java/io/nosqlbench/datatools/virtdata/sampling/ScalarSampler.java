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

/// Sampler that produces variates from a bound ScalarModel distribution.
///
/// # Tensor Hierarchy
///
/// ScalarSampler corresponds to the first-order tensor level (ScalarModel):
/// - ScalarSampler - First-order (samples from a single ScalarModel)
/// - VectorGenerator - Second-order (generates vectors from VectorModel)
///
/// # Overview
///
/// ScalarSampler is functionally equivalent to {@link ComponentSampler} and is
/// provided for consistency with the tensor model terminology. This interface
/// extends ComponentSampler to maintain backward compatibility.
///
/// ```text
/// ┌─────────────────────────────────────────────────────────────────────────┐
/// │                         SAMPLING ARCHITECTURE                           │
/// └─────────────────────────────────────────────────────────────────────────┘
///
///   Construction (once)                    Sampling (per value)
///  ┌─────────────────┐                   ┌─────────────────┐
///  │   ScalarModel   │                   │   double u      │
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
/// ScalarSampler sampler = ScalarSamplerFactory.forModel(model);
///
/// // Sample with just u - no model reference needed
/// double value = sampler.sample(0.5);
/// ```
///
/// @see ScalarSamplerFactory
/// @see io.nosqlbench.vshapes.model.ScalarModel
@FunctionalInterface
public interface ScalarSampler extends ComponentSampler {
    // Inherits sample(double u) from ComponentSampler
}
