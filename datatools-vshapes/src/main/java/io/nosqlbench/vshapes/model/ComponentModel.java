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

/**
 * Legacy interface for per-dimension distribution models.
 *
 * <h2>Deprecation Notice</h2>
 *
 * <p>This interface has been superseded by {@link ScalarModel} as part of
 * the tensor model hierarchy. Use {@link ScalarModel} directly for new code.
 * This interface is retained for backward compatibility and will be removed
 * in a future release.
 *
 * <h2>Migration</h2>
 *
 * <ul>
 *   <li>{@code ComponentModel} → {@link ScalarModel}</li>
 *   <li>{@code GaussianComponentModel} → {@link GaussianScalarModel}</li>
 *   <li>{@code UniformComponentModel} → {@link UniformScalarModel}</li>
 *   <li>{@code EmpiricalComponentModel} → {@link EmpiricalScalarModel}</li>
 *   <li>{@code CompositeComponentModel} → {@link CompositeScalarModel}</li>
 * </ul>
 *
 * <h2>Purpose</h2>
 *
 * <p>Each dimension in a vector space can have its own distribution model.
 * This interface serves as a common type for heterogeneous component models,
 * allowing {@link VectorSpaceModel} to hold different distribution types
 * per dimension.
 *
 * <h2>Design Principle</h2>
 *
 * <p>Each concrete model type defines only the parameters natural to its
 * distribution. There are no forced generic measures - a Gaussian model
 * has mean/stdDev, a Uniform model has lower/upper, an Empirical model
 * has binEdges/cdf, etc.
 *
 * <h2>Models vs Samplers</h2>
 *
 * <p>ComponentModel is a pure data description. It holds distribution
 * parameters but does not know how to generate samples. Sampling is
 * handled by ComponentSampler implementations in the virtdata module,
 * where each sampler binds to its specific model type.
 *
 * <h2>Implementations</h2>
 *
 * <ul>
 *   <li>{@link GaussianComponentModel} - Normal distribution N(μ, σ²)</li>
 *   <li>{@link UniformComponentModel} - Uniform distribution over [lower, upper]</li>
 *   <li>{@link EmpiricalComponentModel} - Histogram-based empirical distribution</li>
 *   <li>{@link CompositeComponentModel} - Mixture of multiple models</li>
 * </ul>
 *
 * @see ScalarModel
 * @see VectorSpaceModel
 * @deprecated Use {@link ScalarModel} directly. This interface is retained
 *             for backward compatibility and will be removed in a future release.
 */
@Deprecated(since = "2.0", forRemoval = true)
public interface ComponentModel extends ScalarModel {
    // getModelType() is inherited from ScalarModel
}
