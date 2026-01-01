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

/// First-order tensor model representing a single-dimensional distribution.
///
/// ## Purpose
///
/// ScalarModel is the base type for all per-dimension distribution models.
/// Each dimension in a [VectorModel] can have its own ScalarModel with
/// different distribution parameters.
///
/// ## Tensor Hierarchy
///
/// The tensor model hierarchy spans three levels of abstraction:
///
/// ```
/// ┌─────────────────────────────────────────────────────────────────────┐
/// │                      TENSOR MODEL HIERARCHY                         │
/// └─────────────────────────────────────────────────────────────────────┘
///
///   First-Order           Second-Order            Third-Order
///  ┌─────────────┐       ┌─────────────┐         ┌─────────────┐
///  │ ScalarModel │──────►│ VectorModel │────────►│ MatrixModel │
///  │ (1 dim)     │   M   │ (M dims)    │    K    │ (K vectors) │
///  └─────────────┘       └─────────────┘         └─────────────┘
///        │                     │                       │
///        ▼                     ▼                       ▼
///   Distribution          Vector of M            Collection of K
///   for single            ScalarModels           VectorModels
///   dimension
/// ```
///
/// ## ScalarModel Anatomy
///
/// Each ScalarModel implementation encapsulates a probability distribution:
///
/// ```
/// ┌───────────────────────────────────────────────────────┐
/// │                    SCALAR MODEL                       │
/// ├───────────────────────────────────────────────────────┤
/// │ modelType: "gaussian" | "uniform" | "empirical" | ... │
/// ├───────────────────────────────────────────────────────┤
/// │ DISTRIBUTION PARAMETERS (type-specific):              │
/// │                                                       │
/// │   Gaussian:  μ (mean), σ (stdDev), [lower, upper]     │
/// │   Uniform:   lower, upper                             │
/// │   Empirical: binEdges[], cdf[]                        │
/// │   Composite: models[], weights[]                      │
/// └───────────────────────────────────────────────────────┘
///                        │
///                        ▼ samples via u ∈ [0,1)
///                  ┌─────────────┐
///                  │   value     │
///                  └─────────────┘
/// ```
///
/// ## Design Principle
///
/// Each concrete model type defines only the parameters natural to its
/// distribution. There are no forced generic measures - a Gaussian model
/// has mean/stdDev, a Uniform model has lower/upper, an Empirical model
/// has binEdges/cdf, etc.
///
/// ## Models vs Samplers
///
/// ScalarModel is a pure data description. It holds distribution
/// parameters but does not know how to generate samples. Sampling is
/// handled by ScalarSampler implementations in the virtdata module,
/// where each sampler binds to its specific model type.
///
/// ## Implementations
///
/// | Model Type | Distribution | Parameters |
/// |------------|--------------|------------|
/// | [GaussianScalarModel] | Normal N(μ, σ²) | mean, stdDev, [lower, upper] |
/// | [UniformScalarModel] | Uniform | lower, upper |
/// | [EmpiricalScalarModel] | Histogram-based | binEdges, cdf |
/// | [CompositeScalarModel] | Mixture | models, weights |
///
/// @see VectorModel
/// @see MatrixModel
public interface ScalarModel {

    /**
     * Returns the model type identifier for serialization.
     *
     * <p>This string is used in JSON serialization to identify the
     * concrete implementation type, enabling polymorphic deserialization.
     *
     * @return the model type identifier (e.g., "gaussian", "uniform", "empirical")
     */
    String getModelType();
}
