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
/// handled by ComponentSampler implementations in the virtdata module,
/// where each sampler binds to its specific model type.
///
/// ## Implementations
///
/// | Model Type | Distribution | Parameters |
/// |------------|--------------|------------|
/// | {@link NormalScalarModel} | Normal N(μ, σ²) | mean, stdDev, [lower, upper] |
/// | {@link UniformScalarModel} | Uniform | lower, upper |
/// | {@link EmpiricalScalarModel} | Histogram-based | binEdges, cdf |
/// | {@link CompositeScalarModel} | Mixture | models, weights |
///
/// @see VectorModel
/// @see MatrixModel
/// @see TensorModel
public interface ScalarModel extends TensorModel {

    /// Returns the model shape of this model.
    ///
    /// ScalarModels are always [ModelShape#SCALAR] (order 0).
    ///
    /// @return [ModelShape#SCALAR]
    @Override
    default ModelShape getModelShape() {
        return ModelShape.SCALAR;
    }

    /// Returns the model type identifier for serialization.
    ///
    /// This string is used in JSON serialization to identify the
    /// concrete implementation type, enabling polymorphic deserialization.
    ///
    /// @return the model type identifier (e.g., "normal", "uniform", "empirical")
    @Override
    String getModelType();

    /// Computes the cumulative distribution function (CDF) at a given value.
    ///
    /// The CDF gives the probability P(X ≤ x) for a random variable X
    /// following this distribution.
    ///
    /// ## Purpose
    ///
    /// The CDF method enables uniform goodness-of-fit scoring across all
    /// distribution types. By requiring every ScalarModel to provide its
    /// theoretical CDF, the model fitting framework can use a single
    /// scoring algorithm (Kolmogorov-Smirnov D-statistic) for all models.
    ///
    /// ## Contract
    ///
    /// Implementations must satisfy:
    /// - Return values in [0, 1]
    /// - Be monotonically non-decreasing
    /// - Approach 0 as x → -∞ (or lower bound)
    /// - Approach 1 as x → +∞ (or upper bound)
    ///
    /// @param x the value at which to evaluate the CDF
    /// @return the cumulative probability P(X ≤ x), in range [0, 1]
    double cdf(double x);

    /// Returns this model in canonical form for deterministic comparison.
    ///
    /// For most model types, canonical form is the model itself. For
    /// {@link CompositeScalarModel}, canonical form has components sorted
    /// by their characteristic location (mean/mode).
    ///
    /// Canonical form enables:
    /// - Deterministic serialization and comparison
    /// - Round-trip verification testing
    /// - Consistent ordering of composite components
    ///
    /// @return this model in canonical form
    /// @see ScalarModelComparator
    default ScalarModel toCanonicalForm() {
        return this;  // Default: most models are already canonical
    }
}
