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

/// A [VectorModel] that can indicate whether all dimensions share the same
/// distribution type, enabling optimized vector-wise sampling strategies.
///
/// ## Purpose
///
/// When all dimensions of a vector model have the same [ScalarModel] type
/// (e.g., all Gaussian), samplers can use vectorized implementations that
/// are significantly faster than component-wise sampling. This interface
/// provides the contract for sampler resolvers to detect and exploit this
/// optimization opportunity.
///
/// ## Isomorphism Definition
///
/// A vector model is **isomorphic** when:
/// 1. All M dimensions use the same ScalarModel implementation class
/// 2. The sampler can apply a uniform algorithm across all dimensions
///
/// ```
/// ┌─────────────────────────────────────────────────────────────────────┐
/// │                    ISOMORPHIC vs HETEROGENEOUS                      │
/// └─────────────────────────────────────────────────────────────────────┘
///
///   ISOMORPHIC (all same type):              HETEROGENEOUS (mixed types):
///   ┌─────────────────────────┐              ┌─────────────────────────┐
///   │ dim 0: Gaussian(μ₀, σ₀) │              │ dim 0: Gaussian(μ₀, σ₀) │
///   │ dim 1: Gaussian(μ₁, σ₁) │              │ dim 1: Uniform(a₁, b₁)  │
///   │ dim 2: Gaussian(μ₂, σ₂) │              │ dim 2: Empirical(hist₂) │
///   │ ...                     │              │ ...                     │
///   └─────────────────────────┘              └─────────────────────────┘
///           │                                        │
///           ▼                                        ▼
///   Vector-wise sampler                      Component-wise sampler
///   (single algorithm)                       (per-dimension dispatch)
/// ```
///
/// ## Sampler Resolution Strategy
///
/// Sampler resolvers can use this interface for optimization:
///
/// ```java
/// VectorSampler resolveSampler(VectorModel model) {
///     if (model instanceof IsomorphicVectorModel ivm && ivm.isIsomorphic()) {
///         Class<?> type = ivm.scalarModelClass();
///         if (type == NormalScalarModel.class) {
///             return new VectorizedNormalSampler(model);
///         }
///         // ... other vectorized implementations
///     }
///     // Fall back to component-wise sampling
///     return new ComponentWiseSampler(model);
/// }
/// ```
///
/// ## Parameter Uniformity
///
/// Note that isomorphism refers to **type uniformity**, not **parameter uniformity**.
/// An isomorphic Gaussian model may have different μ and σ per dimension:
///
/// | Dimension | Type | Parameters |
/// |-----------|------|------------|
/// | 0 | Gaussian | μ=0.1, σ=0.05 |
/// | 1 | Gaussian | μ=-0.3, σ=0.08 |
/// | 2 | Gaussian | μ=0.0, σ=0.12 |
///
/// This is still isomorphic because the sampling algorithm (inverse Gaussian CDF)
/// is the same for all dimensions, just with different parameters.
///
/// @see VectorModel
/// @see ScalarModel
public interface IsomorphicVectorModel extends VectorModel {

    /// Returns whether all dimensions use the same [ScalarModel] implementation type.
    ///
    /// When this returns `true`, the sampler resolver can safely use a vectorized
    /// sampling strategy that applies the same algorithm across all dimensions.
    ///
    /// @return true if all M dimensions share the same ScalarModel class
    boolean isIsomorphic();

    /// Returns a representative [ScalarModel] instance from this model.
    ///
    /// When [#isIsomorphic()] returns `true`, this provides a sample instance
    /// that can be used to determine the appropriate sampler type. The returned
    /// model is typically `scalarModel(0)`, but implementations may choose any
    /// representative dimension.
    ///
    /// @return a representative ScalarModel, or null if the model is empty
    /// @throws IllegalStateException if called when [#isIsomorphic()] is false
    ScalarModel representativeScalarModel();

    /// Returns the [ScalarModel] implementation class used across all dimensions.
    ///
    /// When [#isIsomorphic()] returns `true`, this provides the concrete class
    /// for type-based sampler dispatch without needing to inspect individual
    /// dimensions.
    ///
    /// @return the Class of the ScalarModel implementation
    /// @throws IllegalStateException if called when [#isIsomorphic()] is false
    Class<? extends ScalarModel> scalarModelClass();

    /// Returns the model type identifier shared by all dimensions.
    ///
    /// This is equivalent to `representativeScalarModel().getModelType()` but
    /// may be more efficient for implementations that cache this value.
    ///
    /// @return the model type string (e.g., "gaussian", "uniform")
    /// @throws IllegalStateException if called when [#isIsomorphic()] is false
    default String isomorphicModelType() {
        if (!isIsomorphic()) {
            throw new IllegalStateException("Cannot get isomorphic model type: model is heterogeneous");
        }
        return representativeScalarModel().getModelType();
    }
}
