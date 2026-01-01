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

/// # Tensor Model Hierarchy
///
/// This package defines a three-level tensor model hierarchy for representing
/// statistical distributions over vector spaces.
///
/// ## Model Hierarchy
///
/// ```text
/// ┌─────────────────────────────────────────────────────────────────────────┐
/// │                      TENSOR MODEL HIERARCHY                             │
/// └─────────────────────────────────────────────────────────────────────────┘
///
///   First-Order           Second-Order            Third-Order
///  ┌─────────────┐       ┌─────────────┐         ┌─────────────┐
///  │ ScalarModel │──────►│ VectorModel │────────►│ MatrixModel │
///  │ (1 dim)     │   M   │ (M dims)    │    K    │ (K vectors) │
///  └─────────────┘       └─────────────┘         └─────────────┘
///        │                     │                       │
///        ▼                     ▼                       ▼
/// ┌─────────────────┐  ┌────────────────┐     ┌───────────────┐
/// │ GaussianScalar  │  │ VectorSpaceModel│     │ (future impls) │
/// │ UniformScalar   │  │ (implements)   │     │               │
/// │ EmpiricalScalar │  └────────────────┘     └───────────────┘
/// │ CompositeScalar │
/// └─────────────────┘
/// ```
///
/// ## Interface Definitions
///
/// ### ScalarModel (First-Order)
/// A single-dimensional distribution model. Each dimension in a vector space
/// can have its own ScalarModel with different parameters.
///
/// Implementations:
/// - {@link GaussianScalarModel} - Normal distribution N(mu, sigma^2)
/// - {@link UniformScalarModel} - Uniform distribution over [lower, upper]
/// - {@link EmpiricalScalarModel} - Histogram-based empirical distribution
/// - {@link CompositeScalarModel} - Mixture of multiple distributions
///
/// ### VectorModel (Second-Order)
/// A vector space model with N unique vectors and M dimensions.
/// Each dimension has its own ScalarModel defining its distribution.
///
/// Implementations:
/// - {@link VectorSpaceModel} - Primary implementation
///
/// ### MatrixModel (Third-Order)
/// A collection of VectorModels for multi-cluster or hierarchical data.
/// Currently interface-only; implementations deferred.
///
/// ## Legacy Compatibility
///
/// The following legacy types are deprecated but maintained for backward compatibility:
///
/// | Legacy Type | New Type |
/// |-------------|----------|
/// | ComponentModel | ScalarModel |
/// | GaussianComponentModel | GaussianScalarModel |
/// | UniformComponentModel | UniformScalarModel |
/// | EmpiricalComponentModel | EmpiricalScalarModel |
/// | CompositeComponentModel | CompositeScalarModel |
///
/// The *ComponentModel classes still work and implement ScalarModel via ComponentModel.
/// New code should use the *ScalarModel classes directly.
///
/// ## Design Principle
///
/// **Models are pure data descriptions.** They hold distribution parameters but
/// do not contain sampling logic. Sampling is handled by ScalarSampler implementations
/// in the virtdata module.
///
/// @see ScalarModel
/// @see VectorModel
/// @see MatrixModel
/// @see VectorSpaceModel
package io.nosqlbench.vshapes.model;
