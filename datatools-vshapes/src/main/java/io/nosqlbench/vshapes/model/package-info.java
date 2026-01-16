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

/// # Tensor Model Hierarchy with Pearson Distribution System
///
/// This package defines a three-level tensor model hierarchy for representing
/// statistical distributions over vector spaces, using the Pearson distribution
/// system for classification.
///
/// ## Pearson Distribution System
///
/// Karl Pearson's distribution system classifies continuous probability
/// distributions based on their first four moments, specifically:
/// - β₁ = skewness² (squared skewness)
/// - β₂ = kurtosis (standard kurtosis, not excess)
///
/// ```text
/// ┌─────────────────────────────────────────────────────────────────────────┐
/// │                    PEARSON DISTRIBUTION SYSTEM                          │
/// └─────────────────────────────────────────────────────────────────────────┘
///
///  Type │ Distribution      │ β₁    │ β₂      │ Support     │ Implementation
/// ──────┼───────────────────┼───────┼─────────┼─────────────┼─────────────────
///   0   │ Normal            │ 0     │ 3       │ (-∞, +∞)    │ NormalScalarModel
///   I   │ Beta              │ varies│ < 3     │ [a, b]      │ BetaScalarModel
///  II   │ Symmetric Beta    │ 0     │ < 3     │ [a, b]      │ BetaScalarModel
///  III  │ Gamma             │ varies│ varies  │ [0, +∞)     │ GammaScalarModel
///  IV   │ Pearson IV        │ varies│ varies  │ (-∞, +∞)    │ PearsonIVScalarModel
///   V   │ Inverse Gamma     │ varies│ varies  │ (0, +∞)     │ InverseGammaScalarModel
///  VI   │ Beta Prime (F)    │ varies│ varies  │ (0, +∞)     │ BetaPrimeScalarModel
///  VII  │ Student's t       │ 0     │ > 3     │ (-∞, +∞)    │ StudentTScalarModel
/// ──────┴───────────────────┴───────┴─────────┴─────────────┴─────────────────
/// ```
///
/// ### Classification Criterion
///
/// The discriminant criterion κ (kappa) classifies distributions:
///
/// ```text
/// κ = β₁(β₂ + 3)² / [4(2β₂ - 3β₁ - 6)(4β₂ - 3β₁)]
///
/// Classification:
///   κ < 0  → Type I (Beta)
///   κ = 0  → Type III (Gamma) - on the "gamma line"
///   0 < κ < 1 → Type IV
///   κ = 1  → Type V (Inverse Gamma)
///   κ > 1  → Type VI (Beta Prime)
///
/// For symmetric distributions (β₁ = 0):
///   β₂ < 3 → Type II (Symmetric Beta)
///   β₂ = 3 → Type 0 (Normal)
///   β₂ > 3 → Type VII (Student's t)
/// ```
///
/// ## Tensor Model Hierarchy
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
/// │ NormalScalar    │  │VectorSpaceModel│     │ (future impls)│
/// │ BetaScalar      │  │ (implements)   │     │               │
/// │ GammaScalar     │  └────────────────┘     └───────────────┘
/// │ StudentTScalar  │
/// │ InverseGamma    │
/// │ BetaPrime       │
/// │ PearsonIV       │
/// │ UniformScalar   │
/// │ EmpiricalScalar │
/// │ CompositeScalar │
/// └─────────────────┘
/// ```
///
/// ## ScalarModel Implementations (First-Order)
///
/// ### Pearson Type 0: Normal Distribution
/// {@link NormalScalarModel} - Standard Gaussian/normal distribution N(μ, σ²)
/// - Parameters: mean (μ), standard deviation (σ)
/// - Support: (-∞, +∞)
/// - Truncation support: can be bounded to [lower, upper]
///
/// ### Pearson Type I: Beta Distribution
/// {@link BetaScalarModel} - Flexible bounded distribution
/// - Parameters: alpha (α), beta (β), lower, upper
/// - Support: [lower, upper] (default [0, 1])
/// - Special case: α = β gives symmetric Type II
///
/// ### Pearson Type III: Gamma Distribution
/// {@link GammaScalarModel} - Right-skewed, semi-bounded
/// - Parameters: shape (k), scale (θ), location (γ)
/// - Support: [γ, +∞)
/// - Special cases: k=1 is exponential, k=n/2,θ=2 is chi-squared(n)
///
/// ### Pearson Type IV: General Asymmetric
/// {@link PearsonIVScalarModel} - Asymmetric unbounded distribution
/// - Parameters: m (shape), ν (skewness), a (scale), λ (location)
/// - Support: (-∞, +∞)
/// - For asymmetric data not fitting other types
///
/// ### Pearson Type V: Inverse Gamma
/// {@link InverseGammaScalarModel} - Reciprocal of gamma-distributed variable
/// - Parameters: shape (α), scale (β)
/// - Support: (0, +∞)
/// - Common in Bayesian statistics as conjugate prior
///
/// ### Pearson Type VI: Beta Prime (F-distribution family)
/// {@link BetaPrimeScalarModel} - Beta distribution of the second kind
/// - Parameters: alpha (α), beta (β), scale (σ)
/// - Support: (0, +∞)
/// - F-distribution is a scaled special case
///
/// ### Pearson Type VII: Student's t
/// {@link StudentTScalarModel} - Symmetric heavy-tailed distribution
/// - Parameters: degrees of freedom (ν), location (μ), scale (σ)
/// - Support: (-∞, +∞)
/// - ν=1 gives Cauchy; ν→∞ converges to normal
///
/// ### Non-Pearson Types
/// {@link UniformScalarModel} - Constant density over interval
/// {@link EmpiricalScalarModel} - Histogram-based from observed data
/// {@link CompositeScalarModel} - Mixture of multiple distributions
///
/// ## Design Principle
///
/// **Models are pure data descriptions.** They hold distribution parameters but
/// do not contain sampling logic. Sampling is handled by ComponentSampler implementations
/// in the virtdata module.
///
/// @see ScalarModel
/// @see VectorModel
/// @see MatrixModel
/// @see VectorSpaceModel
/// @see PearsonType
package io.nosqlbench.vshapes.model;
