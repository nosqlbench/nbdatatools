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

/// Second-order tensor model representing a vector space distribution.
///
/// ## Purpose
///
/// VectorModel defines a space of N unique vectors, each with M dimensions.
/// Each dimension has its own [ScalarModel] defining the distribution
/// for that dimension's values.
///
/// ## Tensor Hierarchy
///
/// VectorModel is the second level in the tensor model hierarchy:
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
///                              ▲
///                              │
///                         YOU ARE HERE
/// ```
///
/// ## VectorModel Anatomy
///
/// A VectorModel captures the complete statistical shape of a vector dataset:
///
/// ```
/// ┌─────────────────────────────────────────────────────────────────────┐
/// │                         VECTOR MODEL                                │
/// └─────────────────────────────────────────────────────────────────────┘
///
/// ┌─────────────────────────────────────────────────────────────────────┐
/// │ SPACE PARAMETERS                                                    │
/// │   N = uniqueVectors()    (cardinality, e.g., 1,000,000)             │
/// │   M = dimensions()       (vector size, e.g., 128)                   │
/// └─────────────────────────────────────────────────────────────────────┘
///                                  │
///                                  ▼
/// ┌─────────────────────────────────────────────────────────────────────┐
/// │ PER-DIMENSION SCALAR MODELS                                         │
/// │                                                                     │
/// │   scalarModel(0): GaussianScalarModel(μ₀=0.12, σ₀=0.03)             │
/// │   scalarModel(1): GaussianScalarModel(μ₁=-0.05, σ₁=0.02)            │
/// │   scalarModel(2): UniformScalarModel(lower₂=-1.0, upper₂=1.0)       │
/// │   ...                                                               │
/// │   scalarModel(M-1): EmpiricalScalarModel(histogram)                 │
/// │                                                                     │
/// │   Each dimension can have a DIFFERENT distribution type!            │
/// └─────────────────────────────────────────────────────────────────────┘
///                                  │
///                                  ▼
///                        ┌─────────────────────┐
///                        │ Generated Vector    │
///                        │ [v₀, v₁, v₂, ..., ] │
///                        │  ↑   ↑   ↑          │
///                        │  │   │   │          │
///                        │  samples from each  │
///                        │  ScalarModel        │
///                        └─────────────────────┘
/// ```
///
/// ## Ordinal-Based Generation
///
/// VectorModels support deterministic generation from ordinal values:
///
/// ```
/// ordinal ∈ [0, N)   ──────────►   VectorGenerator   ──────────►   float[M]
///                                       │
///                                       ▼
///                    ┌──────────────────────────────────────┐
///                    │ For each dimension d ∈ [0, M):       │
///                    │   1. u = hash(ordinal, d) → [0,1)    │
///                    │   2. value = scalarModel(d).sample(u)│
///                    └──────────────────────────────────────┘
/// ```
///
/// ## Heterogeneous Model Support
///
/// VectorModel supports heterogeneous per-dimension distributions:
///
/// | ScalarModel Type | Distribution | Use Case |
/// |------------------|--------------|----------|
/// | [GaussianScalarModel] | Normal N(μ, σ²) | Most real-world embeddings |
/// | [UniformScalarModel] | Uniform [a, b] | Bounded synthetic data |
/// | [EmpiricalScalarModel] | Histogram-based | Exact match to source data |
/// | [CompositeScalarModel] | Mixture | Multi-modal distributions |
///
/// @see ScalarModel
/// @see MatrixModel
/// @see VectorSpaceModel
public interface VectorModel {

    /**
     * Returns the number of unique vectors in the space.
     *
     * <p>This defines N, the cardinality of the vector space. Generators
     * can produce N distinct vectors, each deterministically derived
     * from an ordinal in [0, N-1].
     *
     * @return N, the number of unique vectors that can be generated
     */
    long uniqueVectors();

    /**
     * Returns the dimensionality of the vector space.
     *
     * <p>This defines M, the number of components in each generated vector.
     *
     * @return M, the number of dimensions in each generated vector
     */
    int dimensions();

    /**
     * Returns the distribution model for a specific dimension.
     *
     * @param dimension the dimension index (0-based, must be in [0, M-1])
     * @return the scalar model for that dimension
     * @throws IndexOutOfBoundsException if dimension is out of range
     */
    ScalarModel scalarModel(int dimension);

    /**
     * Returns all scalar models.
     *
     * <p>The returned array has length M (dimensions), with one
     * {@link ScalarModel} per dimension.
     *
     * @return a defensive copy of the scalar models array
     */
    ScalarModel[] scalarModels();
}
