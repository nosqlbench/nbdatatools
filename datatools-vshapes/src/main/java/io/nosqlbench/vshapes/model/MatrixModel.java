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

/// Third-order tensor model representing a collection of VectorModels.
///
/// ## Purpose
///
/// MatrixModel enables modeling of structured collections of vector spaces,
/// such as multi-cluster datasets, hierarchical embeddings, or correlated
/// vector distributions.
///
/// ## Tensor Hierarchy
///
/// MatrixModel is the third and highest level in the tensor model hierarchy:
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
///                                                      ▲
///                                                      │
///                                                 YOU ARE HERE
/// ```
///
/// ## MatrixModel Anatomy
///
/// A MatrixModel contains K independent VectorModels, each with its own parameters:
///
/// ```
/// ┌─────────────────────────────────────────────────────────────────────┐
/// │                         MATRIX MODEL                                │
/// └─────────────────────────────────────────────────────────────────────┘
///
/// ┌─────────────────────────────────────────────────────────────────────┐
/// │ K = vectorModelCount()    (number of vector models, e.g., 5)        │
/// └─────────────────────────────────────────────────────────────────────┘
///                                  │
///                                  ▼
/// ┌─────────────────────────────────────────────────────────────────────┐
/// │ VECTOR MODELS                                                       │
/// │                                                                     │
/// │   vectorModel(0): VectorModel(N₀=100K,  M₀=128, ScalarModel[]₀)     │
/// │   vectorModel(1): VectorModel(N₁=500K,  M₁=128, ScalarModel[]₁)     │
/// │   vectorModel(2): VectorModel(N₂=1M,    M₂=256, ScalarModel[]₂)     │
/// │   ...                                                               │
/// │   vectorModel(K-1): VectorModel(Nₖ₋₁, Mₖ₋₁, ScalarModel[]ₖ₋₁)       │
/// │                                                                     │
/// │   Each VectorModel can have different N, M, and distributions!      │
/// └─────────────────────────────────────────────────────────────────────┘
///                                  │
///                                  ▼
/// ┌─────────────────────────────────────────────────────────────────────┐
/// │ OPTIONAL METADATA (implementation-specific)                         │
/// │                                                                     │
/// │   • Cluster weights:     [w₀, w₁, w₂, ..., wₖ₋₁]                    │
/// │   • Correlation matrix:  K×K matrix of cross-cluster correlations  │
/// │   • Hierarchy metadata:  parent-child relationships                 │
/// │   • Mixing proportions:  probability of selecting each cluster     │
/// └─────────────────────────────────────────────────────────────────────┘
/// ```
///
/// ## Cluster-Based Generation Example
///
/// MatrixModels enable multi-cluster dataset generation:
///
/// ```
///                    ┌─────────────────────┐
///   ordinal ─────────►│    MatrixModel     │
///                    │     Generator       │
///                    └─────────┬───────────┘
///                              │
///          ┌───────────────────┼───────────────────┐
///          ▼                   ▼                   ▼
///   ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
///   │  Cluster 0  │     │  Cluster 1  │     │  Cluster 2  │
///   │ VectorModel │     │ VectorModel │     │ VectorModel │
///   └──────┬──────┘     └──────┬──────┘     └──────┬──────┘
///          │                   │                   │
///          ▼                   ▼                   ▼
///      float[M₀]           float[M₁]           float[M₂]
/// ```
///
/// ## Use Cases
///
/// | Use Case | Description |
/// |----------|-------------|
/// | **Multi-cluster datasets** | Each VectorModel represents a distinct cluster |
/// | **Hierarchical embeddings** | Parent vectors → child vectors relationships |
/// | **Correlated distributions** | Cross-vector statistical dependencies |
/// | **Multi-modal data** | Different embedding spaces for text/image/audio |
/// | **Synthetic benchmarks** | Controlled similarity neighborhoods for testing |
///
/// ## Implementation Note
///
/// This interface defines the minimum contract for MatrixModel. Implementations
/// may include additional fields such as correlation matrices, cluster weights,
/// or hierarchical structure metadata.
///
/// **Note:** Concrete implementations are planned for a future release.
///
/// @see ScalarModel
/// @see VectorModel
public interface MatrixModel {

    /**
     * Returns the number of VectorModels in this matrix.
     *
     * @return K, the number of vector models
     */
    int vectorModelCount();

    /**
     * Returns a specific VectorModel.
     *
     * @param index the vector model index (0-based, must be in [0, K-1])
     * @return the vector model at that index
     * @throws IndexOutOfBoundsException if index is out of range
     */
    VectorModel vectorModel(int index);

    /**
     * Returns all VectorModels.
     *
     * @return a defensive copy of the vector models array
     */
    VectorModel[] vectorModels();
}
