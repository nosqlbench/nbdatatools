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

/// Enumeration of tensor model shapes representing the dimensionality rank.
///
/// ## Tensor Order
///
/// In tensor terminology, the "order" or "rank" of a tensor describes its
/// dimensionality structure:
///
/// ```
/// ┌─────────────────────────────────────────────────────────────────────────┐
/// │                      TENSOR MODEL SHAPES                                │
/// └─────────────────────────────────────────────────────────────────────────┘
///
///   Order 0 (Scalar)      Order 1 (Vector)       Order 2 (Matrix)
///  ┌─────────────┐       ┌─────────────┐         ┌─────────────┐
///  │    value    │       │ [v₀,v₁,...] │         │  ┌───┬───┐  │
///  │             │       │             │         │  │   │   │  │
///  │  single     │       │  1D array   │         │  ├───┼───┤  │
///  │  number     │       │  of values  │         │  │   │   │  │
///  └─────────────┘       └─────────────┘         │  └───┴───┘  │
///                                                │  2D array   │
///                                                └─────────────┘
/// ```
///
/// ## Model Hierarchy Mapping
///
/// | Shape  | Order | Interface                | Contains               |
/// |--------|-------|--------------------------|------------------------|
/// | SCALAR | 0     | [ScalarModel]            | Distribution parameters|
/// | VECTOR | 1     | [VectorModel]            | M ScalarModels         |
/// | MATRIX | 2     | [MatrixModel]            | K VectorModels         |
///
/// @see TensorModel
/// @see ScalarModel
/// @see VectorModel
/// @see MatrixModel
public enum ModelShape {

    /// First-order tensor (rank 0): A single scalar distribution.
    ///
    /// ScalarModels represent probability distributions for individual
    /// dimension values (e.g., Normal, Uniform, Empirical).
    SCALAR(0, "scalar"),

    /// Second-order tensor (rank 1): A vector of scalar distributions.
    ///
    /// VectorModels contain M ScalarModels, one per dimension, defining
    /// the complete distribution for M-dimensional vectors.
    VECTOR(1, "vector"),

    /// Third-order tensor (rank 2): A matrix of vector distributions.
    ///
    /// MatrixModels contain K VectorModels, enabling multi-cluster or
    /// hierarchical dataset modeling.
    MATRIX(2, "matrix");

    private final int order;
    private final String name;

    ModelShape(int order, String name) {
        this.order = order;
        this.name = name;
    }

    /// Returns the tensor order (rank) of this shape.
    ///
    /// Order 0 = scalar, Order 1 = vector, Order 2 = matrix.
    ///
    /// @return the tensor order
    public int order() {
        return order;
    }

    /// Returns the canonical name of this shape.
    ///
    /// @return the shape name (lowercase)
    public String shapeName() {
        return name;
    }

    /// Returns the ModelShape for a given order.
    ///
    /// @param order the tensor order (0, 1, or 2)
    /// @return the corresponding ModelShape
    /// @throws IllegalArgumentException if order is not 0, 1, or 2
    public static ModelShape fromOrder(int order) {
        return switch (order) {
            case 0 -> SCALAR;
            case 1 -> VECTOR;
            case 2 -> MATRIX;
            default -> throw new IllegalArgumentException(
                "Invalid tensor order: " + order + ". Must be 0, 1, or 2.");
        };
    }

    /// Returns the ModelShape for a given name.
    ///
    /// @param name the shape name (case-insensitive)
    /// @return the corresponding ModelShape
    /// @throws IllegalArgumentException if name is not recognized
    public static ModelShape fromName(String name) {
        return switch (name.toLowerCase()) {
            case "scalar" -> SCALAR;
            case "vector" -> VECTOR;
            case "matrix" -> MATRIX;
            default -> throw new IllegalArgumentException(
                "Unknown shape name: " + name + ". Expected scalar, vector, or matrix.");
        };
    }

    @Override
    public String toString() {
        return name;
    }
}
