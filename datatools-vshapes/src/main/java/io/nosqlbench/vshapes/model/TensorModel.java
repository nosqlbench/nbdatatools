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

/// Base interface for all tensor model types in the hierarchy.
///
/// ## Purpose
///
/// TensorModel provides a common base for all model types in the tensor
/// hierarchy, enabling polymorphic handling while preserving type-specific
/// operations through the sub-interfaces.
///
/// ## Tensor Model Hierarchy
///
/// ```
/// ┌─────────────────────────────────────────────────────────────────────────┐
/// │                      TENSOR MODEL HIERARCHY                             │
/// └─────────────────────────────────────────────────────────────────────────┘
///
///                          ┌─────────────────┐
///                          │   TensorModel   │
///                          │  (base type)    │
///                          │                 │
///                          │ + getModelShape()│
///                          │ + getModelType()│
///                          └────────┬────────┘
///                                   │
///           ┌───────────────────────┼───────────────────────┐
///           │                       │                       │
///           ▼                       ▼                       ▼
///  ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
///  │   ScalarModel   │     │   VectorModel   │     │   MatrixModel   │
///  │   (order=0)     │     │   (order=1)     │     │   (order=2)     │
///  └─────────────────┘     └─────────────────┘     └─────────────────┘
///           │                       │                       │
///           ▼                       ▼                       ▼
///  NormalScalarModel       VectorSpaceModel           (future)
///  UniformScalarModel
///  BetaScalarModel
///  ...
/// ```
///
/// ## Design Principles
///
/// - **Shape Identification:** Every model knows its tensor order via [#getModelShape()]
/// - **Type Discrimination:** Every model has a type identifier via [#getModelType()]
/// - **Polymorphism:** Code can handle models generically while preserving type safety
///
/// ## Serialization
///
/// The [#getModelType()] method returns a string identifier used for
/// JSON serialization. Combined with the [ModelType] annotation on
/// implementations, this enables polymorphic serialization and deserialization.
///
/// @see ModelShape
/// @see ModelType
/// @see ScalarModel
/// @see VectorModel
/// @see MatrixModel
public interface TensorModel {

    /// Returns the model shape (tensor order) of this model.
    ///
    /// The shape indicates the dimensionality rank:
    /// - [ModelShape#SCALAR] - Order 0: single distribution
    /// - [ModelShape#VECTOR] - Order 1: vector of distributions
    /// - [ModelShape#MATRIX] - Order 2: matrix of distributions
    ///
    /// @return the model shape
    ModelShape getModelShape();

    /// Returns the model type identifier for serialization.
    ///
    /// This string is used in JSON serialization to identify the
    /// concrete implementation type, enabling polymorphic deserialization.
    ///
    /// Examples: "normal", "uniform", "beta", "vector_space", etc.
    ///
    /// @return the model type identifier
    String getModelType();
}
