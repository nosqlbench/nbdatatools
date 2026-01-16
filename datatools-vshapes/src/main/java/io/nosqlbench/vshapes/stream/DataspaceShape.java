package io.nosqlbench.vshapes.stream;

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

/// Describes the shape and layout of a vector dataspace.
///
/// ## Purpose
///
/// This record encapsulates the fundamental properties of a vector dataset:
/// - **cardinality** - The total number of vectors in the dataset
/// - **dimensionality** - The number of dimensions per vector
/// - **layout** - The memory layout of chunk data ([DataLayout])
///
/// ## Type-Safe Layout Handling
///
/// The [DataLayout] enum forces callers to explicitly handle the data layout
/// when accessing chunk data. This prevents bugs from assuming a particular
/// memory layout:
///
/// ```java
/// DataspaceShape shape = source.getShape();
/// DataLayout layout = shape.layout();
///
/// for (float[][] chunk : source.chunks(1000)) {
///     // Use layout methods to access data correctly regardless of format
///     int vectorCount = layout.vectorCount(chunk);
///
///     for (int v = 0; v < vectorCount; v++) {
///         float[] vector = layout.getVector(chunk, v);
///         // process vector
///     }
/// }
/// ```
///
/// ## Usage Examples
///
/// ```java
/// // Row-major shape (default)
/// DataspaceShape rowMajor = new DataspaceShape(1_000_000, 128);
///
/// // Columnar shape
/// DataspaceShape columnar = new DataspaceShape(1_000_000, 128, DataLayout.COLUMNAR);
///
/// // Check layout
/// if (shape.layout() == DataLayout.COLUMNAR) {
///     // optimized columnar processing
/// }
///
/// // Convert layout
/// DataspaceShape asColumnar = shape.withLayout(DataLayout.COLUMNAR);
/// ```
///
/// @param cardinality the number of vectors in the dataspace (must be non-negative)
/// @param dimensionality the number of dimensions per vector (must be positive)
/// @param layout the memory layout of chunk data
/// @see DataLayout
public record DataspaceShape(
    long cardinality,
    int dimensionality,
    DataLayout layout
) {

    /// Creates a DataspaceShape with validation.
    public DataspaceShape {
        if (cardinality < 0) {
            throw new IllegalArgumentException("cardinality must be non-negative, got: " + cardinality);
        }
        if (dimensionality <= 0) {
            throw new IllegalArgumentException("dimensionality must be positive, got: " + dimensionality);
        }
        if (layout == null) {
            throw new IllegalArgumentException("layout cannot be null");
        }
    }

    /// Creates a DataspaceShape with row-major layout (default).
    ///
    /// @param cardinality the number of vectors
    /// @param dimensionality the number of dimensions per vector
    public DataspaceShape(long cardinality, int dimensionality) {
        this(cardinality, dimensionality, DataLayout.ROW_MAJOR);
    }

    /// Creates a new DataspaceShape with a different layout.
    ///
    /// @param layout the new layout
    /// @return a new DataspaceShape with the specified layout
    public DataspaceShape withLayout(DataLayout layout) {
        if (this.layout == layout) {
            return this;
        }
        return new DataspaceShape(cardinality, dimensionality, layout);
    }

    /// Returns true if this shape uses columnar layout.
    ///
    /// @return true if layout is [DataLayout#COLUMNAR]
    public boolean isColumnar() {
        return layout == DataLayout.COLUMNAR;
    }

    /// Returns true if this shape uses row-major layout.
    ///
    /// @return true if layout is [DataLayout#ROW_MAJOR]
    public boolean isRowMajor() {
        return layout == DataLayout.ROW_MAJOR;
    }

    /// Creates a columnar version of this shape.
    ///
    /// @return a DataspaceShape with columnar layout
    public DataspaceShape columnar() {
        return withLayout(DataLayout.COLUMNAR);
    }

    /// Creates a row-major version of this shape.
    ///
    /// @return a DataspaceShape with row-major layout
    public DataspaceShape rowMajor() {
        return withLayout(DataLayout.ROW_MAJOR);
    }

    @Override
    public String toString() {
        return String.format("DataspaceShape[cardinality=%d, dimensionality=%d, layout=%s]",
            cardinality, dimensionality, layout);
    }
}
