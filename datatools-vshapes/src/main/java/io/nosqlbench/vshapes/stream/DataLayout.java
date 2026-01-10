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

/// Describes the memory layout of vector data in chunks.
///
/// ## Layouts
///
/// Vector data can be organized in two layouts:
///
/// ```text
/// ┌─────────────────────────────────────────────────────────────────────────┐
/// │                         DATA LAYOUT FORMATS                             │
/// └─────────────────────────────────────────────────────────────────────────┘
///
///   ROW_MAJOR (vectors × dimensions):
///   ┌─────────────────────────────────┐
///   │ chunk[0] = [x₀, y₀, z₀]  ← v₀  │  Each row is one vector
///   │ chunk[1] = [x₁, y₁, z₁]  ← v₁  │
///   │ chunk[2] = [x₂, y₂, z₂]  ← v₂  │
///   └─────────────────────────────────┘
///
///   COLUMNAR (dimensions × vectors):
///   ┌─────────────────────────────────┐
///   │ chunk[0] = [x₀, x₁, x₂]  ← dim0│  Each row is one dimension
///   │ chunk[1] = [y₀, y₁, y₂]  ← dim1│
///   │ chunk[2] = [z₀, z₁, z₂]  ← dim2│
///   └─────────────────────────────────┘
/// ```
///
/// ## Usage
///
/// Callers must explicitly handle the layout when accessing chunk data:
///
/// ```java
/// DataspaceShape shape = source.getShape();
/// for (float[][] chunk : source.chunks(1000)) {
///     int vectorCount = shape.layout().vectorCount(chunk);
///     int dims = shape.layout().dimensionCount(chunk);
///
///     for (int v = 0; v < vectorCount; v++) {
///         for (int d = 0; d < dims; d++) {
///             float value = shape.layout().getValue(chunk, v, d);
///             // process value
///         }
///     }
/// }
/// ```
///
/// @see DataspaceShape
public enum DataLayout {

    /**
     * Row-major layout: chunk[vectorIndex][dimensionIndex].
     *
     * <p>Each row in the chunk array represents one complete vector.
     * This is the natural layout for most vector operations.
     */
    ROW_MAJOR {
        @Override
        public int vectorCount(float[][] chunk) {
            return chunk.length;
        }

        @Override
        public int dimensionCount(float[][] chunk) {
            return chunk.length > 0 ? chunk[0].length : 0;
        }

        @Override
        public float getValue(float[][] chunk, int vectorIndex, int dimensionIndex) {
            return chunk[vectorIndex][dimensionIndex];
        }

        @Override
        public void setValue(float[][] chunk, int vectorIndex, int dimensionIndex, float value) {
            chunk[vectorIndex][dimensionIndex] = value;
        }

        @Override
        public float[] getVector(float[][] chunk, int vectorIndex) {
            return chunk[vectorIndex];
        }

        @Override
        public float[] getDimensionValues(float[][] chunk, int dimensionIndex) {
            float[] values = new float[chunk.length];
            for (int v = 0; v < chunk.length; v++) {
                values[v] = chunk[v][dimensionIndex];
            }
            return values;
        }

        @Override
        public float[][] toColumnar(float[][] chunk) {
            if (chunk.length == 0) return new float[0][];
            int vectors = chunk.length;
            int dims = chunk[0].length;
            float[][] columnar = new float[dims][vectors];
            for (int v = 0; v < vectors; v++) {
                for (int d = 0; d < dims; d++) {
                    columnar[d][v] = chunk[v][d];
                }
            }
            return columnar;
        }

        @Override
        public float[][] toRowMajor(float[][] chunk) {
            return chunk; // Already row-major
        }
    },

    /**
     * Column-major (columnar) layout: chunk[dimensionIndex][vectorIndex].
     *
     * <p>Each row in the chunk array represents all values for one dimension.
     * This layout is optimal for per-dimension statistical operations.
     */
    COLUMNAR {
        @Override
        public int vectorCount(float[][] chunk) {
            return chunk.length > 0 ? chunk[0].length : 0;
        }

        @Override
        public int dimensionCount(float[][] chunk) {
            return chunk.length;
        }

        @Override
        public float getValue(float[][] chunk, int vectorIndex, int dimensionIndex) {
            return chunk[dimensionIndex][vectorIndex];
        }

        @Override
        public void setValue(float[][] chunk, int vectorIndex, int dimensionIndex, float value) {
            chunk[dimensionIndex][vectorIndex] = value;
        }

        @Override
        public float[] getVector(float[][] chunk, int vectorIndex) {
            float[] vector = new float[chunk.length];
            for (int d = 0; d < chunk.length; d++) {
                vector[d] = chunk[d][vectorIndex];
            }
            return vector;
        }

        @Override
        public float[] getDimensionValues(float[][] chunk, int dimensionIndex) {
            return chunk[dimensionIndex];
        }

        @Override
        public float[][] toColumnar(float[][] chunk) {
            return chunk; // Already columnar
        }

        @Override
        public float[][] toRowMajor(float[][] chunk) {
            if (chunk.length == 0) return new float[0][];
            int dims = chunk.length;
            int vectors = chunk[0].length;
            float[][] rowMajor = new float[vectors][dims];
            for (int d = 0; d < dims; d++) {
                for (int v = 0; v < vectors; v++) {
                    rowMajor[v][d] = chunk[d][v];
                }
            }
            return rowMajor;
        }
    };

    /// Returns the number of vectors in the chunk.
    ///
    /// @param chunk the data chunk
    /// @return vector count
    public abstract int vectorCount(float[][] chunk);

    /// Returns the number of dimensions per vector.
    ///
    /// @param chunk the data chunk
    /// @return dimension count
    public abstract int dimensionCount(float[][] chunk);

    /// Gets a single value from the chunk.
    ///
    /// @param chunk the data chunk
    /// @param vectorIndex the vector index
    /// @param dimensionIndex the dimension index
    /// @return the value at [vector, dimension]
    public abstract float getValue(float[][] chunk, int vectorIndex, int dimensionIndex);

    /// Sets a single value in the chunk.
    ///
    /// @param chunk the data chunk
    /// @param vectorIndex the vector index
    /// @param dimensionIndex the dimension index
    /// @param value the value to set
    public abstract void setValue(float[][] chunk, int vectorIndex, int dimensionIndex, float value);

    /// Gets a complete vector from the chunk.
    ///
    /// @param chunk the data chunk
    /// @param vectorIndex the vector index
    /// @return the vector as a float array
    public abstract float[] getVector(float[][] chunk, int vectorIndex);

    /// Gets all values for a single dimension across all vectors.
    ///
    /// @param chunk the data chunk
    /// @param dimensionIndex the dimension index
    /// @return all values for that dimension
    public abstract float[] getDimensionValues(float[][] chunk, int dimensionIndex);

    /// Converts the chunk to columnar layout.
    ///
    /// @param chunk the data chunk
    /// @return the chunk in columnar format (may be same reference if already columnar)
    public abstract float[][] toColumnar(float[][] chunk);

    /// Converts the chunk to row-major layout.
    ///
    /// @param chunk the data chunk
    /// @return the chunk in row-major format (may be same reference if already row-major)
    public abstract float[][] toRowMajor(float[][] chunk);
}
