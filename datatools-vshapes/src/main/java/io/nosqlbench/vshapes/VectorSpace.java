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

package io.nosqlbench.vshapes;

import java.util.Optional;

/// # VectorSpace Interface
///
/// Represents a vector space containing vectors for analysis.
/// This is the primary input data structure for all vector space analysis operations.
///
/// ## Purpose
/// - Provides a unified interface for accessing vector data
/// - Supports both labeled and unlabeled datasets  
/// - Enables caching of computational artifacts via unique identifiers
///
/// ## Usage
/// ```java
/// VectorSpace space = new MyVectorSpace("dataset-id", vectors, labels);
/// int count = space.getVectorCount();
/// float[] vector = space.getVector(0);
/// ```
///
/// ## Implementation Notes
/// - Vectors are expected to have consistent dimensionality
/// - Class labels are optional but required for supervised measures
/// - The `getId()` method should return a stable, unique identifier for caching
public interface VectorSpace {

    /// Gets the number of vectors in this space.
    /// 
    /// @return the vector count
    int getVectorCount();

    /// Gets the dimensionality of vectors in this space.
    /// All vectors in the space must have this dimensionality.
    /// 
    /// @return the vector dimension
    int getDimension();

    /// Gets a vector by index.
    /// 
    /// @param index the vector index (0-based)
    /// @return the vector as a float array (should be copied to prevent mutation)
    /// @throws IndexOutOfBoundsException if index is invalid
    float[] getVector(int index);

    /// Gets all vectors as a 2D array.
    ///
    /// @return array where first index is vector, second is dimension
    float[][] getAllVectors();

    /// Gets a range of vectors as a 2D array.
    ///
    /// Default implementation calls getVector() for each index.
    /// Implementations should override for efficient bulk access.
    ///
    /// @param startIndex the starting index (inclusive)
    /// @param endIndex the ending index (exclusive)
    /// @return array where first index is vector, second is dimension
    /// @throws IndexOutOfBoundsException if indices are out of range
    default float[][] getVectors(int startIndex, int endIndex) {
        if (startIndex < 0 || endIndex > getVectorCount() || startIndex > endIndex) {
            throw new IndexOutOfBoundsException(
                "Range [" + startIndex + ", " + endIndex + ") out of bounds for size " + getVectorCount());
        }
        int count = endIndex - startIndex;
        float[][] vectors = new float[count][];
        for (int i = 0; i < count; i++) {
            vectors[i] = getVector(startIndex + i);
        }
        return vectors;
    }

    /// Gets the class label for a vector, if available.
    /// This is used for measures that require ground truth labels like margin analysis.
    /// 
    /// @param index the vector index
    /// @return the class label, or empty if not available
    Optional<Integer> getClassLabel(int index);

    /// Checks if this vector space has class labels.
    /// 
    /// @return true if class labels are available for analysis
    default boolean hasClassLabels() {
        return getVectorCount() > 0 && getClassLabel(0).isPresent();
    }

    /// Gets a unique identifier for this vector space.
    /// Used for caching computational artifacts and should be stable across runs.
    /// 
    /// @return a unique identifier string
    String getId();
}