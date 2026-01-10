package io.nosqlbench.vshapes.io;

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

import io.nosqlbench.nbdatatools.api.fileio.VectorFileArray;
import io.nosqlbench.nbdatatools.api.services.FileType;
import io.nosqlbench.nbdatatools.api.services.VectorFileIO;
import io.nosqlbench.vshapes.VectorSpace;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

/// VectorSpace implementation backed by VectorFileIO for efficient file-based access.
///
/// ## Purpose
///
/// Provides a VectorSpace interface over vector files using VectorFileIO's random
/// access capabilities. This enables memory-efficient analysis of large vector files
/// without loading the entire dataset into memory.
///
/// ## Usage
///
/// ```java
/// // Open a vector file
/// try (VectorFileIOVectorSpace space = VectorFileIOVectorSpace.open(
///         Path.of("vectors.fvec"), FileType.fvec)) {
///
///     // Use with any VectorSpace consumer
///     DataSource source = new VectorSpaceDataSource(space);
///     AnalyzerHarness harness = new AnalyzerHarness();
///     harness.run(source, 1000);
/// }
/// ```
///
/// ## Memory Efficiency
///
/// Unlike in-memory VectorSpace implementations, this class only loads vectors
/// on demand. For memory-mapped file types (like fvec), the OS handles paging
/// efficiently.
///
/// @see VectorSpace
/// @see VectorFileIO
public final class VectorFileIOVectorSpace implements VectorSpace, Closeable {

    private final VectorFileArray<float[]> vectorArray;
    private final String id;
    private final int vectorCount;
    private final int dimension;

    /// Creates a VectorSpace backed by VectorFileIO.
    ///
    /// @param vectorArray the underlying vector file array (takes ownership)
    /// @param id the identifier for this vector space
    private VectorFileIOVectorSpace(VectorFileArray<float[]> vectorArray, String id) {
        this.vectorArray = Objects.requireNonNull(vectorArray, "vectorArray cannot be null");
        this.id = Objects.requireNonNull(id, "id cannot be null");
        this.vectorCount = vectorArray.getSize();

        // Get dimension from first vector
        if (vectorCount > 0) {
            float[] first = vectorArray.get(0);
            this.dimension = first != null ? first.length : 0;
        } else {
            this.dimension = 0;
        }
    }

    /// Opens a vector file and creates a VectorSpace.
    ///
    /// @param path the path to the vector file
    /// @param fileType the file type/encoding
    /// @return a new VectorFileIOVectorSpace
    /// @throws IllegalArgumentException if the file cannot be opened
    public static VectorFileIOVectorSpace open(Path path, FileType fileType) {
        Objects.requireNonNull(path, "path cannot be null");
        Objects.requireNonNull(fileType, "fileType cannot be null");

        VectorFileArray<float[]> array = VectorFileIO.randomAccess(fileType, float[].class, path);
        String id = path.getFileName().toString();
        return new VectorFileIOVectorSpace(array, id);
    }

    /// Attempts to open a vector file, returning empty if VectorFileIO is unavailable.
    ///
    /// This method provides graceful fallback support when VectorFileIO services
    /// are not available.
    ///
    /// @param path the path to the vector file
    /// @param fileType the file type/encoding
    /// @return an Optional containing the VectorSpace, or empty if unavailable
    public static Optional<VectorFileIOVectorSpace> tryOpen(Path path, FileType fileType) {
        try {
            return Optional.of(open(path, fileType));
        } catch (Exception e) {
            // VectorFileIO service unavailable or file cannot be opened
            return Optional.empty();
        }
    }

    @Override
    public int getVectorCount() {
        return vectorCount;
    }

    @Override
    public int getDimension() {
        return dimension;
    }

    @Override
    public float[] getVector(int index) {
        if (index < 0 || index >= vectorCount) {
            throw new IndexOutOfBoundsException(
                "Index " + index + " out of bounds for vector count " + vectorCount);
        }
        // Return a copy to prevent mutation
        float[] vector = vectorArray.get(index);
        return vector != null ? Arrays.copyOf(vector, vector.length) : null;
    }

    @Override
    public float[][] getVectors(int startIndex, int endIndex) {
        if (startIndex < 0 || endIndex > vectorCount || startIndex > endIndex) {
            throw new IndexOutOfBoundsException(
                "Range [" + startIndex + ", " + endIndex + ") out of bounds for size " + vectorCount);
        }
        // Use the underlying array's bulk read capabilities via subList().toArray()
        // This leverages optimized getRange() implementations in readers like UniformFvecReader
        Object[] raw = vectorArray.subList(startIndex, endIndex).toArray();
        float[][] result = new float[raw.length][];
        for (int i = 0; i < raw.length; i++) {
            float[] vec = (float[]) raw[i];
            result[i] = vec != null ? Arrays.copyOf(vec, vec.length) : null;
        }
        return result;
    }

    @Override
    public float[][] getAllVectors() {
        return getVectors(0, vectorCount);
    }

    @Override
    public Optional<Integer> getClassLabel(int index) {
        // VectorFileIO doesn't support class labels
        return Optional.empty();
    }

    @Override
    public String getId() {
        return id;
    }

    /// Returns the underlying VectorFileArray for advanced use cases.
    ///
    /// Note: The returned array should not be closed separately; use
    /// {@link #close()} on this VectorSpace instead.
    ///
    /// @return the underlying vector file array
    public VectorFileArray<float[]> getVectorArray() {
        return vectorArray;
    }

    @Override
    public void close() throws IOException {
        vectorArray.close();
    }

    @Override
    public String toString() {
        return String.format("VectorFileIOVectorSpace[id=%s, vectors=%d, dims=%d]",
            id, vectorCount, dimension);
    }
}
