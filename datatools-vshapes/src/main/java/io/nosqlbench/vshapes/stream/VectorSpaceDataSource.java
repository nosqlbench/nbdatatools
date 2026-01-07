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

import io.nosqlbench.nbdatatools.api.services.FileType;
import io.nosqlbench.vshapes.VectorSpace;
import io.nosqlbench.vshapes.io.VectorFileIOVectorSpace;

import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;

/**
 * DataSource implementation backed by a {@link VectorSpace}.
 *
 * <h2>Usage</h2>
 *
 * <pre>{@code
 * VectorSpace vectorSpace = loadVectorSpace();
 * DataSource source = new VectorSpaceDataSource(vectorSpace);
 *
 * // Use with harness
 * AnalyzerHarness harness = new AnalyzerHarness();
 * harness.register(myAnalyzer);
 * AnalysisResults results = harness.run(source, 1000);
 * }</pre>
 *
 * @see DataSource
 * @see VectorSpace
 */
public final class VectorSpaceDataSource implements DataSource {

    private final VectorSpace vectorSpace;
    private final DataspaceShape shape;

    /**
     * Creates a data source from a VectorSpace.
     *
     * @param vectorSpace the vector space to wrap
     */
    public VectorSpaceDataSource(VectorSpace vectorSpace) {
        this(vectorSpace, Map.of());
    }

    /**
     * Creates a data source with additional parameters.
     *
     * @param vectorSpace the vector space to wrap
     * @param additionalParameters additional configuration parameters
     */
    public VectorSpaceDataSource(VectorSpace vectorSpace, Map<String, Object> additionalParameters) {
        Objects.requireNonNull(vectorSpace, "vectorSpace cannot be null");
        this.vectorSpace = vectorSpace;
        this.shape = new DataspaceShape(
            vectorSpace.getVectorCount(),
            vectorSpace.getDimension(),
            additionalParameters
        );
    }

    /**
     * Creates a data source from a vector file using VectorFileIO.
     *
     * <p>This factory method uses VectorFileIO for efficient file-backed access,
     * supporting memory-mapped I/O when available. The returned DataSource
     * should be closed when no longer needed to release file handles.
     *
     * @param path the path to the vector file
     * @param fileType the file type/encoding
     * @return a new VectorSpaceDataSource backed by VectorFileIO
     * @throws IllegalArgumentException if the file cannot be opened
     */
    public static VectorSpaceDataSource fromFile(Path path, FileType fileType) {
        VectorFileIOVectorSpace vectorSpace = VectorFileIOVectorSpace.open(path, fileType);
        return new VectorSpaceDataSource(vectorSpace);
    }

    /**
     * Attempts to create a data source from a vector file using VectorFileIO.
     *
     * <p>This method provides graceful fallback support. If VectorFileIO services
     * are not available, returns empty instead of throwing.
     *
     * @param path the path to the vector file
     * @param fileType the file type/encoding
     * @return an Optional containing the DataSource, or empty if unavailable
     */
    public static Optional<VectorSpaceDataSource> tryFromFile(Path path, FileType fileType) {
        return VectorFileIOVectorSpace.tryOpen(path, fileType)
            .map(VectorSpaceDataSource::new);
    }

    @Override
    public DataspaceShape getShape() {
        return shape;
    }

    @Override
    public String getId() {
        return vectorSpace.getId();
    }

    @Override
    public Iterable<float[][]> chunks(int chunkSize) {
        if (chunkSize <= 0) {
            throw new IllegalArgumentException("chunkSize must be positive, got: " + chunkSize);
        }
        return () -> new ChunkIterator(chunkSize);
    }

    /**
     * Returns the underlying VectorSpace.
     *
     * @return the backing VectorSpace
     */
    public VectorSpace getVectorSpace() {
        return vectorSpace;
    }

    private class ChunkIterator implements Iterator<float[][]> {
        private final int chunkSize;
        private int position = 0;
        private final int total;

        ChunkIterator(int chunkSize) {
            this.chunkSize = chunkSize;
            this.total = vectorSpace.getVectorCount();
        }

        @Override
        public boolean hasNext() {
            return position < total;
        }

        @Override
        public float[][] next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            int remaining = total - position;
            int size = Math.min(chunkSize, remaining);

            float[][] chunk = new float[size][];
            for (int i = 0; i < size; i++) {
                chunk[i] = vectorSpace.getVector(position + i);
            }

            position += size;
            return chunk;
        }
    }
}
