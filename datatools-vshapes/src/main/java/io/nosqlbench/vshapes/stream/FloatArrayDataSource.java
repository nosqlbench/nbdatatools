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

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * DataSource implementation backed by an in-memory float[][] array.
 *
 * <h2>Usage</h2>
 *
 * <pre>{@code
 * float[][] data = loadVectors();  // [numVectors][dimensionality]
 * DataSource source = new FloatArrayDataSource(data);
 *
 * // Or with parameters
 * DataSource source = new FloatArrayDataSource(data, Map.of("source", "my-dataset"));
 *
 * // Use with harness
 * AnalyzerHarness harness = new AnalyzerHarness();
 * harness.register(myAnalyzer);
 * AnalysisResults results = harness.run(source, 1000);
 * }</pre>
 *
 * @see DataSource
 */
public final class FloatArrayDataSource implements DataSource {

    private final float[][] data;
    private final DataspaceShape shape;
    private final String id;

    /**
     * Creates a data source from a float array.
     *
     * @param data the vector data, shape [numVectors][dimensionality]
     * @throws IllegalArgumentException if data is null, empty, or jagged
     */
    public FloatArrayDataSource(float[][] data) {
        this(data, Map.of(), "float-array");
    }

    /**
     * Creates a data source with additional parameters.
     *
     * @param data the vector data
     * @param parameters additional configuration parameters
     */
    public FloatArrayDataSource(float[][] data, Map<String, Object> parameters) {
        this(data, parameters, "float-array");
    }

    /**
     * Creates a data source with parameters and an identifier.
     *
     * @param data the vector data
     * @param parameters additional configuration parameters
     * @param id identifier for logging/caching
     */
    public FloatArrayDataSource(float[][] data, Map<String, Object> parameters, String id) {
        Objects.requireNonNull(data, "data cannot be null");
        if (data.length == 0) {
            throw new IllegalArgumentException("data cannot be empty");
        }
        Objects.requireNonNull(data[0], "first row cannot be null");
        if (data[0].length == 0) {
            throw new IllegalArgumentException("vectors cannot have zero dimensions");
        }

        // Validate all rows have same dimensionality
        int dimensionality = data[0].length;
        for (int i = 1; i < data.length; i++) {
            if (data[i] == null || data[i].length != dimensionality) {
                throw new IllegalArgumentException(
                    "data is jagged: row 0 has " + dimensionality +
                    " dimensions, but row " + i + " has " +
                    (data[i] == null ? "null" : data[i].length));
            }
        }

        this.data = data;
        this.shape = new DataspaceShape(data.length, dimensionality, parameters);
        this.id = id != null ? id : "float-array";
    }

    @Override
    public DataspaceShape getShape() {
        return shape;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public Iterable<float[][]> chunks(int chunkSize) {
        if (chunkSize <= 0) {
            throw new IllegalArgumentException("chunkSize must be positive, got: " + chunkSize);
        }
        return () -> new ChunkIterator(chunkSize);
    }

    /**
     * Returns the underlying data array.
     *
     * <p><b>Warning:</b> This returns the actual backing array, not a copy.
     * Modifying the returned array will affect this data source.
     *
     * @return the backing data array
     */
    public float[][] getData() {
        return data;
    }

    private class ChunkIterator implements Iterator<float[][]> {
        private final int chunkSize;
        private int position = 0;

        ChunkIterator(int chunkSize) {
            this.chunkSize = chunkSize;
        }

        @Override
        public boolean hasNext() {
            return position < data.length;
        }

        @Override
        public float[][] next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            int remaining = data.length - position;
            int size = Math.min(chunkSize, remaining);

            float[][] chunk = Arrays.copyOfRange(data, position, position + size);
            position += size;

            return chunk;
        }
    }
}
