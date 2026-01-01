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

import java.util.Iterator;
import java.util.function.BiConsumer;

/**
 * Abstraction for a source of vector data that can be streamed in chunks.
 *
 * <h2>Purpose</h2>
 *
 * <p>Provides a uniform interface for accessing vector data regardless of
 * the underlying storage (in-memory arrays, VectorSpace, memory-mapped files, etc.).
 *
 * <h2>Chunked Iteration</h2>
 *
 * <p>Data is accessed in chunks to enable:
 * <ul>
 *   <li>Memory-efficient processing of large datasets</li>
 *   <li>Parallel distribution to multiple analyzers</li>
 *   <li>Progress tracking during processing</li>
 * </ul>
 *
 * <h2>Usage</h2>
 *
 * <pre>{@code
 * DataSource source = new FloatArrayDataSource(myData);
 *
 * // Get shape
 * DataspaceShape shape = source.getShape();
 * System.out.println("Vectors: " + shape.cardinality());
 *
 * // Iterate chunks
 * for (float[][] chunk : source.chunks(1000)) {
 *     processChunk(chunk);
 * }
 *
 * // Or with callback
 * source.forEachChunk(1000, (chunk, startIndex) -> {
 *     System.out.println("Processing vectors " + startIndex + " to " + (startIndex + chunk.length));
 * });
 * }</pre>
 *
 * @see FloatArrayDataSource
 * @see VectorSpaceDataSource
 */
public interface DataSource {

    /**
     * Returns the shape of the dataspace.
     *
     * @return the dataspace shape (cardinality, dimensionality, parameters)
     */
    DataspaceShape getShape();

    /**
     * Returns an iterator over chunks of vectors.
     *
     * <p>Each chunk is a 2D array of shape [chunkSize][dimensionality],
     * except the last chunk which may be smaller.
     *
     * @param chunkSize the maximum number of vectors per chunk
     * @return an iterable over chunks
     */
    Iterable<float[][]> chunks(int chunkSize);

    /**
     * Iterates over chunks with a callback that receives the chunk and its start index.
     *
     * @param chunkSize the maximum number of vectors per chunk
     * @param consumer callback receiving (chunk, startIndex) for each chunk
     */
    default void forEachChunk(int chunkSize, BiConsumer<float[][], Long> consumer) {
        long index = 0;
        for (float[][] chunk : chunks(chunkSize)) {
            consumer.accept(chunk, index);
            index += chunk.length;
        }
    }

    /**
     * Returns the total number of vectors.
     *
     * <p>Convenience method equivalent to {@code getShape().cardinality()}.
     *
     * @return the number of vectors
     */
    default long size() {
        return getShape().cardinality();
    }

    /**
     * Returns the dimensionality of vectors.
     *
     * <p>Convenience method equivalent to {@code getShape().dimensionality()}.
     *
     * @return the number of dimensions per vector
     */
    default int dimensionality() {
        return getShape().dimensionality();
    }

    /**
     * Returns an optional identifier for this data source.
     *
     * <p>Used for logging, caching, and debugging.
     *
     * @return an identifier, or "anonymous" if not set
     */
    default String getId() {
        return "anonymous";
    }
}
