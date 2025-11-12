package io.nosqlbench.command.compute.panama;

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

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Memory-mapped vector file reader using Panama's Foreign Function & Memory API.
 * Provides zero-copy access to .fvec/.ivec files for maximum I/O performance.
 *
 * <p>Format (xvec):
 * <pre>
 * [dimension:4bytes][value1:4bytes][value2:4bytes]...[valueN:4bytes]
 * [dimension:4bytes][value1:4bytes][value2:4bytes]...[valueN:4bytes]
 * ...
 * </pre>
 *
 * <p>Performance benefits:
 * <ul>
 *   <li>Zero-copy: OS maps file directly into process address space
 *   <li>Automatic prefetching: OS handles read-ahead
 *   <li>Efficient paging: Only used pages loaded into memory
 *   <li>2-4x faster than traditional file I/O
 * </ul>
 *
 * <p>Memory safety: Uses Arena for automatic lifecycle management.
 */
public class MemoryMappedVectorFile implements AutoCloseable {
    private final Arena arena;
    private final MemorySegment mappedFile;
    private final int vectorCount;
    private final int dimension;
    private final long vectorStride;
    private final ByteOrder byteOrder;

    /**
     * Memory-map a vector file for zero-copy access.
     *
     * @param path path to .fvec or .ivec file
     * @throws IOException if file cannot be mapped
     */
    public MemoryMappedVectorFile(Path path) throws IOException {
        this(path, 0, -1);  // Map entire file
    }

    /**
     * Memory-map a specific range of a vector file for zero-copy access.
     * This is more efficient for partitioned processing as it only maps the needed region.
     *
     * @param path path to .fvec or .ivec file
     * @param startVectorIndex starting vector index (inclusive)
     * @param endVectorIndex ending vector index (exclusive), or -1 for entire file
     * @throws IOException if file cannot be mapped
     */
    public MemoryMappedVectorFile(Path path, int startVectorIndex, int endVectorIndex) throws IOException {
        // Create SHARED arena for multi-threaded access
        // Memory-mapped files will be accessed from multiple query processing threads
        this.arena = Arena.ofShared();
        this.byteOrder = ByteOrder.LITTLE_ENDIAN;

        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            long fileSize = channel.size();

            // Read dimension from first vector (absolute position 0)
            ByteBuffer dimBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
            channel.read(dimBuffer, 0);
            dimBuffer.flip();
            this.dimension = dimBuffer.getInt();
            this.vectorStride = (long) (1 + dimension) * Integer.BYTES;

            // Calculate total vectors in file
            int totalVectorCount = (int) (fileSize / vectorStride);

            // Determine range to map
            long mapStart;
            long mapSize;
            if (endVectorIndex == -1) {
                // Map entire file
                mapStart = 0;
                mapSize = fileSize;
                this.vectorCount = totalVectorCount;
            } else {
                // Map only the specified range
                if (endVectorIndex > totalVectorCount) {
                    endVectorIndex = totalVectorCount;
                }
                mapStart = startVectorIndex * vectorStride;
                mapSize = (endVectorIndex - startVectorIndex) * vectorStride;
                this.vectorCount = endVectorIndex - startVectorIndex;
            }

            // Memory-map only the needed region
            this.mappedFile = channel.map(FileChannel.MapMode.READ_ONLY, mapStart, mapSize, arena);
        }
    }

    /**
     * Get the number of vectors in the file.
     */
    public int getVectorCount() {
        return vectorCount;
    }

    /**
     * Get the dimension of each vector.
     */
    public int getDimension() {
        return dimension;
    }

    /**
     * Read a single vector from the mapped file (creates a copy).
     * For best performance, use {@link #readVectorsDirect} or {@link #asPanamaVectorBatch}.
     *
     * @param index vector index
     * @return copy of the vector as float array
     */
    public float[] getVector(int index) {
        if (index < 0 || index >= vectorCount) {
            throw new IndexOutOfBoundsException("Index: " + index + ", size: " + vectorCount);
        }

        float[] vector = new float[dimension];
        long baseOffset = index * vectorStride;

        // Skip dimension field (4 bytes)
        long dataOffset = baseOffset + Integer.BYTES;

        // Read floats from mapped memory
        for (int i = 0; i < dimension; i++) {
            int intBits = mappedFile.get(ValueLayout.JAVA_INT, dataOffset + (long) i * Integer.BYTES);
            vector[i] = Float.intBitsToFloat(intBits);
        }

        return vector;
    }

    /**
     * Read a range of vectors from the mapped file (creates copies).
     *
     * @param startIndex starting vector index (inclusive)
     * @param endIndex ending vector index (exclusive)
     * @return array of vectors
     */
    public float[][] getVectors(int startIndex, int endIndex) {
        if (startIndex < 0 || endIndex > vectorCount || startIndex >= endIndex) {
            throw new IndexOutOfBoundsException("Invalid range: [" + startIndex + ", " + endIndex + ")");
        }

        int count = endIndex - startIndex;
        float[][] vectors = new float[count][];

        for (int i = 0; i < count; i++) {
            vectors[i] = getVector(startIndex + i);
        }

        return vectors;
    }

    /**
     * Get a zero-copy slice of the mapped file for a range of vectors.
     * This is the most efficient way to access a subset of vectors.
     *
     * @param startIndex starting vector index (inclusive)
     * @param endIndex ending vector index (exclusive)
     * @return memory segment containing the vector range
     */
    public MemorySegment getVectorSlice(int startIndex, int endIndex) {
        if (startIndex < 0 || endIndex > vectorCount || startIndex >= endIndex) {
            throw new IndexOutOfBoundsException("Invalid range: [" + startIndex + ", " + endIndex + ")");
        }

        long sliceOffset = startIndex * vectorStride;
        long sliceSize = (endIndex - startIndex) * vectorStride;

        return mappedFile.asSlice(sliceOffset, sliceSize);
    }

    /**
     * Create a PanamaVectorBatch from a range of vectors (single-copy optimization).
     * Reads directly from memory-mapped file, strips dimension fields in one pass.
     * This eliminates the double-copy (file→arrays→MemorySegment becomes file→MemorySegment).
     *
     * @param startIndex starting vector index (inclusive)
     * @param endIndex ending vector index (exclusive)
     * @return Panama-optimized vector batch with single copy
     */
    public PanamaVectorBatch asPanamaVectorBatch(int startIndex, int endIndex) {
        if (startIndex < 0 || endIndex > vectorCount || startIndex >= endIndex) {
            throw new IndexOutOfBoundsException("Invalid range: [" + startIndex + ", " + endIndex + ")");
        }

        int count = endIndex - startIndex;
        long sliceOffset = startIndex * vectorStride;
        long sliceSize = count * vectorStride;

        // Slice the memory-mapped segment for this range
        MemorySegment rangeSegment = mappedFile.asSlice(sliceOffset, sliceSize);

        // Create PanamaVectorBatch using the new single-copy constructor
        // This strips dimension fields in one pass directly from mapped file
        return new PanamaVectorBatch(rangeSegment, count, dimension, vectorStride);
    }

    /**
     * Get direct memory segment for the entire file.
     * Advanced usage - caller must handle vector layout manually.
     *
     * @return memory segment for entire mapped file
     */
    public MemorySegment getRawSegment() {
        return mappedFile;
    }

    /**
     * Get the vector stride (bytes between consecutive vectors).
     */
    public long getVectorStride() {
        return vectorStride;
    }

    @Override
    public void close() {
        arena.close();
    }

    /**
     * Check if memory-mapped vector files are supported on this JVM.
     */
    public static boolean isSupported() {
        try {
            Class.forName("java.lang.foreign.MemorySegment");
            Class.forName("java.nio.channels.FileChannel");
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }
}
