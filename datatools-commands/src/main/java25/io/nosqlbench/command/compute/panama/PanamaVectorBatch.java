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

import jdk.incubator.vector.*;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.List;

/**
 * Panama-optimized storage for vector batches using contiguous memory layout.
 * This provides significantly better cache locality compared to ArrayList<float[]>
 * and enables more efficient SIMD operations.
 *
 * <p>Vectors are stored in a single MemorySegment with row-major layout:
 * [v0_dim0, v0_dim1, ..., v0_dimN, v1_dim0, v1_dim1, ..., v1_dimN, ...]
 *
 * <p>This contiguous layout improves:
 * - CPU cache utilization (prefetching works better)
 * - Memory bandwidth (fewer cache misses)
 * - SIMD operations (can process multiple dimensions at once)
 */
public class PanamaVectorBatch implements AutoCloseable {
    // Select widest available SIMD species at runtime
    // AVX-512 (512-bit) > AVX2 (256-bit) > SSE (128-bit)
    private static final VectorSpecies<Float> SPECIES = selectOptimalSpecies();

    private final Arena arena;
    private final MemorySegment segment;
    private final int vectorCount;
    private final int dimension;
    private final long vectorStride;  // bytes between vectors

    /**
     * Select the widest available SIMD species for optimal performance.
     * Checks what the hardware actually supports and uses the best available.
     */
    private static VectorSpecies<Float> selectOptimalSpecies() {
        // Try AVX-512 first (512-bit = 16 floats per lane)
        if (FloatVector.SPECIES_512.vectorBitSize() == 512 &&
            FloatVector.SPECIES_PREFERRED.vectorBitSize() >= 512) {
            System.out.println("SIMD: Using AVX-512 (512-bit, 16 floats/lane)");
            return FloatVector.SPECIES_512;
        }

        // Fall back to AVX2 (256-bit = 8 floats per lane)
        if (FloatVector.SPECIES_256.vectorBitSize() == 256) {
            System.out.println("SIMD: Using AVX2 (256-bit, 8 floats/lane)");
            return FloatVector.SPECIES_256;
        }

        // Last resort: SSE (128-bit = 4 floats per lane)
        System.out.println("SIMD: Using SSE (128-bit, 4 floats/lane)");
        return FloatVector.SPECIES_128;
    }

    /**
     * Creates a Panama-optimized vector batch from a list of float arrays.
     *
     * @param vectors list of vectors to store
     * @param dimension dimension of each vector
     */
    /**
     * Create PanamaVectorBatch from a list of float arrays (copies data).
     * Use this for non-memory-mapped sources.
     */
    public PanamaVectorBatch(List<float[]> vectors, int dimension) {
        this.vectorCount = vectors.size();
        this.dimension = dimension;
        this.vectorStride = (long) dimension * Float.BYTES;

        // Create SHARED arena for multi-threaded access (parallel query processing)
        // IMPORTANT: Must use ofShared() not ofConfined() for thread safety
        this.arena = Arena.ofShared();
        this.segment = arena.allocate(vectorStride * vectorCount, 64);  // 64-byte alignment for AVX-512

        // Copy vectors into contiguous memory
        for (int i = 0; i < vectorCount; i++) {
            float[] vector = vectors.get(i);
            if (vector.length != dimension) {
                throw new IllegalArgumentException("Vector " + i + " has dimension " + vector.length + ", expected " + dimension);
            }

            long offset = i * vectorStride;
            MemorySegment vectorSegment = segment.asSlice(offset, vectorStride);
            MemorySegment.copy(vector, 0, vectorSegment, ValueLayout.JAVA_FLOAT, 0, dimension);
        }
    }

    /**
     * Create PanamaVectorBatch from memory-mapped .fvec file segment.
     * Strips dimension fields and creates contiguous MemorySegment with just vector data.
     * This eliminates one copy (file → arrays → MemorySegment becomes file → MemorySegment).
     *
     * @param mappedFile the memory-mapped .fvec file
     * @param vectorCount number of vectors
     * @param dimension dimension of each vector
     * @param fileVectorStride bytes between vectors in .fvec file (includes dimension field)
     */
    public PanamaVectorBatch(MemorySegment mappedFile, int vectorCount, int dimension, long fileVectorStride) {
        this.vectorCount = vectorCount;
        this.dimension = dimension;
        this.vectorStride = (long) dimension * Float.BYTES;  // Our stride (no dimension field)

        // Allocate contiguous segment WITHOUT dimension fields
        this.arena = Arena.ofShared();
        this.segment = arena.allocate(vectorStride * vectorCount, 64);

        // Copy vectors, skipping dimension fields (ONE copy instead of TWO!)
        for (int i = 0; i < vectorCount; i++) {
            long fileOffset = i * fileVectorStride + Integer.BYTES;  // Skip dimension field
            long destOffset = i * vectorStride;

            // Bulk copy - must use UNALIGNED access due to .fvec format
            for (int d = 0; d < dimension; d++) {
                float val = mappedFile.get(ValueLayout.JAVA_FLOAT_UNALIGNED, fileOffset + (long) d * Float.BYTES);
                segment.set(ValueLayout.JAVA_FLOAT, destOffset + (long) d * Float.BYTES, val);
            }
        }
    }

    /**
     * Compute distance from a query vector to all vectors in this batch.
     * Uses SIMD operations for maximum performance.
     *
     * @param queryVector the query vector
     * @param distances output array for distances (must have length >= vectorCount)
     * @param distanceFunction 0=L2, 1=L1, 2=cosine
     */
    public void computeDistances(float[] queryVector, double[] distances, int distanceFunction) {
        if (queryVector.length != dimension) {
            throw new IllegalArgumentException("Query vector dimension mismatch");
        }
        if (distances.length < vectorCount) {
            throw new IllegalArgumentException("Distance array too small");
        }

        switch (distanceFunction) {
            case 0 -> computeL2Distances(queryVector, distances);
            case 1 -> computeL1Distances(queryVector, distances);
            case 2 -> computeCosineDistances(queryVector, distances);
            default -> throw new IllegalArgumentException("Unknown distance function: " + distanceFunction);
        }
    }

    /**
     * Compute distance to a single vector in the batch (zero-copy).
     * More efficient than computeDistances when you only need one distance.
     *
     * @param queryVector the query vector
     * @param vectorIndex index of vector in batch
     * @param distanceFunction 0=L2, 1=L1, 2=cosine
     * @return distance value
     */
    public double computeSingleDistance(float[] queryVector, int vectorIndex, int distanceFunction) {
        if (vectorIndex < 0 || vectorIndex >= vectorCount) {
            throw new IndexOutOfBoundsException("Vector index: " + vectorIndex);
        }
        if (queryVector.length != dimension) {
            throw new IllegalArgumentException("Query vector dimension mismatch");
        }

        long vectorOffset = vectorIndex * vectorStride;

        return switch (distanceFunction) {
            case 0 -> computeSingleL2Distance(queryVector, vectorOffset);
            case 1 -> computeSingleL1Distance(queryVector, vectorOffset);
            case 2 -> computeSingleCosineDistance(queryVector, vectorOffset);
            default -> throw new IllegalArgumentException("Unknown distance function: " + distanceFunction);
        };
    }

    private double computeSingleL2Distance(float[] queryVector, long vectorOffset) {
        // Use dimension-specific optimized kernel if available
        return switch (dimension) {
            case 128 -> DimensionSpecificKernels.euclidean128(queryVector, segment, vectorOffset);
            case 1024 -> DimensionSpecificKernels.euclidean1024(queryVector, segment, vectorOffset);
            default -> computeSingleL2DistanceGeneric(queryVector, vectorOffset);
        };
    }

    private double computeSingleL2DistanceGeneric(float[] queryVector, long vectorOffset) {
        int upperBound = SPECIES.loopBound(dimension);
        int lanes = SPECIES.length();

        // Accumulate 4 iterations before reducing (4x fewer reduceLanes calls)
        var accSq = FloatVector.zero(SPECIES);

        int d = 0;
        int unrollBound = upperBound - (3 * lanes);

        // 4-way unrolled loop with FMA
        for (; d <= unrollBound; d += 4 * lanes) {
            var vq0 = FloatVector.fromArray(SPECIES, queryVector, d);
            var vb0 = FloatVector.fromMemorySegment(SPECIES, segment, vectorOffset + (long) d * Float.BYTES, java.nio.ByteOrder.nativeOrder());
            var diff0 = vq0.sub(vb0);
            accSq = diff0.fma(diff0, accSq);

            int d1 = d + lanes;
            var vq1 = FloatVector.fromArray(SPECIES, queryVector, d1);
            var vb1 = FloatVector.fromMemorySegment(SPECIES, segment, vectorOffset + (long) d1 * Float.BYTES, java.nio.ByteOrder.nativeOrder());
            var diff1 = vq1.sub(vb1);
            accSq = diff1.fma(diff1, accSq);

            int d2 = d + 2 * lanes;
            var vq2 = FloatVector.fromArray(SPECIES, queryVector, d2);
            var vb2 = FloatVector.fromMemorySegment(SPECIES, segment, vectorOffset + (long) d2 * Float.BYTES, java.nio.ByteOrder.nativeOrder());
            var diff2 = vq2.sub(vb2);
            accSq = diff2.fma(diff2, accSq);

            int d3 = d + 3 * lanes;
            var vq3 = FloatVector.fromArray(SPECIES, queryVector, d3);
            var vb3 = FloatVector.fromMemorySegment(SPECIES, segment, vectorOffset + (long) d3 * Float.BYTES, java.nio.ByteOrder.nativeOrder());
            var diff3 = vq3.sub(vb3);
            accSq = diff3.fma(diff3, accSq);
        }

        // Reduce once
        double sumSquares = accSq.reduceLanes(VectorOperators.ADD);

        // Handle remaining vectors
        for (; d < upperBound; d += lanes) {
            var vq = FloatVector.fromArray(SPECIES, queryVector, d);
            var vb = FloatVector.fromMemorySegment(SPECIES, segment, vectorOffset + (long) d * Float.BYTES, java.nio.ByteOrder.nativeOrder());
            var diff = vq.sub(vb);
            sumSquares += diff.mul(diff).reduceLanes(VectorOperators.ADD);
        }

        // Scalar tail
        for (; d < dimension; d++) {
            float qVal = queryVector[d];
            float bVal = segment.get(ValueLayout.JAVA_FLOAT, vectorOffset + (long) d * Float.BYTES);
            float diff = qVal - bVal;
            sumSquares += diff * diff;
        }

        return Math.sqrt(sumSquares);
    }

    private double computeSingleL1Distance(float[] queryVector, long vectorOffset) {
        double sum = 0.0;
        int d = 0;
        int upperBound = SPECIES.loopBound(dimension);

        for (; d < upperBound; d += SPECIES.length()) {
            var vq = FloatVector.fromArray(SPECIES, queryVector, d);
            var vb = FloatVector.fromMemorySegment(SPECIES, segment, vectorOffset + (long) d * Float.BYTES, java.nio.ByteOrder.nativeOrder());
            var diff = vq.sub(vb);
            var abs = diff.abs();
            sum += abs.reduceLanes(VectorOperators.ADD);
        }

        for (; d < dimension; d++) {
            float qVal = queryVector[d];
            float bVal = segment.get(ValueLayout.JAVA_FLOAT, vectorOffset + (long) d * Float.BYTES);
            sum += Math.abs(qVal - bVal);
        }

        return sum;
    }

    private double computeSingleCosineDistance(float[] queryVector, long vectorOffset) {
        // Use dimension-specific optimized kernel if available
        return switch (dimension) {
            case 1024 -> DimensionSpecificKernels.cosine1024(queryVector, segment, vectorOffset);
            default -> computeSingleCosineDistanceGeneric(queryVector, vectorOffset);
        };
    }

    private double computeSingleCosineDistanceGeneric(float[] queryVector, long vectorOffset) {
        int upperBound = SPECIES.loopBound(dimension);
        int lanes = SPECIES.length();

        // Accumulate across 4 iterations before reducing (reduceLanes is expensive!)
        // This reduces the number of reduceLanes calls by 4x
        var accProd = FloatVector.zero(SPECIES);
        var accNormA = FloatVector.zero(SPECIES);
        var accNormB = FloatVector.zero(SPECIES);

        int d = 0;
        int unrollBound = upperBound - (3 * lanes);  // Leave room for 4-way unroll

        // 4-way unrolled loop: accumulate 4 vectors before reducing
        for (; d <= unrollBound; d += 4 * lanes) {
            // Iteration 1
            var vq0 = FloatVector.fromArray(SPECIES, queryVector, d);
            var vb0 = FloatVector.fromMemorySegment(SPECIES, segment, vectorOffset + (long) d * Float.BYTES, java.nio.ByteOrder.nativeOrder());
            accProd = vq0.fma(vb0, accProd);  // FMA: accProd + (vq0 * vb0)
            accNormA = vq0.fma(vq0, accNormA);
            accNormB = vb0.fma(vb0, accNormB);

            // Iteration 2
            int d1 = d + lanes;
            var vq1 = FloatVector.fromArray(SPECIES, queryVector, d1);
            var vb1 = FloatVector.fromMemorySegment(SPECIES, segment, vectorOffset + (long) d1 * Float.BYTES, java.nio.ByteOrder.nativeOrder());
            accProd = vq1.fma(vb1, accProd);
            accNormA = vq1.fma(vq1, accNormA);
            accNormB = vb1.fma(vb1, accNormB);

            // Iteration 3
            int d2 = d + 2 * lanes;
            var vq2 = FloatVector.fromArray(SPECIES, queryVector, d2);
            var vb2 = FloatVector.fromMemorySegment(SPECIES, segment, vectorOffset + (long) d2 * Float.BYTES, java.nio.ByteOrder.nativeOrder());
            accProd = vq2.fma(vb2, accProd);
            accNormA = vq2.fma(vq2, accNormA);
            accNormB = vb2.fma(vb2, accNormB);

            // Iteration 4
            int d3 = d + 3 * lanes;
            var vq3 = FloatVector.fromArray(SPECIES, queryVector, d3);
            var vb3 = FloatVector.fromMemorySegment(SPECIES, segment, vectorOffset + (long) d3 * Float.BYTES, java.nio.ByteOrder.nativeOrder());
            accProd = vq3.fma(vb3, accProd);
            accNormA = vq3.fma(vq3, accNormA);
            accNormB = vb3.fma(vb3, accNormB);
        }

        // Reduce accumulated vectors (only once per 4 iterations!)
        double dotProduct = accProd.reduceLanes(VectorOperators.ADD);
        double normA = accNormA.reduceLanes(VectorOperators.ADD);
        double normB = accNormB.reduceLanes(VectorOperators.ADD);

        // Handle remaining vectors (0-3 iterations)
        for (; d < upperBound; d += lanes) {
            var vq = FloatVector.fromArray(SPECIES, queryVector, d);
            var vb = FloatVector.fromMemorySegment(SPECIES, segment, vectorOffset + (long) d * Float.BYTES, java.nio.ByteOrder.nativeOrder());
            dotProduct += vq.mul(vb).reduceLanes(VectorOperators.ADD);
            normA += vq.mul(vq).reduceLanes(VectorOperators.ADD);
            normB += vb.mul(vb).reduceLanes(VectorOperators.ADD);
        }

        // Scalar tail (0-15 elements for AVX-512, 0-7 for AVX2)
        for (; d < dimension; d++) {
            float qVal = queryVector[d];
            float bVal = segment.get(ValueLayout.JAVA_FLOAT, vectorOffset + (long) d * Float.BYTES);
            dotProduct += qVal * bVal;
            normA += qVal * qVal;
            normB += bVal * bVal;
        }

        double magnitudeA = Math.sqrt(normA);
        double magnitudeB = Math.sqrt(normB);
        if (magnitudeA == 0 || magnitudeB == 0) {
            return 1.0;
        }
        double cosineSimilarity = dotProduct / (magnitudeA * magnitudeB);
        return 1.0 - cosineSimilarity;
    }

    /**
     * Compute L2 (Euclidean) distances using SIMD operations.
     * This processes multiple vectors and dimensions simultaneously.
     */
    private void computeL2Distances(float[] queryVector, double[] distances) {
        for (int vecIdx = 0; vecIdx < vectorCount; vecIdx++) {
            long vectorOffset = vecIdx * vectorStride;
            double sumSquares = 0.0;

            int d = 0;
            int upperBound = SPECIES.loopBound(dimension);

            // SIMD loop - process multiple dimensions at once
            for (; d < upperBound; d += SPECIES.length()) {
                var vq = FloatVector.fromArray(SPECIES, queryVector, d);
                var vb = FloatVector.fromMemorySegment(SPECIES, segment, vectorOffset + (long) d * Float.BYTES, java.nio.ByteOrder.nativeOrder());

                var diff = vq.sub(vb);
                var squared = diff.mul(diff);
                sumSquares += squared.reduceLanes(VectorOperators.ADD);
            }

            // Scalar tail
            for (; d < dimension; d++) {
                float qVal = queryVector[d];
                float bVal = segment.get(ValueLayout.JAVA_FLOAT, vectorOffset + (long) d * Float.BYTES);
                float diff = qVal - bVal;
                sumSquares += diff * diff;
            }

            distances[vecIdx] = Math.sqrt(sumSquares);
        }
    }

    /**
     * Compute L1 (Manhattan) distances using SIMD operations.
     */
    private void computeL1Distances(float[] queryVector, double[] distances) {
        for (int vecIdx = 0; vecIdx < vectorCount; vecIdx++) {
            long vectorOffset = vecIdx * vectorStride;
            double sum = 0.0;

            int d = 0;
            int upperBound = SPECIES.loopBound(dimension);

            // SIMD loop
            for (; d < upperBound; d += SPECIES.length()) {
                var vq = FloatVector.fromArray(SPECIES, queryVector, d);
                var vb = FloatVector.fromMemorySegment(SPECIES, segment, vectorOffset + (long) d * Float.BYTES, java.nio.ByteOrder.nativeOrder());

                var diff = vq.sub(vb);
                var abs = diff.abs();
                sum += abs.reduceLanes(VectorOperators.ADD);
            }

            // Scalar tail
            for (; d < dimension; d++) {
                float qVal = queryVector[d];
                float bVal = segment.get(ValueLayout.JAVA_FLOAT, vectorOffset + (long) d * Float.BYTES);
                sum += Math.abs(qVal - bVal);
            }

            distances[vecIdx] = sum;
        }
    }

    /**
     * Compute cosine distances using SIMD operations.
     */
    private void computeCosineDistances(float[] queryVector, double[] distances) {
        for (int vecIdx = 0; vecIdx < vectorCount; vecIdx++) {
            long vectorOffset = vecIdx * vectorStride;
            double dotProduct = 0.0;
            double normA = 0.0;
            double normB = 0.0;

            int d = 0;
            int upperBound = SPECIES.loopBound(dimension);

            // SIMD loop
            for (; d < upperBound; d += SPECIES.length()) {
                var vq = FloatVector.fromArray(SPECIES, queryVector, d);
                var vb = FloatVector.fromMemorySegment(SPECIES, segment, vectorOffset + (long) d * Float.BYTES, java.nio.ByteOrder.nativeOrder());

                var prod = vq.mul(vb);
                dotProduct += prod.reduceLanes(VectorOperators.ADD);

                var sqA = vq.mul(vq);
                var sqB = vb.mul(vb);
                normA += sqA.reduceLanes(VectorOperators.ADD);
                normB += sqB.reduceLanes(VectorOperators.ADD);
            }

            // Scalar tail
            for (; d < dimension; d++) {
                float qVal = queryVector[d];
                float bVal = segment.get(ValueLayout.JAVA_FLOAT, vectorOffset + (long) d * Float.BYTES);
                dotProduct += qVal * bVal;
                normA += qVal * qVal;
                normB += bVal * bVal;
            }

            double magnitudeA = Math.sqrt(normA);
            double magnitudeB = Math.sqrt(normB);

            if (magnitudeA == 0 || magnitudeB == 0) {
                distances[vecIdx] = 1.0;
            } else {
                double cosineSimilarity = dotProduct / (magnitudeA * magnitudeB);
                distances[vecIdx] = 1.0 - cosineSimilarity;
            }
        }
    }

    /**
     * Get a vector from the batch (creates a copy).
     *
     * @param index vector index
     * @return copy of the vector as float array
     */
    public float[] getVector(int index) {
        if (index < 0 || index >= vectorCount) {
            throw new IndexOutOfBoundsException("Index: " + index + ", size: " + vectorCount);
        }

        float[] result = new float[dimension];
        long offset = index * vectorStride;
        MemorySegment vectorSegment = segment.asSlice(offset, vectorStride);
        MemorySegment.copy(vectorSegment, ValueLayout.JAVA_FLOAT, 0, result, 0, dimension);
        return result;
    }

    public int size() {
        return vectorCount;
    }

    public int dimension() {
        return dimension;
    }

    /**
     * Get the raw MemorySegment for advanced zero-copy operations.
     * Used by batched/transposed algorithms.
     */
    public MemorySegment getRawSegment() {
        return segment;
    }

    /**
     * Get the stride between consecutive vectors in bytes.
     */
    public long getVectorStride() {
        return vectorStride;
    }

    @Override
    public void close() {
        // Only close arena if we own it (allocated ourselves)
        // For memory-mapped batches, arena is null (external ownership)
        if (arena != null) {
            arena.close();
        }
    }
}
