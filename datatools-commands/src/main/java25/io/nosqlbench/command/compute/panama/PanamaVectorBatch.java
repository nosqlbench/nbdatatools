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
 *
 * <p>SIMD species selection is centralized via {@link LocalSpecies}.
 */
public class PanamaVectorBatch implements AutoCloseable {
    // Use centralized species selection from LocalSpecies
    private static final VectorSpecies<Float> SPECIES = LocalSpecies.floatSpecies();

    private final Arena arena;
    private final MemorySegment segment;
    private final int vectorCount;
    private final int dimension;
    private final long vectorStride;  // bytes between vectors

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
            case 3 -> -computeDotProductInternal(queryVector, vectorOffset);  // Negate for min-heap (higher dot = closer)
            default -> throw new IllegalArgumentException("Unknown distance function: " + distanceFunction);
        };
    }

    /**
     * Compute distance with PRE-COMPUTED query norm (cosine only - MASSIVE optimization!).
     * For cosine distance, the query norm is constant across all base vectors.
     * Pre-computing it eliminates 33% of the work.
     *
     * @param queryVector the query vector
     * @param precomputedQueryNorm ||queryVector|| (pre-computed once)
     * @param vectorIndex index of base vector
     * @return cosine distance
     */
    public double computeSingleCosineDistanceWithPrecomputedNorm(
        float[] queryVector,
        double precomputedQueryNorm,
        int vectorIndex
    ) {
        if (vectorIndex < 0 || vectorIndex >= vectorCount) {
            throw new IndexOutOfBoundsException("Vector index: " + vectorIndex);
        }

        long vectorOffset = vectorIndex * vectorStride;

        // Compute ONLY dot product and base norm (query norm already known!)
        return computeCosineWithPrecomputedQueryNorm(queryVector, precomputedQueryNorm, vectorOffset);
    }

    /**
     * Pre-compute the L2 norm of a query vector.
     * Call this ONCE per query, then reuse for all base vectors.
     */
    public static double precomputeQueryNorm(float[] queryVector) {
        VectorSpecies<Float> species = FloatVector.SPECIES_PREFERRED;
        int dimension = queryVector.length;
        int lanes = species.length();

        var accSq = FloatVector.zero(species);
        int d = 0;
        int upperBound = species.loopBound(dimension);

        // 8-way unroll
        int unrollBound = upperBound - (7 * lanes);
        for (; d <= unrollBound; d += 8 * lanes) {
            for (int i = 0; i < 8; i++) {
                int offset = d + i * lanes;
                var vq = FloatVector.fromArray(species, queryVector, offset);
                accSq = vq.fma(vq, accSq);
            }
        }

        double normSq = accSq.reduceLanes(VectorOperators.ADD);

        // Handle tail
        for (; d < dimension; d++) {
            normSq += queryVector[d] * queryVector[d];
        }

        return Math.sqrt(normSq);
    }

    private double computeSingleL2Distance(float[] queryVector, long vectorOffset) {
        // Use optimized kernel if dimension is a multiple of 8*lanes
        if (DimensionSpecificKernels.hasOptimizedKernel(dimension)) {
            // Covers 128, 256, 512, 768, 1024, 1536, 2048, 2560, 3072, 4096, etc.
            return switch (dimension) {
                case 128 -> DimensionSpecificKernels.euclidean128(queryVector, segment, vectorOffset);
                case 1024 -> DimensionSpecificKernels.euclidean1024(queryVector, segment, vectorOffset);
                default -> DimensionSpecificKernels.euclideanGenericOptimized(queryVector, segment, vectorOffset, dimension);
            };
        }
        return computeSingleL2DistanceGeneric(queryVector, vectorOffset);
    }

    private double computeSingleL2DistanceGeneric(float[] queryVector, long vectorOffset) {
        int upperBound = SPECIES.loopBound(dimension);
        int lanes = SPECIES.length();

        // Accumulate 8 iterations before reducing (8x fewer reduceLanes calls!)
        var accSq = FloatVector.zero(SPECIES);

        int d = 0;
        int unrollBound = upperBound - (7 * lanes);

        // 8-way unrolled loop with FMA
        for (; d <= unrollBound; d += 8 * lanes) {
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

            int d4 = d + 4 * lanes;
            var vq4 = FloatVector.fromArray(SPECIES, queryVector, d4);
            var vb4 = FloatVector.fromMemorySegment(SPECIES, segment, vectorOffset + (long) d4 * Float.BYTES, java.nio.ByteOrder.nativeOrder());
            var diff4 = vq4.sub(vb4);
            accSq = diff4.fma(diff4, accSq);

            int d5 = d + 5 * lanes;
            var vq5 = FloatVector.fromArray(SPECIES, queryVector, d5);
            var vb5 = FloatVector.fromMemorySegment(SPECIES, segment, vectorOffset + (long) d5 * Float.BYTES, java.nio.ByteOrder.nativeOrder());
            var diff5 = vq5.sub(vb5);
            accSq = diff5.fma(diff5, accSq);

            int d6 = d + 6 * lanes;
            var vq6 = FloatVector.fromArray(SPECIES, queryVector, d6);
            var vb6 = FloatVector.fromMemorySegment(SPECIES, segment, vectorOffset + (long) d6 * Float.BYTES, java.nio.ByteOrder.nativeOrder());
            var diff6 = vq6.sub(vb6);
            accSq = diff6.fma(diff6, accSq);

            int d7 = d + 7 * lanes;
            var vq7 = FloatVector.fromArray(SPECIES, queryVector, d7);
            var vb7 = FloatVector.fromMemorySegment(SPECIES, segment, vectorOffset + (long) d7 * Float.BYTES, java.nio.ByteOrder.nativeOrder());
            var diff7 = vq7.sub(vb7);
            accSq = diff7.fma(diff7, accSq);
        }

        // Reduce once (only once per 8 iterations!)
        double sumSquares = accSq.reduceLanes(VectorOperators.ADD);

        // Handle remaining vectors (0-7 iterations)
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
        // Use optimized kernel if dimension is a multiple of 8*lanes
        if (DimensionSpecificKernels.hasOptimizedKernel(dimension)) {
            // Covers ALL common dimensions: 128, 256, 512, 768, 1024, 1536, 2048, 2560, 3072, 4096
            return switch (dimension) {
                case 1024 -> DimensionSpecificKernels.cosine1024(queryVector, segment, vectorOffset);
                default -> DimensionSpecificKernels.cosineGenericOptimized(queryVector, segment, vectorOffset, dimension);
            };
        }
        return computeSingleCosineDistanceGeneric(queryVector, vectorOffset);
    }

    /**
     * Compute cosine distance with pre-computed query norm (33% less work!).
     */
    private double computeCosineWithPrecomputedQueryNorm(
        float[] queryVector,
        double queryNorm,
        long vectorOffset
    ) {
        int upperBound = SPECIES.loopBound(dimension);
        int lanes = SPECIES.length();

        // Only compute dot product and base norm (query norm already known!)
        var accProd = FloatVector.zero(SPECIES);
        var accNormB = FloatVector.zero(SPECIES);

        int d = 0;
        int unrollBound = upperBound - (7 * lanes);

        // 8-way unrolled loop - but ONLY 2/3 of the work (no query norm!)
        for (; d <= unrollBound; d += 8 * lanes) {
            for (int i = 0; i < 8; i++) {
                int offset = d + i * lanes;
                var vq = FloatVector.fromArray(SPECIES, queryVector, offset);
                var vb = FloatVector.fromMemorySegment(SPECIES, segment, vectorOffset + (long) offset * Float.BYTES, java.nio.ByteOrder.nativeOrder());
                accProd = vq.fma(vb, accProd);
                accNormB = vb.fma(vb, accNormB);
            }
        }

        double dotProduct = accProd.reduceLanes(VectorOperators.ADD);
        double normB = accNormB.reduceLanes(VectorOperators.ADD);

        // Tail
        for (; d < upperBound; d += lanes) {
            var vq = FloatVector.fromArray(SPECIES, queryVector, d);
            var vb = FloatVector.fromMemorySegment(SPECIES, segment, vectorOffset + (long) d * Float.BYTES, java.nio.ByteOrder.nativeOrder());
            dotProduct += vq.mul(vb).reduceLanes(VectorOperators.ADD);
            normB += vb.mul(vb).reduceLanes(VectorOperators.ADD);
        }

        for (; d < dimension; d++) {
            float qVal = queryVector[d];
            float bVal = segment.get(ValueLayout.JAVA_FLOAT, vectorOffset + (long) d * Float.BYTES);
            dotProduct += qVal * bVal;
            normB += bVal * bVal;
        }

        double magnitudeB = Math.sqrt(normB);
        if (queryNorm == 0 || magnitudeB == 0) {
            return 1.0;
        }
        return 1.0 - (dotProduct / (queryNorm * magnitudeB));
    }

    /**
     * Compute DOT PRODUCT similarity for normalized vectors (66% less work than cosine!).
     * For normalized vectors, this is equivalent to cosine similarity but faster.
     * Returns similarity (higher = more similar), not distance.
     */
    public double computeDotProduct(float[] queryVector, int vectorIndex) {
        if (vectorIndex < 0 || vectorIndex >= vectorCount) {
            throw new IndexOutOfBoundsException("Vector index: " + vectorIndex);
        }
        long vectorOffset = vectorIndex * vectorStride;
        return computeDotProductInternal(queryVector, vectorOffset);
    }

    private double computeDotProductInternal(float[] queryVector, long vectorOffset) {
        // Use optimized kernel for all common dimensions
        if (DimensionSpecificKernels.hasOptimizedKernel(dimension)) {
            // Covers ALL common dimensions: 128, 256, 512, 768, 1024, 1536, 2048, 2560, 3072, 4096
            return switch (dimension) {
                case 128 -> DimensionSpecificKernels.dotProduct128(queryVector, segment, vectorOffset);
                case 1024 -> DimensionSpecificKernels.dotProduct1024(queryVector, segment, vectorOffset);
                default -> DimensionSpecificKernels.dotProductGenericOptimized(queryVector, segment, vectorOffset, dimension);
            };
        }
        return computeDotProductGeneric(queryVector, vectorOffset);
    }

    private double computeDotProductGeneric(float[] queryVector, long vectorOffset) {
        int upperBound = SPECIES.loopBound(dimension);
        int lanes = SPECIES.length();

        var accProd = FloatVector.zero(SPECIES);
        int d = 0;
        int unrollBound = upperBound - (7 * lanes);

        // 8-way unrolled - ONLY dot product!
        for (; d <= unrollBound; d += 8 * lanes) {
            for (int i = 0; i < 8; i++) {
                int offset = d + i * lanes;
                var vq = FloatVector.fromArray(SPECIES, queryVector, offset);
                var vb = FloatVector.fromMemorySegment(SPECIES, segment, vectorOffset + (long) offset * Float.BYTES, java.nio.ByteOrder.nativeOrder());
                accProd = vq.fma(vb, accProd);
            }
        }

        double dotProduct = accProd.reduceLanes(VectorOperators.ADD);

        // Tail
        for (; d < upperBound; d += lanes) {
            var vq = FloatVector.fromArray(SPECIES, queryVector, d);
            var vb = FloatVector.fromMemorySegment(SPECIES, segment, vectorOffset + (long) d * Float.BYTES, java.nio.ByteOrder.nativeOrder());
            dotProduct += vq.mul(vb).reduceLanes(VectorOperators.ADD);
        }

        for (; d < dimension; d++) {
            dotProduct += queryVector[d] * segment.get(ValueLayout.JAVA_FLOAT, vectorOffset + (long) d * Float.BYTES);
        }

        return dotProduct;  // Return similarity (not distance!)
    }

    /**
     * Compute cosine distance for NORMALIZED vectors (66% less work!).
     * When ||query||=1 and ||base||=1, cosine = 1 - dot_product.
     * No need to compute norms at all!
     */
    private double computeCosineNormalized(float[] queryVector, long vectorOffset) {
        int upperBound = SPECIES.loopBound(dimension);
        int lanes = SPECIES.length();

        // ONLY compute dot product (norms are 1.0 by definition!)
        var accProd = FloatVector.zero(SPECIES);

        int d = 0;
        int unrollBound = upperBound - (7 * lanes);

        // 8-way unrolled - ONLY dot product, no norms!
        for (; d <= unrollBound; d += 8 * lanes) {
            for (int i = 0; i < 8; i++) {
                int offset = d + i * lanes;
                var vq = FloatVector.fromArray(SPECIES, queryVector, offset);
                var vb = FloatVector.fromMemorySegment(SPECIES, segment, vectorOffset + (long) offset * Float.BYTES, java.nio.ByteOrder.nativeOrder());
                accProd = vq.fma(vb, accProd);
            }
        }

        double dotProduct = accProd.reduceLanes(VectorOperators.ADD);

        // Tail
        for (; d < upperBound; d += lanes) {
            var vq = FloatVector.fromArray(SPECIES, queryVector, d);
            var vb = FloatVector.fromMemorySegment(SPECIES, segment, vectorOffset + (long) d * Float.BYTES, java.nio.ByteOrder.nativeOrder());
            dotProduct += vq.mul(vb).reduceLanes(VectorOperators.ADD);
        }

        for (; d < dimension; d++) {
            dotProduct += queryVector[d] * segment.get(ValueLayout.JAVA_FLOAT, vectorOffset + (long) d * Float.BYTES);
        }

        return 1.0 - dotProduct;  // For normalized vectors, ||q||=1, ||b||=1
    }

    private double computeSingleCosineDistanceGeneric(float[] queryVector, long vectorOffset) {
        int upperBound = SPECIES.loopBound(dimension);
        int lanes = SPECIES.length();

        // Accumulate across 8 iterations before reducing (further reduce reduceLanes calls!)
        // This reduces reduceLanes calls by 8x compared to naive
        var accProd = FloatVector.zero(SPECIES);
        var accNormA = FloatVector.zero(SPECIES);
        var accNormB = FloatVector.zero(SPECIES);

        int d = 0;
        int unrollBound = upperBound - (7 * lanes);  // Leave room for 8-way unroll

        // 8-way unrolled loop with FMA
        for (; d <= unrollBound; d += 8 * lanes) {
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

            // Iteration 5
            int d4 = d + 4 * lanes;
            var vq4 = FloatVector.fromArray(SPECIES, queryVector, d4);
            var vb4 = FloatVector.fromMemorySegment(SPECIES, segment, vectorOffset + (long) d4 * Float.BYTES, java.nio.ByteOrder.nativeOrder());
            accProd = vq4.fma(vb4, accProd);
            accNormA = vq4.fma(vq4, accNormA);
            accNormB = vb4.fma(vb4, accNormB);

            // Iteration 6
            int d5 = d + 5 * lanes;
            var vq5 = FloatVector.fromArray(SPECIES, queryVector, d5);
            var vb5 = FloatVector.fromMemorySegment(SPECIES, segment, vectorOffset + (long) d5 * Float.BYTES, java.nio.ByteOrder.nativeOrder());
            accProd = vq5.fma(vb5, accProd);
            accNormA = vq5.fma(vq5, accNormA);
            accNormB = vb5.fma(vb5, accNormB);

            // Iteration 7
            int d6 = d + 6 * lanes;
            var vq6 = FloatVector.fromArray(SPECIES, queryVector, d6);
            var vb6 = FloatVector.fromMemorySegment(SPECIES, segment, vectorOffset + (long) d6 * Float.BYTES, java.nio.ByteOrder.nativeOrder());
            accProd = vq6.fma(vb6, accProd);
            accNormA = vq6.fma(vq6, accNormA);
            accNormB = vb6.fma(vb6, accNormB);

            // Iteration 8
            int d7 = d + 7 * lanes;
            var vq7 = FloatVector.fromArray(SPECIES, queryVector, d7);
            var vb7 = FloatVector.fromMemorySegment(SPECIES, segment, vectorOffset + (long) d7 * Float.BYTES, java.nio.ByteOrder.nativeOrder());
            accProd = vq7.fma(vb7, accProd);
            accNormA = vq7.fma(vq7, accNormA);
            accNormB = vb7.fma(vb7, accNormB);
        }

        // Reduce accumulated vectors (only once per 8 iterations!)
        double dotProduct = accProd.reduceLanes(VectorOperators.ADD);
        double normA = accNormA.reduceLanes(VectorOperators.ADD);
        double normB = accNormB.reduceLanes(VectorOperators.ADD);

        // Handle remaining vectors (0-7 iterations)
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
