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

import java.lang.foreign.MemorySegment;

/**
 * Unified dispatcher for optimized distance kernels.
 * Selects the best kernel based on dimension and metric.
 *
 * <p>Kernel priority:
 * <ol>
 *   <li>Hand-optimized kernels for specific dimensions (128, 1024)
 *   <li>Generic optimized for multiples of 8*lanes (256, 512, 768, 1536, 2048, etc.)
 *   <li>Fallback generic (any dimension)
 * </ol>
 */
public class OptimizedKernelDispatcher {

    public enum Metric {
        L2, L1, COSINE, DOT_PRODUCT
    }

    /**
     * Dispatch to the best available kernel for this dimension and metric.
     *
     * @param metric distance metric
     * @param dimension vector dimension
     * @param query query vector
     * @param base base vector MemorySegment
     * @param baseOffset offset in base segment
     * @return distance or similarity value
     */
    public static double dispatch(
        Metric metric,
        int dimension,
        float[] query,
        MemorySegment base,
        long baseOffset
    ) {
        // Check if we have an optimized kernel
        if (!hasOptimizedKernel(dimension)) {
            throw new UnsupportedOperationException("No optimized kernel for dimension: " + dimension);
        }

        return switch (metric) {
            case DOT_PRODUCT -> dispatchDotProduct(dimension, query, base, baseOffset);
            case COSINE -> dispatchCosine(dimension, query, base, baseOffset);
            case L2 -> dispatchL2(dimension, query, base, baseOffset);
            case L1 -> throw new UnsupportedOperationException("L1 uses generic only");
        };
    }

    private static double dispatchDotProduct(int dimension, float[] query, MemorySegment base, long baseOffset) {
        return switch (dimension) {
            case 128 -> DimensionSpecificKernels.dotProduct128(query, base, baseOffset);
            case 1024 -> DimensionSpecificKernels.dotProduct1024(query, base, baseOffset);
            default -> DimensionSpecificKernels.dotProductGenericOptimized(query, base, baseOffset, dimension);
        };
    }

    private static double dispatchCosine(int dimension, float[] query, MemorySegment base, long baseOffset) {
        return switch (dimension) {
            case 1024 -> DimensionSpecificKernels.cosine1024(query, base, baseOffset);
            default -> DimensionSpecificKernels.cosineGenericOptimized(query, base, baseOffset, dimension);
        };
    }

    private static double dispatchL2(int dimension, float[] query, MemorySegment base, long baseOffset) {
        return switch (dimension) {
            case 128 -> DimensionSpecificKernels.euclidean128(query, base, baseOffset);
            case 1024 -> DimensionSpecificKernels.euclidean1024(query, base, baseOffset);
            default -> DimensionSpecificKernels.euclideanGenericOptimized(query, base, baseOffset, dimension);
        };
    }

    /**
     * Check if we have an optimized kernel for this dimension.
     * True for multiples of 8*lanes (covers 128, 256, 512, 768, 1024, 1536, 2048, etc.)
     */
    public static boolean hasOptimizedKernel(int dimension) {
        return DimensionSpecificKernels.hasOptimizedKernel(dimension);
    }
}
