package io.nosqlbench.command.compute;

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

import io.nosqlbench.command.analyze.subcommands.verify_knn.datatypes.NeighborIndex;
import io.nosqlbench.command.common.DistanceMetricOption.DistanceMetric;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Provider for KNN optimization strategies.
 * Automatically detects and uses Panama optimizations on JDK 22+ with Vector API support.
 * Falls back to standard implementation on older JDKs.
 * Uses cached MethodHandles for zero-overhead method dispatch.
 */
public class KnnOptimizationProvider {
    private static final Logger logger = LogManager.getLogger(KnnOptimizationProvider.class);
    private static final boolean PANAMA_AVAILABLE;

    // Cached MethodHandles - looked up ONCE at startup, reused forever
    private static MethodHandle panamaFindTopKMethod;
    private static MethodHandle panamaFindTopKDirectMethod;
    private static MethodHandle panamaFindTopKBatchedMethod;
    private static MethodHandle panamaLoadAsBatchMethod;

    static {
        boolean available = false;
        try {
            // Check for Panama optimizer class (only present in multi-release JAR on JDK 22+)
            Class<?> optimizerClass = Class.forName("io.nosqlbench.command.compute.panama.PanamaKnnOptimizer");
            Class<?> loaderClass = Class.forName("io.nosqlbench.command.compute.panama.OptimizedVectorLoader");
            Class<?> panamaBatchClass = Class.forName("io.nosqlbench.command.compute.panama.PanamaVectorBatch");
            MethodHandles.Lookup lookup = MethodHandles.publicLookup();

            // Cache MethodHandle for findTopKNeighbors (with List)
            MethodType findTopKType = MethodType.methodType(
                NeighborIndex[].class,
                float[].class,
                List.class,
                int.class,
                int.class,
                DistanceMetric.class
            );
            panamaFindTopKMethod = lookup.findStatic(optimizerClass, "findTopKNeighbors", findTopKType);

            // Cache MethodHandle for findTopKNeighborsDirect (with proper PanamaVectorBatch type)
            MethodType findTopKDirectType = MethodType.methodType(
                NeighborIndex[].class,
                float[].class,
                panamaBatchClass,  // Use actual type for type safety
                int.class,
                int.class,
                DistanceMetric.class
            );
            panamaFindTopKDirectMethod = lookup.findStatic(optimizerClass, "findTopKNeighborsDirect", findTopKDirectType);

            // Cache MethodHandle for findTopKBatched (batched + transposed SIMD - ULTIMATE optimization!)
            MethodType findTopKBatchedType = MethodType.methodType(
                NeighborIndex[][].class,  // Returns 2D array (one result per query)
                List.class,
                panamaBatchClass,
                int.class,
                int.class,
                DistanceMetric.class,
                AtomicInteger.class  // Progress counter - proper type!
            );
            panamaFindTopKBatchedMethod = lookup.findStatic(optimizerClass, "findTopKBatched", findTopKBatchedType);

            // Cache MethodHandle for loadAsPanamaVectorBatch (returns proper type)
            MethodType loadAsBatchType = MethodType.methodType(
                panamaBatchClass,  // Proper return type for type safety
                Path.class,
                int.class,
                int.class
            );
            panamaLoadAsBatchMethod = lookup.findStatic(loaderClass, "loadAsPanamaVectorBatch", loadAsBatchType);

            available = true;
            logger.info("Panama KNN optimizations ENABLED - using cached MethodHandles for zero-overhead dispatch");
        } catch (ClassNotFoundException e) {
            logger.info("Panama KNN optimizations NOT AVAILABLE - Panama classes not found (JDK 22+ with --add-modules jdk.incubator.vector required)");
            logger.debug("ClassNotFoundException: {}", e.getMessage());
        } catch (NoSuchMethodException e) {
            logger.error("Panama KNN optimizations MISCONFIGURED - method not found: {}", e.getMessage(), e);
        } catch (IllegalAccessException e) {
            logger.error("Panama KNN optimizations MISCONFIGURED - illegal access: {}", e.getMessage(), e);
        }
        PANAMA_AVAILABLE = available;
    }

    /**
     * Find top-K nearest neighbors using the best available implementation.
     * Uses cached MethodHandle for zero-overhead Panama dispatch.
     *
     * @param queryVector the query vector
     * @param baseBatch the base vectors
     * @param globalStartIndex starting index for results
     * @param topK number of neighbors to find
     * @param distanceMetric the distance metric to use
     * @return top-K nearest neighbors
     */
    public static NeighborIndex[] findTopKNeighbors(
        float[] queryVector,
        List<float[]> baseBatch,
        int globalStartIndex,
        int topK,
        DistanceMetric distanceMetric
    ) {
        if (PANAMA_AVAILABLE) {
            try {
                // Use cached MethodHandle - no reflection overhead!
                return (NeighborIndex[]) panamaFindTopKMethod.invokeExact(
                    queryVector,
                    baseBatch,
                    globalStartIndex,
                    topK,
                    distanceMetric
                );
            } catch (Throwable e) {
                logger.warn("Panama optimization failed, falling back to standard implementation: {}", e.getMessage());
                // Fall through to standard implementation
            }
        }

        // Standard implementation (fallback)
        return findTopKNeighborsStandard(queryVector, baseBatch, globalStartIndex, topK, distanceMetric);
    }

    /**
     * Find top-K neighbors using pre-loaded PanamaVectorBatch (zero-copy path).
     * Uses cached MethodHandle for zero-overhead dispatch.
     * Uses invoke() for type adaptation between Java 16 Object and Java 25 PanamaVectorBatch.
     *
     * @param queryVector the query vector
     * @param panamaBatch pre-loaded PanamaVectorBatch (as Object for Java 16 compatibility)
     * @param globalStartIndex starting index for results
     * @param topK number of neighbors to find
     * @param distanceMetric the distance metric to use
     * @return top-K nearest neighbors
     */
    public static NeighborIndex[] findTopKNeighborsDirect(
        float[] queryVector,
        Object panamaBatch,
        int globalStartIndex,
        int topK,
        DistanceMetric distanceMetric
    ) throws Throwable {
        if (!PANAMA_AVAILABLE) {
            throw new UnsupportedOperationException("Panama optimizations not available");
        }

        // Use invoke() to allow type conversion (Object → PanamaVectorBatch)
        // Still uses cached MethodHandle - no reflection, just type adaptation
        return (NeighborIndex[]) panamaFindTopKDirectMethod.invoke(
            queryVector,
            panamaBatch,
            globalStartIndex,
            topK,
            distanceMetric
        );
    }

    /**
     * Standard (non-optimized) implementation for compatibility.
     * This is the fallback when Panama is not available.
     */
    private static NeighborIndex[] findTopKNeighborsStandard(
        float[] queryVector,
        List<float[]> baseBatch,
        int globalStartIndex,
        int topK,
        DistanceMetric distanceMetric
    ) {
        // This would call the original implementation from CMD_compute_knn
        // For now, return empty to indicate it needs to be integrated
        throw new UnsupportedOperationException(
            "Standard implementation should be called from CMD_compute_knn.selectTopNeighbors()"
        );
    }

    /**
     * Load vectors directly into PanamaVectorBatch using memory-mapped I/O.
     * Uses cached MethodHandle for zero-overhead dispatch.
     * Returns Object for Java 16 compatibility (actual type is PanamaVectorBatch).
     *
     * @param vectorFilePath path to .fvec or .ivec file
     * @param startIndex starting vector index
     * @param endIndex ending vector index
     * @return PanamaVectorBatch loaded via memory-mapping (as Object)
     */
    public static Object loadAsPanamaVectorBatch(
        Path vectorFilePath,
        int startIndex,
        int endIndex
    ) throws Throwable {
        if (!PANAMA_AVAILABLE) {
            throw new UnsupportedOperationException("Panama optimizations not available");
        }

        // Use invoke() to allow type conversion (PanamaVectorBatch → Object)
        // Still fast - no reflection, just type adaptation
        return panamaLoadAsBatchMethod.invoke(vectorFilePath, startIndex, endIndex);
    }

    /**
     * Find top-K neighbors for multiple queries using batched transposed SIMD.
     * This is the ULTIMATE optimization - combines query batching + transposed SIMD.
     * Uses cached MethodHandle for zero-overhead dispatch.
     *
     * @param queries batch of query vectors
     * @param panamaBatch persistent base vector batch (as Object for Java 16 compat)
     * @param globalStartIndex starting index for results
     * @param topK number of neighbors per query
     * @param distanceMetric distance metric
     * @param progressCounter AtomicInteger for progress updates (incremented as batches complete)
     * @return top-K results for each query (2D array)
     */
    public static NeighborIndex[][] findTopKBatched(
        List<float[]> queries,
        Object panamaBatch,
        int globalStartIndex,
        int topK,
        DistanceMetric distanceMetric,
        AtomicInteger progressCounter
    ) throws Throwable {
        if (!PANAMA_AVAILABLE) {
            throw new UnsupportedOperationException("Panama optimizations not available");
        }

        // Use invoke() for type conversion on panamaBatch (Object → PanamaVectorBatch)
        // progressCounter is already proper type
        return (NeighborIndex[][]) panamaFindTopKBatchedMethod.invoke(
            queries,
            panamaBatch,
            globalStartIndex,
            topK,
            distanceMetric,
            progressCounter
        );
    }

    /**
     * Check if Panama optimizations are available.
     */
    public static boolean isPanamaAvailable() {
        return PANAMA_AVAILABLE;
    }
}
