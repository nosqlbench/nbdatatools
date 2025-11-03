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

import io.nosqlbench.command.analyze.subcommands.verify_knn.datatypes.NeighborIndex;
import io.nosqlbench.command.common.DistanceMetricOption.DistanceMetric;

import java.util.List;

/**
 * Process MULTIPLE queries per thread for memory bandwidth efficiency.
 *
 * <p>Key insight: Each thread processes 2-4 queries instead of 1.
 * For each base vector, compute distances to ALL queries assigned to this thread.
 *
 * <p>Benefits:
 * <ul>
 *   <li>Simple (no transpose, no complex batching)
 *   <li>Memory bandwidth: 2-4x reduction (base vector reused immediately)
 *   <li>Cache hot: Base vector stays in L1 for 2-4 distance calculations
 *   <li>Parallel: Still uses all CPU cores
 * </ul>
 */
public class MultiQueryProcessor {

    /**
     * Process a group of queries with BASE VECTOR SPLITTING for 2D parallelism.
     * Processes only a slice of base vectors, allowing multiple threads to work simultaneously.
     *
     * @param queryGroup 8 queries assigned to this thread
     * @param panamaBatch persistent base vectors (shared)
     * @param globalStartIndex starting base vector index
     * @param baseStartIdx starting base vector for THIS thread
     * @param baseEndIdx ending base vector for THIS thread
     * @param topK neighbors per query
     * @param metric distance metric
     * @return partial results for merge
     */
    public static NeighborIndex[][] processQueryGroup(
        List<float[]> queryGroup,
        PanamaVectorBatch panamaBatch,
        int globalStartIndex,
        int topK,
        DistanceMetric metric
    ) {
        int groupSize = queryGroup.size();
        int baseCount = panamaBatch.size();
        int baseStartIdx = 0;
        int baseEndIdx = baseCount;

        // Pre-compute query norms if needed (COSINE optimization)
        double[] queryNorms = null;
        if (metric == DistanceMetric.COSINE) {
            queryNorms = new double[groupSize];
            for (int i = 0; i < groupSize; i++) {
                queryNorms[i] = PanamaVectorBatch.precomputeQueryNorm(queryGroup.get(i));
            }
        }

        // One primitive heap per query
        PrimitiveMinHeap[] heaps = new PrimitiveMinHeap[groupSize];
        for (int i = 0; i < groupSize; i++) {
            heaps[i] = new PrimitiveMinHeap(topK);
        }

        // MONOMORPHIZATION: Separate code paths per metric (eliminates branches!)
        if (metric == DistanceMetric.COSINE && queryNorms != null) {
            // COSINE with precomputed norms (branch-free hot path)
            processWithPrecomputedNorms(queryGroup, queryNorms, panamaBatch, heaps, globalStartIndex, baseStartIdx, baseEndIdx);
        } else {
            // All other metrics (branch-free hot path)
            int metricCode = encodeMetric(metric);
            processWithMetricCode(queryGroup, panamaBatch, heaps, globalStartIndex, baseStartIdx, baseEndIdx, metricCode);
        }

        // Extract results
        NeighborIndex[][] results = new NeighborIndex[groupSize][];
        for (int i = 0; i < groupSize; i++) {
            results[i] = heaps[i].toSortedArray();
        }

        return results;
    }

    /**
     * MONOMORPHIC: COSINE with precomputed norms (ZERO branches in hot loop!).
     */
    private static void processWithPrecomputedNorms(
        List<float[]> queryGroup,
        double[] queryNorms,
        PanamaVectorBatch panamaBatch,
        PrimitiveMinHeap[] heaps,
        int globalStartIndex,
        int baseStartIdx,
        int baseEndIdx
    ) {
        int groupSize = queryGroup.size();
        int baseIdx = baseStartIdx;
        int unrollBound = baseEndIdx - 1;

        for (; baseIdx < unrollBound; baseIdx += 2) {
            for (int qIdx = 0; qIdx < groupSize; qIdx++) {
                float[] query = queryGroup.get(qIdx);
                double d0 = panamaBatch.computeSingleCosineDistanceWithPrecomputedNorm(query, queryNorms[qIdx], baseIdx);
                heaps[qIdx].offer(globalStartIndex + baseIdx, d0);
            }

            int baseIdx1 = baseIdx + 1;
            for (int qIdx = 0; qIdx < groupSize; qIdx++) {
                float[] query = queryGroup.get(qIdx);
                double d1 = panamaBatch.computeSingleCosineDistanceWithPrecomputedNorm(query, queryNorms[qIdx], baseIdx1);
                heaps[qIdx].offer(globalStartIndex + baseIdx1, d1);
            }
        }

        if (baseIdx < baseEndIdx) {
            for (int qIdx = 0; qIdx < groupSize; qIdx++) {
                float[] query = queryGroup.get(qIdx);
                double d = panamaBatch.computeSingleCosineDistanceWithPrecomputedNorm(query, queryNorms[qIdx], baseIdx);
                heaps[qIdx].offer(globalStartIndex + baseIdx, d);
            }
        }
    }

    /**
     * MONOMORPHIC: Generic metric (DOT_PRODUCT/L2/L1 - ZERO branches in hot loop!).
     */
    private static void processWithMetricCode(
        List<float[]> queryGroup,
        PanamaVectorBatch panamaBatch,
        PrimitiveMinHeap[] heaps,
        int globalStartIndex,
        int baseStartIdx,
        int baseEndIdx,
        int metricCode
    ) {
        int groupSize = queryGroup.size();
        int baseIdx = baseStartIdx;
        int unrollBound = baseEndIdx - 1;

        for (; baseIdx < unrollBound; baseIdx += 2) {
            for (int qIdx = 0; qIdx < groupSize; qIdx++) {
                float[] query = queryGroup.get(qIdx);
                double d0 = panamaBatch.computeSingleDistance(query, baseIdx, metricCode);
                heaps[qIdx].offer(globalStartIndex + baseIdx, d0);
            }

            int baseIdx1 = baseIdx + 1;
            for (int qIdx = 0; qIdx < groupSize; qIdx++) {
                float[] query = queryGroup.get(qIdx);
                double d1 = panamaBatch.computeSingleDistance(query, baseIdx1, metricCode);
                heaps[qIdx].offer(globalStartIndex + baseIdx1, d1);
            }
        }

        if (baseIdx < baseEndIdx) {
            for (int qIdx = 0; qIdx < groupSize; qIdx++) {
                float[] query = queryGroup.get(qIdx);
                double d = panamaBatch.computeSingleDistance(query, baseIdx, metricCode);
                heaps[qIdx].offer(globalStartIndex + baseIdx, d);
            }
        }
    }

    private static int encodeMetric(DistanceMetric metric) {
        return switch (metric) {
            case L2 -> 0;
            case L1 -> 1;
            case COSINE -> 2;
            case DOT_PRODUCT -> 3;
        };
    }
}
