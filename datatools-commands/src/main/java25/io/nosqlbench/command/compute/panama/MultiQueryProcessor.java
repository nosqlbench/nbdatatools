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
     * Process a group of queries assigned to one thread.
     * Each base vector is loaded ONCE and used for ALL queries in the group.
     *
     * @param queryGroup 2-4 queries assigned to this thread
     * @param panamaBatch persistent base vectors
     * @param globalStartIndex starting base vector index
     * @param topK neighbors per query
     * @param metric distance metric
     * @return results for each query in group
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

        // THE KEY LOOP: For each base vector, process ALL queries in group
        for (int baseIdx = 0; baseIdx < baseCount; baseIdx++) {

            // Process each query in this group against the SAME base vector
            for (int qIdx = 0; qIdx < groupSize; qIdx++) {
                float[] query = queryGroup.get(qIdx);

                double distance;
                int metricCode = encodeMetric(metric);

                if (metric == DistanceMetric.COSINE && queryNorms != null) {
                    distance = panamaBatch.computeSingleCosineDistanceWithPrecomputedNorm(query, queryNorms[qIdx], baseIdx);
                } else {
                    distance = panamaBatch.computeSingleDistance(query, baseIdx, metricCode);
                }

                heaps[qIdx].offer(globalStartIndex + baseIdx, distance);
            }
        }

        // Extract results
        NeighborIndex[][] results = new NeighborIndex[groupSize][];
        for (int i = 0; i < groupSize; i++) {
            results[i] = heaps[i].toSortedArray();
        }

        return results;
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
