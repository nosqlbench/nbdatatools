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

package io.nosqlbench.command.compute;

import io.nosqlbench.command.analyze.subcommands.verify_knn.computation.NeighborhoodComparison;
import io.nosqlbench.command.analyze.subcommands.verify_knn.datatypes.NeighborIndex;
import io.nosqlbench.nbdatatools.api.fileio.VectorFileArray;
import io.nosqlbench.nbdatatools.api.fileio.VectorFileStreamStore;
import io.nosqlbench.nbdatatools.api.services.FileType;
import io.nosqlbench.nbdatatools.api.services.VectorFileIO;
import io.nosqlbench.vectordata.spec.datasets.types.DistanceFunction;
import io.nosqlbench.vectordata.spec.datasets.types.Indexed;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for compute_knn and verify_knn logic.
 * Tests partition caching and verification with synthetic data.
 */
public class ComputeAndVerifyKnnIntegrationTest {

    /**
     * Test compute_knn partition logic followed by verify_knn verification.
     * Uses small synthetic dataset to test end-to-end correctness.
     */
    @Test
    public void testComputeAndVerifyWithPartitions(@TempDir Path tempDir) throws Exception {
        // Configuration
        final int BASE_COUNT = 1000;      // Total base vectors
        final int QUERY_COUNT = 10;       // Query vectors
        final int DIMENSION = 128;        // Vector dimension
        final int K = 10;                 // Neighbors to find
        final int PARTITION_SIZE = 250;   // Small partitions to test multi-partition logic
        final DistanceFunction distanceFunction = DistanceFunction.COSINE;

        // Generate synthetic test data
        Path baseFile = tempDir.resolve("base.fvec");
        Path queryFile = tempDir.resolve("query.fvec");

        Random random = new Random(42); // Fixed seed for reproducibility
        List<float[]> baseVectors = generateNormalizedVectors(BASE_COUNT, DIMENSION, random);
        List<float[]> queryVectors = generateNormalizedVectors(QUERY_COUNT, DIMENSION, random);

        writeVectors(baseFile, baseVectors);
        writeVectors(queryFile, queryVectors);

        // Compute partitions
        int numPartitions = (int) Math.ceil((double) BASE_COUNT / PARTITION_SIZE);
        assertEquals(4, numPartitions, "Should have 4 partitions");

        List<PartitionResult> partitionResults = new ArrayList<>();

        // Compute KNN for each partition
        for (int partIdx = 0; partIdx < numPartitions; partIdx++) {
            int startIdx = partIdx * PARTITION_SIZE;
            int endIdx = Math.min(startIdx + PARTITION_SIZE, BASE_COUNT);

            Path partitionNeighbors = tempDir.resolve(String.format("partition_%d_neighbors.ivec", partIdx));
            Path partitionDistances = tempDir.resolve(String.format("partition_%d_distances.fvec", partIdx));

            // Load partition of base vectors
            List<float[]> partitionBase = baseVectors.subList(startIdx, endIdx);

            // Compute KNN for this partition
            computePartitionKNN(queryVectors, partitionBase, startIdx, K, distanceFunction,
                partitionNeighbors, partitionDistances);

            partitionResults.add(new PartitionResult(partitionNeighbors, partitionDistances, startIdx, endIdx));
        }

        // Merge partitions
        Path finalNeighbors = tempDir.resolve("final_neighbors.ivec");
        Path finalDistances = tempDir.resolve("final_distances.fvec");

        mergePartitionResults(partitionResults, K, QUERY_COUNT, finalNeighbors, finalDistances);

        // Verify each partition individually
        for (int partIdx = 0; partIdx < numPartitions; partIdx++) {
            PartitionResult partition = partitionResults.get(partIdx);

            // Load partition base vectors
            int startIdx = partition.startIndex;
            int endIdx = partition.endIndex;
            List<float[]> partitionBase = baseVectors.subList(startIdx, endIdx);

            // Verify partition results
            verifyPartition(partition.neighborsPath, queryVectors, partitionBase, startIdx,
                K, distanceFunction, "Partition " + partIdx);
        }

        // Verify merged results against full base vectors
        verifyMergedResults(finalNeighbors, queryVectors, baseVectors, K, distanceFunction);
    }

    /**
     * Test cache reuse: compute partitions, then "recompute" using cache
     */
    @Test
    public void testPartitionCacheReuse(@TempDir Path tempDir) throws Exception {
        final int BASE_COUNT = 500;
        final int QUERY_COUNT = 5;
        final int DIMENSION = 64;
        final int K = 10;
        final int PARTITION_SIZE = 150;  // 4 partitions
        final DistanceFunction distanceFunction = DistanceFunction.COSINE;

        Random random = new Random(999);
        List<float[]> baseVectors = generateNormalizedVectors(BASE_COUNT, DIMENSION, random);
        List<float[]> queryVectors = generateNormalizedVectors(QUERY_COUNT, DIMENSION, random);

        Path cacheDir = tempDir.resolve(".knn-cache");
        Files.createDirectories(cacheDir);

        // First pass: compute all partitions
        List<PartitionResult> firstPassResults = new ArrayList<>();
        int numPartitions = (int) Math.ceil((double) BASE_COUNT / PARTITION_SIZE);

        for (int partIdx = 0; partIdx < numPartitions; partIdx++) {
            int startIdx = partIdx * PARTITION_SIZE;
            int endIdx = Math.min(startIdx + PARTITION_SIZE, BASE_COUNT);

            Path partitionNeighbors = cacheDir.resolve(
                String.format("base.range_%06d_%06d.k%d.cosine.neighbors.ivec", startIdx, endIdx, K));
            Path partitionDistances = cacheDir.resolve(
                String.format("base.range_%06d_%06d.k%d.cosine.distances.fvec", startIdx, endIdx, K));

            List<float[]> partitionBase = baseVectors.subList(startIdx, endIdx);
            computePartitionKNN(queryVectors, partitionBase, startIdx, K, distanceFunction,
                partitionNeighbors, partitionDistances);

            firstPassResults.add(new PartitionResult(partitionNeighbors, partitionDistances, startIdx, endIdx));

            // Verify each partition immediately
            verifyPartition(partitionNeighbors, queryVectors, partitionBase, startIdx, K,
                distanceFunction, "First pass partition " + partIdx);
        }

        // Second pass: reuse cached partitions (simulate cache hit)
        for (int partIdx = 0; partIdx < numPartitions; partIdx++) {
            PartitionResult cachedPartition = firstPassResults.get(partIdx);

            assertTrue(Files.exists(cachedPartition.neighborsPath),
                "Cache file should exist: " + cachedPartition.neighborsPath);
            assertTrue(Files.exists(cachedPartition.distancesPath),
                "Cache file should exist: " + cachedPartition.distancesPath);

            // Verify cached partition (should still be correct)
            int startIdx = cachedPartition.startIndex;
            int endIdx = cachedPartition.endIndex;
            List<float[]> partitionBase = baseVectors.subList(startIdx, endIdx);

            verifyPartition(cachedPartition.neighborsPath, queryVectors, partitionBase, startIdx, K,
                distanceFunction, "Cached partition " + partIdx);
        }

        // Merge cached partitions
        Path finalNeighbors = tempDir.resolve("final_neighbors.ivec");
        Path finalDistances = tempDir.resolve("final_distances.fvec");
        mergePartitionResults(firstPassResults, K, QUERY_COUNT, finalNeighbors, finalDistances);

        // Verify merged results
        verifyMergedResults(finalNeighbors, queryVectors, baseVectors, K, distanceFunction);
    }

    /**
     * Test with single partition (no merging needed)
     */
    @Test
    public void testComputeAndVerifySinglePartition(@TempDir Path tempDir) throws Exception {
        final int BASE_COUNT = 200;
        final int QUERY_COUNT = 5;
        final int DIMENSION = 64;
        final int K = 5;
        final DistanceFunction distanceFunction = DistanceFunction.L2;

        Random random = new Random(123);
        List<float[]> baseVectors = generateVectors(BASE_COUNT, DIMENSION, random);
        List<float[]> queryVectors = generateVectors(QUERY_COUNT, DIMENSION, random);

        Path baseFile = tempDir.resolve("base.fvec");
        Path queryFile = tempDir.resolve("query.fvec");
        Path neighborsFile = tempDir.resolve("neighbors.ivec");
        Path distancesFile = tempDir.resolve("distances.fvec");

        writeVectors(baseFile, baseVectors);
        writeVectors(queryFile, queryVectors);

        // Compute KNN (single partition)
        computePartitionKNN(queryVectors, baseVectors, 0, K, distanceFunction,
            neighborsFile, distancesFile);

        // Verify results
        verifyPartition(neighborsFile, queryVectors, baseVectors, 0, K, distanceFunction, "Single partition");
    }

    /**
     * Test with uneven partition sizes (last partition smaller)
     */
    @Test
    public void testUnevenPartitionSizes(@TempDir Path tempDir) throws Exception {
        final int BASE_COUNT = 350;      // Not evenly divisible by partition size
        final int QUERY_COUNT = 3;
        final int DIMENSION = 32;
        final int K = 5;
        final int PARTITION_SIZE = 100;  // Will create 4 partitions: 100, 100, 100, 50
        final DistanceFunction distanceFunction = DistanceFunction.L2;

        Random random = new Random(777);
        List<float[]> baseVectors = generateVectors(BASE_COUNT, DIMENSION, random);
        List<float[]> queryVectors = generateVectors(QUERY_COUNT, DIMENSION, random);

        int numPartitions = (int) Math.ceil((double) BASE_COUNT / PARTITION_SIZE);
        assertEquals(4, numPartitions, "Should have 4 partitions");

        List<PartitionResult> partitionResults = new ArrayList<>();

        // Compute each partition
        for (int partIdx = 0; partIdx < numPartitions; partIdx++) {
            int startIdx = partIdx * PARTITION_SIZE;
            int endIdx = Math.min(startIdx + PARTITION_SIZE, BASE_COUNT);
            int partitionSize = endIdx - startIdx;

            if (partIdx == numPartitions - 1) {
                assertEquals(50, partitionSize, "Last partition should be smaller (50 vectors)");
            } else {
                assertEquals(100, partitionSize, "Regular partitions should be 100 vectors");
            }

            Path partitionNeighbors = tempDir.resolve(String.format("partition_%d_neighbors.ivec", partIdx));
            Path partitionDistances = tempDir.resolve(String.format("partition_%d_distances.fvec", partIdx));

            List<float[]> partitionBase = baseVectors.subList(startIdx, endIdx);
            computePartitionKNN(queryVectors, partitionBase, startIdx, K, distanceFunction,
                partitionNeighbors, partitionDistances);

            partitionResults.add(new PartitionResult(partitionNeighbors, partitionDistances, startIdx, endIdx));

            // Verify partition
            verifyPartition(partitionNeighbors, queryVectors, partitionBase, startIdx, K,
                distanceFunction, "Partition " + partIdx + " (size=" + partitionSize + ")");
        }

        // Merge and verify
        Path finalNeighbors = tempDir.resolve("final_neighbors.ivec");
        Path finalDistances = tempDir.resolve("final_distances.fvec");
        mergePartitionResults(partitionResults, K, QUERY_COUNT, finalNeighbors, finalDistances);
        verifyMergedResults(finalNeighbors, queryVectors, baseVectors, K, distanceFunction);
    }

    /**
     * Compute KNN for a partition using the same logic as CMD_compute_knn
     */
    private void computePartitionKNN(
        List<float[]> queryVectors,
        List<float[]> baseVectors,
        int globalStartIndex,
        int k,
        DistanceFunction distanceFunction,
        Path neighborsOutput,
        Path distancesOutput
    ) throws IOException {

        try (VectorFileStreamStore<int[]> neighborsStore = VectorFileIO.streamOut(FileType.xvec, int[].class, neighborsOutput)
                .orElseThrow(() -> new IOException("Could not create neighbors file"));
             VectorFileStreamStore<float[]> distancesStore = VectorFileIO.streamOut(FileType.xvec, float[].class, distancesOutput)
                .orElseThrow(() -> new IOException("Could not create distances file"))) {

            for (float[] queryVector : queryVectors) {
                // Find top-K neighbors for this query
                NeighborIndex[] topK = findTopKNeighbors(queryVector, baseVectors, globalStartIndex, k, distanceFunction);

                // Write results
                int[] neighborIndices = new int[topK.length];
                float[] distances = new float[topK.length];
                for (int i = 0; i < topK.length; i++) {
                    neighborIndices[i] = (int) topK[i].index();
                    distances[i] = (float) topK[i].distance();
                }

                neighborsStore.write(neighborIndices);
                distancesStore.write(distances);
            }
        }
    }

    /**
     * Find top-K neighbors using standard algorithm (same as CMD_compute_knn fallback)
     */
    private NeighborIndex[] findTopKNeighbors(
        float[] queryVector,
        List<float[]> baseVectors,
        int globalStartIndex,
        int k,
        DistanceFunction distanceFunction
    ) {
        PriorityQueue<NeighborIndex> heap = new PriorityQueue<>(
            Math.min(k, baseVectors.size()),
            Comparator.comparingDouble(NeighborIndex::distance).reversed()
        );

        for (int i = 0; i < baseVectors.size(); i++) {
            float[] baseVector = baseVectors.get(i);
            double distance = distanceFunction.distance(queryVector, baseVector);
            NeighborIndex candidate = new NeighborIndex(globalStartIndex + i, distance);

            if (heap.size() < k) {
                heap.offer(candidate);
            } else if (distance < heap.peek().distance()) {
                heap.poll();
                heap.offer(candidate);
            }
        }

        // Extract results in sorted order (ascending distance)
        int resultSize = heap.size();
        NeighborIndex[] result = new NeighborIndex[resultSize];
        for (int i = resultSize - 1; i >= 0; i--) {
            result[i] = heap.poll();
        }
        return result;
    }

    /**
     * Merge multiple partition results into final output
     */
    private void mergePartitionResults(
        List<PartitionResult> partitions,
        int k,
        int queryCount,
        Path finalNeighbors,
        Path finalDistances
    ) throws IOException {

        try (VectorFileStreamStore<int[]> neighborsStore = VectorFileIO.streamOut(FileType.xvec, int[].class, finalNeighbors)
                .orElseThrow(() -> new IOException("Could not create final neighbors file"));
             VectorFileStreamStore<float[]> distancesStore = VectorFileIO.streamOut(FileType.xvec, float[].class, finalDistances)
                .orElseThrow(() -> new IOException("Could not create final distances file"))) {

            // Open all partition files
            List<VectorFileArray<int[]>> neighborArrays = new ArrayList<>();
            List<VectorFileArray<float[]>> distanceArrays = new ArrayList<>();

            try {
                for (PartitionResult partition : partitions) {
                    neighborArrays.add(VectorFileIO.randomAccess(FileType.xvec, int[].class, partition.neighborsPath));
                    distanceArrays.add(VectorFileIO.randomAccess(FileType.xvec, float[].class, partition.distancesPath));
                }

                // Merge query by query
                for (int queryIdx = 0; queryIdx < queryCount; queryIdx++) {
                    List<NeighborIndex> combined = new ArrayList<>();

                    // Collect results from all partitions for this query
                    for (int partIdx = 0; partIdx < partitions.size(); partIdx++) {
                        int[] partitionNeighbors = neighborArrays.get(partIdx).get(queryIdx);
                        float[] partitionDistances = distanceArrays.get(partIdx).get(queryIdx);

                        for (int i = 0; i < partitionNeighbors.length; i++) {
                            combined.add(new NeighborIndex(partitionNeighbors[i], partitionDistances[i]));
                        }
                    }

                    // Sort and take top-K
                    combined.sort(Comparator.comparingDouble(NeighborIndex::distance));
                    int resultSize = Math.min(k, combined.size());

                    int[] finalNeighborIndices = new int[resultSize];
                    float[] finalDistanceValues = new float[resultSize];
                    for (int i = 0; i < resultSize; i++) {
                        finalNeighborIndices[i] = (int) combined.get(i).index();
                        finalDistanceValues[i] = (float) combined.get(i).distance();
                    }

                    neighborsStore.write(finalNeighborIndices);
                    distancesStore.write(finalDistanceValues);
                }
            } finally {
                for (VectorFileArray<?> array : neighborArrays) {
                    try { array.close(); } catch (Exception e) {}
                }
                for (VectorFileArray<?> array : distanceArrays) {
                    try { array.close(); } catch (Exception e) {}
                }
            }
        }
    }

    /**
     * Verify partition results using verify_knn logic
     */
    private void verifyPartition(
        Path neighborsFile,
        List<float[]> queryVectors,
        List<float[]> baseVectors,
        int globalStartIndex,
        int k,
        DistanceFunction distanceFunction,
        String partitionName
    ) throws IOException {

        try (VectorFileArray<int[]> neighborsArray = VectorFileIO.randomAccess(FileType.xvec, int[].class, neighborsFile)) {

            assertEquals(queryVectors.size(), neighborsArray.size(),
                partitionName + ": neighbors file should have entry for each query");

            int errors = 0;
            for (int queryIdx = 0; queryIdx < queryVectors.size(); queryIdx++) {
                float[] queryVector = queryVectors.get(queryIdx);
                int[] providedNeighbors = neighborsArray.get(queryIdx);

                // Compute expected neighbors for this partition
                NeighborIndex[] expectedTopK = findTopKNeighbors(
                    queryVector, baseVectors, globalStartIndex, k, distanceFunction);

                int[] expectedNeighbors = new int[expectedTopK.length];
                for (int i = 0; i < expectedTopK.length; i++) {
                    expectedNeighbors[i] = (int) expectedTopK[i].index();
                }

                // Create comparison (same as verify_knn command)
                Indexed<float[]> indexedQuery = new Indexed<>(queryIdx, queryVector);
                NeighborhoodComparison comparison = new NeighborhoodComparison(
                    indexedQuery, providedNeighbors, expectedNeighbors);

                if (comparison.isError()) {
                    System.err.println(partitionName + " Query " + queryIdx + ": " + comparison);
                    errors++;
                }
            }

            assertEquals(0, errors, partitionName + " should have no verification errors");
        }
    }

    /**
     * Verify merged results against full base vectors
     */
    private void verifyMergedResults(
        Path neighborsFile,
        List<float[]> queryVectors,
        List<float[]> baseVectors,
        int k,
        DistanceFunction distanceFunction
    ) throws IOException {

        try (VectorFileArray<int[]> neighborsArray = VectorFileIO.randomAccess(FileType.xvec, int[].class, neighborsFile)) {

            assertEquals(queryVectors.size(), neighborsArray.size(),
                "Final neighbors file should have entry for each query");

            int errors = 0;
            int totalMissing = 0;
            int totalExtra = 0;

            for (int queryIdx = 0; queryIdx < queryVectors.size(); queryIdx++) {
                float[] queryVector = queryVectors.get(queryIdx);
                int[] providedNeighbors = neighborsArray.get(queryIdx);

                assertEquals(k, providedNeighbors.length,
                    "Query " + queryIdx + " should have exactly k=" + k + " neighbors");

                // Compute ground truth from full base vectors
                NeighborIndex[] expectedTopK = findTopKNeighbors(
                    queryVector, baseVectors, 0, k, distanceFunction);

                int[] expectedNeighbors = new int[expectedTopK.length];
                for (int i = 0; i < expectedTopK.length; i++) {
                    expectedNeighbors[i] = (int) expectedTopK[i].index();
                }

                // Verify using verify_knn logic
                Indexed<float[]> indexedQuery = new Indexed<>(queryIdx, queryVector);
                NeighborhoodComparison comparison = new NeighborhoodComparison(
                    indexedQuery, providedNeighbors, expectedNeighbors);

                if (comparison.isError()) {
                    String compStr = comparison.toString();
                    System.err.println("Query " + queryIdx + ": " + compStr);

                    // Parse the comparison string to extract metrics
                    if (compStr.contains("missing=")) {
                        String[] parts = compStr.split("missing=");
                        if (parts.length > 1) {
                            String missingStr = parts[1].replaceAll("[^0-9].*", "");
                            totalMissing += Integer.parseInt(missingStr);
                        }
                    }
                    if (compStr.contains("extra=")) {
                        String[] parts = compStr.split("extra=");
                        if (parts.length > 1) {
                            String extraStr = parts[1].split(",")[0];
                            totalExtra += Integer.parseInt(extraStr);
                        }
                    }

                    errors++;
                }
            }

            if (errors > 0) {
                fail(String.format("Merged results verification FAILED: %d/%d queries had errors (total missing=%d, extra=%d)",
                    errors, queryVectors.size(), totalMissing, totalExtra));
            }
        }
    }

    /**
     * Generate normalized random vectors (for COSINE distance)
     */
    private List<float[]> generateNormalizedVectors(int count, int dimension, Random random) {
        List<float[]> vectors = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            float[] vector = new float[dimension];
            double sumSquares = 0.0;
            for (int d = 0; d < dimension; d++) {
                vector[d] = (float) (random.nextGaussian());
                sumSquares += vector[d] * vector[d];
            }
            // Normalize to unit length
            double norm = Math.sqrt(sumSquares);
            if (norm > 0) {
                for (int d = 0; d < dimension; d++) {
                    vector[d] /= norm;
                }
            }
            vectors.add(vector);
        }
        return vectors;
    }

    /**
     * Generate random vectors (for L2/L1 distance)
     */
    private List<float[]> generateVectors(int count, int dimension, Random random) {
        List<float[]> vectors = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            float[] vector = new float[dimension];
            for (int d = 0; d < dimension; d++) {
                vector[d] = (float) (random.nextGaussian() * 10.0);
            }
            vectors.add(vector);
        }
        return vectors;
    }

    /**
     * Write vectors to file using VectorFileIO
     */
    private void writeVectors(Path path, List<float[]> vectors) throws IOException {
        try (VectorFileStreamStore<float[]> store = VectorFileIO.streamOut(FileType.xvec, float[].class, path)
                .orElseThrow(() -> new IOException("Could not create vector file: " + path))) {
            for (float[] vector : vectors) {
                store.write(vector);
            }
        }
    }

    /**
     * Test that specifically checks partition index mapping.
     * This test ensures that global indices are correctly preserved across partitions.
     */
    @Test
    public void testPartitionIndexMapping(@TempDir Path tempDir) throws Exception {
        final int BASE_COUNT = 300;
        final int QUERY_COUNT = 2;
        final int DIMENSION = 16;
        final int K = 3;
        final int PARTITION_SIZE = 100;
        final DistanceFunction distanceFunction = DistanceFunction.L2;

        Random random = new Random(555);

        // Create easily identifiable vectors: vector[i] = [i, i, i, ...]
        List<float[]> baseVectors = new ArrayList<>();
        for (int i = 0; i < BASE_COUNT; i++) {
            float[] vector = new float[DIMENSION];
            Arrays.fill(vector, (float) i);
            baseVectors.add(vector);
        }

        // Query vectors: [0.5, 0.5, ...] and [150.5, 150.5, ...]
        // Should find nearest neighbors around those values
        List<float[]> queryVectors = new ArrayList<>();
        float[] query1 = new float[DIMENSION];
        Arrays.fill(query1, 0.5f);
        queryVectors.add(query1);

        float[] query2 = new float[DIMENSION];
        Arrays.fill(query2, 150.5f);
        queryVectors.add(query2);

        // Compute with partitions
        List<PartitionResult> partitionResults = new ArrayList<>();
        for (int partIdx = 0; partIdx < 3; partIdx++) {
            int startIdx = partIdx * PARTITION_SIZE;
            int endIdx = Math.min(startIdx + PARTITION_SIZE, BASE_COUNT);

            Path partitionNeighbors = tempDir.resolve(String.format("partition_%d_neighbors.ivec", partIdx));
            Path partitionDistances = tempDir.resolve(String.format("partition_%d_distances.fvec", partIdx));

            List<float[]> partitionBase = baseVectors.subList(startIdx, endIdx);
            computePartitionKNN(queryVectors, partitionBase, startIdx, K, distanceFunction,
                partitionNeighbors, partitionDistances);

            partitionResults.add(new PartitionResult(partitionNeighbors, partitionDistances, startIdx, endIdx));

            // Read and log what this partition computed
            try (VectorFileArray<int[]> neighbors = VectorFileIO.randomAccess(FileType.xvec, int[].class, partitionNeighbors);
                 VectorFileArray<float[]> distances = VectorFileIO.randomAccess(FileType.xvec, float[].class, partitionDistances)) {

                for (int qIdx = 0; qIdx < queryVectors.size(); qIdx++) {
                    int[] neighborIndices = neighbors.get(qIdx);
                    float[] neighborDistances = distances.get(qIdx);

                    System.out.println(String.format("Partition %d [%d..%d), Query %d neighbors: %s",
                        partIdx, startIdx, endIdx, qIdx, Arrays.toString(neighborIndices)));

                    // Verify that all neighbor indices are within this partition's range
                    for (int neighborIdx : neighborIndices) {
                        assertTrue(neighborIdx >= startIdx && neighborIdx < endIdx,
                            String.format("Partition %d neighbor index %d should be in range [%d..%d)",
                                partIdx, neighborIdx, startIdx, endIdx));
                    }
                }
            }
        }

        // Merge partitions
        Path finalNeighbors = tempDir.resolve("final_neighbors.ivec");
        Path finalDistances = tempDir.resolve("final_distances.fvec");
        mergePartitionResults(partitionResults, K, QUERY_COUNT, finalNeighbors, finalDistances);

        // Verify merged results - verify correctness by recomputing from scratch
        verifyMergedResults(finalNeighbors, queryVectors, baseVectors, K, distanceFunction);
    }

    /**
     * Metadata for a partition result
     */
    private static class PartitionResult {
        final Path neighborsPath;
        final Path distancesPath;
        final int startIndex;
        final int endIndex;

        PartitionResult(Path neighborsPath, Path distancesPath, int startIndex, int endIndex) {
            this.neighborsPath = neighborsPath;
            this.distancesPath = distancesPath;
            this.startIndex = startIndex;
            this.endIndex = endIndex;
        }
    }
}

