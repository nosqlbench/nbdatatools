package io.nosqlbench.command.compute.subcommands;

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
import io.nosqlbench.command.common.BaseVectorsFileOption;
import io.nosqlbench.command.common.QueryVectorsFileOption;
import io.nosqlbench.command.common.IndicesFileOption;
import io.nosqlbench.command.common.DistancesFileOption;
import io.nosqlbench.command.common.DistanceMetricOption;
import io.nosqlbench.command.common.DistanceMetricOption.DistanceMetric;
import io.nosqlbench.command.common.RangeOption;
import io.nosqlbench.command.compute.KnnOptimizationProvider;
import io.nosqlbench.nbdatatools.api.fileio.BoundedVectorFileStream;
import io.nosqlbench.nbdatatools.api.fileio.VectorFileArray;
import io.nosqlbench.nbdatatools.api.fileio.VectorFileStreamStore;
import io.nosqlbench.nbdatatools.api.services.FileType;
import io.nosqlbench.nbdatatools.api.services.VectorFileIO;
import io.nosqlbench.vectordata.spec.datasets.types.DistanceFunction;
import io.nosqlbench.vectordata.spec.datasets.types.FloatVectors;
import io.nosqlbench.status.StatusContext;
import io.nosqlbench.status.StatusScope;
import io.nosqlbench.status.StatusSinkMode;
import io.nosqlbench.status.StatusTracker;
import io.nosqlbench.status.eventing.RunState;
import io.nosqlbench.status.eventing.StatusSource;
import io.nosqlbench.status.eventing.StatusUpdate;
import io.nosqlbench.status.exec.TrackedExecutorService;
import io.nosqlbench.status.exec.TrackedExecutors;
import io.nosqlbench.status.exec.TrackingMode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/// Command to compute k-nearest neighbors ground truth dataset from base and query vectors
///
/// Range Specification:
/// - Via --range option: --range 1000, --range [0,1000), or --range 0..999
/// - Inline with base file path: -b base.fvec:1000, -b base.fvec:[0,1000), or -b base.fvec:0..999
/// - Currently only ranges starting from 0 are supported
@CommandLine.Command(name = "knn",
    description = "Compute k-nearest neighbors ground truth dataset from base and query vectors")
public class CMD_compute_knn implements Callable<Integer> {
    private static final Logger logger = LogManager.getLogger(CMD_compute_knn.class);

    private static final int EXIT_SUCCESS = 0;
    private static final int EXIT_FILE_EXISTS = 1;
    private static final int EXIT_ERROR = 2;

    @CommandLine.Mixin
    private BaseVectorsFileOption baseVectorsOption = new BaseVectorsFileOption();

    @CommandLine.Mixin
    private QueryVectorsFileOption queryVectorsOption = new QueryVectorsFileOption();

    @CommandLine.Mixin
    private IndicesFileOption indicesFileOption = new IndicesFileOption();

    @CommandLine.Mixin
    private DistancesFileOption distancesFileOption = new DistancesFileOption();

    @CommandLine.Option(names = {"-k", "--neighbors"}, description = "Number of nearest neighbors to find", required = true)
    private int k;

    @CommandLine.Mixin
    private DistanceMetricOption distanceMetricOption = new DistanceMetricOption();

    @CommandLine.Mixin
    private RangeOption rangeOption = new RangeOption();

    @CommandLine.Option(
        names = {"--cache-dir"},
        description = "Directory for partition cache files (default: .knn-cache in current directory)",
        defaultValue = ".knn-cache"
    )
    private Path cacheDir = Paths.get(".knn-cache");

    @CommandLine.Option(
        names = {"--partition-size"},
        description = "Number of base vectors per partition (default: 1000000, auto-scaled if insufficient memory)",
        defaultValue = "1000000"
    )
    private int partitionSize = 1_000_000;

    /// SIMD optimization strategy options
    public enum SimdStrategy {
        AUTO("Automatically select fastest strategy (defaults to BATCH)"),
        PER_QUERY("Process queries individually with SIMD across dimensions"),
        BATCH("Process 8-16 queries simultaneously with transposed SIMD (DEFAULT, FASTEST)"),
        BATCHED("Alias for BATCH");

        private final String description;

        SimdStrategy(String description) {
            this.description = description;
        }

        public String getDescription() {
            return description;
        }

        public boolean isBatched() {
            return this == BATCH || this == BATCHED;
        }
    }

    @CommandLine.Option(
        names = {"--simd-strategy"},
        description = "SIMD optimization strategy for Panama (JDK 25+ only). Default: AUTO (batched). Valid values: ${COMPLETION-CANDIDATES}",
        defaultValue = "AUTO"
    )
    private SimdStrategy simdStrategy = SimdStrategy.AUTO;

    @CommandLine.Spec
    private CommandLine.Model.CommandSpec spec;

    // Track if range came from path
    private boolean rangeFromPath = false;

    // Cached SIMD strategy (decided once at startup)
    private boolean useBatchedSIMD = false;

    /**
     * Validates the input and output paths before execution.
     */
    private void validatePaths() {
        // Validate base vectors file exists
        baseVectorsOption.validateBaseVectors();

        // Validate query vectors file exists
        queryVectorsOption.validateQueryVectors();

        // Validate indices output file
        indicesFileOption.validateIndicesOutput();

        // Set the indices path for the distances option so it can derive the path if needed
        distancesFileOption.setIndicesPath(indicesFileOption.getNormalizedIndicesPath());

        // Validate distances output file
        distancesFileOption.validateDistancesOutput(indicesFileOption.isForce());
    }

    /**
     * Parse a file path that may include an inline range specification.
     * Format: "path/to/file:rangespec" where rangespec uses the same formats as --range.
     *
     * @param pathString The path string to parse
     * @return An array with [actualPath, rangeSpec], where rangeSpec may be null
     */
    private String[] parsePathWithRange(String pathString) {
        if (pathString == null || pathString.isEmpty()) {
            return new String[]{pathString, null};
        }

        // Find the last colon that's not part of a Windows drive letter (e.g., C:)
        int colonIndex = -1;

        // Skip potential Windows drive letter (e.g., "C:")
        int searchStart = 0;
        if (pathString.length() >= 2 && pathString.charAt(1) == ':') {
            searchStart = 2;
        }

        // Look for a colon after the drive letter position
        colonIndex = pathString.indexOf(':', searchStart);

        if (colonIndex == -1) {
            // No range specification found
            return new String[]{pathString, null};
        }

        // Split into path and range spec
        String actualPath = pathString.substring(0, colonIndex);
        String rangeSpec = pathString.substring(colonIndex + 1);

        return new String[]{actualPath, rangeSpec};
    }


    /**
     * Calculate distance between two vectors based on the selected distance metric.
     * Uses the central DistanceFunction enum from datatools-vectordata.
     *
     * @param vec1 First vector
     * @param vec2 Second vector
     * @return Distance between the vectors
     */
    private double calculateDistance(float[] vec1, float[] vec2) {
        DistanceFunction distanceFunction = toDistanceFunction(distanceMetricOption.getDistanceMetric());
        return distanceFunction.distance(vec1, vec2);
    }

    /**
     * Maps DistanceMetric from command options to DistanceFunction enum.
     *
     * @param metric the distance metric from command options
     * @return the corresponding DistanceFunction
     */
    private DistanceFunction toDistanceFunction(DistanceMetric metric) {
        switch (metric) {
            case L2:
                return DistanceFunction.L2;
            case L1:
                return DistanceFunction.L1;
            case COSINE:
                return DistanceFunction.COSINE;
            case DOT_PRODUCT:
                return DistanceFunction.DOT_PRODUCT;
            default:
                throw new IllegalArgumentException("Unsupported distance metric: " + metric);
        }
    }

    /**
     * Determine batch/partition size for processing base vectors.
     * Starts with user-specified partitionSize (default 1M), scales down by half if insufficient memory.
     *
     * @param baseCount Total number of base vectors
     * @param dimension Vector dimensionality
     * @return Partition size that fits in available memory
     */
    private int determineBatchSize(int baseCount, int dimension) {
        Runtime runtime = Runtime.getRuntime();
        long maxHeapBytes = runtime.maxMemory();
        long totalMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();
        long allocated = totalMemory - freeMemory;
        long headroom = Math.max(0L, maxHeapBytes - allocated);
        long usable = Math.max((long) (maxHeapBytes * 0.2), (long) (headroom * 0.8));

        long bytesPerVector = estimateVectorBytes(dimension);
        if (bytesPerVector <= 0) {
            bytesPerVector = Math.max(16L, (long) dimension * Float.BYTES);
        }

        // Start with user-specified partition size (default 1M)
        int targetSize = partitionSize;

        // Scale down by half until it fits in memory
        while (targetSize > 0) {
            long requiredMemory = targetSize * bytesPerVector;
            if (requiredMemory <= usable) {
                logger.info("Selected partition size: {} vectors (requires ~{}MB, have ~{}MB available)",
                    targetSize, requiredMemory / (1024*1024), usable / (1024*1024));
                return Math.min(targetSize, baseCount);
            }

            // Scale down by half
            int previousSize = targetSize;
            targetSize = targetSize / 2;
            logger.warn("Partition size {} vectors requires {}MB but only {}MB available - scaling down to {}",
                previousSize, requiredMemory / (1024*1024), usable / (1024*1024), targetSize);
        }

        // Fallback to minimum viable size
        logger.warn("Unable to fit even small partition in memory, using minimal size");
        return Math.max(1, Math.min(1000, baseCount));
    }

    private long estimateVectorBytes(int dimension) {
        final int arrayHeaderBytes = 16;
        final double safetyFactor = 2.0d;
        return (long) ((dimension * (long) Float.BYTES + arrayHeaderBytes) * safetyFactor);
    }

    /**
     * Select top-K neighbors using the best available method.
     * When called from PartitionComputation with panamaVectorBatch, uses that directly.
     * Otherwise tries Panama with the provided baseBatch, or falls back to standard.
     */
    private NeighborIndex[] selectTopNeighbors(float[] queryVector, List<float[]> baseBatch, int globalStartIndex, int topK) {
        // If baseBatch is null, this must be called from Panama path - not supported here
        if (baseBatch == null) {
            throw new IllegalStateException("baseBatch is null - caller should use Panama batch directly");
        }

        // Try Panama-optimized implementation first (JDK 22+ with Vector API)
        // Falls back automatically if not available
        try {
            return KnnOptimizationProvider.findTopKNeighbors(
                queryVector,
                baseBatch,
                globalStartIndex,
                topK,
                distanceMetricOption.getDistanceMetric()
            );
        } catch (UnsupportedOperationException e) {
            // Fallback to standard implementation
            return selectTopNeighborsStandard(queryVector, baseBatch, globalStartIndex, topK);
        }
    }

    /**
     * Select top-K neighbors using Panama batch directly (for PartitionComputation).
     * This is the zero-copy path when memory-mapped I/O is used.
     * Uses cached MethodHandle from KnnOptimizationProvider (no reflection overhead).
     */
    private NeighborIndex[] selectTopNeighborsPanama(float[] queryVector, Object panamaBatch, int globalStartIndex, int topK) throws Throwable {
        // Use KnnOptimizationProvider with cached MethodHandle
        return KnnOptimizationProvider.findTopKNeighborsDirect(
            queryVector,
            panamaBatch,
            globalStartIndex,
            topK,
            distanceMetricOption.getDistanceMetric()
        );
    }

    /**
     * Standard (non-optimized) implementation for compatibility.
     * Used as fallback when Panama optimizations are not available.
     */
    private NeighborIndex[] selectTopNeighborsStandard(float[] queryVector, List<float[]> baseBatch, int globalStartIndex, int topK) {
        if (topK <= 0 || baseBatch.isEmpty()) {
            return new NeighborIndex[0];
        }

        PriorityQueue<NeighborIndex> heap = new PriorityQueue<>(topK, Comparator.comparingDouble(NeighborIndex::distance).reversed());
        for (int i = 0; i < baseBatch.size(); i++) {
            float[] baseVector = baseBatch.get(i);
            double distance = calculateDistance(queryVector, baseVector);
            NeighborIndex candidate = new NeighborIndex(globalStartIndex + i, distance);

            if (heap.size() < topK) {
                heap.offer(candidate);
            } else if (distance < heap.peek().distance()) {
                heap.poll();
                heap.offer(candidate);
            }
        }

        int resultSize = heap.size();
        NeighborIndex[] result = new NeighborIndex[resultSize];
        for (int i = resultSize - 1; i >= 0; i--) {
            result[i] = heap.poll();
        }
        return result;
    }

    /**
     * Map-Reduce algorithm for finding top-K neighbors.
     *
     * MAP PHASE: Split base vectors into chunks, process each chunk in parallel to find top-K.
     * REDUCE PHASE: Virtually merge chunk results using indirection to avoid copying.
     *
     * This provides 2x parallelism: across queries (caller) AND within each query (this method).
     *
     * @param queryVector the query vector
     * @param baseBatch all base vectors for this partition
     * @param globalStartIndex starting index for this partition
     * @param topK number of nearest neighbors to return
     * @param chunkSize number of base vectors per chunk (tune for cache vs parallelism)
     * @return top-K nearest neighbors
     */
    private NeighborIndex[] selectTopNeighborsMapReduce(
        float[] queryVector,
        List<float[]> baseBatch,
        int globalStartIndex,
        int topK,
        int chunkSize
    ) throws Exception {
        if (topK <= 0 || baseBatch.isEmpty()) {
            return new NeighborIndex[0];
        }

        // MAP PHASE: Split into chunks and process in parallel
        int numChunks = (int) Math.ceil((double) baseBatch.size() / chunkSize);
        ExecutorService chunkPool = Executors.newFixedThreadPool(
            Math.min(Runtime.getRuntime().availableProcessors(), numChunks)
        );

        try {
            List<Future<ChunkResult>> chunkFutures = new ArrayList<>();

            for (int chunkIdx = 0; chunkIdx < numChunks; chunkIdx++) {
                final int chunkStart = chunkIdx * chunkSize;
                final int chunkEnd = Math.min(chunkStart + chunkSize, baseBatch.size());
                final int chunkIndex = chunkIdx;

                Future<ChunkResult> future = chunkPool.submit(() -> {
                    // Create a sublist for this chunk
                    List<float[]> chunkVectors = baseBatch.subList(chunkStart, chunkEnd);

                    // Use Panama-optimized batch processing for this chunk
                    // This processes the entire chunk with SIMD operations
                    NeighborIndex[] chunkTopK;
                    try {
                        chunkTopK = KnnOptimizationProvider.findTopKNeighbors(
                            queryVector,
                            chunkVectors,
                            globalStartIndex + chunkStart,
                            topK,
                            distanceMetricOption.getDistanceMetric()
                        );
                    } catch (UnsupportedOperationException e) {
                        // Fallback: manual calculation
                        PriorityQueue<NeighborIndex> heap = new PriorityQueue<>(
                            topK,
                            Comparator.comparingDouble(NeighborIndex::distance).reversed()
                        );

                        for (int i = chunkStart; i < chunkEnd; i++) {
                            float[] baseVector = baseBatch.get(i);
                            double distance = calculateDistance(queryVector, baseVector);
                            NeighborIndex candidate = new NeighborIndex(globalStartIndex + i, distance);

                            if (heap.size() < topK) {
                                heap.offer(candidate);
                            } else if (distance < heap.peek().distance()) {
                                heap.poll();
                                heap.offer(candidate);
                            }
                        }

                        int resultSize = heap.size();
                        chunkTopK = new NeighborIndex[resultSize];
                        for (int i = resultSize - 1; i >= 0; i--) {
                            chunkTopK[i] = heap.poll();
                        }
                    }

                    return new ChunkResult(chunkIndex, chunkTopK);
                });

                chunkFutures.add(future);
            }

            // Wait for all chunks to complete
            ChunkResult[] chunkResults = new ChunkResult[numChunks];
            for (int i = 0; i < numChunks; i++) {
                chunkResults[i] = chunkFutures.get(i).get();
            }

            // REDUCE PHASE: Virtual merge using indirection (no data copying)
            return mergeChunkResultsVirtual(chunkResults, topK);

        } finally {
            chunkPool.shutdown();
        }
    }

    /**
     * Merges chunk results using virtual indirection - compares without copying data.
     * Uses a min-heap of IndirectNeighbor objects that reference chunk results.
     *
     * This avoids creating a large array of (K × numChunks) entries.
     *
     * @param chunkResults results from each chunk
     * @param topK number of neighbors to return
     * @return merged top-K neighbors across all chunks
     */
    private NeighborIndex[] mergeChunkResultsVirtual(ChunkResult[] chunkResults, int topK) {
        // Create a min-heap of indirect references (ascending distance order)
        PriorityQueue<IndirectNeighbor> heap = new PriorityQueue<>(topK);

        // Initialize heap with first element from each chunk (already sorted within chunk)
        for (int chunkIdx = 0; chunkIdx < chunkResults.length; chunkIdx++) {
            NeighborIndex[] chunkTopK = chunkResults[chunkIdx].topK;
            if (chunkTopK.length > 0) {
                heap.offer(new IndirectNeighbor(chunkIdx, 0, chunkResults));
            }
        }

        // Track next position to fetch from each chunk
        int[] nextPos = new int[chunkResults.length];
        Arrays.fill(nextPos, 1);  // We already added position 0

        // Extract top-K by repeatedly taking minimum from heap
        List<NeighborIndex> result = new ArrayList<>(topK);
        while (!heap.isEmpty() && result.size() < topK) {
            IndirectNeighbor indirect = heap.poll();
            result.add(indirect.getNeighbor());

            // Add next element from the same chunk if available
            int chunkIdx = indirect.chunkIndex;
            int nextPosition = nextPos[chunkIdx];
            if (nextPosition < chunkResults[chunkIdx].topK.length) {
                heap.offer(new IndirectNeighbor(chunkIdx, nextPosition, chunkResults));
                nextPos[chunkIdx]++;
            }
        }

        return result.toArray(new NeighborIndex[0]);
    }

    private int[] toIndexArray(NeighborIndex[] neighbors) {
        int[] indices = new int[neighbors.length];
        for (int i = 0; i < neighbors.length; i++) {
            indices[i] = (int) neighbors[i].index();
        }
        return indices;
    }

    private float[] toDistanceArray(NeighborIndex[] neighbors) {
        float[] distances = new float[neighbors.length];
        for (int i = 0; i < neighbors.length; i++) {
            distances[i] = (float) neighbors[i].distance();
        }
        return distances;
    }

    /**
     * Creates a PartitionComputation task for computing k-NN on a partition of base vectors.
     * Loads the base vectors for the partition and constructs the computation task.
     * Uses Panama memory-mapped I/O when available for 2-4x faster loading.
     *
     * @return a PartitionComputation ready to be executed
     * @throws IOException if loading base vectors fails
     */
    /**
     * Creates a PartitionComputation task (lightweight - NO data loading yet).
     * Data loading happens INSIDE compute() to ensure only one partition in memory at a time.
     */
    private PartitionComputation createPartitionComputation(
        FloatVectors baseVectors,
        Path baseVectorsPath,
        int partitionIndex,
        int startIndex,
        int endIndex,
        int baseDimension,
        Path neighborsPath,
        Path distancesPath,
        int effectiveK,
        Path queryVectorsPath,
        boolean useBatchedSIMD
    ) {
        int partitionSize = endIndex - startIndex;
        if (partitionSize <= 0) {
            throw new IllegalArgumentException("Partition size must be positive");
        }

        int partitionK = Math.min(effectiveK, partitionSize);

        // Create and return lightweight task (data loading deferred to compute())
        return new PartitionComputation(
            partitionIndex,
            baseVectors,
            baseVectorsPath,
            startIndex,
            endIndex,
            partitionK,
            baseDimension,
            neighborsPath,
            distancesPath,
            queryVectorsPath,
            useBatchedSIMD  // Pass cached strategy
        );
    }

    private void mergePartitions(
        StatusScope mergeScope,
        List<PartitionMetadata> partitions,
        Path finalNeighborsPath,
        Path finalDistancesPath,
        int effectiveK,
        int queryCount
    ) throws IOException {

        // Create progress tracker for merge operation
        MergeOperation mergeOp = new MergeOperation(queryCount, partitions.size(), effectiveK);
        mergeOp.setState(RunState.RUNNING);
        StatusTracker<MergeOperation> tracker = mergeScope.trackTask(mergeOp);

        try {
            if (finalNeighborsPath.getParent() != null) {
                Files.createDirectories(finalNeighborsPath.getParent());
            }
            if (finalDistancesPath.getParent() != null) {
                Files.createDirectories(finalDistancesPath.getParent());
            }

            List<BoundedVectorFileStream<int[]>> neighborStreams = new ArrayList<>();
            List<BoundedVectorFileStream<float[]>> distanceStreams = new ArrayList<>();

            try {
                for (PartitionMetadata partition : partitions) {
                    neighborStreams.add(VectorFileIO.streamIn(FileType.xvec, int[].class, partition.neighborsPath)
                        .orElseThrow(() -> new IOException("Could not open intermediate neighbors file: " + partition.neighborsPath)));
                    distanceStreams.add(VectorFileIO.streamIn(FileType.xvec, float[].class, partition.distancesPath)
                        .orElseThrow(() -> new IOException("Could not open intermediate distances file: " + partition.distancesPath)));
                }

                List<Iterator<int[]>> neighborIterators = new ArrayList<>(neighborStreams.size());
                List<Iterator<float[]>> distanceIterators = new ArrayList<>(distanceStreams.size());

                for (BoundedVectorFileStream<int[]> stream : neighborStreams) {
                    neighborIterators.add(stream.iterator());
                }
                for (BoundedVectorFileStream<float[]> stream : distanceStreams) {
                    distanceIterators.add(stream.iterator());
                }

                try (VectorFileStreamStore<int[]> finalNeighborsStore = VectorFileIO.streamOut(FileType.xvec, int[].class, finalNeighborsPath)
                         .orElseThrow(() -> new IOException("Could not create final neighbors file: " + finalNeighborsPath));
                     VectorFileStreamStore<float[]> finalDistancesStore = VectorFileIO.streamOut(FileType.xvec, float[].class, finalDistancesPath)
                         .orElseThrow(() -> new IOException("Could not create final distances file: " + finalDistancesPath))) {

                    for (int queryIndex = 0; queryIndex < queryCount; queryIndex++) {
                        List<NeighborIndex> combined = new ArrayList<>(partitions.size() * Math.max(1, effectiveK));

                        for (int partitionIndex = 0; partitionIndex < partitions.size(); partitionIndex++) {
                            Iterator<int[]> neighborIterator = neighborIterators.get(partitionIndex);
                            Iterator<float[]> distanceIterator = distanceIterators.get(partitionIndex);

                            if (!neighborIterator.hasNext() || !distanceIterator.hasNext()) {
                                throw new IOException("Intermediate results ended unexpectedly for partition "
                                    + partitions.get(partitionIndex).startIndex + "-" + partitions.get(partitionIndex).endIndex);
                            }

                            int[] neighborIndices = neighborIterator.next();
                            float[] distanceValues = distanceIterator.next();

                            if (neighborIndices.length != distanceValues.length) {
                                throw new IOException("Mismatched neighbor and distance counts in partition "
                                    + partitions.get(partitionIndex).neighborsPath);
                            }

                        for (int i = 0; i < neighborIndices.length; i++) {
                            combined.add(new NeighborIndex(neighborIndices[i], distanceValues[i]));
                        }
                    }

                    // Deduplicate by index (keep the best/closest distance for any duplicate that may
                    // show up due to overlapping ranges or cache reuse) before taking the global top-K.
                    Map<Long, NeighborIndex> unique = new HashMap<>(combined.size());
                    for (NeighborIndex ni : combined) {
                        NeighborIndex existing = unique.get(ni.index());
                        if (existing == null || ni.distance() < existing.distance()) {
                            unique.put(ni.index(), ni);
                        }
                    }

                    List<NeighborIndex> deduped = new ArrayList<>(unique.values());
                    deduped.sort(Comparator
                        .comparingDouble(NeighborIndex::distance)
                        .thenComparingLong(NeighborIndex::index));
                    int resultSize = Math.min(effectiveK, deduped.size());
                    int[] finalIndices = new int[resultSize];
                    float[] finalDistances = new float[resultSize];

                    for (int i = 0; i < resultSize; i++) {
                        NeighborIndex neighbor = deduped.get(i);
                        finalIndices[i] = (int) neighbor.index();
                        finalDistances[i] = (float) neighbor.distance();
                    }

                    finalNeighborsStore.write(finalIndices);
                        finalDistancesStore.write(finalDistances);

                        // Update progress
                        mergeOp.incrementMerged();
                    }
                }

                mergeOp.setState(RunState.SUCCESS);
            } finally {
                for (BoundedVectorFileStream<int[]> stream : neighborStreams) {
                    try {
                        stream.close();
                    } catch (Exception e) {
                        logger.warn("Error closing intermediate neighbors stream {}: {}", stream.getName(), e.getMessage());
                    }
                }
                for (BoundedVectorFileStream<float[]> stream : distanceStreams) {
                    try {
                        stream.close();
                    } catch (Exception e) {
                        logger.warn("Error closing intermediate distances stream {}: {}", stream.getName(), e.getMessage());
                    }
                }
            }
        } catch (IOException e) {
            mergeOp.setState(RunState.FAILED);
            throw e;
        } finally {
            tracker.close();
        }
    }

    /**
     * Build path for partition cache file in the cache directory.
     * Cache files are named based on: base file, partition range, k value, distance metric, and data type (neighbors/distances)
     *
     * @param baseVectorsPath Path to base vectors file (for stable cache key)
     * @param startIndex Starting index of partition
     * @param endIndex Ending index of partition
     * @param k Number of neighbors
     * @param distanceMetric Distance metric used
     * @param suffix Type of data ("neighbors" or "distances")
     * @param extension File extension ("ivec" or "fvec")
     * @return Path to cache file
     */
    private Path buildCachePath(Path baseVectorsPath, int startIndex, int endIndex, int k,
                                DistanceMetric distanceMetric, String suffix, String extension) {
        String baseName = removeLastExtension(baseVectorsPath.getFileName().toString());
        String metricName = distanceMetric.name().toLowerCase();
        String fileName = String.format("%s.range_%06d_%06d.k%d.%s.%s.%s",
            baseName, startIndex, endIndex, k, metricName, suffix, extension);
        return cacheDir.resolve(fileName);
    }

    /**
     * Build path for final result cache file (represents complete computation for a range).
     *
     * @param baseVectorsPath Path to base vectors file
     * @param startIndex Starting index of range
     * @param endIndex Ending index of range
     * @param k Number of neighbors
     * @param distanceMetric Distance metric used
     * @param suffix Type of data ("neighbors" or "distances")
     * @param extension File extension ("ivec" or "fvec")
     * @return Path to final cache file
     */
    private Path buildFinalCachePath(Path baseVectorsPath, int startIndex, int endIndex, int k,
                                     DistanceMetric distanceMetric, String suffix, String extension) {
        String baseName = removeLastExtension(baseVectorsPath.getFileName().toString());
        String metricName = distanceMetric.name().toLowerCase();
        String fileName = String.format("%s.final_%06d_%06d.k%d.%s.%s.%s",
            baseName, startIndex, endIndex, k, metricName, suffix, extension);
        return cacheDir.resolve(fileName);
    }

    /**
     * Check if cache files exist for a partition
     * @return true if both neighbors and distances cache files exist
     */
    private boolean cacheFilesExist(Path neighborsPath, Path distancesPath) {
        return Files.exists(neighborsPath) && Files.exists(distancesPath);
    }

    /**
     * Scan cache directory for all available cache files matching this computation.
     * Finds both partition caches and final result caches.
     *
     * @param baseVectorsPath Base vectors file path
     * @param k Number of neighbors
     * @param distanceMetric Distance metric
     * @param effectiveStart Start of requested range
     * @param effectiveEnd End of requested range
     * @return List of all matching cache entries
     */
    private List<PartitionMetadata> scanCacheDirectory(
        Path baseVectorsPath,
        int k,
        DistanceMetric distanceMetric,
        int effectiveStart,
        int effectiveEnd
    ) throws IOException {
        List<PartitionMetadata> caches = new ArrayList<>();

        if (!Files.exists(cacheDir)) {
            return caches;
        }

        String baseName = removeLastExtension(baseVectorsPath.getFileName().toString());
        String metricName = distanceMetric.name().toLowerCase();

        // Pattern to match cache files: baseName.(range_|final_)XXXXXX_YYYYYY.kN.metric.neighbors.ivec
        String pattern = baseName + "\\.(range_|final_)(\\d+)_(\\d+)\\.k" + k + "\\." + metricName + "\\.neighbors\\.ivec";
        java.util.regex.Pattern cachePattern = java.util.regex.Pattern.compile(pattern);

        try (java.util.stream.Stream<Path> paths = Files.list(cacheDir)) {
            paths.forEach(path -> {
                java.util.regex.Matcher matcher = cachePattern.matcher(path.getFileName().toString());
                if (matcher.matches()) {
                    boolean isFinal = "final_".equals(matcher.group(1));
                    int start = Integer.parseInt(matcher.group(2));
                    int end = Integer.parseInt(matcher.group(3));

                    // Build corresponding distances path
                    Path distPath = buildCachePath(baseVectorsPath, start, end, k, distanceMetric, "distances", "fvec");
                    if (isFinal) {
                        distPath = buildFinalCachePath(baseVectorsPath, start, end, k, distanceMetric, "distances", "fvec");
                    }

                    if (Files.exists(distPath)) {
                        try {
                            // Get query count from file
                            int queryCount = VectorFileIO.randomAccess(FileType.xvec, int[].class, path).size();
                            caches.add(new PartitionMetadata(path, distPath, start, end, queryCount, k, isFinal));
                        } catch (Exception e) {
                            logger.debug("Failed to load cache metadata from {}: {}", path, e.getMessage());
                        }
                    }
                }
            });
        }

        return caches;
    }

    /**
     * Find the optimal combination of cache entries that covers the requested range
     * with the fewest merge operations.
     *
     * @param allCaches All available cache entries
     * @param targetStart Start of target range
     * @param targetEnd End of target range
     * @return Optimal set of caches to use (null if no valid coverage)
     */
    private List<PartitionMetadata> findOptimalCacheCombination(
        List<PartitionMetadata> allCaches,
        int targetStart,
        int targetEnd
    ) {
        // First, check if any single final cache covers the entire range
        for (PartitionMetadata cache : allCaches) {
            if (cache.isFinal && cache.covers(targetStart, targetEnd)) {
                logger.info("Found complete final cache covering [{}, {})", targetStart, targetEnd);
                return List.of(cache);
            }
        }

        // Next, try to find minimal set using dynamic programming approach
        // This finds the combination requiring fewest merges
        List<PartitionMetadata> best = findMinimalCoverageSet(allCaches, targetStart, targetEnd);

        if (best != null && !best.isEmpty()) {
            logger.info("Found optimal cache combination: {} entries covering [{}, {}) (requires {} merges)",
                best.size(), targetStart, targetEnd, best.size() - 1);
        }

        return best;
    }

    /**
     * Represents a range that needs to be computed (not covered by cache).
     */
    private static class UncoveredRange {
        final int start;
        final int end;

        UncoveredRange(int start, int end) {
            this.start = start;
            this.end = end;
        }

        int size() {
            return end - start;
        }
    }

    /**
     * Find which parts of the target range are NOT covered by existing caches.
     * Returns list of ranges that need to be computed.
     *
     * @param allCaches All available cache entries
     * @param targetStart Start of target range
     * @param targetEnd End of target range
     * @return List of uncovered ranges that need computation
     */
    private List<UncoveredRange> findUncoveredRanges(
        List<PartitionMetadata> allCaches,
        int targetStart,
        int targetEnd
    ) {
        // Sort caches by start index
        List<PartitionMetadata> sorted = new ArrayList<>(allCaches);
        sorted.sort(Comparator.comparingInt(c -> c.startIndex));

        List<UncoveredRange> uncovered = new ArrayList<>();
        int position = targetStart;

        for (PartitionMetadata cache : sorted) {
            // Skip caches that are completely before or after our range
            if (cache.endIndex <= targetStart || cache.startIndex >= targetEnd) {
                continue;
            }

            // Clip cache to our target range
            int cacheStart = Math.max(cache.startIndex, targetStart);
            int cacheEnd = Math.min(cache.endIndex, targetEnd);

            // If there's a gap before this cache, add it as uncovered
            if (position < cacheStart) {
                uncovered.add(new UncoveredRange(position, cacheStart));
            }

            // Move position forward
            position = Math.max(position, cacheEnd);
        }

        // If there's a gap at the end, add it
        if (position < targetEnd) {
            uncovered.add(new UncoveredRange(position, targetEnd));
        }

        return uncovered;
    }

    /**
     * Find minimal set of non-overlapping caches that cover the target range.
     * Uses greedy algorithm: prefer larger caches to minimize merge count.
     */
    private List<PartitionMetadata> findMinimalCoverageSet(
        List<PartitionMetadata> allCaches,
        int targetStart,
        int targetEnd
    ) {
        // Sort by size descending (prefer larger chunks)
        List<PartitionMetadata> sorted = new ArrayList<>(allCaches);
        sorted.sort((a, b) -> Integer.compare(b.size(), a.size()));

        List<PartitionMetadata> selected = new ArrayList<>();
        int covered = targetStart;

        while (covered < targetEnd) {
            PartitionMetadata best = null;
            int bestEnd = covered;

            // Find cache that starts at/before 'covered' and extends furthest
            for (PartitionMetadata cache : sorted) {
                if (cache.startIndex <= covered && cache.endIndex > covered && cache.endIndex > bestEnd) {
                    best = cache;
                    bestEnd = cache.endIndex;
                }
            }

            if (best == null) {
                // No cache covers this gap - incomplete coverage
                return null;
            }

            selected.add(best);
            covered = bestEnd;
        }

        return selected;
    }

    /**
     * Find all caches (including newly computed partitions) that overlap the target range.
     * Returns them sorted by start index for efficient merging.
     */
    private List<PartitionMetadata> findAllRelevantCaches(
        List<PartitionMetadata> allCaches,
        int targetStart,
        int targetEnd
    ) {
        List<PartitionMetadata> relevant = new ArrayList<>();

        for (PartitionMetadata cache : allCaches) {
            // Include if cache overlaps with target range
            if (cache.endIndex > targetStart && cache.startIndex < targetEnd) {
                relevant.add(cache);
            }
        }

        // Sort by start index for efficient merging
        relevant.sort(Comparator.comparingInt(c -> c.startIndex));
        return relevant;
    }

    /**
     * Validate that a partition cache file contains only indices from its partition range.
     * This ensures each partition cache stores ground truth for ONLY its base vectors.
     *
     * @param neighborsPath Path to partition neighbors cache file
     * @param startIndex Expected minimum index (inclusive)
     * @param endIndex Expected maximum index (exclusive)
     * @throws IOException if validation fails
     */
    private void validatePartitionCacheIndices(Path neighborsPath, int startIndex, int endIndex) throws IOException {
        try (VectorFileArray<int[]> neighborsArray = VectorFileIO.randomAccess(FileType.xvec, int[].class, neighborsPath)) {
            for (int queryIdx = 0; queryIdx < neighborsArray.size(); queryIdx++) {
                int[] neighbors = neighborsArray.get(queryIdx);
                for (int neighborIdx : neighbors) {
                    if (neighborIdx < startIndex || neighborIdx >= endIndex) {
                        throw new IOException(String.format(
                            "Partition cache validation FAILED: Query %d has neighbor index %d outside partition range [%d..%d). " +
                            "This indicates partition contamination!",
                            queryIdx, neighborIdx, startIndex, endIndex));
                    }
                }
            }
            logger.debug("Partition cache validated: all {} queries have neighbors within range [{}..{})",
                neighborsArray.size(), startIndex, endIndex);
        }
    }

    /**
     * Validate that cached partition files are compatible (same query count, same K)
     * @return true if cache files are valid and compatible
     */
    private boolean validateCacheFiles(Path neighborsPath, Path distancesPath, int expectedK) {
        try {
            // Open files and check dimensions
            try (VectorFileArray<int[]> neighborsArray = VectorFileIO.randomAccess(FileType.xvec, int[].class, neighborsPath);
                 VectorFileArray<float[]> distancesArray = VectorFileIO.randomAccess(FileType.xvec, float[].class, distancesPath)) {

                if (neighborsArray.size() != distancesArray.size()) {
                    logger.warn("Cache validation failed: neighbors count ({}) != distances count ({})",
                        neighborsArray.size(), distancesArray.size());
                    return false;
                }

                if (neighborsArray.size() > 0) {
                    int cachedK = neighborsArray.get(0).length;
                    if (cachedK != expectedK) {
                        logger.warn("Cache validation failed: cached K ({}) != expected K ({})",
                            cachedK, expectedK);
                        return false;
                    }
                }

                return true;
            }
        } catch (Exception e) {
            logger.warn("Cache validation failed: {}", e.getMessage());
            return false;
        }
    }

    private Path buildIntermediatePath(Path baseOutputPath, int startIndex, int endIndex, String suffix, String extension) {
        String baseName = removeLastExtension(baseOutputPath.getFileName().toString());
        String fileName = String.format("%s.part_%06d_%06d.%s.%s", baseName, startIndex, endIndex, suffix, extension);
        Path parent = baseOutputPath.getParent();
        return parent != null ? parent.resolve(fileName) : Paths.get(fileName);
    }


    private String removeLastExtension(String filename) {
        int idx = filename.lastIndexOf('.');
        return idx >= 0 ? filename.substring(0, idx) : filename;
    }

    /**
     * Encapsulates the computation of k-nearest neighbors for a single partition of base vectors.
     * IMPORTANT: Data loading is deferred until compute() to ensure only ONE partition in memory at a time.
     * This enables larger-than-memory datasets by processing partitions sequentially.
     */
    private class PartitionComputation implements StatusSource<PartitionComputation>, Callable<PartitionMetadata>, AutoCloseable {
        private final int partitionIndex;
        private final FloatVectors baseVectors;
        private final Path baseVectorsPath;
        private final int startIndex;
        private final int endIndex;
        private final int partitionK;
        private final int baseDimension;
        private final Path neighborsPath;
        private final Path distancesPath;
        private final Path queryVectorsPath;

        // Runtime state (populated during compute())
        private List<float[]> baseBatch;
        private Object panamaVectorBatch;

        private final boolean useBatchedSIMD;
        private final AtomicInteger processedQueries = new AtomicInteger(0);
        private final AtomicInteger totalQueries = new AtomicInteger(0);
        private final AtomicInteger loadedVectors = new AtomicInteger(0);
        private final int partitionSize;
        private volatile RunState state = RunState.PENDING;
        private volatile String phase = "pending";

        PartitionComputation(
            int partitionIndex,
            FloatVectors baseVectors,
            Path baseVectorsPath,
            int startIndex,
            int endIndex,
            int partitionK,
            int baseDimension,
            Path neighborsPath,
            Path distancesPath,
            Path queryVectorsPath,
            boolean useBatchedSIMD
        ) {
            this.partitionIndex = partitionIndex;
            this.baseVectors = baseVectors;
            this.baseVectorsPath = baseVectorsPath;
            this.startIndex = startIndex;
            this.endIndex = endIndex;
            this.partitionSize = endIndex - startIndex;
            this.partitionK = partitionK;
            this.baseDimension = baseDimension;
            this.neighborsPath = neighborsPath;
            this.distancesPath = distancesPath;
            this.queryVectorsPath = queryVectorsPath;
            this.useBatchedSIMD = useBatchedSIMD;
        }

        @Override
        public void close() {
            // Close Panama resources if present
            if (panamaVectorBatch != null) {
                try {
                    java.lang.reflect.Method closeMethod = panamaVectorBatch.getClass().getMethod("close");
                    closeMethod.invoke(panamaVectorBatch);
                } catch (Exception e) {
                    logger.warn("Failed to close Panama vector batch for partition {}: {}", partitionIndex, e.getMessage());
                }
            }
            // Clear references to allow GC
            baseBatch = null;
            panamaVectorBatch = null;
        }

        /**
         * Pre-load base vectors in background thread (for double-buffered I/O pipeline).
         */
        void preload() throws IOException {
            if (panamaVectorBatch != null || baseBatch != null) {
                return;  // Already loaded
            }

            phase = "loading";
            logger.debug("Partition {}: pre-loading {} base vectors in background", partitionIndex, partitionSize);

            try {
                // Always use getRange() for fast bulk read (single I/O, works at any offset)
                float[][] vectors = baseVectors.getRange(startIndex, endIndex);
                baseBatch = new ArrayList<>(vectors.length);
                for (float[] vector : vectors) {
                    baseBatch.add(vector);
                }
                loadedVectors.addAndGet(vectors.length);

                // If Panama available, create PanamaVectorBatch from loaded vectors
                if (KnnOptimizationProvider.isPanamaAvailable()) {
                    try {
                        Class<?> batchClass = Class.forName("io.nosqlbench.command.compute.panama.PanamaVectorBatch");
                        java.lang.reflect.Constructor<?> constructor = batchClass.getConstructor(List.class, int.class);
                        panamaVectorBatch = constructor.newInstance(baseBatch, baseDimension);
                        logger.debug("Partition {}: created Panama batch from loaded vectors", partitionIndex);
                    } catch (Throwable e) {
                        logger.debug("Panama batch creation failed: {}", e.getMessage());
                    }
                } else {
                    logger.debug("Partition {}: pre-loaded {} base vectors using chunked random access", partitionIndex, partitionSize);
                }

                phase = "loaded";
            } catch (Exception e) {
                state = RunState.FAILED;
                phase = "failed loading";
                throw new IOException("Failed to pre-load partition " + partitionIndex, e);
            }
        }

        /**
         * Callable interface implementation - delegates to compute().
         *
         * @return metadata about the computed partition
         * @throws Exception if computation fails
         */
        @Override
        public PartitionMetadata call() throws Exception {
            return compute();
        }

        /**
         * Executes the k-NN computation for this partition with parallel query processing.
         * If preload() was called, base vectors are already loaded.
         *
         * @return metadata about the computed partition
         * @throws IOException if I/O errors occur
         */
        PartitionMetadata compute() throws IOException {
            state = RunState.RUNNING;

            // Load base vectors if not already pre-loaded
            if (panamaVectorBatch == null && baseBatch == null) {
                phase = "loading";
                logger.info("Partition {}: loading {} base vectors (indices [{}..{}))", partitionIndex, partitionSize, startIndex, endIndex);

                try {
                    // Always use getRange() for fast bulk read (single I/O, works at any offset)
                    float[][] vectors = baseVectors.getRange(startIndex, endIndex);
                    baseBatch = new ArrayList<>(vectors.length);
                    for (float[] vector : vectors) {
                        baseBatch.add(vector);
                    }
                    loadedVectors.addAndGet(vectors.length);

                    // If Panama available, create PanamaVectorBatch from loaded vectors
                    if (KnnOptimizationProvider.isPanamaAvailable()) {
                        try {
                            Class<?> batchClass = Class.forName("io.nosqlbench.command.compute.panama.PanamaVectorBatch");
                            java.lang.reflect.Constructor<?> constructor = batchClass.getConstructor(List.class, int.class);
                            panamaVectorBatch = constructor.newInstance(baseBatch, baseDimension);
                            logger.info("Partition {}: loaded {} base vectors with Panama batch", partitionIndex, partitionSize);
                        } catch (Throwable e) {
                            logger.info("Partition {}: loaded {} base vectors (Panama unavailable)", partitionIndex, partitionSize);
                        }
                    } else {
                        logger.info("Partition {}: loaded {} base vectors using chunked random access",
                            partitionIndex, partitionSize);
                    }

                    phase = "loaded";
                } catch (Exception e) {
                    state = RunState.FAILED;
                    phase = "failed loading";
                    throw e;
                }
            } else {
                logger.info("Partition {}: using pre-loaded {} base vectors (double-buffered)", partitionIndex, partitionSize);
            }

            // Step 1: Load all queries into memory
            List<float[]> queries = new ArrayList<>();
            try (VectorFileArray<float[]> queryReader = VectorFileIO.randomAccess(FileType.xvec, float[].class, queryVectorsPath)) {
                totalQueries.set(queryReader.getSize());
                logger.info("Partition {}: loading {} query vectors into memory", partitionIndex, totalQueries.get());

                for (int i = 0; i < queryReader.getSize(); i++) {
                    float[] queryVector = queryReader.get(i);
                    if (queryVector.length != baseDimension) {
                        state = RunState.FAILED;
                        throw new IllegalArgumentException("Query vector dimension (" + queryVector.length
                            + ") does not match base vector dimension (" + baseDimension + ")");
                    }
                    queries.add(queryVector);
                }
            }

            // Create output directories
            if (neighborsPath.getParent() != null) {
                Files.createDirectories(neighborsPath.getParent());
            }
            if (distancesPath.getParent() != null) {
                Files.createDirectories(distancesPath.getParent());
            }

            try {
                // Step 2: Process queries in parallel
                phase = "processing";
                logger.info("Partition {}: processing {} query vectors for base range [{}, {})",
                    partitionIndex, queries.size(), startIndex, endIndex);

                List<QueryResult> results = processQueriesParallel(queries);

                // Step 3: Write results sequentially to preserve order
                try (VectorFileStreamStore<int[]> neighborsStore = VectorFileIO.streamOut(FileType.xvec, int[].class, neighborsPath)
                         .orElseThrow(() -> new IOException("Could not create intermediate neighbors file: " + neighborsPath));
                     VectorFileStreamStore<float[]> distancesStore = VectorFileIO.streamOut(FileType.xvec, float[].class, distancesPath)
                         .orElseThrow(() -> new IOException("Could not create intermediate distances file: " + distancesPath))) {

                    for (QueryResult result : results) {
                        neighborsStore.write(result.neighborIndices);
                        distancesStore.write(result.distances);
                    }
                }

                state = RunState.SUCCESS;
                int processed = queries.size();
                logger.info("Partition {} complete: wrote {} query results (k={}) to cache", partitionIndex, processed, partitionK);

                // Validate that cached results contain only partition-local indices
                validatePartitionCacheIndices(neighborsPath, startIndex, endIndex);

                return new PartitionMetadata(neighborsPath, distancesPath, startIndex, endIndex, processed, partitionK);
            } catch (Exception e) {
                state = RunState.FAILED;
                throw new IOException("Failed to process partition " + partitionIndex, e);
            }
        }

        /**
         * Processes all queries in parallel using a thread pool.
         * Uses Panama-optimized direct processing when available (no map-reduce overhead),
         * or falls back to map-reduce for standard implementation.
         *
         * @param queries the list of query vectors to process
         * @return list of results in the same order as input queries
         */
        private List<QueryResult> processQueriesParallel(List<float[]> queries) throws Exception {
            int availableProcessors = Runtime.getRuntime().availableProcessors();

            // PANAMA PATH: Check which strategy was selected at session start
            if (panamaVectorBatch != null) {
                if (useBatchedSIMD) {
                    logger.info("Partition {}: processing {} queries (batched SIMD)",
                        partitionIndex, queries.size());

                    try {
                        NeighborIndex[][] batchResults = KnnOptimizationProvider.findTopKBatched(
                            queries,
                            panamaVectorBatch,
                            startIndex,
                            partitionK,
                            distanceMetricOption.getDistanceMetric(),
                            processedQueries
                        );

                        List<QueryResult> results = new ArrayList<>(queries.size());
                        for (int i = 0; i < batchResults.length; i++) {
                            NeighborIndex[] neighbors = batchResults[i];
                            if (neighbors == null) {
                                throw new IllegalStateException("Batched result[" + i + "] is null - thread did not complete?");
                            }
                            results.add(new QueryResult(i, toIndexArray(neighbors), toDistanceArray(neighbors)));
                        }

                        logger.info("Partition {}: completed all {} queries using batched SIMD", partitionIndex, queries.size());
                        return results;

                    } catch (Throwable e) {
                        logger.warn("Batched Panama optimization failed, falling back to per-query: {}", e.getMessage());
                        logger.debug("Error details:", e);
                        // Fall through to per-query path below
                    }
                }

                // PER-QUERY strategy: Parallel query processing (default, FAST)
                logger.info("Partition {}: using Panama PER-QUERY SIMD optimization for {} queries",
                    partitionIndex, queries.size());

                int threadCount = Math.min(availableProcessors, queries.size());
                ExecutorService queryPool = Executors.newFixedThreadPool(threadCount);

                try {
                    List<Future<QueryResult>> futures = new ArrayList<>();

                    for (int queryIndex = 0; queryIndex < queries.size(); queryIndex++) {
                        final int idx = queryIndex;
                        final float[] queryVector = queries.get(idx);

                        Future<QueryResult> future = queryPool.submit(() -> {
                            try {
                                NeighborIndex[] neighbors = selectTopNeighborsPanama(queryVector, panamaVectorBatch, startIndex, partitionK);
                                processedQueries.incrementAndGet();
                                return new QueryResult(idx, toIndexArray(neighbors), toDistanceArray(neighbors));
                            } catch (Throwable e) {
                                logger.error("Failed to process query {} with Panama: {}", idx, e.getMessage());
                                throw new RuntimeException("Failed to process query " + idx, e);
                            }
                        });

                        futures.add(future);
                    }

                    List<QueryResult> results = new ArrayList<>();
                    for (Future<QueryResult> future : futures) {
                        results.add(future.get());
                    }
                    return results;
                } finally {
                    queryPool.shutdown();
                }
            }

            // STANDARD PATH: Map-reduce with chunking
            int targetChunksPerQuery = Math.max(2, availableProcessors);
            int chunkSize = Math.max(1000, baseBatch.size() / targetChunksPerQuery);

            logger.info("Partition {}: using standard map-reduce with chunkSize={} ({} chunks per query)",
                partitionIndex, chunkSize, (baseBatch.size() + chunkSize - 1) / chunkSize);

            // Create thread pool - use available processors
            int threadCount = Math.min(availableProcessors, queries.size());
            ExecutorService queryPool = Executors.newFixedThreadPool(threadCount);

            try {
                // Submit all queries as parallel tasks
                List<Future<QueryResult>> futures = new ArrayList<>();

                for (int queryIndex = 0; queryIndex < queries.size(); queryIndex++) {
                    final int idx = queryIndex;
                    final float[] queryVector = queries.get(idx);
                    final int finalChunkSize = chunkSize;

                    Future<QueryResult> future = queryPool.submit(() -> {
                        // Compute k-NN for this query using map-reduce
                        NeighborIndex[] neighbors = selectTopNeighborsMapReduce(
                            queryVector, baseBatch, startIndex, partitionK, finalChunkSize
                        );

                        // Update progress
                        processedQueries.incrementAndGet();

                        // Return result with index to preserve order
                        return new QueryResult(idx, toIndexArray(neighbors), toDistanceArray(neighbors));
                    });

                    futures.add(future);
                }

                // Collect results in order
                List<QueryResult> results = new ArrayList<>();
                for (Future<QueryResult> future : futures) {
                    results.add(future.get());
                }

                return results;
            } finally {
                queryPool.shutdown();
            }
        }

        @Override
        public StatusUpdate<PartitionComputation> getTaskStatus() {
            double progress;
            if ("loading".equals(phase)) {
                // Show loading progress
                progress = partitionSize > 0 ? (double) loadedVectors.get() / partitionSize : 0.0;
            } else {
                // Show query processing progress
                int total = totalQueries.get();
                int processed = processedQueries.get();
                progress = total > 0 ? (double) processed / total : 0.0;
            }
            return new StatusUpdate<>(progress, state, this);
        }

        @Override
        public String toString() {
            if ("loading".equals(phase)) {
                return String.format("Partition %d: loading %d/%d base vectors [%d..%d] k=%d",
                    partitionIndex, loadedVectors.get(), partitionSize, startIndex, endIndex, partitionK);
            } else {
                return String.format("Partition %d [base %d..%d, k=%d]: %d/%d queries",
                    partitionIndex, startIndex, endIndex, partitionK, processedQueries.get(), totalQueries.get());
            }
        }
    }

    /**
     * Holds the result of computing k-NN for a single query vector.
     * Used to preserve ordering when processing queries in parallel.
     */
    private static class QueryResult {
        final int queryIndex;
        final int[] neighborIndices;
        final float[] distances;

        QueryResult(int queryIndex, int[] neighborIndices, float[] distances) {
            this.queryIndex = queryIndex;
            this.neighborIndices = neighborIndices;
            this.distances = distances;
        }
    }

    /**
     * Holds top-K results from processing a single chunk of base vectors.
     * Used in the map phase of the map-reduce algorithm.
     */
    private static class ChunkResult {
        final NeighborIndex[] topK;
        final int chunkIndex;

        ChunkResult(int chunkIndex, NeighborIndex[] topK) {
            this.chunkIndex = chunkIndex;
            this.topK = topK;
        }
    }

    /**
     * Indirect reference to a neighbor in a chunk result.
     * Enables virtual merging without copying data - we just compare by looking up
     * the actual distance from the chunk results array.
     */
    private static class IndirectNeighbor implements Comparable<IndirectNeighbor> {
        final int chunkIndex;
        final int positionInChunk;
        final ChunkResult[] chunkResults;  // Reference to all chunks for lookup

        IndirectNeighbor(int chunkIndex, int positionInChunk, ChunkResult[] chunkResults) {
            this.chunkIndex = chunkIndex;
            this.positionInChunk = positionInChunk;
            this.chunkResults = chunkResults;
        }

        /**
         * Get the actual neighbor by indirection.
         */
        NeighborIndex getNeighbor() {
            return chunkResults[chunkIndex].topK[positionInChunk];
        }

        /**
         * Compare by looking up actual distance values - virtual comparison without copying.
         */
        @Override
        public int compareTo(IndirectNeighbor other) {
            double thisDistance = this.getNeighbor().distance();
            double otherDistance = other.getNeighbor().distance();
            return Double.compare(thisDistance, otherDistance);
        }
    }

    /**
     * Tracks progress of loading base vectors into memory for a partition.
     * This operation reads vectors from disk in batches.
     */
    private static class LoadOperation implements StatusSource<LoadOperation> {
        private final int partitionIndex;
        private final int totalVectors;
        private final AtomicInteger loadedVectors = new AtomicInteger(0);
        private volatile RunState state = RunState.PENDING;

        LoadOperation(int partitionIndex, int totalVectors) {
            this.partitionIndex = partitionIndex;
            this.totalVectors = totalVectors;
        }

        void addLoaded(int count) {
            loadedVectors.addAndGet(count);
        }

        void setState(RunState state) {
            this.state = state;
        }

        @Override
        public StatusUpdate<LoadOperation> getTaskStatus() {
            double progress = totalVectors > 0 ? (double) loadedVectors.get() / totalVectors : 0.0;
            return new StatusUpdate<>(progress, state, this);
        }

        @Override
        public String toString() {
            return String.format("Loading partition %d: %d/%d vectors",
                partitionIndex, loadedVectors.get(), totalVectors);
        }
    }

    /**
     * Tracks progress of merging partition results into final output files.
     * This operation reads all intermediate partition files and merges them query-by-query.
     */
    private static class MergeOperation implements StatusSource<MergeOperation> {
        private final int totalQueries;
        private final int partitionCount;
        private final int k;
        private final AtomicInteger mergedQueries = new AtomicInteger(0);
        private volatile RunState state = RunState.PENDING;

        MergeOperation(int totalQueries, int partitionCount, int k) {
            this.totalQueries = totalQueries;
            this.partitionCount = partitionCount;
            this.k = k;
        }

        void incrementMerged() {
            mergedQueries.incrementAndGet();
        }

        void setState(RunState state) {
            this.state = state;
        }

        @Override
        public StatusUpdate<MergeOperation> getTaskStatus() {
            double progress = totalQueries > 0 ? (double) mergedQueries.get() / totalQueries : 0.0;
            return new StatusUpdate<>(progress, state, this);
        }

        @Override
        public String toString() {
            return String.format("Merging %d partitions (k=%d): %d/%d queries",
                partitionCount, k, mergedQueries.get(), totalQueries);
        }
    }

    private static class PartitionMetadata {
        final Path neighborsPath;
        final Path distancesPath;
        final int startIndex;
        final int endIndex;
        final int queryCount;
        final int partitionK;
        final boolean isFinal;  // True if this is a final result cache, false if partition cache

        PartitionMetadata(Path neighborsPath, Path distancesPath, int startIndex, int endIndex, int queryCount, int partitionK) {
            this(neighborsPath, distancesPath, startIndex, endIndex, queryCount, partitionK, false);
        }

        PartitionMetadata(Path neighborsPath, Path distancesPath, int startIndex, int endIndex, int queryCount, int partitionK, boolean isFinal) {
            this.neighborsPath = neighborsPath;
            this.distancesPath = distancesPath;
            this.startIndex = startIndex;
            this.endIndex = endIndex;
            this.queryCount = queryCount;
            this.partitionK = partitionK;
            this.isFinal = isFinal;
        }

        int size() {
            return endIndex - startIndex;
        }

        boolean covers(int start, int end) {
            return startIndex <= start && endIndex >= end;
        }

        boolean overlaps(PartitionMetadata other) {
            return !(endIndex <= other.startIndex || startIndex >= other.endIndex);
        }
    }

    /**
     * Load query vectors from file (helper for incremental merge).
     */
    private List<float[]> loadQueryVectors(Path queryVectorsPath, FloatVectors baseVectors) throws IOException {
        String queryFileName = queryVectorsPath.getFileName().toString();
        String queryExtension = queryFileName.substring(queryFileName.lastIndexOf('.') + 1);
        java.nio.channels.AsynchronousFileChannel queryChannel =
            java.nio.channels.AsynchronousFileChannel.open(queryVectorsPath, java.nio.file.StandardOpenOption.READ);
        long queryFileSize = java.nio.file.Files.size(queryVectorsPath);

        try {
            FloatVectors queryVectors = new io.nosqlbench.vectordata.spec.datasets.impl.xvec.FloatVectorsXvecImpl(
                queryChannel, queryFileSize, null, queryExtension);
            int queryCount = queryVectors.getCount();
            float[][] queryArray = queryVectors.getRange(0, queryCount);
            List<float[]> queries = new ArrayList<>(queryCount);
            for (float[] query : queryArray) {
                queries.add(query);
            }
            return queries;
        } finally {
            queryChannel.close();
        }
    }

    /**
     * Incrementally merge partition results into accumulator.
     * Reads partition output files, merges with existing best results, keeps top-K.
     */
    private void mergePartitionIncremental(
        PartitionMetadata partition,
        io.nosqlbench.command.analyze.subcommands.verify_knn.datatypes.NeighborIndex[][] accumulator,
        int k
    ) throws IOException {
        // Read partition results from disk
        try (BoundedVectorFileStream<int[]> neighborStream = VectorFileIO.streamIn(FileType.xvec, int[].class, partition.neighborsPath)
                .orElseThrow(() -> new IOException("Could not read partition neighbors: " + partition.neighborsPath));
             BoundedVectorFileStream<float[]> distanceStream = VectorFileIO.streamIn(FileType.xvec, float[].class, partition.distancesPath)
                .orElseThrow(() -> new IOException("Could not read partition distances: " + partition.distancesPath))) {

            java.util.Iterator<int[]> neighborIter = neighborStream.iterator();
            java.util.Iterator<float[]> distanceIter = distanceStream.iterator();

            for (int queryIdx = 0; queryIdx < partition.queryCount; queryIdx++) {
                int[] partitionNeighbors = neighborIter.next();
                float[] partitionDistances = distanceIter.next();

                // Convert to NeighborIndex array
                io.nosqlbench.command.analyze.subcommands.verify_knn.datatypes.NeighborIndex[] partitionResults =
                    new io.nosqlbench.command.analyze.subcommands.verify_knn.datatypes.NeighborIndex[partitionNeighbors.length];
                for (int i = 0; i < partitionNeighbors.length; i++) {
                    partitionResults[i] = new io.nosqlbench.command.analyze.subcommands.verify_knn.datatypes.NeighborIndex(
                        partitionNeighbors[i], partitionDistances[i]);
                }

                // Merge with existing results
                if (accumulator[queryIdx] == null) {
                    accumulator[queryIdx] = partitionResults;
                } else {
                    accumulator[queryIdx] = mergeTopK(accumulator[queryIdx], partitionResults, k);
                }
            }
        }

        // Cache files are kept for reuse - do NOT delete them
    }

    /**
     * Merge two result sets and keep top-K.
     */
    private io.nosqlbench.command.analyze.subcommands.verify_knn.datatypes.NeighborIndex[] mergeTopK(
        io.nosqlbench.command.analyze.subcommands.verify_knn.datatypes.NeighborIndex[] existing,
        io.nosqlbench.command.analyze.subcommands.verify_knn.datatypes.NeighborIndex[] newResults,
        int k
    ) {
        // Combine both arrays
        java.util.List<io.nosqlbench.command.analyze.subcommands.verify_knn.datatypes.NeighborIndex> combined =
            new java.util.ArrayList<>(existing.length + newResults.length);
        java.util.Collections.addAll(combined, existing);
        java.util.Collections.addAll(combined, newResults);

        // Sort by distance (ascending)
        combined.sort(java.util.Comparator.comparingDouble(
            io.nosqlbench.command.analyze.subcommands.verify_knn.datatypes.NeighborIndex::distance));

        // Take top-K
        int size = Math.min(k, combined.size());
        io.nosqlbench.command.analyze.subcommands.verify_knn.datatypes.NeighborIndex[] result =
            new io.nosqlbench.command.analyze.subcommands.verify_knn.datatypes.NeighborIndex[size];
        for (int i = 0; i < size; i++) {
            result[i] = combined.get(i);
        }
        return result;
    }

    /**
     * Write final results directly from accumulator (no merge needed).
     */
    private void writeFinalResults(
        io.nosqlbench.command.analyze.subcommands.verify_knn.datatypes.NeighborIndex[][] results,
        Path neighborsOutput,
        Path distancesOutput
    ) throws IOException {
        if (neighborsOutput.getParent() != null) {
            Files.createDirectories(neighborsOutput.getParent());
        }
        if (distancesOutput.getParent() != null) {
            Files.createDirectories(distancesOutput.getParent());
        }

        try (VectorFileStreamStore<int[]> neighborsStore = VectorFileIO.streamOut(FileType.xvec, int[].class, neighborsOutput)
                .orElseThrow(() -> new IOException("Could not create neighbors file: " + neighborsOutput));
             VectorFileStreamStore<float[]> distancesStore = VectorFileIO.streamOut(FileType.xvec, float[].class, distancesOutput)
                .orElseThrow(() -> new IOException("Could not create distances file: " + distancesOutput))) {

            for (io.nosqlbench.command.analyze.subcommands.verify_knn.datatypes.NeighborIndex[] queryResults : results) {
                int[] neighbors = new int[queryResults.length];
                float[] distances = new float[queryResults.length];

                for (int i = 0; i < queryResults.length; i++) {
                    neighbors[i] = (int) queryResults[i].index();
                    distances[i] = (float) queryResults[i].distance();
                }

                neighborsStore.write(neighbors);
                distancesStore.write(distances);
            }
        }
    }

    /**
     * Save final results as cache files for reuse.
     *
     * @param neighborsInput Path to read neighbors from
     * @param distancesInput Path to read distances from
     * @param cacheNeighbors Path to save cached neighbors
     * @param cacheDistances Path to save cached distances
     */
    private void saveFinalCache(
        Path neighborsInput,
        Path distancesInput,
        Path cacheNeighbors,
        Path cacheDistances
    ) throws IOException {
        logger.info("Saving final result cache for range");

        if (cacheNeighbors.getParent() != null) {
            Files.createDirectories(cacheNeighbors.getParent());
        }

        // Copy files to cache
        Files.copy(neighborsInput, cacheNeighbors, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        Files.copy(distancesInput, cacheDistances, java.nio.file.StandardCopyOption.REPLACE_EXISTING);

        logger.info("Saved final cache: {} and {}", cacheNeighbors.getFileName(), cacheDistances.getFileName());
    }

    public static void main(String[] args) {
        CMD_compute_knn cmd = new CMD_compute_knn();
        int exitCode = new CommandLine(cmd).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() throws Exception {
        // Get the paths from the new mixins
        Path baseVectorsPath = baseVectorsOption.getBasePath();
        Path queryVectorsPath = queryVectorsOption.getQueryPath();

        // Handle range specifications: inline path range vs --range option
        RangeOption.Range effectiveRange = null;
        try {
            // Set the indices path for distances option before validation
            distancesFileOption.setIndicesPath(indicesFileOption.getNormalizedIndicesPath());

            validatePaths();

            String pathFromPathSpec = baseVectorsOption.getInlineRange();

            if (pathFromPathSpec != null && rangeOption.isRangeSpecified()) {
                // Both specified - this is an error
                throw new IllegalArgumentException(
                    "Range specified both in path (" + pathFromPathSpec + ") and via --range option. " +
                    "Please specify range only once.");
            } else if (pathFromPathSpec != null) {
                // Use range from path - parse it manually
                rangeFromPath = true;
                effectiveRange = new RangeOption.RangeConverter().convert(pathFromPathSpec);
            } else if (rangeOption.isRangeSpecified()) {
                // Use range from --range option
                effectiveRange = rangeOption.getRange();
            }
            // else: effectiveRange remains null (process all vectors)
        } catch (CommandLine.ParameterException | IllegalArgumentException e) {
            System.err.println(e.getMessage());
            return EXIT_ERROR;
        }

        if (k <= 0) {
            System.err.println("Error: Number of neighbors (k) must be positive");
            return EXIT_ERROR;
        }

        // Validate that rangeStart is 0 (non-zero start behavior is not yet defined)
        if (effectiveRange != null && effectiveRange.start() != 0) {
            System.err.println("Error: Range start must be 0 (got: " + effectiveRange.start() + "). Non-zero start is not yet supported.");
            logger.error("Range start must be 0, but got: {}", effectiveRange.start());
            return EXIT_ERROR;
        }

        Path neighborsOutput = indicesFileOption.getNormalizedIndicesPath();
        Path distancesOutput = distancesFileOption.getNormalizedDistancesPath();

        // Output file validation is already handled in validatePaths()

        try {
            if (neighborsOutput.getParent() != null) {
                Files.createDirectories(neighborsOutput.getParent());
            }
            if (distancesOutput.getParent() != null) {
                Files.createDirectories(distancesOutput.getParent());
            }

            // Load base vectors using FloatVectorsXvecImpl for efficient random access
            String baseFileName = baseVectorsPath.getFileName().toString();
            String baseExtension = baseFileName.substring(baseFileName.lastIndexOf('.') + 1);
            java.nio.channels.AsynchronousFileChannel baseChannel =
                java.nio.channels.AsynchronousFileChannel.open(baseVectorsPath, java.nio.file.StandardOpenOption.READ);
            long baseFileSize = java.nio.file.Files.size(baseVectorsPath);

            FloatVectors baseVectors = new io.nosqlbench.vectordata.spec.datasets.impl.xvec.FloatVectorsXvecImpl(
                baseChannel, baseFileSize, null, baseExtension);

            // Create StatusContext with LOG mode for simple text logging output
            try (StatusContext ctx = new StatusContext("compute-knn", Optional.of(StatusSinkMode.LOG))) {

                logger.info("Using FloatVectorsXvecImpl for efficient chunked random access");

                int totalBaseCount = baseVectors.getCount();
                if (totalBaseCount <= 0) {
                    System.err.println("Error: No base vectors found in file");
                    return EXIT_ERROR;
                }

                // Apply range constraints
                RangeOption.Range constrainedRange = effectiveRange != null
                    ? effectiveRange.constrain(totalBaseCount)
                    : new RangeOption.Range(0, totalBaseCount);
                long effectiveStart = constrainedRange.start();
                long effectiveEnd = constrainedRange.end();

                if (effectiveStart >= totalBaseCount) {
                    System.err.println("Error: Range start " + effectiveStart + " is beyond file size " + totalBaseCount);
                    return EXIT_ERROR;
                }

                int baseCount = (int)(effectiveEnd - effectiveStart);
                if (baseCount <= 0) {
                    System.err.println("Error: No base vectors in specified range");
                    return EXIT_ERROR;
                }

                // Get dimension from the FloatVectors interface (no vector read needed)
                int baseDimension = baseVectors.getVectorDimensions();
                if (baseDimension <= 0) {
                    System.err.println("Error: Base vectors have zero dimension");
                    return EXIT_ERROR;
                }

                int effectiveK = Math.min(k, baseCount);
                int batchSize = Math.max(1, determineBatchSize(baseCount, baseDimension));
                int partitionCount = (int) Math.ceil((double) baseCount / batchSize);

                String rangeStr = effectiveRange != null ? effectiveRange.toString() : "all";
                logger.info("compute knn: baseCount={}, baseDimension={}, k={}, effectiveK={}, batchSize={}, partitions={}, range={}",
                    baseCount, baseDimension, k, effectiveK, batchSize, partitionCount, rangeStr);

                // Print configuration to stdout for user visibility
                System.out.println("Configuration:");
                System.out.println("  Base vectors: " + baseCount + " (dimension: " + baseDimension + ")");
                System.out.println("  K (neighbors): " + k + " (effective: " + effectiveK + ")");
                System.out.println("  Distance metric: " + distanceMetricOption.getDistanceMetric());
                System.out.println("  Partition size: " + batchSize + " vectors");
                System.out.println("  Total partitions: " + partitionCount);
                System.out.println("  Cache directory: " + cacheDir.toAbsolutePath());
                if (effectiveRange != null) {
                    System.out.println("  Range: " + rangeStr + " (processing " + baseCount + " out of " + totalBaseCount + " total vectors)");
                }

                if (effectiveRange != null) {
                    logger.info("Using range {} - processing {} out of {} total base vectors",
                        rangeStr, baseCount, totalBaseCount);
                }

                // Decide and cache SIMD strategy for this session
                if (simdStrategy == SimdStrategy.AUTO) {
                    // Auto-select: Use batched mode as default (proven fast with optimizations)
                    useBatchedSIMD = true;
                    logger.info("Auto-selected BATCHED strategy (default - optimized multi-query SIMD processing)");
                } else {
                    useBatchedSIMD = simdStrategy.isBatched();
                    if (useBatchedSIMD) {
                        logger.info("BATCHED strategy selected - using optimized multi-query SIMD processing");
                    } else {
                        logger.info("PER-QUERY strategy selected");
                    }
                }

                // Create cache directory if it doesn't exist
                Files.createDirectories(cacheDir);
                logger.info("Using cache directory: {}", cacheDir.toAbsolutePath());

                // Check ISA capabilities and warn if Panama isn't being used but could be
                if (io.nosqlbench.command.compute.ISACapabilityDetector.isAvailable()) {
                    String bestISA = io.nosqlbench.command.compute.ISACapabilityDetector.getBestSIMDCapability();
                    logger.info("CPU SIMD Capabilities: {}", bestISA);

                    if (io.nosqlbench.command.compute.ISACapabilityDetector.shouldUsePanama() &&
                        !KnnOptimizationProvider.isPanamaAvailable()) {
                        logger.warn("╔═══════════════════════════════════════════════════════════════╗");
                        logger.warn("║ PERFORMANCE WARNING: CPU supports {} but Panama is OFF    ║", bestISA);
                        logger.warn("║ Add: --add-modules jdk.incubator.vector                      ║");
                        logger.warn("║ Expected speedup: 3-10x with Panama enabled!                 ║");
                        logger.warn("╚═══════════════════════════════════════════════════════════════╝");
                    }
                }

                // Check vector normalization and warn if metric mismatch
                try {
                    boolean isNormalized = io.nosqlbench.command.compute.VectorNormalizationDetector.areVectorsNormalized(baseVectorsPath);
                    String metric = distanceMetricOption.getDistanceMetric().name();

                    logger.info("Base vectors normalization: {}", isNormalized ? "NORMALIZED (||v||=1)" : "NOT NORMALIZED");

                    // CRITICAL: DOT_PRODUCT with non-normalized vectors is WRONG
                    if (io.nosqlbench.command.compute.VectorNormalizationDetector.isDotProductWithNonNormalized(metric, isNormalized)) {
                        logger.error("╔═══════════════════════════════════════════════════════════════╗");
                        logger.error("║ CRITICAL ERROR: DOT_PRODUCT used with NON-NORMALIZED vectors!║");
                        logger.error("║ This will produce MEANINGLESS results!                       ║");
                        logger.error("║ DOT_PRODUCT only valid for normalized vectors (||v||=1)      ║");
                        logger.error("║ Recommended: Use COSINE, L2, or L1 for non-normalized data   ║");
                        logger.error("╚═══════════════════════════════════════════════════════════════╝");
                        System.err.println("\nERROR: DOT_PRODUCT requires normalized vectors. Use COSINE, L2, or L1 instead.\n");
                        return EXIT_ERROR;
                    }

                    // Standard metric mismatch warning
                    if (!io.nosqlbench.command.compute.VectorNormalizationDetector.isMetricAppropriate(metric, isNormalized)) {
                        String recommended = io.nosqlbench.command.compute.VectorNormalizationDetector.getRecommendedMetric(isNormalized);
                        logger.warn("╔═══════════════════════════════════════════════════════════════╗");
                        logger.warn("║ METRIC WARNING: Vectors are {} but using {}",
                            isNormalized ? "NORMALIZED  " : "NOT NORMALIZED", String.format("%-14s", metric));
                        logger.warn("║ Recommended metric: {}                                        ║",
                            String.format("%-43s", recommended));
                        logger.warn("║ Results may not be meaningful with this combination!         ║");
                        logger.warn("╚═══════════════════════════════════════════════════════════════╝");
                    }
                } catch (Exception e) {
                    logger.debug("Could not detect vector normalization: {}", e.getMessage());
                }

                // Log Panama optimization status
                if (KnnOptimizationProvider.isPanamaAvailable()) {
                    String strategyName = useBatchedSIMD ? "BATCHED (experimental)" : "PER-QUERY (default)";
                    logger.info("Panama KNN optimizations ENABLED - using {} strategy with Vector API + FFM", strategyName);
                } else {
                    logger.info("Panama KNN optimizations NOT AVAILABLE - using standard implementation");
                }

                // === PHASE 1: PLANNING ===
                // Scan cache directory for all available cache files (partition + final)
                logger.info("=== PHASE 1: CACHE ANALYSIS ===");
                List<PartitionMetadata> allCaches = scanCacheDirectory(
                    baseVectorsPath, effectiveK, distanceMetricOption.getDistanceMetric(),
                    (int)effectiveStart, (int)effectiveEnd);

                logger.info("Found {} cache entries", allCaches.size());
                for (PartitionMetadata cache : allCaches) {
                    logger.debug("Cache: [{}, {}) size={} type={}",
                        cache.startIndex, cache.endIndex, cache.size(),
                        cache.isFinal ? "FINAL" : "partition");
                }

                // Find optimal cache combination (fewest merges)
                List<PartitionMetadata> optimalCaches = findOptimalCacheCombination(
                    allCaches, (int)effectiveStart, (int)effectiveEnd);

                // Check if we have complete cache coverage
                if (optimalCaches != null && !optimalCaches.isEmpty()) {
                    // Verify all caches have same query count
                    int queryCount = optimalCaches.get(0).queryCount;
                    boolean consistent = optimalCaches.stream().allMatch(c -> c.queryCount == queryCount);

                    if (consistent) {
                        if (optimalCaches.size() == 1 && optimalCaches.get(0).isFinal) {
                            // Single final cache - just copy it
                            logger.info("Using complete final cache - no computation or merging needed");
                            Files.copy(optimalCaches.get(0).neighborsPath, neighborsOutput,
                                java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                            Files.copy(optimalCaches.get(0).distancesPath, distancesOutput,
                                java.nio.file.StandardCopyOption.REPLACE_EXISTING);

                            System.out.println("Successfully retrieved KNN from final cache for " + queryCount + " query vectors");
                            System.out.println("Base vectors: " + baseCount + ", k: " + effectiveK + ", distance metric: " + distanceMetricOption.getDistanceMetric());
                            System.out.println("Neighbors file: " + neighborsOutput);
                            System.out.println("Distances file: " + distancesOutput);
                            return EXIT_SUCCESS;
                        } else {
                            // Multiple caches provide complete coverage - merge them
                            logger.info("Complete cache coverage with {} entries (requires {} merge operations)",
                                optimalCaches.size(), optimalCaches.size() - 1);

                            try (StatusScope partitionsScope = ctx.createScope("KNN Partitions");
                                 StatusScope mergeScope = partitionsScope.createChildScope("Merge Caches")) {
                                mergePartitions(mergeScope, optimalCaches, neighborsOutput, distancesOutput, effectiveK, queryCount);
                            }

                            // Save merged result as final cache
                            Path finalCacheNeighbors = buildFinalCachePath(baseVectorsPath, (int)effectiveStart, (int)effectiveEnd,
                                effectiveK, distanceMetricOption.getDistanceMetric(), "neighbors", "ivec");
                            Path finalCacheDistances = buildFinalCachePath(baseVectorsPath, (int)effectiveStart, (int)effectiveEnd,
                                effectiveK, distanceMetricOption.getDistanceMetric(), "distances", "fvec");

                            if (!neighborsOutput.equals(finalCacheNeighbors)) {
                                saveFinalCache(neighborsOutput, distancesOutput, finalCacheNeighbors, finalCacheDistances);
                            }

                            System.out.println("Successfully merged KNN from cache for " + queryCount + " query vectors");
                            System.out.println("Base vectors: " + baseCount + ", k: " + effectiveK + ", distance metric: " + distanceMetricOption.getDistanceMetric());
                            System.out.println("Neighbors file: " + neighborsOutput);
                            System.out.println("Distances file: " + distancesOutput);
                            return EXIT_SUCCESS;
                        }
                    }
                }

                // === PHASE 2: PLAN COMPUTATION ===
                // Find which ranges are NOT covered by cache
                List<UncoveredRange> uncoveredRanges = findUncoveredRanges(allCaches, (int)effectiveStart, (int)effectiveEnd);

                if (uncoveredRanges.isEmpty()) {
                    logger.error("Logic error: no uncovered ranges but cache combination failed - this should not happen");
                    System.err.println("Error: Internal inconsistency in cache logic");
                    return EXIT_ERROR;
                }

                logger.info("=== PHASE 2: COMPUTATION PLAN ===");
                logger.info("Need to compute {} uncovered ranges:", uncoveredRanges.size());
                int totalUncovered = 0;
                for (UncoveredRange range : uncoveredRanges) {
                    logger.info("  Uncovered: [{}, {}) size={}", range.start, range.end, range.size());
                    totalUncovered += range.size();
                }
                logger.info("Total uncovered vectors: {} ({} of total {})",
                    totalUncovered, String.format("%.1f%%", 100.0 * totalUncovered / baseCount), baseCount);

                // Create partition tasks for uncovered ranges only
                // IMPORTANT: Use standard batchSize for partitioning so caches are reusable
                // This creates partition caches that can be combined in different ways for future requests
                List<PartitionComputation> computations = new ArrayList<>();
                int partitionIndex = 0;
                int totalPartitions = 0;

                for (UncoveredRange range : uncoveredRanges) {
                    // Partition this uncovered range using standard batch size
                    // This ensures caches are created at standard partition boundaries for maximum reusability
                    int partitionsInRange = (int) Math.ceil((double)(range.end - range.start) / batchSize);
                    logger.info("  Will compute {} partitions of size {} for range [{}, {})",
                        partitionsInRange, batchSize, range.start, range.end);
                    totalPartitions += partitionsInRange;

                    for (int start = range.start; start < range.end; start += batchSize) {
                        int end = Math.min(start + batchSize, range.end);

                        Path cacheNeighbors = buildCachePath(baseVectorsPath, start, end,
                            effectiveK, distanceMetricOption.getDistanceMetric(), "neighbors", "ivec");
                        Path cacheDistances = buildCachePath(baseVectorsPath, start, end,
                            effectiveK, distanceMetricOption.getDistanceMetric(), "distances", "fvec");

                        PartitionComputation computation = createPartitionComputation(
                            baseVectors,
                            baseVectorsPath,
                            partitionIndex++,
                            start,
                            end,
                            baseDimension,
                            cacheNeighbors,
                            cacheDistances,
                            effectiveK,
                            queryVectorsPath,
                            useBatchedSIMD
                        );

                        computations.add(computation);
                    }
                }

                logger.info("Created {} standard-sized partition tasks (size={}) for maximum cache reusability",
                    totalPartitions, batchSize);

                // === PHASE 3: COMPUTE MISSING PARTITIONS ===
                logger.info("=== PHASE 3: COMPUTING {} PARTITIONS ===", computations.size());

                List<PartitionMetadata> newlyComputed = new ArrayList<>();
                int expectedQueryCount = -1;

                try (StatusScope partitionsScope = ctx.createScope("KNN Partitions")) {

                    int threadCount = Math.min(Runtime.getRuntime().availableProcessors(), 2);
                    ExecutorService executor = Executors.newFixedThreadPool(threadCount);

                    // Determine expected query count
                    if (!allCaches.isEmpty()) {
                        expectedQueryCount = allCaches.get(0).queryCount;
                    } else {
                        List<float[]> sampleQueries = loadQueryVectors(queryVectorsPath, baseVectors);
                        expectedQueryCount = sampleQueries.size();
                    }

                    // Execute computation tasks (memory-bounded parallelism)
                    int maxConcurrentPartitions = Math.min(2, computations.size());
                    java.util.concurrent.Semaphore memoryPermits = new java.util.concurrent.Semaphore(maxConcurrentPartitions);
                    ExecutorService partitionExecutor = Executors.newFixedThreadPool(maxConcurrentPartitions);
                    List<StatusTracker<PartitionComputation>> trackers = new ArrayList<>();

                    try {
                        List<Future<PartitionMetadata>> futures = new ArrayList<>();

                        for (PartitionComputation computation : computations) {
                            trackers.add(partitionsScope.trackTask(computation));
                            futures.add(partitionExecutor.submit(() -> {
                                memoryPermits.acquire();
                                try {
                                    return computation.call();
                                } finally {
                                    computation.close();
                                    memoryPermits.release();
                                }
                            }));
                        }

                        // Collect newly computed partitions
                        for (int i = 0; i < futures.size(); i++) {
                            try {
                                PartitionMetadata metadata = futures.get(i).get();
                                if (metadata.queryCount != expectedQueryCount) {
                                    throw new IllegalStateException("Query count mismatch: expected "
                                        + expectedQueryCount + " but got " + metadata.queryCount);
                                }
                                newlyComputed.add(metadata);
                            } catch (Exception e) {
                                throw new IOException("Failed to compute partition " + i, e);
                            } finally {
                                trackers.get(i).close();
                            }
                        }
                    } finally {
                        partitionExecutor.shutdown();
                    }

                    logger.info("Completed {} new partition computations", newlyComputed.size());

                } // close partitions scope

                // === PHASE 4: COMBINE ALL CACHES ===
                logger.info("=== PHASE 4: COMBINING RESULTS ===");

                // Add newly computed partitions to available caches
                allCaches.addAll(newlyComputed);

                // Find all caches that overlap with target range
                List<PartitionMetadata> relevantCaches = findAllRelevantCaches(allCaches, (int)effectiveStart, (int)effectiveEnd);
                logger.info("Combining {} cache entries (existing + newly computed)", relevantCaches.size());

                // Merge all relevant caches to produce final result
                try (StatusScope mergeScope = ctx.createScope("Merge All Caches")) {
                    mergePartitions(mergeScope, relevantCaches, neighborsOutput, distancesOutput, effectiveK, expectedQueryCount);
                }

                // Save final result as cache for future reuse
                Path finalCacheNeighbors = buildFinalCachePath(baseVectorsPath, (int)effectiveStart, (int)effectiveEnd,
                    effectiveK, distanceMetricOption.getDistanceMetric(), "neighbors", "ivec");
                Path finalCacheDistances = buildFinalCachePath(baseVectorsPath, (int)effectiveStart, (int)effectiveEnd,
                    effectiveK, distanceMetricOption.getDistanceMetric(), "distances", "fvec");

                if (!neighborsOutput.equals(finalCacheNeighbors)) {
                    saveFinalCache(neighborsOutput, distancesOutput, finalCacheNeighbors, finalCacheDistances);
                }

                System.out.println("Successfully computed KNN ground truth for " + expectedQueryCount + " query vectors");
                System.out.println("Base vectors: " + baseCount + ", k: " + effectiveK + ", distance metric: " + distanceMetricOption.getDistanceMetric());
                System.out.println("Neighbors file: " + neighborsOutput);
                System.out.println("Distances file: " + distancesOutput);
                System.out.println("Cache directory: " + cacheDir.toAbsolutePath() + " (results saved for reuse)");
                return EXIT_SUCCESS;
            } finally {
                baseChannel.close();
            }
        } catch (IOException e) {
            System.err.println("Error: I/O problem - " + e.getMessage());
            logger.error("I/O error during KNN computation", e);
            return EXIT_ERROR;
        } catch (Exception e) {
            System.err.println("Error computing KNN: " + e.getMessage());
            logger.error("Unexpected error during KNN computation", e);
            return EXIT_ERROR;
        }
    }
}
