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
import java.util.Iterator;
import java.util.List;
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
        names = {"--simd-strategy"},
        description = {
            "SIMD optimization strategy for Panama (JDK 25+ only):",
            "  auto:      Automatically select fastest (default - uses batched for normalized vectors)",
            "  per-query: Process queries individually with SIMD across dimensions",
            "  batched:   Process 8-16 queries simultaneously with transposed SIMD (FASTEST)"
        },
        defaultValue = "auto"
    )
    private String simdStrategy = "auto";

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
            default:
                throw new IllegalArgumentException("Unsupported distance metric: " + metric);
        }
    }

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

        long estimated = Math.max(1L, usable / bytesPerVector);
        long capped = Math.min(estimated, baseCount);
        if (capped > Integer.MAX_VALUE) {
            capped = Integer.MAX_VALUE;
        }
        return (int) Math.max(1L, capped);
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
        VectorFileArray<float[]> baseReader,
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
            baseReader,
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
        MergeOperation mergeOp = new MergeOperation(queryCount, partitions.size());
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

                        Collections.sort(combined);
                        int resultSize = Math.min(effectiveK, combined.size());
                        int[] finalIndices = new int[resultSize];
                        float[] finalDistances = new float[resultSize];

                        for (int i = 0; i < resultSize; i++) {
                            NeighborIndex neighbor = combined.get(i);
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
        private final VectorFileArray<float[]> baseReader;
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
            VectorFileArray<float[]> baseReader,
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
            this.baseReader = baseReader;
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
         *
         * @return metadata about the computed partition
         * @throws IOException if I/O errors occur
         */
        PartitionMetadata compute() throws IOException {
            state = RunState.RUNNING;
            phase = "loading";

            logger.info("Partition {}: loading {} base vectors (indices [{}..{}))", partitionIndex, partitionSize, startIndex, endIndex);

            boolean usedPanama = false;
            try {
                if (KnnOptimizationProvider.isPanamaAvailable()) {
                    try {
                        // Zero-copy memory-mapped loading
                        panamaVectorBatch = KnnOptimizationProvider.loadAsPanamaVectorBatch(baseVectorsPath, startIndex, endIndex);
                        usedPanama = true;
                        loadedVectors.set(partitionSize);
                        logger.info("Partition {}: loaded {} base vectors using zero-copy memory-mapped I/O",
                            partitionIndex, partitionSize);
                    } catch (Throwable e) {
                        logger.warn("Memory-mapped I/O failed for partition {}, falling back to standard loading: {}",
                            partitionIndex, e.getMessage());
                        usedPanama = false;
                    }
                }

                // Fallback: Standard I/O loading
                if (!usedPanama) {
                    baseBatch = new ArrayList<>(partitionSize);
                    final int readBatchSize = 1000;
                    for (int batchStart = startIndex; batchStart < endIndex; batchStart += readBatchSize) {
                        int batchEnd = Math.min(batchStart + readBatchSize, endIndex);
                        List<float[]> batch = baseReader.subList(batchStart, batchEnd);
                        baseBatch.addAll(batch);
                        loadedVectors.addAndGet(batch.size());
                    }
                    logger.info("Partition {}: loaded {} base vectors using standard I/O",
                        partitionIndex, partitionSize);
                }

                phase = "loaded";
            } catch (Exception e) {
                state = RunState.FAILED;
                phase = "failed loading";
                throw e;
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
                logger.info("Partition {} complete: wrote {} query results (k={})", partitionIndex, processed, partitionK);
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
                    // BATCHED strategy: Transposed SIMD (experimental)
                    logger.info("Partition {}: using Panama BATCHED SIMD optimization for {} queries",
                        partitionIndex, queries.size());
                    logger.info("  → Each base vector loaded ONCE per query batch (massive memory bandwidth savings)");
                    logger.info("  → Transposed SIMD: processing 8-16 queries simultaneously per base vector");

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
                return String.format("Partition %d: loading %d/%d base vectors [%d..%d]",
                    partitionIndex, loadedVectors.get(), partitionSize, startIndex, endIndex);
            } else {
                return String.format("Partition %d [base vectors %d..%d]: %d/%d queries",
                    partitionIndex, startIndex, endIndex, processedQueries.get(), totalQueries.get());
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
        private final AtomicInteger mergedQueries = new AtomicInteger(0);
        private volatile RunState state = RunState.PENDING;

        MergeOperation(int totalQueries, int partitionCount) {
            this.totalQueries = totalQueries;
            this.partitionCount = partitionCount;
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
            return String.format("Merging %d partitions: %d/%d queries",
                partitionCount, mergedQueries.get(), totalQueries);
        }
    }

    private static class PartitionMetadata {
        final Path neighborsPath;
        final Path distancesPath;
        final int startIndex;
        final int endIndex;
        final int queryCount;
        final int partitionK;

        PartitionMetadata(Path neighborsPath, Path distancesPath, int startIndex, int endIndex, int queryCount, int partitionK) {
            this.neighborsPath = neighborsPath;
            this.distancesPath = distancesPath;
            this.startIndex = startIndex;
            this.endIndex = endIndex;
            this.queryCount = queryCount;
            this.partitionK = partitionK;
        }
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

            // Create StatusContext with LOG mode for simple text logging output
            try (StatusContext ctx = new StatusContext("compute-knn", Optional.of(StatusSinkMode.LOG));
                 VectorFileArray<float[]> baseReader = VectorFileIO.randomAccess(FileType.xvec, float[].class, baseVectorsPath)) {
                int totalBaseCount = baseReader.getSize();
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

                int baseDimension = baseReader.get((int)effectiveStart).length;
                if (baseDimension <= 0) {
                    System.err.println("Error: Base vectors have zero dimension");
                    return EXIT_ERROR;
                }

                int effectiveK = Math.min(k, baseCount);
                int batchSize = Math.max(1, determineBatchSize(baseCount, baseDimension));
                int partitionCount = (int) Math.ceil((double) baseCount / batchSize);

                String rangeStr = effectiveRange != null ? effectiveRange.toString() : "all";
                logger.info("compute knn: baseCount={}, baseDimension={}, effectiveK={}, batchSize={}, partitions={}, range={}",
                    baseCount, baseDimension, effectiveK, batchSize, partitionCount, rangeStr);

                if (effectiveRange != null) {
                    logger.info("Using range {} - processing {} out of {} total base vectors",
                        rangeStr, baseCount, totalBaseCount);
                }

                // Decide and cache SIMD strategy for this session
                if ("auto".equalsIgnoreCase(simdStrategy)) {
                    // Auto-select: Currently always use per-query (batched needs more optimization)
                    // TODO: Re-enable batched when performance issues are resolved
                    useBatchedSIMD = false;
                    logger.info("Auto-selected PER-QUERY strategy (proven fast, production-ready)");
                } else {
                    useBatchedSIMD = "batched".equalsIgnoreCase(simdStrategy);
                    if (useBatchedSIMD) {
                        logger.warn("BATCHED strategy selected manually - currently slow, needs optimization work");
                    }
                }

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

                // Create parent scope for all partition processing
                List<PartitionMetadata> partitions = new ArrayList<>();
                int expectedQueryCount = -1;

                try (StatusScope partitionsScope = ctx.createScope("KNN Partitions")) {

                    // Create thread pool for parallel partition processing
                    int threadCount = Math.min(partitionCount, Runtime.getRuntime().availableProcessors());
                    ExecutorService executor = Executors.newFixedThreadPool(threadCount);

                    // Create partition tasks (lightweight - no data loading yet)
                    // Data loading is deferred until each task executes
                    List<PartitionComputation> computations = new ArrayList<>();
                    int partitionIndex = 0;
                    for (int start = 0; start < baseCount; start += batchSize) {
                        int end = Math.min(baseCount, start + batchSize);
                        // Compute actual indices into the file (accounting for range offset)
                        int actualStart = (int)effectiveStart + start;
                        int actualEnd = (int)effectiveStart + end;

                        Path intermediateNeighbors = buildIntermediatePath(neighborsOutput, actualStart, actualEnd, "neighbors", "ivec");
                        Path intermediateDistances = buildIntermediatePath(neighborsOutput, actualStart, actualEnd, "distances", "fvec");

                        // Create lightweight partition task (data loads during execution)
                        PartitionComputation computation = createPartitionComputation(
                            baseReader,
                            baseVectorsPath,
                            partitionIndex,
                            actualStart,
                            actualEnd,
                            baseDimension,
                            intermediateNeighbors,
                            intermediateDistances,
                            effectiveK,
                            queryVectorsPath,
                            useBatchedSIMD  // Pass cached strategy decision
                        );

                        computations.add(computation);
                        partitionIndex++;
                    }

                    // IMPORTANT: For multiple partitions, process SEQUENTIALLY to keep memory bounded
                    // For single partition, use all threads for query parallelism
                    if (partitionCount > 1) {
                        // Sequential partition processing (one at a time) with tracking
                        logger.info("Processing {} partitions SEQUENTIALLY to keep memory bounded", partitionCount);

                        for (PartitionComputation computation : computations) {
                            // Track this partition's progress
                            StatusTracker<PartitionComputation> tracker = partitionsScope.trackTask(computation);
                            try {
                                PartitionMetadata metadata = computation.call();

                                if (expectedQueryCount < 0) {
                                    expectedQueryCount = metadata.queryCount;
                                } else if (metadata.queryCount != expectedQueryCount) {
                                    throw new IllegalStateException("Mismatch in query counts across partitions: expected "
                                        + expectedQueryCount + " but partition produced " + metadata.queryCount);
                                }

                                partitions.add(metadata);
                            } catch (Exception e) {
                                throw new IOException("Failed to compute partition " + computation.partitionIndex, e);
                            } finally {
                                // Free partition memory before loading next
                                computation.close();
                                tracker.close();
                            }
                        }
                    } else {
                        // Single partition: can use thread pool for query parallelism
                        TrackingMode trackingMode = TrackingMode.INDIVIDUAL;

                        try (TrackedExecutorService trackedExecutor = TrackedExecutors.wrap(executor, partitionsScope)
                                .withMode(trackingMode)
                                .withTaskGroupName("Partition Computation")
                                .build()) {

                            // Submit single partition
                            List<Future<PartitionMetadata>> futures = new ArrayList<>();
                            for (PartitionComputation computation : computations) {
                                futures.add(trackedExecutor.submit(computation));
                            }

                            // Collect results from single partition
                            try {
                                for (int i = 0; i < futures.size(); i++) {
                                    Future<PartitionMetadata> future = futures.get(i);
                                    PartitionComputation computation = computations.get(i);

                                    try {
                                        PartitionMetadata metadata = future.get();

                                        if (expectedQueryCount < 0) {
                                            expectedQueryCount = metadata.queryCount;
                                        } else if (metadata.queryCount != expectedQueryCount) {
                                            throw new IllegalStateException("Mismatch in query counts across partitions: expected "
                                                + expectedQueryCount + " but partition produced " + metadata.queryCount);
                                        }

                                        partitions.add(metadata);
                                    } catch (Exception e) {
                                        throw new IOException("Failed to compute partition", e);
                                    } finally {
                                        // Close Panama resources (MemorySegment, Arena)
                                        computation.close();
                                    }
                                }
                            } finally {
                                // Ensure all computations are closed even on error
                                for (PartitionComputation computation : computations) {
                                    try {
                                        computation.close();
                                    } catch (Exception e) {
                                        logger.warn("Error closing computation resources: {}", e.getMessage());
                                    }
                                }
                            }
                        } // close tracked executor
                    }

                    // Check results (common to both branches)
                    if (expectedQueryCount < 0) {
                        System.err.println("Error: No query vectors found in file");
                        return EXIT_ERROR;
                    }

                    // Merge partitions with progress tracking
                    try (StatusScope mergeScope = partitionsScope.createChildScope("Merge Results")) {
                        mergePartitions(mergeScope, partitions, neighborsOutput, distancesOutput, effectiveK, expectedQueryCount);
                    }
                } // close partitions scope

                System.out.println("Successfully computed KNN ground truth for " + expectedQueryCount + " query vectors");
                System.out.println("Base vectors: " + baseCount + ", k: " + effectiveK + ", distance metric: " + distanceMetricOption.getDistanceMetric());
                System.out.println("Neighbors file: " + neighborsOutput);
                System.out.println("Distances file: " + distancesOutput);
                return EXIT_SUCCESS;
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
