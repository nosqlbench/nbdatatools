package io.nosqlbench.vshapes.extract;

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

import io.nosqlbench.vshapes.model.ScalarModel;
import io.nosqlbench.vshapes.model.VectorSpaceModel;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/**
 * NUMA-aware parallel model extraction that partitions work by NUMA node.
 *
 * <h2>Architecture Overview</h2>
 * <pre>{@code
 * ┌────────────────────────────────────────────────────────────────────────────────┐
 * │                           NUMA-Aware Processing                                 │
 * ├────────────────────────────────────────────────────────────────────────────────┤
 * │                                                                                │
 * │  ┌──────────────────────────┐    ┌──────────────────────────┐                  │
 * │  │     NUMA Node 0          │    │     NUMA Node 1          │                  │
 * │  │  ┌────────────────────┐  │    │  ┌────────────────────┐  │                  │
 * │  │  │ ForkJoinPool-0     │  │    │  │ ForkJoinPool-1     │  │                  │
 * │  │  │ (threads 0-63)     │  │    │  │ (threads 64-127)   │  │                  │
 * │  │  └────────────────────┘  │    │  └────────────────────┘  │                  │
 * │  │           ↓              │    │           ↓              │                  │
 * │  │  ┌────────────────────┐  │    │  ┌────────────────────┐  │                  │
 * │  │  │ Local Memory       │  │    │  │ Local Memory       │  │                  │
 * │  │  │ dims 0 - D/2       │  │    │  │ dims D/2 - D       │  │                  │
 * │  │  │ (first-touch)      │  │    │  │ (first-touch)      │  │                  │
 * │  │  └────────────────────┘  │    │  └────────────────────┘  │                  │
 * │  │           ↓              │    │           ↓              │                  │
 * │  │  ScalarModel[0..D/2]     │    │  ScalarModel[D/2..D]     │                  │
 * │  └──────────────────────────┘    └──────────────────────────┘                  │
 * │                        ↘                  ↙                                    │
 * │                         VectorSpaceModel                                       │
 * └────────────────────────────────────────────────────────────────────────────────┘
 * }</pre>
 *
 * <h2>Performance Benefits</h2>
 * <ul>
 *   <li><b>Memory locality</b>: Each NUMA node processes data in its local memory</li>
 *   <li><b>Bandwidth isolation</b>: No cross-socket memory traffic during processing</li>
 *   <li><b>Cache coherency</b>: Reduced cache invalidation across sockets</li>
 *   <li><b>Scalability</b>: Near-linear scaling on multi-socket systems</li>
 * </ul>
 *
 * <h2>Expected Speedup</h2>
 * <pre>
 * Configuration              | Non-NUMA | NUMA-Aware | Improvement
 * ---------------------------|----------|------------|------------
 * 2-socket, 64 cores each    | 6-8x     | 12-16x     | ~2x
 * 4-socket, 32 cores each    | 4-6x     | 12-20x     | ~3x
 * </pre>
 *
 * <h2>Usage</h2>
 * <pre>{@code
 * // Auto-detect NUMA topology
 * NumaAwareDatasetModelExtractor extractor = new NumaAwareDatasetModelExtractor();
 * VectorSpaceModel model = extractor.extractVectorModel(data);
 *
 * // Custom configuration
 * NumaAwareDatasetModelExtractor extractor = NumaAwareDatasetModelExtractor.builder()
 *     .threadsPerNode(32)
 *     .batchSize(64)
 *     .build();
 * }</pre>
 *
 * @see NumaTopology
 * @see NumaBinding
 * @see ParallelDatasetModelExtractor
 */
public final class NumaAwareDatasetModelExtractor implements ModelExtractor {

    /** Default dimensions per parallel task */
    public static final int DEFAULT_BATCH_SIZE = 64;

    /** Default unique vectors for generated model */
    public static final long DEFAULT_UNIQUE_VECTORS = 1_000_000;

    /** Reserved threads per system (not per node) */
    private static final int RESERVED_THREADS = 10;

    /** Cache block size for transpose */
    private static final int TRANSPOSE_BLOCK_SIZE = 256;

    private final NumaTopology topology;
    private final ForkJoinPool[] nodePools;
    private final int threadsPerNode;
    private final int batchSize;
    private final BestFitSelector selector;
    private final ComponentModelFitter forcedFitter;
    private final long uniqueVectors;

    // Progress tracking
    private final AtomicLong dimensionsCompleted = new AtomicLong(0);
    private volatile int totalDimensions;

    /**
     * Creates a NUMA-aware extractor with auto-detected topology.
     */
    public NumaAwareDatasetModelExtractor() {
        this(NumaTopology.detect(), -1, DEFAULT_BATCH_SIZE,
             BestFitSelector.defaultSelector(), null, DEFAULT_UNIQUE_VECTORS);
    }

    /**
     * Creates a NUMA-aware extractor with specified threads per node.
     *
     * @param threadsPerNode threads per NUMA node (-1 for auto)
     */
    public NumaAwareDatasetModelExtractor(int threadsPerNode) {
        this(NumaTopology.detect(), threadsPerNode, DEFAULT_BATCH_SIZE,
             BestFitSelector.defaultSelector(), null, DEFAULT_UNIQUE_VECTORS);
    }

    private NumaAwareDatasetModelExtractor(NumaTopology topology, int threadsPerNode,
            int batchSize, BestFitSelector selector, ComponentModelFitter forcedFitter,
            long uniqueVectors) {
        this.topology = topology;
        this.batchSize = batchSize;
        this.selector = selector;
        this.forcedFitter = forcedFitter;
        this.uniqueVectors = uniqueVectors;

        // Calculate threads per node
        this.threadsPerNode = threadsPerNode > 0
            ? threadsPerNode
            : topology.threadsPerNode(RESERVED_THREADS);

        // Create per-node thread pools with affinity
        this.nodePools = new ForkJoinPool[topology.nodeCount()];
        for (int node = 0; node < topology.nodeCount(); node++) {
            nodePools[node] = createNodePool(node, this.threadsPerNode);
        }
    }

    /**
     * Creates a ForkJoinPool with threads bound to a specific NUMA node.
     */
    private static ForkJoinPool createNodePool(int numaNode, int threads) {
        return new ForkJoinPool(threads, pool -> {
            ForkJoinWorkerThread worker = new NumaAwareWorkerThread(pool, numaNode);
            worker.setName("numa-" + numaNode + "-worker-" + worker.getPoolIndex());
            return worker;
        }, null, false);
    }

    /**
     * Worker thread that binds to a NUMA node on startup.
     */
    private static class NumaAwareWorkerThread extends ForkJoinWorkerThread {
        private final int numaNode;

        NumaAwareWorkerThread(ForkJoinPool pool, int numaNode) {
            super(pool);
            this.numaNode = numaNode;
        }

        @Override
        protected void onStart() {
            super.onStart();
            // Bind thread to NUMA node (if libnuma available)
            NumaBinding.bindThreadToNode(numaNode);
        }
    }

    /**
     * Returns a builder for custom configuration.
     */
    public static Builder builder() {
        return new Builder();
    }

    @Override
    public VectorSpaceModel extractVectorModel(float[][] data) {
        return extractWithStats(data).model();
    }

    @Override
    public VectorSpaceModel extractFromTransposed(float[][] transposedData) {
        validateTransposedData(transposedData);

        int numDimensions = transposedData.length;
        int numVectors = transposedData[0].length;

        this.totalDimensions = numDimensions;
        this.dimensionsCompleted.set(0);

        ScalarModel[] components = processNumaPartitioned(transposedData, numDimensions, numVectors);
        return new VectorSpaceModel(uniqueVectors, components);
    }

    @Override
    public ExtractionResult extractWithStats(float[][] data) {
        validateData(data);

        long startTime = System.currentTimeMillis();

        int numVectors = data.length;
        int numDimensions = data[0].length;

        this.totalDimensions = numDimensions;
        this.dimensionsCompleted.set(0);

        // Phase 1: NUMA-partitioned transpose
        // Each node transposes its partition (first-touch allocation)
        float[][] transposed = transposeNumaPartitioned(data, numVectors, numDimensions);

        // Phase 2: NUMA-partitioned dimension processing
        ScalarModel[] components = processNumaPartitioned(transposed, numDimensions, numVectors);

        long extractionTime = System.currentTimeMillis() - startTime;

        VectorSpaceModel model = new VectorSpaceModel(uniqueVectors, components);
        return new ExtractionResult(model, null, null, extractionTime);
    }

    /**
     * Transposes data with NUMA-aware allocation.
     * Each NUMA node allocates and populates its own partition.
     */
    private float[][] transposeNumaPartitioned(float[][] data, int numVectors, int numDimensions) {
        float[][] result = new float[numDimensions][];
        int nodeCount = topology.nodeCount();
        int dimsPerNode = (numDimensions + nodeCount - 1) / nodeCount;

        // Submit transpose tasks to each NUMA node
        @SuppressWarnings("unchecked")
        Future<Void>[] futures = new Future[nodeCount];

        for (int node = 0; node < nodeCount; node++) {
            final int nodeId = node;
            final int startDim = node * dimsPerNode;
            final int endDim = Math.min(startDim + dimsPerNode, numDimensions);

            futures[node] = nodePools[node].submit(() -> {
                // First-touch: allocate on this NUMA node
                for (int d = startDim; d < endDim; d++) {
                    result[d] = new float[numVectors];
                }

                // Transpose this partition (cache-blocked within node)
                transposePartitionBlocked(data, result, numVectors, startDim, endDim);
                return null;
            });
        }

        // Wait for all nodes to complete
        for (Future<Void> future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                throw new RuntimeException("NUMA transpose failed", e);
            }
        }

        return result;
    }

    /**
     * Cache-blocked transpose for a dimension partition.
     */
    private static void transposePartitionBlocked(float[][] data, float[][] result,
            int numVectors, int startDim, int endDim) {
        // Process in L2-sized blocks
        for (int bv = 0; bv < numVectors; bv += TRANSPOSE_BLOCK_SIZE) {
            int vEnd = Math.min(bv + TRANSPOSE_BLOCK_SIZE, numVectors);

            for (int bd = startDim; bd < endDim; bd += TRANSPOSE_BLOCK_SIZE) {
                int dEnd = Math.min(bd + TRANSPOSE_BLOCK_SIZE, endDim);

                // Transpose block
                for (int v = bv; v < vEnd; v++) {
                    for (int d = bd; d < dEnd; d++) {
                        result[d][v] = data[v][d];
                    }
                }
            }
        }
    }

    /**
     * Processes dimensions partitioned by NUMA node.
     */
    private ScalarModel[] processNumaPartitioned(float[][] transposed,
            int numDimensions, int numVectors) {

        ScalarModel[] components = new ScalarModel[numDimensions];
        int nodeCount = topology.nodeCount();
        int dimsPerNode = (numDimensions + nodeCount - 1) / nodeCount;

        // Submit processing tasks to each NUMA node
        @SuppressWarnings("unchecked")
        Future<ScalarModel[]>[] futures = new Future[nodeCount];

        for (int node = 0; node < nodeCount; node++) {
            final int nodeId = node;
            final int startDim = node * dimsPerNode;
            final int endDim = Math.min(startDim + dimsPerNode, numDimensions);

            if (startDim >= numDimensions) {
                break;  // No more dimensions to process
            }

            futures[node] = nodePools[node].submit(() ->
                processNodePartition(transposed, startDim, endDim, numVectors));
        }

        // Collect results from each node
        for (int node = 0; node < nodeCount && futures[node] != null; node++) {
            try {
                ScalarModel[] nodeResults = futures[node].get();
                int startDim = node * dimsPerNode;
                System.arraycopy(nodeResults, 0, components, startDim, nodeResults.length);
            } catch (Exception e) {
                throw new RuntimeException("NUMA processing failed on node " + node, e);
            }
        }

        return components;
    }

    /**
     * Processes a partition of dimensions on a single NUMA node.
     * Uses parallel streams within the node's pool.
     */
    private ScalarModel[] processNodePartition(float[][] transposed,
            int startDim, int endDim, int numVectors) {

        int partitionSize = endDim - startDim;
        ScalarModel[] models = new ScalarModel[partitionSize];

        // Create tasks for dimension batches within this partition
        int numTasks = (partitionSize + batchSize - 1) / batchSize;
        List<DimensionBatchTask> tasks = new ArrayList<>(numTasks);

        for (int taskIdx = 0; taskIdx < numTasks; taskIdx++) {
            int taskStart = startDim + taskIdx * batchSize;
            int taskEnd = Math.min(taskStart + batchSize, endDim);
            tasks.add(new DimensionBatchTask(transposed, taskStart, taskEnd, numVectors, startDim));
        }

        // Execute tasks in parallel within this node's pool
        tasks.parallelStream().forEach(task -> {
            ScalarModel[] results = task.compute();
            int localOffset = task.startDim - task.partitionStart;
            System.arraycopy(results, 0, models, localOffset, results.length);
            dimensionsCompleted.addAndGet(results.length);
        });

        return models;
    }

    /**
     * Task for processing a batch of dimensions with SIMD.
     */
    private class DimensionBatchTask {
        final float[][] transposed;
        final int startDim;
        final int endDim;
        final int numVectors;
        final int partitionStart;

        DimensionBatchTask(float[][] transposed, int startDim, int endDim,
                int numVectors, int partitionStart) {
            this.transposed = transposed;
            this.startDim = startDim;
            this.endDim = endDim;
            this.numVectors = numVectors;
            this.partitionStart = partitionStart;
        }

        ScalarModel[] compute() {
            int batchCount = endDim - startDim;
            ScalarModel[] models = new ScalarModel[batchCount];

            // Process 8 dimensions at a time with SIMD
            int simdBatches = batchCount / 8;
            int remainder = batchCount % 8;

            // Pre-allocate interleaved buffer (NUMA-local allocation via first-touch)
            double[] interleavedBuffer = new double[numVectors * 8];

            for (int batch = 0; batch < simdBatches; batch++) {
                int batchStart = startDim + batch * 8;

                // Interleave 8 dimensions
                interleaveInto(transposed, batchStart, 8, numVectors, interleavedBuffer);

                // Compute SIMD statistics
                DimensionStatistics[] stats = BatchDimensionStatistics.computeBatch(
                    interleavedBuffer, numVectors, batchStart);

                // Fit models
                for (int i = 0; i < 8; i++) {
                    int dimIdx = batch * 8 + i;
                    float[] dimData = transposed[batchStart + i];

                    ComponentModelFitter.FitResult result;
                    if (forcedFitter != null) {
                        result = forcedFitter.fit(stats[i], dimData);
                    } else {
                        result = selector.selectBestResult(stats[i], dimData);
                    }

                    models[dimIdx] = result.model();
                }
            }

            // Process remaining dimensions
            for (int i = 0; i < remainder; i++) {
                int dimIdx = simdBatches * 8 + i;
                int globalDim = startDim + dimIdx;
                float[] dimData = transposed[globalDim];

                DimensionStatistics stats = DimensionStatistics.compute(globalDim, dimData);

                ComponentModelFitter.FitResult result;
                if (forcedFitter != null) {
                    result = forcedFitter.fit(stats, dimData);
                } else {
                    result = selector.selectBestResult(stats, dimData);
                }

                models[dimIdx] = result.model();
            }

            return models;
        }
    }

    /**
     * Interleaves dimension data into buffer for SIMD processing.
     */
    private static void interleaveInto(float[][] transposed, int startDim, int numDims,
            int numVectors, double[] buffer) {
        for (int v = 0; v < numVectors; v++) {
            int baseOffset = v * 8;
            for (int d = 0; d < numDims; d++) {
                buffer[baseOffset + d] = transposed[startDim + d][v];
            }
        }
    }

    private void validateData(float[][] data) {
        if (data == null || data.length == 0) {
            throw new IllegalArgumentException("data cannot be null or empty");
        }
        if (data[0] == null || data[0].length == 0) {
            throw new IllegalArgumentException("data rows cannot be null or empty");
        }
    }

    private void validateTransposedData(float[][] transposedData) {
        if (transposedData == null || transposedData.length == 0) {
            throw new IllegalArgumentException("transposedData cannot be null or empty");
        }
        if (transposedData[0] == null || transposedData[0].length == 0) {
            throw new IllegalArgumentException("dimension arrays cannot be null or empty");
        }
    }

    /**
     * Returns current progress (0.0 to 1.0).
     */
    public double getProgress() {
        return totalDimensions > 0
            ? (double) dimensionsCompleted.get() / totalDimensions
            : 0.0;
    }

    /**
     * Returns the detected NUMA topology.
     */
    public NumaTopology getTopology() {
        return topology;
    }

    /**
     * Returns threads per NUMA node.
     */
    public int getThreadsPerNode() {
        return threadsPerNode;
    }

    /**
     * Returns total thread count across all NUMA nodes.
     */
    public int getTotalThreads() {
        return threadsPerNode * topology.nodeCount();
    }

    /**
     * Shuts down all per-node thread pools.
     */
    public void shutdown() {
        for (ForkJoinPool pool : nodePools) {
            if (pool != null && !pool.isShutdown()) {
                pool.shutdown();
            }
        }
    }

    /**
     * Builder for custom NumaAwareDatasetModelExtractor configuration.
     */
    public static final class Builder {
        private NumaTopology topology = null;  // Auto-detect if null
        private int threadsPerNode = -1;       // Auto-calculate if -1
        private int batchSize = DEFAULT_BATCH_SIZE;
        private BestFitSelector selector = BestFitSelector.defaultSelector();
        private ComponentModelFitter forcedFitter = null;
        private long uniqueVectors = DEFAULT_UNIQUE_VECTORS;

        private Builder() {}

        /**
         * Sets the NUMA topology (default: auto-detect).
         */
        public Builder topology(NumaTopology topology) {
            this.topology = topology;
            return this;
        }

        /**
         * Sets threads per NUMA node (default: auto-calculate).
         */
        public Builder threadsPerNode(int threads) {
            if (threads <= 0 && threads != -1) {
                throw new IllegalArgumentException("threadsPerNode must be positive or -1 for auto");
            }
            this.threadsPerNode = threads;
            return this;
        }

        /**
         * Sets the batch size (dimensions per task).
         */
        public Builder batchSize(int batchSize) {
            if (batchSize <= 0) {
                throw new IllegalArgumentException("batchSize must be positive");
            }
            this.batchSize = batchSize;
            return this;
        }

        /**
         * Sets the best-fit selector.
         */
        public Builder selector(BestFitSelector selector) {
            this.selector = selector;
            this.forcedFitter = null;
            return this;
        }

        /**
         * Forces a specific fitter for all dimensions.
         */
        public Builder forcedFitter(ComponentModelFitter fitter) {
            this.forcedFitter = fitter;
            this.selector = null;
            return this;
        }

        /**
         * Sets the unique vector count for the model.
         */
        public Builder uniqueVectors(long uniqueVectors) {
            if (uniqueVectors <= 0) {
                throw new IllegalArgumentException("uniqueVectors must be positive");
            }
            this.uniqueVectors = uniqueVectors;
            return this;
        }

        /**
         * Builds the extractor.
         */
        public NumaAwareDatasetModelExtractor build() {
            NumaTopology effectiveTopology = topology != null
                ? topology
                : NumaTopology.detect();

            BestFitSelector effectiveSelector = forcedFitter == null
                ? (selector != null ? selector : BestFitSelector.defaultSelector())
                : null;

            return new NumaAwareDatasetModelExtractor(
                effectiveTopology, threadsPerNode, batchSize,
                effectiveSelector, forcedFitter, uniqueVectors);
        }
    }
}
