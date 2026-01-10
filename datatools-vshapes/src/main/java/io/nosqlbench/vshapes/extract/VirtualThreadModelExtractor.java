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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/**
 * High-performance model extraction using virtual threads with SIMD and microbatching.
 *
 * <h2>Architecture Overview</h2>
 *
 * <p>This extractor combines three optimization strategies:
 * <ul>
 *   <li><b>Virtual Threads</b>: Lightweight threads (Project Loom) allow spawning
 *       thousands of concurrent tasks without platform thread overhead</li>
 *   <li><b>SIMD Acceleration</b>: Each virtual thread uses {@link DimensionStatistics}
 *       which leverages Panama Vector API when available</li>
 *   <li><b>Microbatching</b>: Dimensions are grouped into small batches (default 4-8)
 *       to balance parallelism overhead vs work per thread</li>
 * </ul>
 *
 * <pre>{@code
 * ┌──────────────────────────────────────────────────────────────────────────┐
 * │ PHASE 1: Cache-Blocked Transpose                                         │
 * │   float[vectors][dims] → float[dims][vectors]                           │
 * │   L2-optimized 256×256 blocks for cache efficiency                      │
 * └──────────────────────────────────────────────────────────────────────────┘
 *          ↓
 * ┌──────────────────────────────────────────────────────────────────────────┐
 * │ PHASE 2: Virtual Thread Microbatch Processing                            │
 * │                                                                          │
 * │  VirtualThread Pool (unbounded)                                          │
 * │  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐            │
 * │  │ VT-0    │ │ VT-1    │ │ VT-2    │ │ VT-3    │ │ VT-N    │ ...        │
 * │  │ dims    │ │ dims    │ │ dims    │ │ dims    │ │ dims    │            │
 * │  │ 0-3     │ │ 4-7     │ │ 8-11    │ │ 12-15   │ │ ...     │            │
 * │  │         │ │         │ │         │ │         │ │         │            │
 * │  │ SIMD    │ │ SIMD    │ │ SIMD    │ │ SIMD    │ │ SIMD    │            │
 * │  │ stats   │ │ stats   │ │ stats   │ │ stats   │ │ stats   │            │
 * │  │    ↓    │ │    ↓    │ │    ↓    │ │    ↓    │ │    ↓    │            │
 * │  │ Fit     │ │ Fit     │ │ Fit     │ │ Fit     │ │ Fit     │            │
 * │  └─────────┘ └─────────┘ └─────────┘ └─────────┘ └─────────┘            │
 * │                                                                          │
 * │  Microbatch size auto-tuned: min(8, dims/availableProcessors)           │
 * └──────────────────────────────────────────────────────────────────────────┘
 *          ↓
 * ┌──────────────────────────────────────────────────────────────────────────┐
 * │ PHASE 3: Assemble Results                                                │
 * │   Collect ScalarModels and DimensionStatistics from all virtual threads │
 * └──────────────────────────────────────────────────────────────────────────┘
 * }</pre>
 *
 * <h2>Microbatch Tuning</h2>
 *
 * <p>The optimal microbatch size balances two factors:
 * <ul>
 *   <li>Too small: Virtual thread scheduling overhead dominates</li>
 *   <li>Too large: Insufficient parallelism, some cores idle</li>
 * </ul>
 *
 * <p>Default auto-tuning: {@code min(8, max(1, dimensions / (2 * processors)))}
 *
 * <h2>Usage</h2>
 *
 * <pre>{@code
 * // Auto-configured (recommended)
 * VirtualThreadModelExtractor extractor = VirtualThreadModelExtractor.optimized();
 * VectorSpaceModel model = extractor.extractVectorModel(data);
 *
 * // Custom microbatch size
 * VirtualThreadModelExtractor extractor = VirtualThreadModelExtractor.builder()
 *     .microbatchSize(4)
 *     .selector(BestFitSelector.parametricOnly())
 *     .build();
 * }</pre>
 *
 * @see DimensionStatistics
 * @see ParallelDatasetModelExtractor
 */
public final class VirtualThreadModelExtractor implements ModelExtractor {

    /** Default microbatch size (dimensions per virtual thread) */
    public static final int DEFAULT_MICROBATCH_SIZE = 4;

    /** Maximum microbatch size (larger batches don't improve performance) */
    public static final int MAX_MICROBATCH_SIZE = 16;

    /** Minimum microbatch size */
    public static final int MIN_MICROBATCH_SIZE = 1;

    /** Default unique vectors for generated model */
    public static final long DEFAULT_UNIQUE_VECTORS = 1_000_000;

    /** Cache block size for transpose (fits in L2) */
    private static final int TRANSPOSE_BLOCK_SIZE = 256;

    private final int microbatchSize;
    private final BestFitSelector selector;
    private final ComponentModelFitter forcedFitter;
    private final long uniqueVectors;
    private final boolean collectAllFits;

    // Progress tracking
    private final AtomicLong dimensionsCompleted = new AtomicLong(0);
    private volatile int totalDimensions;

    /**
     * Creates an optimized extractor with auto-tuned settings.
     *
     * @return a new optimized extractor
     */
    public static VirtualThreadModelExtractor optimized() {
        return new VirtualThreadModelExtractor(
            DEFAULT_MICROBATCH_SIZE,
            BestFitSelector.defaultSelector(),
            null,
            DEFAULT_UNIQUE_VECTORS,
            false
        );
    }

    /**
     * Creates an extractor with specified microbatch size.
     *
     * @param microbatchSize dimensions per virtual thread
     */
    public VirtualThreadModelExtractor(int microbatchSize) {
        this(microbatchSize, BestFitSelector.defaultSelector(), null, DEFAULT_UNIQUE_VECTORS, false);
    }

    private VirtualThreadModelExtractor(int microbatchSize, BestFitSelector selector,
            ComponentModelFitter forcedFitter, long uniqueVectors, boolean collectAllFits) {
        this.microbatchSize = Math.max(MIN_MICROBATCH_SIZE,
            Math.min(MAX_MICROBATCH_SIZE, microbatchSize));
        this.selector = selector;
        this.forcedFitter = forcedFitter;
        this.uniqueVectors = uniqueVectors;
        this.collectAllFits = collectAllFits;
    }

    /**
     * Returns a copy of this extractor configured to collect all fit scores.
     *
     * <p>When enabled, {@link #extractWithStats(float[][])} will populate
     * {@link ExtractionResult#allFitsData()} with scores for all model types.
     *
     * @return a new extractor with all-fits collection enabled
     */
    public VirtualThreadModelExtractor withAllFitsCollection() {
        return new VirtualThreadModelExtractor(
            microbatchSize, selector, forcedFitter, uniqueVectors, true);
    }

    /**
     * Returns a builder for custom configuration.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Computes optimal microbatch size for given dimensions.
     *
     * @param dimensions number of dimensions
     * @return optimal microbatch size
     */
    public static int optimalMicrobatchSize(int dimensions) {
        int processors = Runtime.getRuntime().availableProcessors();
        // Target: 2x processors worth of tasks for good load balancing
        int targetTasks = processors * 2;
        int computed = Math.max(1, dimensions / targetTasks);
        return Math.max(MIN_MICROBATCH_SIZE, Math.min(MAX_MICROBATCH_SIZE, computed));
    }

    @Override
    public VectorSpaceModel extractVectorModel(float[][] data) {
        return extractWithStats(data).model();
    }

    @Override
    public ExtractionResult extractFromTransposed(float[][] transposedData) {
        validateTransposedData(transposedData);

        long startTime = System.currentTimeMillis();

        int numDimensions = transposedData.length;
        int numVectors = transposedData[0].length;

        this.totalDimensions = numDimensions;
        this.dimensionsCompleted.set(0);

        // Compute effective microbatch size
        int effectiveBatchSize = Math.min(microbatchSize, optimalMicrobatchSize(numDimensions));

        // Process dimensions using virtual threads (with stats)
        BatchResult[] batchResults = processWithVirtualThreadsAndStats(
            transposedData, numDimensions, numVectors, effectiveBatchSize);

        // Assemble results
        ScalarModel[] components = new ScalarModel[numDimensions];
        DimensionStatistics[] stats = new DimensionStatistics[numDimensions];
        ComponentModelFitter.FitResult[] fitResults = new ComponentModelFitter.FitResult[numDimensions];

        // For all-fits collection
        AllFitsData allFitsData = null;
        java.util.List<String> modelTypes = null;
        double[][] allFitScores = null;
        int[] bestFitIndices = null;
        String[] sparklines = null;

        if (collectAllFits && selector != null) {
            modelTypes = selector.getFitters().stream()
                .map(ComponentModelFitter::getModelType)
                .toList();
            allFitScores = new double[numDimensions][modelTypes.size()];
            bestFitIndices = new int[numDimensions];
            sparklines = new String[numDimensions];
        }

        for (BatchResult batch : batchResults) {
            for (int i = 0; i < batch.components.length; i++) {
                int dim = batch.startDim + i;
                components[dim] = batch.components[i];
                stats[dim] = batch.stats[i];
                fitResults[dim] = batch.fitResults[i];

                if (collectAllFits && batch.allScores != null) {
                    allFitScores[dim] = batch.allScores[i];
                    bestFitIndices[dim] = batch.bestIndices[i];
                    sparklines[dim] = batch.sparklines != null ? batch.sparklines[i] : "";
                }
            }
        }

        if (collectAllFits && modelTypes != null) {
            allFitsData = new AllFitsData(modelTypes, allFitScores, bestFitIndices, sparklines);
        }

        long extractionTime = System.currentTimeMillis() - startTime;

        VectorSpaceModel model = new VectorSpaceModel(uniqueVectors, components);
        return new ExtractionResult(model, stats, fitResults, extractionTime, allFitsData);
    }

    @Override
    public ExtractionResult extractWithStats(float[][] data) {
        validateData(data);

        long startTime = System.currentTimeMillis();

        int numVectors = data.length;
        int numDimensions = data[0].length;

        this.totalDimensions = numDimensions;
        this.dimensionsCompleted.set(0);

        // Compute effective microbatch size based on dimensions
        int effectiveBatchSize = Math.min(microbatchSize, optimalMicrobatchSize(numDimensions));

        // Phase 1: Cache-blocked transpose
        float[][] transposed = transposeBlocked(data, numVectors, numDimensions);

        // Phase 2: Virtual thread microbatch processing
        BatchResult[] batchResults = processWithVirtualThreadsAndStats(
            transposed, numDimensions, numVectors, effectiveBatchSize);

        // Phase 3: Assemble results
        ScalarModel[] components = new ScalarModel[numDimensions];
        DimensionStatistics[] stats = new DimensionStatistics[numDimensions];
        ComponentModelFitter.FitResult[] fitResults = new ComponentModelFitter.FitResult[numDimensions];

        // For all-fits collection
        AllFitsData allFitsData = null;
        java.util.List<String> modelTypes = null;
        double[][] allFitScores = null;
        int[] bestFitIndices = null;
        String[] sparklines = null;

        if (collectAllFits && selector != null) {
            modelTypes = selector.getFitters().stream()
                .map(ComponentModelFitter::getModelType)
                .toList();
            allFitScores = new double[numDimensions][modelTypes.size()];
            bestFitIndices = new int[numDimensions];
            sparklines = new String[numDimensions];
        }

        for (BatchResult batch : batchResults) {
            for (int i = 0; i < batch.components.length; i++) {
                int dim = batch.startDim + i;
                components[dim] = batch.components[i];
                stats[dim] = batch.stats[i];
                fitResults[dim] = batch.fitResults[i];

                if (collectAllFits && batch.allScores != null) {
                    allFitScores[dim] = batch.allScores[i];
                    bestFitIndices[dim] = batch.bestIndices[i];
                    sparklines[dim] = batch.sparklines != null ? batch.sparklines[i] : "";
                }
            }
        }

        if (collectAllFits && modelTypes != null) {
            allFitsData = new AllFitsData(modelTypes, allFitScores, bestFitIndices, sparklines);
        }

        long extractionTime = System.currentTimeMillis() - startTime;

        VectorSpaceModel model = new VectorSpaceModel(uniqueVectors, components);
        return new ExtractionResult(model, stats, fitResults, extractionTime, allFitsData);
    }

    /**
     * Returns current extraction progress.
     *
     * @return progress from 0.0 to 1.0
     */
    public double getProgress() {
        if (totalDimensions == 0) return 0.0;
        return (double) dimensionsCompleted.get() / totalDimensions;
    }

    // ========== Internal Processing Methods ==========

    private ScalarModel[] processWithVirtualThreads(float[][] transposed,
            int numDimensions, int numVectors, int batchSize) {

        ScalarModel[] components = new ScalarModel[numDimensions];

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<BatchResult>> futures = new ArrayList<>();

            // Submit microbatches
            for (int startDim = 0; startDim < numDimensions; startDim += batchSize) {
                int endDim = Math.min(startDim + batchSize, numDimensions);
                int batchStart = startDim;

                futures.add(executor.submit(() ->
                    processBatch(transposed, batchStart, endDim, numVectors)));
            }

            // Collect results
            for (Future<BatchResult> future : futures) {
                BatchResult result = future.get();
                for (int i = 0; i < result.components.length; i++) {
                    components[result.startDim + i] = result.components[i];
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Virtual thread extraction failed", e);
        }

        return components;
    }

    private BatchResult[] processWithVirtualThreadsAndStats(float[][] transposed,
            int numDimensions, int numVectors, int batchSize) {

        List<BatchResult> results = new ArrayList<>();

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<BatchResult>> futures = new ArrayList<>();

            // Submit microbatches
            int numBatches = (numDimensions + batchSize - 1) / batchSize;
            for (int startDim = 0; startDim < numDimensions; startDim += batchSize) {
                int endDim = Math.min(startDim + batchSize, numDimensions);
                int batchStart = startDim;

                futures.add(executor.submit(() ->
                    processBatch(transposed, batchStart, endDim, numVectors)));
            }

            // Collect results in order
            for (Future<BatchResult> future : futures) {
                results.add(future.get());
            }
        } catch (Exception e) {
            throw new RuntimeException("Virtual thread extraction failed", e);
        }

        return results.toArray(new BatchResult[0]);
    }

    /**
     * Processes a batch of dimensions (called by virtual threads).
     */
    private BatchResult processBatch(float[][] transposed, int startDim, int endDim, int numVectors) {
        int batchSize = endDim - startDim;

        ScalarModel[] components = new ScalarModel[batchSize];
        DimensionStatistics[] stats = new DimensionStatistics[batchSize];
        ComponentModelFitter.FitResult[] fitResults = new ComponentModelFitter.FitResult[batchSize];

        // For all-fits collection
        double[][] allScores = collectAllFits && selector != null
            ? new double[batchSize][]
            : null;
        int[] bestIndices = collectAllFits && selector != null
            ? new int[batchSize]
            : null;
        String[] sparklines = collectAllFits && selector != null
            ? new String[batchSize]
            : null;

        for (int d = startDim; d < endDim; d++) {
            int localIdx = d - startDim;
            float[] dimData = transposed[d];

            // SIMD-optimized statistics computation
            DimensionStatistics dimStats = DimensionStatistics.compute(d, dimData);
            stats[localIdx] = dimStats;

            // Fit model
            ComponentModelFitter.FitResult fitResult;
            if (forcedFitter != null) {
                fitResult = forcedFitter.fit(dimStats, dimData);
            } else if (collectAllFits) {
                BestFitSelector.SelectionWithAllFits selection =
                    selector.selectBestWithAllFits(dimStats, dimData);
                fitResult = selection.bestFit();
                allScores[localIdx] = selection.allScores();
                bestIndices[localIdx] = selection.bestIndex();
                // Generate sparkline histogram for this dimension
                sparklines[localIdx] = Sparkline.generate(dimData, Sparkline.DEFAULT_WIDTH);
            } else {
                fitResult = selector.selectBestResult(dimStats, dimData);
            }

            fitResults[localIdx] = fitResult;
            components[localIdx] = fitResult.model();

            dimensionsCompleted.incrementAndGet();
        }

        return new BatchResult(startDim, components, stats, fitResults, allScores, bestIndices, sparklines);
    }

    /**
     * Cache-blocked transpose for optimal L2 utilization.
     */
    private float[][] transposeBlocked(float[][] data, int numVectors, int numDimensions) {
        float[][] transposed = new float[numDimensions][numVectors];

        // Process in L2-sized blocks
        for (int vBlock = 0; vBlock < numVectors; vBlock += TRANSPOSE_BLOCK_SIZE) {
            int vEnd = Math.min(vBlock + TRANSPOSE_BLOCK_SIZE, numVectors);

            for (int dBlock = 0; dBlock < numDimensions; dBlock += TRANSPOSE_BLOCK_SIZE) {
                int dEnd = Math.min(dBlock + TRANSPOSE_BLOCK_SIZE, numDimensions);

                // Transpose this block
                for (int v = vBlock; v < vEnd; v++) {
                    float[] vector = data[v];
                    for (int d = dBlock; d < dEnd; d++) {
                        transposed[d][v] = vector[d];
                    }
                }
            }
        }

        return transposed;
    }

    private void validateData(float[][] data) {
        if (data == null || data.length == 0) {
            throw new IllegalArgumentException("Data cannot be null or empty");
        }
        if (data[0] == null || data[0].length == 0) {
            throw new IllegalArgumentException("Vectors cannot be null or empty");
        }
    }

    private void validateTransposedData(float[][] transposed) {
        if (transposed == null || transposed.length == 0) {
            throw new IllegalArgumentException("Transposed data cannot be null or empty");
        }
        if (transposed[0] == null || transposed[0].length == 0) {
            throw new IllegalArgumentException("Dimension data cannot be null or empty");
        }
    }

    // ========== Result Container ==========

    private record BatchResult(
        int startDim,
        ScalarModel[] components,
        DimensionStatistics[] stats,
        ComponentModelFitter.FitResult[] fitResults,
        double[][] allScores,  // null if not collecting all fits
        int[] bestIndices,     // null if not collecting all fits
        String[] sparklines    // null if not collecting all fits
    ) {}

    // ========== Builder ==========

    /**
     * Builder for custom VirtualThreadModelExtractor configuration.
     */
    public static final class Builder {
        private int microbatchSize = DEFAULT_MICROBATCH_SIZE;
        private BestFitSelector selector = BestFitSelector.defaultSelector();
        private ComponentModelFitter forcedFitter = null;
        private long uniqueVectors = DEFAULT_UNIQUE_VECTORS;
        private boolean collectAllFits = false;

        /**
         * Sets the microbatch size (dimensions per virtual thread).
         *
         * @param size dimensions per virtual thread (1-16)
         * @return this builder
         */
        public Builder microbatchSize(int size) {
            this.microbatchSize = size;
            return this;
        }

        /**
         * Sets the best-fit selector.
         *
         * @param selector the selector to use
         * @return this builder
         */
        public Builder selector(BestFitSelector selector) {
            this.selector = selector;
            this.forcedFitter = null;
            return this;
        }

        /**
         * Forces a specific model fitter for all dimensions.
         *
         * @param fitter the fitter to use
         * @return this builder
         */
        public Builder forceFitter(ComponentModelFitter fitter) {
            this.forcedFitter = fitter;
            this.selector = null;
            return this;
        }

        /**
         * Sets the unique vectors count for the generated model.
         *
         * @param uniqueVectors unique vector count
         * @return this builder
         */
        public Builder uniqueVectors(long uniqueVectors) {
            this.uniqueVectors = uniqueVectors;
            return this;
        }

        /**
         * Enables collection of all fit scores for fit quality tables.
         *
         * @return this builder
         */
        public Builder collectAllFits() {
            this.collectAllFits = true;
            return this;
        }

        /**
         * Builds the extractor.
         *
         * @return a new VirtualThreadModelExtractor
         */
        public VirtualThreadModelExtractor build() {
            BestFitSelector effectiveSelector = forcedFitter != null
                ? null
                : (selector != null ? selector : BestFitSelector.defaultSelector());

            return new VirtualThreadModelExtractor(
                microbatchSize,
                effectiveSelector,
                forcedFitter,
                uniqueVectors,
                collectAllFits
            );
        }
    }
}
