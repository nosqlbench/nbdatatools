package io.nosqlbench.vshapes.stream;

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

import io.nosqlbench.vshapes.extract.BestFitSelector;
import io.nosqlbench.vshapes.extract.ComponentModelFitter;
import io.nosqlbench.vshapes.extract.CompositeModelFitter;
import io.nosqlbench.vshapes.extract.CompositeModelFitter.ClusteringStrategy;
import io.nosqlbench.vshapes.extract.DimensionStatistics;
import io.nosqlbench.vshapes.extract.EmpiricalModelFitter;
import io.nosqlbench.vshapes.extract.InternalVerifier;
import io.nosqlbench.vshapes.extract.ModelExtractor;
import io.nosqlbench.vshapes.extract.Sparkline;
import io.nosqlbench.vshapes.extract.InternalVerifier.VerificationLevel;
import io.nosqlbench.vshapes.model.CompositeScalarModel;
import io.nosqlbench.vshapes.model.EmpiricalScalarModel;
import io.nosqlbench.vshapes.model.ScalarModel;
import io.nosqlbench.vshapes.model.VectorSpaceModel;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/// Streaming model extractor that processes vector data in columnar chunks.
///
/// ## Purpose
///
/// This analyzer processes columnar vector data, accumulating per-dimension statistics
/// incrementally using Welford's algorithm and fitting distribution models on completion.
///
/// ## Columnar Data Format
///
/// Chunks are received in column-major format:
/// - `chunk[d]` contains all values for dimension d in this chunk
/// - `chunk[d][i]` is dimension d of the i-th vector in this chunk
///
/// This layout enables:
/// - Cache-efficient per-dimension processing
/// - Potential SIMD optimization for contiguous dimension arrays
/// - Reduced memory traffic compared to row-major iteration
///
/// ## Thread Safety
///
/// This class is thread-safe for concurrent calls to [accept].
/// Per-dimension accumulators are protected by fine-grained locks to allow
/// parallel processing while minimizing contention.
///
/// ## Memory Management
///
/// The extractor stores dimension data for model fitting. For very large datasets,
/// consider using {@link io.nosqlbench.vshapes.extract.ConvergentDatasetModelExtractor}
/// which can stop early when statistical estimates converge.
///
/// ## Usage
///
/// ```java
/// // Create and configure
/// StreamingModelExtractor extractor = new StreamingModelExtractor();
/// extractor.setSelector(BestFitSelector.boundedDataSelector());
///
/// // Initialize with shape
/// DataspaceShape shape = new DataspaceShape(1_000_000, 128);
/// extractor.initialize(shape);
///
/// // Process columnar chunks
/// for (float[][] chunk : dataSource.chunks(10000)) {
///     extractor.accept(chunk, startIndex);
/// }
///
/// // Get the model
/// VectorSpaceModel model = extractor.complete();
/// ```
///
/// ## Using with AnalyzerHarness
///
/// ```java
/// AnalyzerHarness harness = new AnalyzerHarness();
/// harness.register(new StreamingModelExtractor());
///
/// TransposedChunkDataSource source = TransposedChunkDataSource.builder()
///     .file(vectorFile)
///     .build();
///
/// AnalysisResults results = harness.run(source, source.getOptimalChunkSize());
/// VectorSpaceModel model = results.getResult("model-extractor", VectorSpaceModel.class);
/// ```
///
/// @see StreamingAnalyzer
/// @see StreamingDimensionAccumulator
/// @see VectorSpaceModel
/// @see BestFitSelector
@AnalyzerName("model-extractor")
public final class StreamingModelExtractor implements StreamingAnalyzer<VectorSpaceModel> {

    /// Default analyzer type identifier
    public static final String ANALYZER_TYPE = "model-extractor";

    /// Default description
    private static final String DESCRIPTION = "Streaming vector space model extractor";

    /// Per-dimension accumulators for statistics
    private StreamingDimensionAccumulator[] accumulators;

    /// Per-dimension locks for thread safety
    private ReentrantLock[] locks;

    /// Per-dimension data storage for model fitting
    private float[][] dimensionData;

    /// Per-dimension write positions
    private int[] writePositions;

    /// Selector for best-fit model selection
    private BestFitSelector selector = BestFitSelector.boundedDataSelector();

    /// Unique vectors count for the model
    private long uniqueVectors = 0;

    /// Total samples processed
    private final AtomicLong samplesProcessed = new AtomicLong(0);

    /// Dataspace shape
    private DataspaceShape shape;

    // ========== Convergence Tracking ==========

    /// Enable/disable convergence tracking
    private boolean convergenceEnabled = false;

    /// Enable/disable early stopping when all dimensions converge
    private boolean earlyStoppingEnabled = true;

    /// Convergence threshold (fraction of standard error)
    private double convergenceThreshold = 0.01;  // 1% of standard error

    /// Minimum samples before convergence can be declared
    private static final long MINIMUM_SAMPLES_FOR_CONVERGENCE = 5000;

    /// Previous statistics for convergence comparison (all four moments)
    private double[] prevMeans;
    private double[] prevVariances;
    private double[] prevSkewness;
    private double[] prevKurtosis;

    /// Convergence status per dimension (all four moments must converge)
    private boolean[] meanConverged;
    private boolean[] varianceConverged;
    private boolean[] skewnessConverged;
    private boolean[] kurtosisConverged;

    /// Overall convergence status per dimension
    private boolean[] converged;

    /// Count of converged dimensions
    private final AtomicInteger convergedCount = new AtomicInteger(0);

    /// Batches processed for convergence tracking
    private int batchesProcessed = 0;

    /// Samples at last convergence check
    private long lastConvergenceCheckSamples = 0;

    // ========== Incremental Model Fitting ==========

    /// Enable/disable incremental model fitting
    private boolean incrementalFittingEnabled = false;

    /// Current best model type per dimension (updated during streaming)
    private String[] currentModelTypes;

    /// Current fitted models (updated incrementally)
    private ScalarModel[] currentModels;

    /// Fit interval - how often to refit models (in batches)
    private int fitInterval = 5;

    /// Track model type changes
    private int modelTypeChanges = 0;

    // ========== Multi-Modal Detection ==========

    /// Enable/disable multi-modal detection via histograms
    private boolean histogramEnabled = false;

    /// Per-dimension histograms for shape analysis
    private StreamingHistogram[] histograms;

    /// Prominence threshold for peak detection (fraction of max count)
    private double prominenceThreshold = 0.1;

    /// Dimensions detected as multi-modal
    private boolean[] multiModal;

    /// Count of multi-modal dimensions
    private int multiModalCount = 0;

    // ========== Adaptive Model Fitting ==========

    /// Enable/disable adaptive composite fallback
    private boolean adaptiveEnabled = true;

    /// KS threshold for parametric models - above this triggers composite fitting
    private double ksThresholdParametric = 0.03;

    /// KS threshold for composite models - above this triggers empirical fallback
    private double ksThresholdComposite = 0.05;

    /// Maximum number of modes for composite fitting (2-10)
    private int maxCompositeComponents = 10;

    /// Clustering strategy for composite models
    private ClusteringStrategy clusteringStrategy = ClusteringStrategy.EM;

    /// Internal verifier for parameter stability checking
    private InternalVerifier internalVerifier;

    /// Enable/disable internal verification
    private boolean internalVerificationEnabled = true;

    /// Verification level for internal mini round-trip
    private VerificationLevel verificationLevel = VerificationLevel.BALANCED;

    /// Track strategy used per dimension
    private AdaptiveStrategy[] dimensionStrategies;

    /// Count of dimensions using each strategy
    private final AtomicInteger parametricCount = new AtomicInteger(0);
    private final AtomicInteger compositeCount = new AtomicInteger(0);
    private final AtomicInteger empiricalCount = new AtomicInteger(0);

    /// Strategy used for a dimension during adaptive fitting
    public enum AdaptiveStrategy {
        PARAMETRIC,
        COMPOSITE_2, COMPOSITE_3, COMPOSITE_4, COMPOSITE_5,
        COMPOSITE_6, COMPOSITE_7, COMPOSITE_8, COMPOSITE_9, COMPOSITE_10,
        EMPIRICAL
    }

    // ========== Reservoir Sampling ==========

    /// Enable/disable reservoir sampling for large datasets
    private boolean reservoirSamplingEnabled = true;

    /// Size of reservoir per dimension (samples to keep for model fitting)
    private int reservoirSize = 10_000;

    /// Per-dimension random number generators for reservoir sampling
    private java.util.Random[] perDimensionRng;

    /// Per-dimension sample counts (for reservoir probability calculation)
    private long[] perDimensionCounts;

    // ========== All-Fits Collection (for --show-fit-table) ==========

    /// Enable/disable collection of all fit scores for diagnostics
    private boolean collectAllFits = false;

    /// Stored dimension statistics (populated during complete())
    private DimensionStatistics[] storedDimensionStats;

    /// Stored fit results (populated during complete())
    private ComponentModelFitter.FitResult[] storedFitResults;

    /// Stored all-fits data (populated during complete() when collectAllFits is true)
    private ModelExtractor.AllFitsData storedAllFitsData;

    /// Extraction time in milliseconds (populated during complete())
    private long extractionTimeMs;

    // ========== Parallelism Configuration ==========

    /// Number of threads for parallel fitting (0 = use all available processors)
    private int parallelism = 0;

    /// Custom ForkJoinPool for fitting (created if parallelism is specified)
    private java.util.concurrent.ForkJoinPool fittingPool;

    /// Whether we own the fitting pool and should shut it down
    private boolean ownsFittingPool = false;

    // ========== NUMA Awareness ==========

    /// Enable NUMA-aware fitting (default: true, detects topology automatically)
    private boolean numaAwareEnabled = true;

    /// NUMA topology (detected if NUMA-aware is enabled)
    private io.nosqlbench.vshapes.extract.NumaTopology numaTopology;

    /// Per-node ForkJoinPools for NUMA-aware processing
    private java.util.concurrent.ForkJoinPool[] numaNodePools;

    /// Creates a new streaming model extractor with default settings.
    public StreamingModelExtractor() {
    }

    /// Creates a new streaming model extractor with the specified selector.
    ///
    /// @param selector the best-fit selector to use for model fitting
    public StreamingModelExtractor(BestFitSelector selector) {
        this.selector = selector != null ? selector : BestFitSelector.boundedDataSelector();
    }

    /// Sets the best-fit selector for model fitting.
    ///
    /// Must be called before [initialize] or after [complete].
    ///
    /// @param selector the selector to use
    /// @return this extractor for chaining
    public StreamingModelExtractor setSelector(BestFitSelector selector) {
        this.selector = selector != null ? selector : BestFitSelector.boundedDataSelector();
        return this;
    }

    /// Sets the unique vectors count for the generated model.
    ///
    /// If not set, defaults to the number of samples processed.
    ///
    /// @param uniqueVectors the unique vectors count
    /// @return this extractor for chaining
    public StreamingModelExtractor setUniqueVectors(long uniqueVectors) {
        this.uniqueVectors = uniqueVectors;
        return this;
    }

    /// Returns a new extractor with all-fits collection enabled.
    ///
    /// When enabled, [completeWithStats] will populate the result with
    /// fit scores for ALL model types, not just the selected best.
    /// This enables fit quality comparison tables (--show-fit-table).
    ///
    /// @return a new extractor with all-fits collection enabled
    public StreamingModelExtractor withAllFitsCollection() {
        StreamingModelExtractor copy = new StreamingModelExtractor(this.selector);
        copy.collectAllFits = true;
        copy.uniqueVectors = this.uniqueVectors;
        copy.convergenceEnabled = this.convergenceEnabled;
        copy.convergenceThreshold = this.convergenceThreshold;
        copy.earlyStoppingEnabled = this.earlyStoppingEnabled;
        copy.adaptiveEnabled = this.adaptiveEnabled;
        copy.ksThresholdParametric = this.ksThresholdParametric;
        copy.ksThresholdComposite = this.ksThresholdComposite;
        copy.maxCompositeComponents = this.maxCompositeComponents;
        copy.clusteringStrategy = this.clusteringStrategy;
        copy.internalVerificationEnabled = this.internalVerificationEnabled;
        copy.verificationLevel = this.verificationLevel;
        copy.reservoirSamplingEnabled = this.reservoirSamplingEnabled;
        copy.reservoirSize = this.reservoirSize;
        copy.histogramEnabled = this.histogramEnabled;
        copy.prominenceThreshold = this.prominenceThreshold;
        copy.parallelism = this.parallelism;
        copy.numaAwareEnabled = this.numaAwareEnabled;
        return copy;
    }

    /// Enables or disables collection of all fit scores for diagnostics.
    ///
    /// @param enabled true to collect all fits (for --show-fit-table)
    /// @return this extractor for chaining
    public StreamingModelExtractor setCollectAllFits(boolean enabled) {
        this.collectAllFits = enabled;
        return this;
    }

    /// Returns whether all-fits collection is enabled.
    public boolean isCollectAllFitsEnabled() {
        return collectAllFits;
    }

    // ========== Parallelism Configuration Methods ==========

    /// Sets the number of threads for parallel model fitting.
    ///
    /// <p>This controls how many threads are used during the `complete()` phase
    /// when fitting models for each dimension. The default (0) uses all available
    /// processors minus 10 reserved for system tasks.
    ///
    /// @param threads number of threads (0 = auto, positive = specific count)
    /// @return this extractor for chaining
    public StreamingModelExtractor setParallelism(int threads) {
        if (threads < 0) {
            throw new IllegalArgumentException("parallelism must be non-negative");
        }
        this.parallelism = threads;
        return this;
    }

    /// Returns the configured parallelism level.
    ///
    /// @return 0 for auto (all processors), or specific thread count
    public int getParallelism() {
        return parallelism;
    }

    /// Returns the effective parallelism that will be used for fitting.
    ///
    /// @return actual number of threads that will be used
    public int getEffectiveParallelism() {
        if (parallelism > 0) {
            return parallelism;
        }
        int available = Runtime.getRuntime().availableProcessors();
        return Math.max(1, available - 10);  // Reserve 10 for system
    }

    // ========== NUMA Awareness Methods ==========

    /// Enables or disables NUMA-aware parallel fitting.
    ///
    /// <p>NUMA-aware fitting is enabled by default. When enabled, the model fitting
    /// phase partitions dimensions across NUMA nodes, binding worker threads to
    /// specific nodes for optimal memory locality. This is beneficial on multi-socket
    /// systems where cross-socket memory access is expensive.
    ///
    /// <p>On single-socket systems (1 NUMA node), this has no performance impact
    /// but causes no harm - it behaves identically to non-NUMA fitting.
    ///
    /// @param enabled true to enable NUMA-aware fitting (default: true)
    /// @return this extractor for chaining
    public StreamingModelExtractor setNumaAware(boolean enabled) {
        this.numaAwareEnabled = enabled;
        return this;
    }

    /// Returns whether NUMA-aware fitting is enabled.
    public boolean isNumaAwareEnabled() {
        return numaAwareEnabled;
    }

    /// Returns the NUMA topology for this system.
    ///
    /// <p>Only meaningful if NUMA-aware fitting is enabled.
    ///
    /// @return detected NUMA topology, or null if not yet initialized
    public io.nosqlbench.vshapes.extract.NumaTopology getNumaTopology() {
        return numaTopology;
    }

    /// Shuts down any thread pools owned by this extractor.
    ///
    /// <p>Should be called when done with the extractor to release resources.
    /// This is automatically called by AnalyzerHarness at the end of processing.
    public void shutdown() {
        if (ownsFittingPool && fittingPool != null && !fittingPool.isShutdown()) {
            fittingPool.shutdown();
        }
        if (numaNodePools != null) {
            for (java.util.concurrent.ForkJoinPool pool : numaNodePools) {
                if (pool != null && !pool.isShutdown()) {
                    pool.shutdown();
                }
            }
        }
    }

    /// Enables or disables adaptive composite fallback.
    ///
    /// When enabled, dimensions that don't fit well with parametric models
    /// will try composite (mixture) models before falling back to empirical.
    ///
    /// @param enabled true to enable adaptive fitting (default: true)
    /// @return this extractor for chaining
    public StreamingModelExtractor setAdaptiveEnabled(boolean enabled) {
        this.adaptiveEnabled = enabled;
        return this;
    }

    /// Sets the maximum number of composite components (2-10).
    ///
    /// @param maxComponents maximum modes for composite fitting
    /// @return this extractor for chaining
    public StreamingModelExtractor setMaxCompositeComponents(int maxComponents) {
        this.maxCompositeComponents = Math.max(2, Math.min(maxComponents, 10));
        return this;
    }

    /// Sets the clustering strategy for composite models.
    ///
    /// @param strategy HARD (fast) or EM (accurate)
    /// @return this extractor for chaining
    public StreamingModelExtractor setClusteringStrategy(ClusteringStrategy strategy) {
        this.clusteringStrategy = strategy;
        return this;
    }

    /// Enables or disables internal verification.
    ///
    /// When enabled, fitted models are verified using mini round-trip testing
    /// to detect parameter instability.
    ///
    /// @param enabled true to enable internal verification
    /// @return this extractor for chaining
    public StreamingModelExtractor setInternalVerificationEnabled(boolean enabled) {
        this.internalVerificationEnabled = enabled;
        return this;
    }

    /// Sets the verification level for internal mini round-trip.
    ///
    /// @param level FAST (500), BALANCED (1000), or THOROUGH (5000) samples
    /// @return this extractor for chaining
    public StreamingModelExtractor setVerificationLevel(VerificationLevel level) {
        this.verificationLevel = level;
        return this;
    }

    /// Sets the KS threshold for parametric models.
    ///
    /// Values above this threshold trigger composite fitting.
    ///
    /// @param threshold KS D-statistic threshold (default: 0.03)
    /// @return this extractor for chaining
    public StreamingModelExtractor setKsThresholdParametric(double threshold) {
        this.ksThresholdParametric = threshold;
        return this;
    }

    /// Sets the KS threshold for composite models.
    ///
    /// Values above this threshold trigger empirical fallback.
    ///
    /// @param threshold KS D-statistic threshold (default: 0.05)
    /// @return this extractor for chaining
    public StreamingModelExtractor setKsThresholdComposite(double threshold) {
        this.ksThresholdComposite = threshold;
        return this;
    }

    /// Enables or disables reservoir sampling for large datasets.
    ///
    /// When enabled, dimension data is sampled using Algorithm R to maintain
    /// a representative random sample without requiring storage proportional
    /// to dataset size.
    ///
    /// @param enabled true to enable reservoir sampling (default: true)
    /// @return this extractor for chaining
    public StreamingModelExtractor setReservoirSamplingEnabled(boolean enabled) {
        this.reservoirSamplingEnabled = enabled;
        return this;
    }

    /// Sets the reservoir size per dimension.
    ///
    /// This determines how many samples are kept for model fitting.
    /// Larger values improve fitting accuracy but use more memory.
    ///
    /// @param size reservoir size per dimension (default: 10,000)
    /// @return this extractor for chaining
    public StreamingModelExtractor setReservoirSize(int size) {
        this.reservoirSize = Math.max(1000, size);  // Minimum 1000 for statistical validity
        return this;
    }

    /// Returns whether reservoir sampling is enabled.
    ///
    /// @return true if reservoir sampling is enabled
    public boolean isReservoirSamplingEnabled() {
        return reservoirSamplingEnabled;
    }

    /// Returns the reservoir size per dimension.
    ///
    /// @return the reservoir size
    public int getReservoirSize() {
        return reservoirSize;
    }

    @Override
    public String getAnalyzerType() {
        return ANALYZER_TYPE;
    }

    @Override
    public String getDescription() {
        return DESCRIPTION;
    }

    @Override
    public void initialize(DataspaceShape shape) {
        this.shape = shape;
        int dims = shape.dimensionality();
        long cardinality = shape.cardinality();

        // Initialize per-dimension accumulators
        accumulators = new StreamingDimensionAccumulator[dims];
        locks = new ReentrantLock[dims];
        for (int d = 0; d < dims; d++) {
            accumulators[d] = new StreamingDimensionAccumulator(d);
            locks[d] = new ReentrantLock();
        }

        // Initialize dimension data storage with memory-aware sizing
        // Storage is used for model fitting - we don't need ALL samples,
        // just enough for statistical fitting (typically 100K-1M is sufficient)
        int storageSize = calculateStorageSize(dims, cardinality);
        dimensionData = new float[dims][storageSize];
        writePositions = new int[dims];

        // CRITICAL: Auto-disable reservoir sampling when dataset fits in storage.
        // The streaming path must produce IDENTICAL results to the non-streaming path -
        // reservoir sampling is only a memory optimization, not an algorithmic compromise.
        // When storage can hold all data, we must use all data for fitting consistency.
        if (reservoirSamplingEnabled && storageSize >= cardinality) {
            // Dataset fits in memory - disable sampling to ensure identical results
            reservoirSamplingEnabled = false;
        } else if (reservoirSamplingEnabled && reservoirSize < storageSize) {
            // If sampling is needed but reservoirSize is smaller than available storage,
            // increase it to use all allocated memory (don't waste storage capacity)
            reservoirSize = storageSize;
        }

        // Initialize histograms for multi-modal detection if enabled
        if (histogramEnabled) {
            histograms = new StreamingHistogram[dims];
            multiModal = new boolean[dims];
            for (int d = 0; d < dims; d++) {
                histograms[d] = new StreamingHistogram(100);  // 100 bins per dimension
            }
        }

        // Initialize adaptive fitting
        if (adaptiveEnabled) {
            dimensionStrategies = new AdaptiveStrategy[dims];
            if (internalVerificationEnabled) {
                internalVerifier = new InternalVerifier(verificationLevel);
            }
            parametricCount.set(0);
            compositeCount.set(0);
            empiricalCount.set(0);
        }

        // Initialize reservoir sampling only if still enabled after auto-disable check
        if (reservoirSamplingEnabled) {
            perDimensionRng = new java.util.Random[dims];
            perDimensionCounts = new long[dims];
            for (int d = 0; d < dims; d++) {
                // Use deterministic seeds for reproducibility (based on dimension index)
                perDimensionRng[d] = new java.util.Random(42L + d);
                perDimensionCounts[d] = 0;
            }
        }

        samplesProcessed.set(0);
    }

    /// Stores dimension values using Algorithm R reservoir sampling.
    ///
    /// Algorithm R (Vitter, 1985):
    /// - Fill the reservoir with the first `reservoirSize` elements
    /// - For the k-th element (k > reservoirSize):
    ///   - Generate random j in [0, k)
    ///   - If j < reservoirSize, replace reservoir[j] with element k
    ///
    /// This ensures uniform random sampling of `reservoirSize` elements from a
    /// stream of any length, using O(reservoirSize) memory.
    ///
    /// @param d the dimension index
    /// @param values the values to sample from
    private void storeWithReservoirSampling(int d, float[] values) {
        int storageCapacity = dimensionData[d].length;
        int currentSize = writePositions[d];
        java.util.Random rng = perDimensionRng[d];

        for (float value : values) {
            perDimensionCounts[d]++;
            long totalSeen = perDimensionCounts[d];

            if (currentSize < storageCapacity) {
                // Phase 1: Fill the reservoir
                dimensionData[d][currentSize] = value;
                currentSize++;
            } else {
                // Phase 2: Reservoir sampling with replacement
                // Replace with probability storageCapacity / totalSeen
                if (rng.nextLong(totalSeen) < storageCapacity) {
                    int replaceIndex = rng.nextInt(storageCapacity);
                    dimensionData[d][replaceIndex] = value;
                }
            }
        }

        writePositions[d] = currentSize;
    }

    /// Calculates a memory-safe storage size for dimension data.
    /// Limits storage based on available heap to avoid OOM.
    ///
    /// @param dims number of dimensions
    /// @param cardinality total vectors in dataset
    /// @return safe storage size per dimension
    private int calculateStorageSize(int dims, long cardinality) {
        // Get available memory (use 40% of max heap for dimension storage)
        Runtime rt = Runtime.getRuntime();
        long maxHeap = rt.maxMemory();
        long availableForStorage = (long) (maxHeap * 0.4);

        // Calculate bytes needed: dims arrays × storageSize × 4 bytes per float
        // Solve for storageSize: storageSize = availableBytes / (dims × 4)
        long maxStoragePerDim = availableForStorage / ((long) dims * Float.BYTES);

        // Cap at reasonable max for model fitting (1M samples is plenty)
        // Also cap at cardinality (don't allocate more than we'll use)
        int maxReasonable = 1_000_000;
        int storageSize = (int) Math.min(maxStoragePerDim, Math.min(cardinality, maxReasonable));

        // Ensure minimum for statistical validity
        storageSize = Math.max(storageSize, 10_000);

        return storageSize;
    }

    @Override
    public void accept(float[][] chunk, long startIndex) {
        if (chunk == null || chunk.length == 0) {
            return;
        }

        int dims = chunk.length;
        int vectors = chunk[0].length;

        // Process each dimension independently (allows parallelism)
        for (int d = 0; d < dims; d++) {
            float[] dimValues = chunk[d];
            locks[d].lock();
            try {
                // Accumulate statistics
                accumulators[d].addAll(dimValues);

                // Update histogram for multi-modal detection
                if (histogramEnabled && histograms != null) {
                    histograms[d].addAll(dimValues);
                }

                // Store data for model fitting using reservoir sampling if enabled
                if (reservoirSamplingEnabled && perDimensionRng != null) {
                    storeWithReservoirSampling(d, dimValues);
                } else {
                    // Simple filling - stop when array is full
                    int pos = writePositions[d];
                    int available = dimensionData[d].length - pos;
                    int toCopy = Math.min(dimValues.length, available);
                    if (toCopy > 0) {
                        System.arraycopy(dimValues, 0, dimensionData[d], pos, toCopy);
                        writePositions[d] = pos + toCopy;
                    }
                }
            } finally {
                locks[d].unlock();
            }
        }

        samplesProcessed.addAndGet(vectors);
    }

    @Override
    public VectorSpaceModel complete() {
        long startTime = System.currentTimeMillis();
        int dims = accumulators.length;
        ScalarModel[] scalarModels = new ScalarModel[dims];

        // Determine unique vectors count for the model
        long modelUniqueVectors = uniqueVectors > 0 ? uniqueVectors : samplesProcessed.get();

        // Initialize storage for statistics and fit results
        storedDimensionStats = new DimensionStatistics[dims];
        storedFitResults = new ComponentModelFitter.FitResult[dims];

        // Initialize all-fits storage if collection is enabled
        java.util.List<String> modelTypes = null;
        double[][] allFitScores = null;
        int[] bestFitIndices = null;
        String[] sparklines = null;

        if (collectAllFits && selector != null) {
            modelTypes = selector.getFitters().stream()
                .map(ComponentModelFitter::getModelType)
                .toList();
            allFitScores = new double[dims][modelTypes.size()];
            bestFitIndices = new int[dims];
            sparklines = new String[dims];
        }

        // Final references for lambda capture
        final double[][] finalAllFitScores = allFitScores;
        final int[] finalBestFitIndices = bestFitIndices;
        final String[] finalSparklines = sparklines;

        // Count multi-modal dimensions
        int multiModalToFit = 0;
        if (histogramEnabled && multiModal != null) {
            for (int d = 0; d < dims; d++) {
                if (multiModal[d]) {
                    multiModalToFit++;
                }
            }
        }

        // Fit all models now that data collection is complete
        int effectiveThreads = getEffectiveParallelism();

        // Initialize NUMA topology if NUMA-aware fitting is enabled
        if (numaAwareEnabled) {
            numaTopology = io.nosqlbench.vshapes.extract.NumaTopology.detect();
            System.out.printf("  Fitting %d dimensions using %d threads across %d NUMA nodes",
                dims, effectiveThreads, numaTopology.nodeCount());
        } else {
            System.out.printf("  Fitting %d dimensions using %d threads", dims, effectiveThreads);
        }
        if (multiModalToFit > 0) {
            System.out.printf(" (%d detected as multi-modal)", multiModalToFit);
        }
        if (collectAllFits) {
            System.out.print(" (collecting all fits)");
        }
        System.out.println("...");

        AtomicInteger fitted = new AtomicInteger(0);
        AtomicInteger usedEmpirical = new AtomicInteger(0);
        AtomicInteger lastReportedPercent = new AtomicInteger(-1);

        // Create custom ForkJoinPool if parallelism is specified or NUMA-aware
        java.util.concurrent.ForkJoinPool pool = null;
        if (numaAwareEnabled && numaTopology.nodeCount() > 1) {
            // For NUMA-aware, create per-node pools and partition dimensions
            fitDimensionsNumaAware(dims, scalarModels, finalAllFitScores, finalBestFitIndices,
                finalSparklines, fitted, lastReportedPercent);
        } else {
            // Use standard parallel fitting
            if (parallelism > 0) {
                pool = new java.util.concurrent.ForkJoinPool(parallelism);
                ownsFittingPool = true;
                fittingPool = pool;
            }

            // Parallel fitting task
            Runnable fittingTask = () -> java.util.stream.IntStream.range(0, dims).parallel().forEach(d -> {
                fitSingleDimension(d, scalarModels, finalAllFitScores, finalBestFitIndices,
                    finalSparklines, fitted, lastReportedPercent, dims);
            });

            // Execute in custom pool or common pool
            if (pool != null) {
                try {
                    pool.submit(fittingTask).get();
                } catch (Exception e) {
                    throw new RuntimeException("Error during parallel fitting", e);
                }
            } else {
                fittingTask.run();
            }
        }

        System.out.println(" done.");

        // Store all-fits data if collected
        if (collectAllFits && modelTypes != null) {
            storedAllFitsData = new ModelExtractor.AllFitsData(
                modelTypes, finalAllFitScores, finalBestFitIndices, finalSparklines);
        }

        // Report adaptive fitting statistics
        if (adaptiveEnabled && dimensionStrategies != null) {
            reportAdaptiveStatistics();
        }

        extractionTimeMs = System.currentTimeMillis() - startTime;

        // Clear storage to free memory
        dimensionData = null;

        return new VectorSpaceModel(modelUniqueVectors, scalarModels);
    }

    /// Fits a single dimension (called from parallel loop).
    private void fitSingleDimension(int d, ScalarModel[] scalarModels,
            double[][] finalAllFitScores, int[] finalBestFitIndices, String[] finalSparklines,
            AtomicInteger fitted, AtomicInteger lastReportedPercent, int dims) {

        DimensionStatistics stats = accumulators[d].toStatistics();
        storedDimensionStats[d] = stats;

        // Get stored data for this dimension (up to what we've written)
        int dataLength = writePositions[d];
        float[] dimValues;
        if (dataLength < dimensionData[d].length) {
            dimValues = new float[dataLength];
            System.arraycopy(dimensionData[d], 0, dimValues, 0, dataLength);
        } else {
            dimValues = dimensionData[d];
        }

        // Use adaptive fitting if enabled, otherwise simple parametric
        ScalarModel model;
        ComponentModelFitter.FitResult fitResult;

        if (collectAllFits && !adaptiveEnabled) {
            // Collect all fit scores for diagnostic table
            BestFitSelector.SelectionWithAllFits selection =
                selector.selectBestWithAllFits(stats, dimValues);
            fitResult = selection.bestFit();
            model = fitResult.model();
            finalAllFitScores[d] = selection.allScores();
            finalBestFitIndices[d] = selection.bestIndex();
            finalSparklines[d] = Sparkline.generate(dimValues, Sparkline.DEFAULT_WIDTH);
        } else if (adaptiveEnabled) {
            model = fitDimensionAdaptive(d, stats, dimValues);
            // Create a fit result for the adaptive model
            String modelTypeName = model.getClass().getSimpleName().replace("ScalarModel", "").toLowerCase();
            fitResult = new ComponentModelFitter.FitResult(model, 0.0, modelTypeName);
        } else {
            fitResult = selector.selectBestResult(stats, dimValues);
            model = fitResult.model();
        }

        scalarModels[d] = model;
        storedFitResults[d] = fitResult;

        int count = fitted.incrementAndGet();
        int percent = count * 100 / dims;
        int lastPercent = lastReportedPercent.get();
        if (percent >= lastPercent + 5 || count == dims) {
            if (lastReportedPercent.compareAndSet(lastPercent, percent)) {
                System.out.printf("\r  Fitting models: %d/%d dimensions (%d%%)...", count, dims, percent);
                System.out.flush();
            }
        }
    }

    /// Fits dimensions using NUMA-aware partitioning.
    private void fitDimensionsNumaAware(int dims, ScalarModel[] scalarModels,
            double[][] finalAllFitScores, int[] finalBestFitIndices, String[] finalSparklines,
            AtomicInteger fitted, AtomicInteger lastReportedPercent) {

        int nodeCount = numaTopology.nodeCount();
        int threadsPerNode = Math.max(1, getEffectiveParallelism() / nodeCount);
        int dimsPerNode = (dims + nodeCount - 1) / nodeCount;

        // Create per-node pools with NUMA thread binding
        numaNodePools = new java.util.concurrent.ForkJoinPool[nodeCount];
        for (int node = 0; node < nodeCount; node++) {
            final int nodeId = node;
            numaNodePools[node] = new java.util.concurrent.ForkJoinPool(threadsPerNode,
                pool -> {
                    java.util.concurrent.ForkJoinWorkerThread worker =
                        new java.util.concurrent.ForkJoinWorkerThread(pool) {
                            @Override
                            protected void onStart() {
                                super.onStart();
                                io.nosqlbench.vshapes.extract.NumaBinding.bindThreadToNode(nodeId);
                            }
                        };
                    worker.setName("numa-" + nodeId + "-worker-" + worker.getPoolIndex());
                    return worker;
                }, null, false);
        }

        // Submit dimension partitions to each node
        @SuppressWarnings("unchecked")
        java.util.concurrent.Future<?>[] futures = new java.util.concurrent.Future[nodeCount];

        for (int node = 0; node < nodeCount; node++) {
            final int startDim = node * dimsPerNode;
            final int endDim = Math.min(startDim + dimsPerNode, dims);

            if (startDim >= dims) {
                break;
            }

            futures[node] = numaNodePools[node].submit(() -> {
                for (int d = startDim; d < endDim; d++) {
                    fitSingleDimension(d, scalarModels, finalAllFitScores, finalBestFitIndices,
                        finalSparklines, fitted, lastReportedPercent, dims);
                }
            });
        }

        // Wait for all nodes to complete
        for (int node = 0; node < nodeCount && futures[node] != null; node++) {
            try {
                futures[node].get();
            } catch (Exception e) {
                throw new RuntimeException("NUMA fitting failed on node " + node, e);
            }
        }
    }

    /// Reports adaptive fitting statistics after completion.
    private void reportAdaptiveStatistics() {
        // Count strategies used
        int parametric = 0, empirical = 0;
        int[] compositeByModes = new int[11];  // Index 2-10 for composite modes
        for (AdaptiveStrategy strategy : dimensionStrategies) {
            if (strategy == null) continue;
            switch (strategy) {
                case PARAMETRIC -> parametric++;
                case COMPOSITE_2 -> compositeByModes[2]++;
                case COMPOSITE_3 -> compositeByModes[3]++;
                case COMPOSITE_4 -> compositeByModes[4]++;
                case COMPOSITE_5 -> compositeByModes[5]++;
                case COMPOSITE_6 -> compositeByModes[6]++;
                case COMPOSITE_7 -> compositeByModes[7]++;
                case COMPOSITE_8 -> compositeByModes[8]++;
                case COMPOSITE_9 -> compositeByModes[9]++;
                case COMPOSITE_10 -> compositeByModes[10]++;
                case EMPIRICAL -> empirical++;
            }
        }

        System.out.println("\n  Adaptive fitting strategies used:");
        if (parametric > 0) System.out.printf("    Parametric: %d%n", parametric);
        for (int modes = 2; modes <= 10; modes++) {
            if (compositeByModes[modes] > 0) {
                System.out.printf("    Composite (%d modes): %d%n", modes, compositeByModes[modes]);
            }
        }
        if (empirical > 0) System.out.printf("    Empirical: %d%n", empirical);
    }

    /// Completes extraction and returns detailed statistics.
    ///
    /// This method provides the same functionality as [complete] but additionally
    /// returns per-dimension statistics, fit results, extraction timing, and
    /// optionally all-fits data (if [withAllFitsCollection] was used).
    ///
    /// This is the preferred method when you need diagnostic information for
    /// the `--show-fit-table` option or similar analysis tools.
    ///
    /// @return an ExtractionResult containing the model and all statistics
    public ModelExtractor.ExtractionResult completeWithStats() {
        VectorSpaceModel model = complete();
        return new ModelExtractor.ExtractionResult(
            model,
            storedDimensionStats,
            storedFitResults,
            extractionTimeMs,
            storedAllFitsData
        );
    }

    /// Returns the stored dimension statistics from the last extraction.
    ///
    /// This is populated after [complete] or [completeWithStats] is called.
    ///
    /// @return array of per-dimension statistics, or null if not yet extracted
    public DimensionStatistics[] getDimensionStatistics() {
        return storedDimensionStats;
    }

    /// Returns the stored fit results from the last extraction.
    ///
    /// This is populated after [complete] or [completeWithStats] is called.
    ///
    /// @return array of per-dimension fit results, or null if not yet extracted
    public ComponentModelFitter.FitResult[] getFitResults() {
        return storedFitResults;
    }

    /// Returns the stored all-fits data from the last extraction.
    ///
    /// This is only populated if [withAllFitsCollection] was used and
    /// [complete] or [completeWithStats] has been called.
    ///
    /// @return all-fits data for diagnostic tables, or null if not collected
    public ModelExtractor.AllFitsData getAllFitsData() {
        return storedAllFitsData;
    }

    /// Returns the extraction time from the last [complete] call.
    ///
    /// @return extraction time in milliseconds, or 0 if not yet extracted
    public long getExtractionTimeMs() {
        return extractionTimeMs;
    }

    @Override
    public long estimatedMemoryBytes() {
        if (shape == null) {
            return 0;
        }
        // Estimate: dimension data arrays + accumulators
        long dataBytes = (long) shape.dimensionality() * shape.cardinality() * Float.BYTES;
        long accumulatorBytes = (long) shape.dimensionality() * 100; // ~100 bytes per accumulator
        return dataBytes + accumulatorBytes;
    }

    /// Returns the number of samples processed so far.
    ///
    /// @return count of vectors processed
    public long getSamplesProcessed() {
        return samplesProcessed.get();
    }

    /// Returns current progress as a fraction in [0.0, 1.0].
    ///
    /// @return progress fraction, or 0 if shape is unknown
    public double getProgress() {
        if (shape == null || shape.cardinality() == 0) {
            return 0.0;
        }
        return (double) samplesProcessed.get() / shape.cardinality();
    }

    /// Returns the configured selector.
    ///
    /// @return the best-fit selector
    public BestFitSelector getSelector() {
        return selector;
    }

    // ========== Convergence Tracking Methods ==========

    /// Enables convergence tracking with the specified threshold.
    ///
    /// When enabled, the extractor tracks whether per-dimension statistics
    /// have stabilized between batches. This can be used for early stopping
    /// or progress reporting.
    ///
    /// @param threshold relative change threshold (e.g., 0.01 for 1%)
    /// @return this extractor for chaining
    public StreamingModelExtractor enableConvergenceTracking(double threshold) {
        this.convergenceEnabled = true;
        this.convergenceThreshold = threshold;
        return this;
    }

    /// Enables incremental model fitting during streaming.
    ///
    /// When enabled, models are fitted periodically during streaming (not just at the end).
    /// This allows tracking of model type changes and provides faster completion when
    /// convergence is reached (models are already fitted).
    ///
    /// @param fitInterval how often to fit models (in batches, e.g., 5 = every 5 batches)
    /// @return this extractor for chaining
    public StreamingModelExtractor enableIncrementalFitting(int fitInterval) {
        this.incrementalFittingEnabled = true;
        this.fitInterval = Math.max(1, fitInterval);
        return this;
    }

    /// Enables histogram-based multi-modal detection.
    ///
    /// When enabled, the extractor maintains per-dimension histograms to detect
    /// multi-modal distributions. Dimensions detected as multi-modal will use
    /// empirical models instead of parametric fits.
    ///
    /// @param prominenceThreshold minimum peak prominence (0.0 to 1.0, e.g., 0.1 = 10%)
    /// @return this extractor for chaining
    public StreamingModelExtractor enableHistogramTracking(double prominenceThreshold) {
        this.histogramEnabled = true;
        this.prominenceThreshold = Math.max(0.01, Math.min(1.0, prominenceThreshold));
        return this;
    }

    /// Checks convergence after processing a batch.
    ///
    /// Should be called after [accept] to update convergence status.
    /// This uses standard error (SE) based convergence detection for each moment:
    /// - SE(mean) = stdDev / sqrt(n)
    /// - SE(variance) = variance * sqrt(2/n)
    /// - SE(skewness) = sqrt(6/n)
    /// - SE(kurtosis) = sqrt(24/n)
    ///
    /// A moment converges when its change is less than threshold × SE.
    public void checkConvergence() {
        if (!convergenceEnabled || accumulators == null) {
            return;
        }

        int dims = accumulators.length;
        long n = samplesProcessed.get();

        // Don't check convergence until we have enough samples
        if (n < MINIMUM_SAMPLES_FOR_CONVERGENCE) {
            return;
        }

        // Initialize on first call
        if (prevMeans == null) {
            prevMeans = new double[dims];
            prevVariances = new double[dims];
            prevSkewness = new double[dims];
            prevKurtosis = new double[dims];
            meanConverged = new boolean[dims];
            varianceConverged = new boolean[dims];
            skewnessConverged = new boolean[dims];
            kurtosisConverged = new boolean[dims];
            converged = new boolean[dims];

            if (incrementalFittingEnabled) {
                currentModelTypes = new String[dims];
                currentModels = new ScalarModel[dims];
            }

            // Store current values as baseline
            for (int d = 0; d < dims; d++) {
                prevMeans[d] = accumulators[d].getMean();
                prevVariances[d] = accumulators[d].getVariance();
                prevSkewness[d] = accumulators[d].getSkewness();
                prevKurtosis[d] = accumulators[d].getKurtosis();
            }
            batchesProcessed = 1;
            lastConvergenceCheckSamples = n;
            return;
        }

        batchesProcessed++;
        int newlyConverged = 0;

        // Precompute SE factors based on sample count
        double sqrtN = Math.sqrt(n);
        double seSkewnessFactor = Math.sqrt(6.0 / n);
        double seKurtosisFactor = Math.sqrt(24.0 / n);

        for (int d = 0; d < dims; d++) {
            if (converged[d]) {
                continue;  // Already converged
            }

            double currentMean = accumulators[d].getMean();
            double currentVariance = accumulators[d].getVariance();
            double currentStdDev = accumulators[d].getStdDev();
            double currentSkewness = accumulators[d].getSkewness();
            double currentKurtosis = accumulators[d].getKurtosis();

            // SE-based convergence: change < threshold × SE

            // Mean: SE(mean) = stdDev / sqrt(n)
            if (!meanConverged[d]) {
                double seMean = currentStdDev / sqrtN;
                double meanChange = Math.abs(currentMean - prevMeans[d]);
                meanConverged[d] = meanChange < convergenceThreshold * seMean;
            }

            // Variance: SE(variance) = variance * sqrt(2/n)
            if (!varianceConverged[d]) {
                double seVariance = currentVariance * Math.sqrt(2.0 / n);
                double varChange = Math.abs(currentVariance - prevVariances[d]);
                varianceConverged[d] = varChange < convergenceThreshold * seVariance;
            }

            // Skewness: SE(skewness) = sqrt(6/n)
            if (!skewnessConverged[d]) {
                double seSkewness = seSkewnessFactor;
                double skewChange = Math.abs(currentSkewness - prevSkewness[d]);
                skewnessConverged[d] = skewChange < convergenceThreshold * seSkewness;
            }

            // Kurtosis: SE(kurtosis) = sqrt(24/n)
            if (!kurtosisConverged[d]) {
                double seKurtosis = seKurtosisFactor;
                double kurtChange = Math.abs(currentKurtosis - prevKurtosis[d]);
                kurtosisConverged[d] = kurtChange < convergenceThreshold * seKurtosis;
            }

            // All four moments must converge
            if (meanConverged[d] && varianceConverged[d] && skewnessConverged[d] && kurtosisConverged[d]) {
                converged[d] = true;
                newlyConverged++;
            }

            // Check for multi-modality using histograms
            if (histogramEnabled && histograms != null && !multiModal[d]) {
                if (histograms[d].isMultiModal(prominenceThreshold)) {
                    multiModal[d] = true;
                    multiModalCount++;
                }
            }

            // Update previous values
            prevMeans[d] = currentMean;
            prevVariances[d] = currentVariance;
            prevSkewness[d] = currentSkewness;
            prevKurtosis[d] = currentKurtosis;
        }

        if (newlyConverged > 0) {
            convergedCount.addAndGet(newlyConverged);
        }

        lastConvergenceCheckSamples = n;
    }

    /// Returns whether processing should stop early due to convergence.
    ///
    /// This returns true when:
    /// - Convergence tracking is enabled
    /// - Early stopping is enabled
    /// - All dimensions have converged
    /// - Minimum samples threshold has been reached
    ///
    /// The AnalyzerHarness can poll this method to stop chunk processing early.
    ///
    /// @return true if early stopping should occur
    public boolean shouldStopEarly() {
        return convergenceEnabled
            && earlyStoppingEnabled
            && samplesProcessed.get() >= MINIMUM_SAMPLES_FOR_CONVERGENCE
            && allConverged();
    }

    /// Enables or disables early stopping when all dimensions converge.
    ///
    /// @param enabled true to enable early stopping (default: true)
    /// @return this extractor for chaining
    public StreamingModelExtractor setEarlyStoppingEnabled(boolean enabled) {
        this.earlyStoppingEnabled = enabled;
        return this;
    }

    /// Returns whether early stopping is enabled.
    ///
    /// @return true if early stopping is enabled
    public boolean isEarlyStoppingEnabled() {
        return earlyStoppingEnabled;
    }

    /// Fits a model for a single dimension and tracks type changes.
    private void fitDimensionModel(int d) {
        DimensionStatistics stats = accumulators[d].toStatistics();

        // Get stored data for this dimension
        int dataLength = writePositions[d];
        float[] dimValues;
        if (dataLength < dimensionData[d].length) {
            dimValues = new float[dataLength];
            System.arraycopy(dimensionData[d], 0, dimValues, 0, dataLength);
        } else {
            dimValues = dimensionData[d];
        }

        // Fit the best model and wrap as composite for unified handling
        ComponentModelFitter.FitResult result = selector.selectBestResult(stats, dimValues);
        ScalarModel rawModel = result.model();
        ScalarModel newModel = CompositeScalarModel.wrap(rawModel);
        // Track type based on the effective type (underlying model for simple composites)
        String newType = rawModel.getClass().getSimpleName();

        // Check for model type change
        if (currentModelTypes[d] != null && !currentModelTypes[d].equals(newType)) {
            modelTypeChanges++;
        }

        currentModelTypes[d] = newType;
        currentModels[d] = newModel;
    }

    /// Returns whether convergence tracking is enabled.
    ///
    /// @return true if convergence tracking is enabled
    public boolean isConvergenceEnabled() {
        return convergenceEnabled;
    }

    /// Returns the number of converged dimensions.
    ///
    /// @return count of converged dimensions
    public int getConvergedCount() {
        return convergedCount.get();
    }

    /// Returns whether all dimensions have converged.
    ///
    /// @return true if all dimensions are converged
    public boolean allConverged() {
        return accumulators != null && convergedCount.get() >= accumulators.length;
    }

    /// Returns the convergence rate (fraction of dimensions converged).
    ///
    /// @return convergence rate from 0.0 to 1.0
    public double getConvergenceRate() {
        if (accumulators == null || accumulators.length == 0) {
            return 0.0;
        }
        return (double) convergedCount.get() / accumulators.length;
    }

    /// Returns the configured convergence threshold.
    ///
    /// @return the convergence threshold
    public double getConvergenceThreshold() {
        return convergenceThreshold;
    }

    /// Returns the number of batches processed.
    ///
    /// @return batch count
    public int getBatchesProcessed() {
        return batchesProcessed;
    }

    /// Returns the number of model type changes detected during incremental fitting.
    ///
    /// @return count of model type changes
    public int getModelTypeChanges() {
        return modelTypeChanges;
    }

    /// Returns whether incremental fitting is enabled.
    ///
    /// @return true if incremental fitting is enabled
    public boolean isIncrementalFittingEnabled() {
        return incrementalFittingEnabled;
    }

    /// Returns the current model types per dimension (if incremental fitting is enabled).
    ///
    /// @return array of model type names, or null if not available
    public String[] getCurrentModelTypes() {
        return currentModelTypes;
    }

    /// Returns whether histogram tracking is enabled.
    ///
    /// @return true if histogram tracking is enabled
    public boolean isHistogramEnabled() {
        return histogramEnabled;
    }

    /// Returns the number of dimensions detected as multi-modal.
    ///
    /// @return count of multi-modal dimensions
    public int getMultiModalCount() {
        return multiModalCount;
    }

    /// Returns whether a specific dimension is multi-modal.
    ///
    /// @param dimension the dimension index
    /// @return true if the dimension is detected as multi-modal
    public boolean isMultiModal(int dimension) {
        return multiModal != null && dimension >= 0 && dimension < multiModal.length && multiModal[dimension];
    }

    /// Returns the array of multi-modal flags per dimension.
    ///
    /// @return array of multi-modal flags, or null if not tracking
    public boolean[] getMultiModalDimensions() {
        return multiModal;
    }

    /// Returns a summary of the current convergence status.
    ///
    /// @return convergence status summary
    public ConvergenceStatus getConvergenceStatus() {
        if (!convergenceEnabled || accumulators == null) {
            return null;
        }

        return new ConvergenceStatus(
            accumulators.length,
            convergedCount.get(),
            batchesProcessed,
            samplesProcessed.get(),
            convergenceThreshold,
            modelTypeChanges,
            incrementalFittingEnabled,
            multiModalCount,
            histogramEnabled
        );
    }

    /// Finds the fitter that matches the given model type.
    ///
    /// @param modelType the model type string (e.g., "normal", "beta")
    /// @return the matching fitter, or null if not found
    private ComponentModelFitter findFitterForModelType(String modelType) {
        if (modelType == null || selector == null) {
            return null;
        }
        for (ComponentModelFitter fitter : selector.getFitters()) {
            if (modelType.equals(fitter.getModelType())) {
                return fitter;
            }
        }
        return null;
    }

    /// Fits a model for a dimension using adaptive fallback chain.
    ///
    /// The adaptive chain tries increasingly flexible model types:
    /// 1. **Parametric**: Best-fit from BestFitSelector (Normal, Uniform, etc.)
    /// 2. **Internal verification**: Optional mini round-trip to detect instability
    /// 3. **Composite**: Mixture models with 2-4 components for multimodal data
    /// 4. **Empirical**: Histogram-based fallback for complex distributions
    ///
    /// @param dimension the dimension index being fitted
    /// @param stats the computed statistics for this dimension
    /// @param data the sample data for model fitting
    /// @return the best-fit model for this dimension
    private ScalarModel fitDimensionAdaptive(int dimension, DimensionStatistics stats, float[] data) {
        // Step 0: Check for multimodality FIRST - this takes priority over parametric fitting
        // because parametric models can achieve low KS on multimodal data while being
        // structurally wrong (e.g., Beta fitting aggregate shape of trimodal distribution)
        boolean isMultimodal = histogramEnabled && histograms != null && histograms[dimension].isMultiModal(prominenceThreshold);
        boolean hasGaps = histogramEnabled && histograms != null && histograms[dimension].hasSignificantGaps(prominenceThreshold);

        // Step 1: Try parametric fit
        ComponentModelFitter.FitResult parametricResult = selector.selectBestResult(stats, data);
        double parametricKS = parametricResult.goodnessOfFit();

        // Good parametric fit - but only accept if NOT multimodal
        // Multimodal data should try composite fitting even if parametric KS is good
        if (parametricKS <= ksThresholdParametric && !isMultimodal && !hasGaps) {
            dimensionStrategies[dimension] = AdaptiveStrategy.PARAMETRIC;
            parametricCount.incrementAndGet();
            return CompositeScalarModel.wrap(parametricResult.model());
        }

        // Step 2: Internal verification (optional) - can rescue borderline parametric
        // But NOT for multimodal data - always try composite for multimodal
        if (!isMultimodal && !hasGaps && internalVerificationEnabled && internalVerifier != null) {
            // Find the fitter that matches the best result's model type
            ComponentModelFitter bestFitter = findFitterForModelType(parametricResult.modelType());
            if (bestFitter != null) {
                InternalVerifier.VerificationResult vr = internalVerifier.verify(parametricResult.model(), data, bestFitter);
                if (vr.passed()) {
                    // Verification passed - parametric is stable enough, wrap as composite
                    dimensionStrategies[dimension] = AdaptiveStrategy.PARAMETRIC;
                    parametricCount.incrementAndGet();
                    return CompositeScalarModel.wrap(parametricResult.model());
                }
            }
        }

        // Step 3: Try composite (mixture) models if multimodal detected or parametric failed
        StreamingHistogram.GapAnalysis gapAnalysis = null;
        if (hasGaps) {
            gapAnalysis = histograms[dimension].analyzeGaps(prominenceThreshold);
        }

        if (isMultimodal || hasGaps || parametricKS > ksThresholdParametric * 1.5) {
            // Hint at number of modes from histogram
            int suggestedModes = 2;
            if (histograms != null) {
                List<StreamingHistogram.Mode> modes = histograms[dimension].findModes(prominenceThreshold);
                suggestedModes = Math.max(modes.size(), 2);

                // For gap-detected distributions, number of modes = gaps + 1
                if (gapAnalysis != null && gapAnalysis.hasGaps()) {
                    int gapBasedModes = gapAnalysis.gaps().size() + 1;
                    suggestedModes = Math.max(suggestedModes, gapBasedModes);
                }

                suggestedModes = Math.min(suggestedModes, maxCompositeComponents);
            }

            // For gap-detected distributions, use a relaxed threshold since the
            // discontinuous nature makes fitting harder
            double effectiveThreshold = hasGaps ? ksThresholdComposite * 1.5 : ksThresholdComposite;

            // Try composite models with all mode counts from 2 to max
            // We iterate over ALL mode counts (not just from suggestedModes upward) because:
            // 1. The suggested mode count might fail (ModeDetector doesn't confirm multimodality)
            // 2. A simpler composite (fewer modes) might fit better than a complex one
            // 3. When maxCompositeComponents equals suggestedModes, we'd only try one fit
            ComponentModelFitter.FitResult bestComposite = null;
            int bestNumModes = 0;
            double bestScore = Double.MAX_VALUE;

            for (int numModes = 2; numModes <= maxCompositeComponents; numModes++) {
                try {
                    CompositeModelFitter compositeFitter = new CompositeModelFitter(
                        selector, numModes, effectiveThreshold, clusteringStrategy);
                    ComponentModelFitter.FitResult compositeResult = compositeFitter.fit(stats, data);

                    // Track the best composite fit even if it doesn't meet threshold
                    if (compositeResult.goodnessOfFit() < bestScore) {
                        bestScore = compositeResult.goodnessOfFit();
                        bestComposite = compositeResult;
                        bestNumModes = numModes;
                    }

                    if (compositeResult.goodnessOfFit() <= effectiveThreshold) {
                        // Good composite fit
                        dimensionStrategies[dimension] = switch (numModes) {
                            case 2 -> AdaptiveStrategy.COMPOSITE_2;
                            case 3 -> AdaptiveStrategy.COMPOSITE_3;
                            case 4 -> AdaptiveStrategy.COMPOSITE_4;
                            case 5 -> AdaptiveStrategy.COMPOSITE_5;
                            case 6 -> AdaptiveStrategy.COMPOSITE_6;
                            case 7 -> AdaptiveStrategy.COMPOSITE_7;
                            case 8 -> AdaptiveStrategy.COMPOSITE_8;
                            case 9 -> AdaptiveStrategy.COMPOSITE_9;
                            case 10 -> AdaptiveStrategy.COMPOSITE_10;
                            default -> AdaptiveStrategy.COMPOSITE_10;
                        };
                        compositeCount.incrementAndGet();
                        // CompositeModelFitter already returns CompositeScalarModel, wrap ensures consistency
                        return CompositeScalarModel.wrap(compositeResult.model());
                    }
                } catch (Exception e) {
                    // Composite fitting can fail for edge cases - continue to next option
                }
            }

            // For gap-detected distributions, prefer composite over parametric if
            // composite score is better, even if it doesn't meet threshold
            if (hasGaps && bestComposite != null && bestScore < parametricKS) {
                dimensionStrategies[dimension] = switch (bestNumModes) {
                    case 2 -> AdaptiveStrategy.COMPOSITE_2;
                    case 3 -> AdaptiveStrategy.COMPOSITE_3;
                    case 4 -> AdaptiveStrategy.COMPOSITE_4;
                    case 5 -> AdaptiveStrategy.COMPOSITE_5;
                    case 6 -> AdaptiveStrategy.COMPOSITE_6;
                    case 7 -> AdaptiveStrategy.COMPOSITE_7;
                    case 8 -> AdaptiveStrategy.COMPOSITE_8;
                    case 9 -> AdaptiveStrategy.COMPOSITE_9;
                    case 10 -> AdaptiveStrategy.COMPOSITE_10;
                    default -> AdaptiveStrategy.COMPOSITE_10;
                };
                compositeCount.incrementAndGet();
                return CompositeScalarModel.wrap(bestComposite.model());
            }
        }

        // Step 4: Compare parametric vs empirical with penalty before falling back
        // The parametric model might still be better than empirical even if it didn't
        // pass the tight threshold - we should only use empirical if it truly scores better.
        EmpiricalModelFitter empiricalFitter = new EmpiricalModelFitter();
        ComponentModelFitter.FitResult empiricalResult = empiricalFitter.fit(stats, data);

        // Apply empirical penalty (consistent with BestFitSelector default behavior)
        // This prevents empirical from winning just because of tight parametric threshold
        double empiricalPenalty = 0.15;  // Same as pearsonMultimodalSelector
        double empiricalScoreWithPenalty = empiricalResult.goodnessOfFit() + empiricalPenalty;

        // If parametric score is better than penalized empirical, use parametric
        if (parametricKS <= empiricalScoreWithPenalty) {
            dimensionStrategies[dimension] = AdaptiveStrategy.PARAMETRIC;
            parametricCount.incrementAndGet();
            return CompositeScalarModel.wrap(parametricResult.model());
        }

        dimensionStrategies[dimension] = AdaptiveStrategy.EMPIRICAL;
        empiricalCount.incrementAndGet();
        return CompositeScalarModel.wrap(empiricalResult.model());
    }

    /// Returns the strategy used for a specific dimension during adaptive fitting.
    ///
    /// @param dimension the dimension index
    /// @return the strategy used, or null if not tracked or not adaptive
    public AdaptiveStrategy getDimensionStrategy(int dimension) {
        if (dimensionStrategies == null || dimension < 0 || dimension >= dimensionStrategies.length) {
            return null;
        }
        return dimensionStrategies[dimension];
    }

    /// Returns whether adaptive fitting is enabled.
    ///
    /// @return true if adaptive fitting is enabled
    public boolean isAdaptiveEnabled() {
        return adaptiveEnabled;
    }

    /// Returns the strategy counts for all dimensions.
    ///
    /// @return record with counts per strategy
    public AdaptiveStrategyCounts getStrategyCounts() {
        return new AdaptiveStrategyCounts(
            parametricCount.get(),
            compositeCount.get(),
            empiricalCount.get()
        );
    }

    /// Counts of dimensions using each adaptive strategy.
    ///
    /// @param parametric count using parametric models
    /// @param composite count using composite (mixture) models
    /// @param empirical count using empirical (histogram) models
    public record AdaptiveStrategyCounts(int parametric, int composite, int empirical) {
        /// Returns total dimensions fitted.
        public int total() {
            return parametric + composite + empirical;
        }

        @Override
        public String toString() {
            return String.format("Strategies: %d parametric, %d composite, %d empirical",
                parametric, composite, empirical);
        }
    }

    /// Summary of convergence status.
    ///
    /// @param totalDimensions total number of dimensions
    /// @param convergedDimensions number of converged dimensions
    /// @param batchesProcessed number of batches processed
    /// @param samplesProcessed total samples processed
    /// @param threshold the convergence threshold used
    /// @param modelTypeChanges number of model type changes detected
    /// @param incrementalFittingEnabled whether incremental fitting is enabled
    /// @param multiModalDimensions number of dimensions detected as multi-modal
    /// @param histogramEnabled whether histogram tracking is enabled
    public record ConvergenceStatus(
        int totalDimensions,
        int convergedDimensions,
        int batchesProcessed,
        long samplesProcessed,
        double threshold,
        int modelTypeChanges,
        boolean incrementalFittingEnabled,
        int multiModalDimensions,
        boolean histogramEnabled
    ) {
        /// Returns the convergence rate (fraction of dimensions converged).
        public double convergenceRate() {
            return totalDimensions > 0 ? (double) convergedDimensions / totalDimensions : 0;
        }

        /// Returns true if all dimensions have converged.
        public boolean allConverged() {
            return convergedDimensions >= totalDimensions;
        }

        @Override
        public String toString() {
            String base = String.format(
                "Convergence: %d/%d dims (%.1f%%) after %d batches, %,d samples (threshold=%.2f%%)",
                convergedDimensions, totalDimensions, convergenceRate() * 100,
                batchesProcessed, samplesProcessed, threshold * 100);
            if (incrementalFittingEnabled && modelTypeChanges > 0) {
                base += String.format(" [%d type changes]", modelTypeChanges);
            }
            if (histogramEnabled && multiModalDimensions > 0) {
                base += String.format(" [%d multi-modal]", multiModalDimensions);
            }
            return base;
        }
    }

    // ========== Builder Pattern ==========

    /// Creates a new builder for fluent configuration.
    ///
    /// @return a new Builder instance
    public static Builder builder() {
        return new Builder();
    }

    /// Builder for fluent configuration of StreamingModelExtractor.
    ///
    /// ## Usage
    ///
    /// ```java
    /// StreamingModelExtractor extractor = StreamingModelExtractor.builder()
    ///     .selector(BestFitSelector.pearsonWithEmpirical())
    ///     .convergenceEnabled(true)
    ///     .convergenceThreshold(0.005)
    ///     .earlyStoppingEnabled(true)
    ///     .adaptiveEnabled(true)
    ///     .ksThresholdParametric(0.03)
    ///     .ksThresholdComposite(0.05)
    ///     .maxCompositeComponents(4)
    ///     .internalVerificationEnabled(true)
    ///     .reservoirSamplingEnabled(true)
    ///     .reservoirSize(10_000)
    ///     .histogramEnabled(true)
    ///     .prominenceThreshold(0.1)
    ///     .build();
    /// ```
    public static final class Builder {

        private BestFitSelector selector = BestFitSelector.boundedDataSelector();
        private long uniqueVectors = 0;

        // Convergence settings
        private boolean convergenceEnabled = false;
        private double convergenceThreshold = 0.01;
        private boolean earlyStoppingEnabled = true;

        // Incremental fitting settings
        private boolean incrementalFittingEnabled = false;
        private int fitInterval = 5;

        // Histogram settings
        private boolean histogramEnabled = false;
        private double prominenceThreshold = 0.1;

        // Adaptive settings
        private boolean adaptiveEnabled = true;
        private double ksThresholdParametric = 0.03;
        private double ksThresholdComposite = 0.05;
        private int maxCompositeComponents = 10;
        private ClusteringStrategy clusteringStrategy = ClusteringStrategy.EM;
        private boolean internalVerificationEnabled = true;
        private VerificationLevel verificationLevel = VerificationLevel.BALANCED;

        // Reservoir sampling settings
        private boolean reservoirSamplingEnabled = true;
        private int reservoirSize = 10_000;

        /// Sets the best-fit selector.
        public Builder selector(BestFitSelector selector) {
            this.selector = selector;
            return this;
        }

        /// Sets the unique vectors count.
        public Builder uniqueVectors(long uniqueVectors) {
            this.uniqueVectors = uniqueVectors;
            return this;
        }

        /// Enables convergence tracking with the specified threshold.
        public Builder convergenceEnabled(boolean enabled) {
            this.convergenceEnabled = enabled;
            return this;
        }

        /// Sets the convergence threshold (fraction of standard error).
        public Builder convergenceThreshold(double threshold) {
            this.convergenceThreshold = threshold;
            return this;
        }

        /// Enables or disables early stopping.
        public Builder earlyStoppingEnabled(boolean enabled) {
            this.earlyStoppingEnabled = enabled;
            return this;
        }

        /// Enables incremental model fitting.
        public Builder incrementalFittingEnabled(boolean enabled) {
            this.incrementalFittingEnabled = enabled;
            return this;
        }

        /// Sets the fit interval for incremental fitting.
        public Builder fitInterval(int interval) {
            this.fitInterval = interval;
            return this;
        }

        /// Enables histogram tracking for multimodal detection.
        public Builder histogramEnabled(boolean enabled) {
            this.histogramEnabled = enabled;
            return this;
        }

        /// Sets the prominence threshold for peak detection.
        public Builder prominenceThreshold(double threshold) {
            this.prominenceThreshold = threshold;
            return this;
        }

        /// Enables adaptive composite fallback.
        public Builder adaptiveEnabled(boolean enabled) {
            this.adaptiveEnabled = enabled;
            return this;
        }

        /// Sets the KS threshold for parametric models.
        public Builder ksThresholdParametric(double threshold) {
            this.ksThresholdParametric = threshold;
            return this;
        }

        /// Sets the KS threshold for composite models.
        public Builder ksThresholdComposite(double threshold) {
            this.ksThresholdComposite = threshold;
            return this;
        }

        /// Sets the maximum number of composite components.
        public Builder maxCompositeComponents(int max) {
            this.maxCompositeComponents = max;
            return this;
        }

        /// Sets the clustering strategy for composite models.
        public Builder clusteringStrategy(ClusteringStrategy strategy) {
            this.clusteringStrategy = strategy;
            return this;
        }

        /// Enables internal verification.
        public Builder internalVerificationEnabled(boolean enabled) {
            this.internalVerificationEnabled = enabled;
            return this;
        }

        /// Sets the verification level.
        public Builder verificationLevel(VerificationLevel level) {
            this.verificationLevel = level;
            return this;
        }

        /// Enables reservoir sampling.
        public Builder reservoirSamplingEnabled(boolean enabled) {
            this.reservoirSamplingEnabled = enabled;
            return this;
        }

        /// Sets the reservoir size per dimension.
        public Builder reservoirSize(int size) {
            this.reservoirSize = size;
            return this;
        }

        /// Builds the configured StreamingModelExtractor.
        ///
        /// @return a new StreamingModelExtractor with the specified configuration
        public StreamingModelExtractor build() {
            StreamingModelExtractor extractor = new StreamingModelExtractor(selector);
            extractor.setUniqueVectors(uniqueVectors);

            // Convergence
            if (convergenceEnabled) {
                extractor.enableConvergenceTracking(convergenceThreshold);
            }
            extractor.setEarlyStoppingEnabled(earlyStoppingEnabled);

            // Incremental fitting
            if (incrementalFittingEnabled) {
                extractor.enableIncrementalFitting(fitInterval);
            }

            // Histogram
            if (histogramEnabled) {
                extractor.enableHistogramTracking(prominenceThreshold);
            }

            // Adaptive
            extractor.setAdaptiveEnabled(adaptiveEnabled);
            extractor.setKsThresholdParametric(ksThresholdParametric);
            extractor.setKsThresholdComposite(ksThresholdComposite);
            extractor.setMaxCompositeComponents(maxCompositeComponents);
            extractor.setClusteringStrategy(clusteringStrategy);
            extractor.setInternalVerificationEnabled(internalVerificationEnabled);
            extractor.setVerificationLevel(verificationLevel);

            // Reservoir sampling
            extractor.setReservoirSamplingEnabled(reservoirSamplingEnabled);
            extractor.setReservoirSize(reservoirSize);

            return extractor;
        }
    }
}
