package io.nosqlbench.command.analyze.subcommands;

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

import io.nosqlbench.command.analyze.VectorLoadingProvider;
import io.nosqlbench.command.common.BaseVectorsFileOption;
import io.nosqlbench.command.common.RangeOption;
import io.nosqlbench.common.types.VectorFileExtension;
import io.nosqlbench.vshapes.ComputeMode;
import io.nosqlbench.vshapes.extract.BestFitSelector;
import io.nosqlbench.vshapes.extract.DatasetModelExtractor;
import io.nosqlbench.vshapes.extract.DimensionStatistics;
import io.nosqlbench.vshapes.extract.EmpiricalModelFitter;
import io.nosqlbench.vshapes.extract.ModelEquivalenceAnalyzer;
import io.nosqlbench.vshapes.extract.ModelExtractor;
import io.nosqlbench.vshapes.extract.NormalModelFitter;
import io.nosqlbench.vshapes.extract.NumaAwareDatasetModelExtractor;
import io.nosqlbench.vshapes.extract.NumaTopology;
import io.nosqlbench.vshapes.extract.ParallelDatasetModelExtractor;
import io.nosqlbench.vshapes.extract.UniformModelFitter;
import io.nosqlbench.vshapes.extract.VirtualThreadModelExtractor;
import io.nosqlbench.vshapes.extract.DimensionFitReport;
import io.nosqlbench.vshapes.model.NormalScalarModel;
import io.nosqlbench.vshapes.model.ScalarModel;
import io.nosqlbench.vshapes.model.VectorSpaceModel;
import io.nosqlbench.vshapes.model.VectorSpaceModelConfig;
import io.nosqlbench.nbdatatools.api.fileio.VectorFileArray;
import io.nosqlbench.nbdatatools.api.services.FileType;
import io.nosqlbench.nbdatatools.api.services.VectorFileIO;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Analyze a vector dataset and build a VectorSpaceModel configuration.
 *
 * <p>This command samples vectors from a dataset file, analyzes the statistical
 * distribution of each dimension, and saves the resulting VectorSpaceModel
 * configuration to a JSON file.
 *
 * <h2>Compute Strategies (--compute)</h2>
 * <ul>
 *   <li><b>OPTIMIZED</b>: Virtual threads + SIMD + microbatching (default, recommended)</li>
 *   <li><b>SIMD</b>: Single-threaded with SIMD acceleration</li>
 *   <li><b>PARALLEL</b>: Multi-threaded with SIMD acceleration (ForkJoinPool)</li>
 *   <li><b>NUMA</b>: NUMA-aware multi-threaded for multi-socket systems</li>
 *   <li><b>FAST</b>: Fast Gaussian-only statistics (mean/stdDev only, SIMD)</li>
 * </ul>
 *
 * <h2>Model Types (for OPTIMIZED, SIMD, PARALLEL, NUMA strategies)</h2>
 * <ul>
 *   <li><b>auto</b>: Best-fit from Normal, Uniform, Empirical (fast default)</li>
 *   <li><b>pearson</b>: Full Pearson family - Normal, Beta, Gamma, StudentT, PearsonIV,
 *       InverseGamma, BetaPrime, Uniform (comprehensive but slower)</li>
 *   <li><b>normal</b>: Force Normal (Gaussian) distribution for all dimensions</li>
 *   <li><b>uniform</b>: Force Uniform distribution for all dimensions</li>
 *   <li><b>empirical</b>: Use empirical histogram for all dimensions</li>
 *   <li><b>parametric</b>: Select from parametric distributions only (no empirical)</li>
 * </ul>
 *
 * <p>Note: When using {@code --show-fit-table}, the full Pearson family is automatically
 * used regardless of the model type setting to enable comprehensive comparison.
 *
 * <h2>Model Equivalence Analysis</h2>
 *
 * <p>When using full extraction with Pearson distribution fitting, you can analyze
 * whether higher-order moment parameters (skewness, kurtosis) provide meaningful
 * improvement over simpler models:
 *
 * <ul>
 *   <li><b>--analyze-equivalence</b>: Run equivalence analysis after extraction</li>
 *   <li><b>--equivalence-threshold</b>: Max CDF difference for equivalence (default: 0.02)</li>
 *   <li><b>--apply-simplifications</b>: Apply recommended simplifications to saved model</li>
 * </ul>
 *
 * <h2>Input File Specification</h2>
 *
 * <p>The input file supports inline range specification for processing a subset of vectors:
 * <ul>
 *   <li><b>file.fvec</b>: Process all vectors in the file</li>
 *   <li><b>file.fvec:1000</b>: Process the first 1000 vectors (indices 0-999)</li>
 *   <li><b>file.fvec:[100,500)</b>: Process vectors 100-499 (half-open interval)</li>
 *   <li><b>file.fvec:100..499</b>: Process vectors 100-499 (inclusive)</li>
 * </ul>
 *
 * <h2>Usage Examples</h2>
 * <pre>{@code
 * # Optimized virtual thread + SIMD + microbatching (default, recommended)
 * nbvectors analyze profile -b base_vectors.fvec -o model.json
 *
 * # Single-threaded SIMD extraction
 * nbvectors analyze profile -b base_vectors.fvec -o model.json --compute SIMD
 *
 * # Fast Gaussian-only profiling (mean/stdDev only)
 * nbvectors analyze profile -b base_vectors.fvec -o model.json --compute FAST
 *
 * # Parallel extraction with ForkJoinPool (16 threads)
 * nbvectors analyze profile -b base_vectors.fvec -o model.json --compute PARALLEL --threads 16
 *
 * # NUMA-aware extraction on multi-socket systems
 * nbvectors analyze profile -b base_vectors.fvec -o model.json --compute NUMA
 *
 * # Profile only the first 10000 vectors
 * nbvectors analyze profile -b base_vectors.fvec:10000 -o model.json
 *
 * # Profile a specific range of vectors
 * nbvectors analyze profile -b base_vectors.fvec:[5000,15000) -o model.json
 *
 * # Force Normal distribution with truncation bounds
 * nbvectors analyze profile -b base_vectors.fvec -o model.json --model-type normal --truncated
 *
 * # Sample 10000 vectors with empirical models
 * nbvectors analyze profile -b base_vectors.fvec -o model.json --model-type empirical --sample 10000
 *
 * # Analyze model equivalence and find simplification opportunities
 * nbvectors analyze profile -b base_vectors.fvec -o model.json --analyze-equivalence
 *
 * # Apply simplifications with stricter threshold
 * nbvectors analyze profile -b base_vectors.fvec -o model.json \
 *     --analyze-equivalence --equivalence-threshold 0.01 --apply-simplifications
 * }</pre>
 */
@CommandLine.Command(name = "profile",
    header = "Profile a vector dataset to build a VectorSpaceModel",
    description = "Analyzes vectors to compute per-dimension distribution models and saves as JSON",
    exitCodeList = {"0: success", "1: error processing file"})
public class CMD_analyze_profile implements Callable<Integer> {
    private static final Logger logger = LogManager.getLogger(CMD_analyze_profile.class);

    /// Compute strategy for profile extraction.
    ///
    /// All strategies use SIMD-optimized statistics computation when available
    /// (Panama Vector API on JDK 25+). The difference is in parallelization approach.
    public enum ComputeStrategy {
        /// Optimized extraction using virtual threads + SIMD + microbatching.
        /// Best performance for most workloads. Uses lightweight virtual threads
        /// with auto-tuned microbatch sizes for optimal parallelism.
        OPTIMIZED("Virtual threads + SIMD + microbatching (recommended)"),

        /// Single-threaded SIMD extraction.
        /// Uses Panama Vector API for dimension statistics when available.
        SIMD("Single-threaded with SIMD acceleration"),

        /// Multi-threaded SIMD extraction using ForkJoinPool.
        /// Each thread uses SIMD for its assigned dimensions.
        PARALLEL("Multi-threaded with SIMD acceleration"),

        /// NUMA-aware multi-threaded SIMD extraction.
        /// Optimized for multi-socket systems, binds threads to NUMA nodes.
        NUMA("NUMA-aware multi-threaded with SIMD acceleration"),

        /// Fast Gaussian-only statistics (legacy mode).
        /// Single-threaded with SIMD-optimized dimension statistics.
        FAST("Fast Gaussian-only (single-threaded, SIMD)");

        private final String description;

        ComputeStrategy(String description) {
            this.description = description;
        }

        public String getDescription() {
            return description;
        }
    }

    public enum ModelType {
        auto,       // Automatic best-fit selection (Normal, Uniform, Empirical)
        pearson,    // Full Pearson family (Normal, Beta, Gamma, StudentT, etc.)
        normal,     // Force Normal distribution
        uniform,    // Force Uniform distribution
        empirical,  // Force empirical histogram
        parametric  // Parametric distributions only (no empirical)
    }

    @CommandLine.Mixin
    private BaseVectorsFileOption baseVectorsOption = new BaseVectorsFileOption();

    @CommandLine.Option(names = {"-o", "--output"}, description = "Output JSON file for VectorSpaceModel config",
        required = true)
    private Path outputFile;

    @CommandLine.Option(names = {"-n", "--unique-vectors"},
        description = "Number of unique vectors for the model (default: count from input file)")
    private Long uniqueVectors;

    @CommandLine.Option(names = {"--sample"},
        description = "Number of vectors to sample (default: all vectors)")
    private Integer sampleSize;

    @CommandLine.Option(names = {"--seed"},
        description = "Random seed for sampling (default: 42)")
    private long seed = 42;

    @CommandLine.Option(names = {"--truncated"},
        description = "Include truncation bounds based on observed min/max values")
    private boolean truncated = false;

    @CommandLine.Option(names = {"--per-dimension"},
        description = "Store per-dimension statistics (default: uniform if all dimensions are similar)")
    private boolean perDimension = false;

    @CommandLine.Option(names = {"--tolerance"},
        description = "Tolerance for considering dimensions uniform (default: 0.01)")
    private double tolerance = 0.01;

    @CommandLine.Option(names = {"--compute", "-c"},
        description = "Compute strategy: OPTIMIZED, SIMD, PARALLEL, NUMA, FAST (default: OPTIMIZED). " +
            "All use Panama Vector API SIMD when available. Valid values: ${COMPLETION-CANDIDATES}")
    private ComputeStrategy computeStrategy = ComputeStrategy.OPTIMIZED;

    @CommandLine.Option(names = {"--model-type", "-m"},
        description = "Model type: auto (Normal/Uniform/Empirical), pearson (full Pearson family), normal, uniform, empirical, parametric (default: auto)")
    private ModelType modelType = ModelType.auto;

    @CommandLine.Option(names = {"--threads", "-t"},
        description = "Number of threads for parallel extraction (default: auto)")
    private Integer threads;

    @CommandLine.Option(names = {"--batch-size"},
        description = "Batch size for parallel extraction (default: 64)")
    private int batchSize = 64;

    @CommandLine.Option(names = {"--analyze-equivalence"},
        description = "Run model equivalence analysis to find simplification opportunities")
    private boolean analyzeEquivalence = false;

    @CommandLine.Option(names = {"--equivalence-threshold"},
        description = "Max CDF difference threshold for model equivalence (default: 0.02)")
    private double equivalenceThreshold = 0.02;

    @CommandLine.Option(names = {"--apply-simplifications"},
        description = "Apply recommended simplifications to the saved model")
    private boolean applySimplifications = false;

    @CommandLine.Option(names = {"--show-fit-table"},
        description = "Display per-dimension fit quality table for all Pearson distribution types")
    private boolean showFitTable = false;

    @CommandLine.Option(names = {"--fit-table-max-dims"},
        description = "Maximum dimensions to show in fit table (default: 50)")
    private int fitTableMaxDims = 50;

    @Override
    public Integer call() {
        try {
            // Validate base vectors file
            baseVectorsOption.validateBaseVectors();
            Path inputFile = baseVectorsOption.getBasePath();

            // Parse inline range if specified
            RangeOption.Range inlineRange = null;
            if (baseVectorsOption.hasInlineRange()) {
                RangeOption.RangeConverter converter = new RangeOption.RangeConverter();
                inlineRange = converter.convert(baseVectorsOption.getInlineRange());
            }

            String fileExtension = getFileExtension(inputFile);
            VectorFileExtension vectorFileExtension = VectorFileExtension.fromExtension(fileExtension);

            if (vectorFileExtension == null) {
                logger.error("Unsupported file type: {}", fileExtension);
                System.err.println("Error: Unsupported file type: " + fileExtension);
                return 1;
            }

            FileType fileType = vectorFileExtension.getFileType();
            Class<?> dataType = vectorFileExtension.getDataType();

            if (dataType != float[].class) {
                logger.error("Only float vector files are supported for profiling");
                System.err.println("Error: Only float vector files are supported for profiling");
                return 1;
            }

            // Display compute mode capabilities
            printComputeCapabilities();

            System.out.printf("Profiling vector file: %s%n", inputFile);
            if (inlineRange != null) {
                System.out.printf("  Vector range: %s%n", inlineRange);
            }
            System.out.printf("  Compute strategy: %s (%s)%n", computeStrategy, computeStrategy.getDescription());
            if (computeStrategy != ComputeStrategy.FAST) {
                System.out.printf("  Model type: %s%n", modelType);
            }

            VectorSpaceModel model;
            if (computeStrategy == ComputeStrategy.FAST) {
                model = profileVectorsFast(inputFile, fileType, inlineRange);
            } else {
                model = profileVectorsFull(inputFile, fileType, inlineRange);
            }

            if (model == null) {
                return 1;
            }

            // Run equivalence analysis if requested
            if (analyzeEquivalence && computeStrategy != ComputeStrategy.FAST) {
                model = runEquivalenceAnalysis(model);
            }

            VectorSpaceModelConfig.saveToFile(model, outputFile);
            System.out.printf("VectorSpaceModel config saved to: %s%n", outputFile);

            return 0;

        } catch (Exception e) {
            logger.error("Error profiling vectors", e);
            System.err.println("Error: " + e.getMessage());
            return 1;
        }
    }

    private String getFileExtension(Path file) {
        String fileName = file.getFileName().toString();
        int lastDotIndex = fileName.lastIndexOf('.');
        return lastDotIndex > 0 ? fileName.substring(lastDotIndex + 1) : "";
    }

    /**
     * Prints compute mode capabilities for diagnostic output.
     */
    private void printComputeCapabilities() {
        ComputeMode.Mode mode = ComputeMode.getEffectiveMode();
        System.out.println("═════════════════════════════════════════════════════════");
        System.out.println("                  COMPUTE CAPABILITIES                    ");
        System.out.println("═════════════════════════════════════════════════════════");
        System.out.printf("  Java Version:     %d%n", ComputeMode.getJavaVersion());
        System.out.printf("  Panama Available: %s%n", ComputeMode.isPanamaAvailable() ? "Yes" : "No");
        if (ComputeMode.isPanamaAvailable()) {
            System.out.printf("  Vector Width:     %d bits%n", ComputeMode.getPreferredVectorBits());
            System.out.printf("  Float Lanes:      %d per operation%n", mode.floatLanes());
        }
        System.out.printf("  Active Mode:      %s%n", mode.displayName());
        System.out.printf("  Description:      %s%n", mode.description());
        if (ComputeMode.isCPUDetectionAvailable()) {
            System.out.printf("  CPU SIMD:         %s%n", ComputeMode.getBestSIMDCapability());
        }
        System.out.println("═════════════════════════════════════════════════════════");
        System.out.println();
    }

    /**
     * Full model extraction using DatasetModelExtractor with best-fit selection.
     */
    private VectorSpaceModel profileVectorsFull(Path file, FileType fileType, RangeOption.Range range) {
        try (VectorFileArray<float[]> vectorArray = VectorFileIO.randomAccess(fileType, float[].class, file)) {
            int totalVectorCount = vectorArray.getSize();

            if (totalVectorCount == 0) {
                logger.error("File contains no vectors");
                System.err.println("Error: File contains no vectors");
                return null;
            }

            // Apply range constraint if specified
            int rangeStart = 0;
            int rangeEnd = totalVectorCount;
            if (range != null) {
                RangeOption.Range effectiveRange = range.constrain(totalVectorCount);
                rangeStart = (int) effectiveRange.start();
                rangeEnd = (int) effectiveRange.end();
            }
            int vectorCount = rangeEnd - rangeStart;

            // Get dimensions from first vector in range
            float[] first = vectorArray.get(rangeStart);
            int dimensions = first.length;

            System.out.printf("  Vectors in file: %d%n", totalVectorCount);
            if (range != null) {
                System.out.printf("  Vectors in range: %d (indices %d to %d)%n", vectorCount, rangeStart, rangeEnd - 1);
            }
            System.out.printf("  Dimensions: %d%n", dimensions);

            // Determine sample size within the range
            int actualSampleSize = sampleSize != null ? Math.min(sampleSize, vectorCount) : vectorCount;
            System.out.printf("  Sampling: %d vectors%n", actualSampleSize);

            // Report loading method
            if (VectorLoadingProvider.isMemoryMappedAvailable() && actualSampleSize == vectorCount) {
                System.out.println("  Loading: Memory-mapped I/O (optimized)");
            } else {
                System.out.println("  Loading: Standard I/O");
            }

            // Load vectors into memory with progress display
            long loadStart = System.currentTimeMillis();
            float[][] data = loadVectorsWithProgress(file, vectorArray, rangeStart, vectorCount, actualSampleSize);
            long loadElapsed = System.currentTimeMillis() - loadStart;
            System.out.printf("  Loaded %d vectors in %d ms%n", actualSampleSize, loadElapsed);

            // Create extractor
            ModelExtractor extractor = createExtractor(dimensions);

            // Extract model with progress display
            long startTime = System.currentTimeMillis();
            System.out.printf("  Fitting %d dimensions...%n", dimensions);

            ModelExtractor.ExtractionResult result = extractWithProgressDisplay(extractor, data, dimensions);
            VectorSpaceModel model = result.model();

            long elapsed = System.currentTimeMillis() - startTime;
            System.out.printf("  Extraction completed in %d ms%n", elapsed);

            // Apply truncation bounds if requested
            if (truncated) {
                model = applyTruncationBounds(model, result.dimensionStats());
            }

            // Shutdown parallel extractors
            shutdownExtractor(extractor);

            // Print model summary
            printModelSummary(model);

            // Display fit table if requested
            if (showFitTable) {
                displayFitTable(result, data, dimensions);
            }

            // Update unique vectors if specified
            long modelUniqueVectors = uniqueVectors != null ? uniqueVectors : vectorCount;
            if (modelUniqueVectors != model.uniqueVectors()) {
                model = new VectorSpaceModel(modelUniqueVectors, model.scalarModels());
            }

            return model;

        } catch (Exception e) {
            logger.error("Error reading vector file", e);
            throw new RuntimeException("Failed to profile vectors: " + e.getMessage(), e);
        }
    }

    /**
     * Loads vectors from the file with progress display.
     *
     * <p>Uses memory-mapped I/O when available (2-4x faster), falling back to
     * standard VectorFileArray access otherwise.
     *
     * @param filePath the file path (for memory-mapped loading)
     * @param vectorArray the source vector file (fallback)
     * @param rangeStart the starting index within the file
     * @param rangeSize the number of vectors in the range
     * @param sampleSize the number of vectors to sample from the range
     */
    private float[][] loadVectorsWithProgress(Path filePath, VectorFileArray<float[]> vectorArray,
                                               int rangeStart, int rangeSize, int sampleSize) {

        int[] lastPercent = {-1};
        int totalToLoad = sampleSize;

        // Progress callback that prints a progress bar
        VectorLoadingProvider.ProgressCallback progressCallback = (progress, message) -> {
            int percent = (int) (progress * 100);
            if (percent != lastPercent[0]) {
                printLoadProgressBar(percent, (int)(progress * totalToLoad), totalToLoad);
                lastPercent[0] = percent;
            }
        };

        Random random = sampleSize < rangeSize ? new Random(seed) : null;

        float[][] data = VectorLoadingProvider.loadVectors(
            filePath,
            vectorArray,
            rangeStart,
            rangeSize,
            sampleSize,
            random,
            progressCallback
        );

        // Print final progress
        printLoadProgressBar(100, totalToLoad, totalToLoad);
        System.out.println(); // New line after progress bar

        return data;
    }

    /**
     * Prints a loading progress bar to the console.
     */
    private void printLoadProgressBar(int percent, int loaded, int total) {
        int barWidth = 30;
        int filled = percent * barWidth / 100;
        int empty = barWidth - filled;

        StringBuilder bar = new StringBuilder();
        bar.append("\r  [");
        bar.append("█".repeat(filled));
        bar.append("░".repeat(empty));
        bar.append("] ");
        bar.append(String.format("%3d%% (%d/%d vectors)", percent, loaded, total));

        System.out.print(bar);
        System.out.flush();
    }

    /**
     * Creates the appropriate extractor based on compute strategy.
     *
     * <p>When {@code showFitTable} is enabled, the extractor will be configured to
     * collect all fit scores during extraction (avoiding redundant recomputation).
     */
    private ModelExtractor createExtractor(int dimensions) {
        BestFitSelector selector = createSelector();

        switch (computeStrategy) {
            case OPTIMIZED:
                int optMicrobatch = VirtualThreadModelExtractor.optimalMicrobatchSize(dimensions);
                System.out.printf("  Using optimized virtual thread extractor (microbatch=%d)%n", optMicrobatch);
                VirtualThreadModelExtractor vtExtractor = createVirtualThreadExtractor(selector, optMicrobatch);
                return showFitTable ? vtExtractor.withAllFitsCollection() : vtExtractor;

            case SIMD:
                System.out.println("  Using single-threaded SIMD extractor");
                DatasetModelExtractor simdExtractor = createDatasetExtractor(selector);
                return showFitTable ? simdExtractor.withAllFitsCollection() : simdExtractor;

            case PARALLEL:
                int parallelism = threads != null ? threads : ParallelDatasetModelExtractor.defaultParallelism();
                System.out.printf("  Using parallel SIMD extractor with %d threads%n", parallelism);
                // Note: ParallelDatasetModelExtractor doesn't support all-fits collection yet
                return ParallelDatasetModelExtractor.builder()
                    .parallelism(parallelism)
                    .batchSize(batchSize)
                    .selector(selector)
                    .uniqueVectors(uniqueVectors != null ? uniqueVectors : 1_000_000)
                    .build();

            case NUMA:
                NumaTopology topology = NumaTopology.detect();
                int threadsPerNode = threads != null
                    ? threads / topology.nodeCount()
                    : topology.threadsPerNode(10);
                System.out.printf("  Using NUMA-aware SIMD extractor: %d nodes × %d threads%n",
                    topology.nodeCount(), threadsPerNode);
                // Note: NumaAwareDatasetModelExtractor doesn't support all-fits collection yet
                return NumaAwareDatasetModelExtractor.builder()
                    .threadsPerNode(threadsPerNode)
                    .batchSize(batchSize)
                    .selector(selector)
                    .uniqueVectors(uniqueVectors != null ? uniqueVectors : 1_000_000)
                    .build();

            case FAST:
                // FAST mode is handled by profileVectorsFast(), not here
                throw new IllegalStateException("FAST strategy should not use createExtractor()");

            default:
                throw new IllegalStateException("Unexpected compute strategy: " + computeStrategy);
        }
    }

    /**
     * Creates a DatasetModelExtractor with the appropriate selector/fitter.
     */
    private DatasetModelExtractor createDatasetExtractor(BestFitSelector selector) {
        switch (modelType) {
            case normal:
                return DatasetModelExtractor.normalOnly();
            case uniform:
                return DatasetModelExtractor.uniformOnly();
            case empirical:
                return DatasetModelExtractor.empiricalOnly();
            default:
                return new DatasetModelExtractor(selector,
                    uniqueVectors != null ? uniqueVectors : 1_000_000);
        }
    }

    /**
     * Creates a VirtualThreadModelExtractor with the appropriate selector/fitter.
     */
    private VirtualThreadModelExtractor createVirtualThreadExtractor(BestFitSelector selector, int microbatchSize) {
        VirtualThreadModelExtractor.Builder builder = VirtualThreadModelExtractor.builder()
            .microbatchSize(microbatchSize)
            .uniqueVectors(uniqueVectors != null ? uniqueVectors : 1_000_000);

        switch (modelType) {
            case normal:
                builder.forceFitter(new NormalModelFitter());
                break;
            case uniform:
                builder.forceFitter(new UniformModelFitter());
                break;
            case empirical:
                builder.forceFitter(new EmpiricalModelFitter());
                break;
            default:
                builder.selector(selector);
                break;
        }

        return builder.build();
    }

    /**
     * Creates the BestFitSelector based on model type option.
     */
    private BestFitSelector createSelector() {
        switch (modelType) {
            case pearson:
                return BestFitSelector.fullPearsonSelector();
            case normal:
                return new BestFitSelector(java.util.List.of(new NormalModelFitter()));
            case uniform:
                return new BestFitSelector(java.util.List.of(new UniformModelFitter()));
            case empirical:
                return new BestFitSelector(java.util.List.of(new EmpiricalModelFitter()));
            case parametric:
                return BestFitSelector.parametricOnly();
            case auto:
            default:
                // When showing fit table, use full Pearson selector to compare all distributions
                // Otherwise use default (Normal, Uniform, Empirical) for faster extraction
                if (showFitTable) {
                    return BestFitSelector.fullPearsonSelector();
                }
                return BestFitSelector.defaultSelector();
        }
    }

    /**
     * Extracts the model with a live progress display.
     *
     * <p>For VirtualThreadModelExtractor, polls getProgress() in a background thread.
     * For other extractors, shows a simple progress indicator.
     */
    private ModelExtractor.ExtractionResult extractWithProgressDisplay(
            ModelExtractor extractor, float[][] data, int dimensions) {

        // For VirtualThreadModelExtractor, use its built-in progress tracking
        if (extractor instanceof VirtualThreadModelExtractor vtExtractor) {
            return extractWithVirtualThreadProgress(vtExtractor, data, dimensions);
        }

        // For DatasetModelExtractor with progress callback support
        if (extractor instanceof DatasetModelExtractor dsExtractor) {
            return extractWithCallbackProgress(dsExtractor, data, dimensions);
        }

        // Fallback: just run extraction without progress
        return extractor.extractWithStats(data);
    }

    /**
     * Extracts using VirtualThreadModelExtractor with progress polling.
     */
    private ModelExtractor.ExtractionResult extractWithVirtualThreadProgress(
            VirtualThreadModelExtractor extractor, float[][] data, int dimensions) {

        AtomicBoolean done = new AtomicBoolean(false);

        // Start progress display thread
        Thread progressThread = Thread.startVirtualThread(() -> {
            int lastPercent = -1;
            while (!done.get()) {
                double progress = extractor.getProgress();
                int percent = (int) (progress * 100);

                if (percent != lastPercent && percent > 0) {
                    int completed = (int) (progress * dimensions);
                    printProgressBar(completed, dimensions, percent);
                    lastPercent = percent;
                }

                try {
                    Thread.sleep(100); // Update every 100ms
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });

        try {
            // Run extraction
            ModelExtractor.ExtractionResult result = extractor.extractWithStats(data);

            // Signal completion and wait for progress thread
            done.set(true);
            progressThread.join(500);

            // Print final progress
            printProgressBar(dimensions, dimensions, 100);
            System.out.println(); // New line after progress bar

            return result;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Extraction interrupted", e);
        }
    }

    /**
     * Extracts using DatasetModelExtractor with progress callback.
     */
    private ModelExtractor.ExtractionResult extractWithCallbackProgress(
            DatasetModelExtractor extractor, float[][] data, int dimensions) {

        int[] lastPercent = {-1};

        DatasetModelExtractor.ProgressCallback callback = (progress, message) -> {
            int percent = (int) (progress * 100);
            if (percent != lastPercent[0]) {
                int completed = (int) (progress * dimensions);
                printProgressBar(completed, dimensions, percent);
                lastPercent[0] = percent;
            }
        };

        ModelExtractor.ExtractionResult result = extractor.extractWithProgress(data, callback);

        // Print final progress
        printProgressBar(dimensions, dimensions, 100);
        System.out.println(); // New line after progress bar

        return result;
    }

    /**
     * Prints a progress bar to the console.
     */
    private void printProgressBar(int completed, int total, int percent) {
        int barWidth = 30;
        int filled = (int) ((double) completed / total * barWidth);
        int empty = barWidth - filled;

        StringBuilder bar = new StringBuilder();
        bar.append("\r  [");
        bar.append("█".repeat(filled));
        bar.append("░".repeat(empty));
        bar.append("] ");
        bar.append(String.format("%3d%% (%d/%d dims)", percent, completed, total));

        System.out.print(bar);
        System.out.flush();
    }

    /**
     * Shuts down parallel extractors.
     */
    private void shutdownExtractor(ModelExtractor extractor) {
        if (extractor instanceof ParallelDatasetModelExtractor) {
            ((ParallelDatasetModelExtractor) extractor).shutdown();
        } else if (extractor instanceof NumaAwareDatasetModelExtractor) {
            ((NumaAwareDatasetModelExtractor) extractor).shutdown();
        }
    }

    /**
     * Applies truncation bounds to NormalScalarModels that aren't already truncated.
     */
    private VectorSpaceModel applyTruncationBounds(VectorSpaceModel model, DimensionStatistics[] stats) {
        ScalarModel[] updatedModels = new ScalarModel[model.dimensions()];
        int truncatedCount = 0;

        for (int d = 0; d < model.dimensions(); d++) {
            ScalarModel scalar = model.scalarModel(d);
            if (scalar instanceof NormalScalarModel normal && !normal.isTruncated()) {
                // Apply truncation bounds from observed min/max
                updatedModels[d] = new NormalScalarModel(
                    normal.getMean(),
                    normal.getStdDev(),
                    stats[d].min(),
                    stats[d].max()
                );
                truncatedCount++;
            } else {
                updatedModels[d] = scalar;
            }
        }

        if (truncatedCount > 0) {
            System.out.printf("  Applied truncation bounds to %d Normal distributions%n", truncatedCount);
        }

        return new VectorSpaceModel(model.uniqueVectors(), updatedModels);
    }

    /**
     * Prints a summary of the extracted model.
     */
    private void printModelSummary(VectorSpaceModel model) {
        System.out.println("\nModel Summary:");
        System.out.printf("  Dimensions: %d%n", model.dimensions());
        System.out.printf("  Unique vectors: %d%n", model.uniqueVectors());

        // Count model types
        java.util.Map<String, Integer> typeCounts = new java.util.HashMap<>();
        for (int d = 0; d < model.dimensions(); d++) {
            String type = model.scalarModel(d).getModelType();
            typeCounts.merge(type, 1, Integer::sum);
        }

        System.out.println("  Distribution types:");
        typeCounts.forEach((type, count) ->
            System.out.printf("    %s: %d dimensions (%.1f%%)%n",
                type, count, 100.0 * count / model.dimensions()));
    }

    /**
     * Displays a fit quality table showing how well each Pearson distribution type
     * fits each dimension of the data.
     *
     * <p>If pre-computed fit data is available in the extraction result, it will be
     * used directly (no recomputation). Otherwise, falls back to computing fits.
     *
     * @param result the extraction result (may contain pre-computed fit data)
     * @param data the vector data in [vectors][dimensions] format (fallback)
     * @param dimensions the number of dimensions
     */
    private void displayFitTable(ModelExtractor.ExtractionResult result, float[][] data, int dimensions) {
        DimensionFitReport report;

        if (result.hasAllFitsData()) {
            // Use pre-computed data (efficient path)
            System.out.println("\nUsing pre-computed fit quality data...");
            report = DimensionFitReport.fromAllFitsData(result.allFitsData());
        } else {
            // Fall back to computing fits (slower path)
            System.out.println("\nComputing fit quality for all Pearson distribution types...");
            long startTime = System.currentTimeMillis();

            // Transpose data from [vectors][dimensions] to [dimensions][vectors]
            int numVectors = data.length;
            float[][] transposed = new float[dimensions][numVectors];
            for (int v = 0; v < numVectors; v++) {
                for (int d = 0; d < dimensions; d++) {
                    transposed[d][v] = data[v][d];
                }
            }

            // Compute fit report using full Pearson selector
            report = DimensionFitReport.compute(transposed,
                BestFitSelector.fullPearsonSelector());

            long elapsed = System.currentTimeMillis() - startTime;
            System.out.printf("Fit analysis completed in %d ms%n%n", elapsed);
        }

        // Display the table
        System.out.println();
        System.out.println("═══════════════════════════════════════════════════════════════════════════");
        System.out.println("                    PER-DIMENSION FIT QUALITY TABLE                        ");
        System.out.println("═══════════════════════════════════════════════════════════════════════════");
        System.out.println("(Lower scores = better fit. Best fit marked with *)");
        System.out.println();
        System.out.println(report.formatTable(fitTableMaxDims));
    }

    /**
     * Fast Gaussian-only profiling using SIMD-optimized DimensionStatistics.
     *
     * <p>This mode uses Panama Vector API for dimension statistics computation
     * when available, providing significant speedup over scalar loops.
     */
    private VectorSpaceModel profileVectorsFast(Path file, FileType fileType, RangeOption.Range range) {
        try (VectorFileArray<float[]> vectorArray = VectorFileIO.randomAccess(fileType, float[].class, file)) {
            int totalVectorCount = vectorArray.getSize();

            if (totalVectorCount == 0) {
                logger.error("File contains no vectors");
                System.err.println("Error: File contains no vectors");
                return null;
            }

            // Apply range constraint if specified
            int rangeStart = 0;
            int rangeEnd = totalVectorCount;
            if (range != null) {
                RangeOption.Range effectiveRange = range.constrain(totalVectorCount);
                rangeStart = (int) effectiveRange.start();
                rangeEnd = (int) effectiveRange.end();
            }
            int vectorCount = rangeEnd - rangeStart;

            // Get dimensions from first vector in range
            float[] first = vectorArray.get(rangeStart);
            int dimensions = first.length;

            System.out.printf("  Vectors in file: %d%n", totalVectorCount);
            if (range != null) {
                System.out.printf("  Vectors in range: %d (indices %d to %d)%n", vectorCount, rangeStart, rangeEnd - 1);
            }
            System.out.printf("  Dimensions: %d%n", dimensions);

            // Determine sample size within the range
            int actualSampleSize = sampleSize != null ? Math.min(sampleSize, vectorCount) : vectorCount;
            System.out.printf("  Sampling: %d vectors%n", actualSampleSize);

            // Load vectors and transpose for per-dimension SIMD processing
            System.out.println("  Loading and transposing data for SIMD processing...");
            long loadStart = System.currentTimeMillis();
            float[][] transposed = loadAndTranspose(vectorArray, rangeStart, vectorCount, actualSampleSize, dimensions);
            long loadTime = System.currentTimeMillis() - loadStart;
            System.out.printf("  Data loaded in %d ms%n", loadTime);

            // Compute SIMD-optimized statistics per dimension
            System.out.println("  Computing dimension statistics (SIMD-optimized)...");
            long statsStart = System.currentTimeMillis();

            double[] mean = new double[dimensions];
            double[] stdDev = new double[dimensions];
            double[] min = new double[dimensions];
            double[] max = new double[dimensions];

            for (int d = 0; d < dimensions; d++) {
                DimensionStatistics stats = DimensionStatistics.compute(d, transposed[d]);
                mean[d] = stats.mean();
                stdDev[d] = stats.stdDev();
                min[d] = stats.min();
                max[d] = stats.max();
            }

            long statsTime = System.currentTimeMillis() - statsStart;
            System.out.printf("  Statistics computed in %d ms%n", statsTime);

            // Print statistics summary
            printStatisticsSummary(mean, stdDev, min, max, dimensions);

            // Determine the unique vectors count
            long modelUniqueVectors = uniqueVectors != null ? uniqueVectors : vectorCount;

            // Build the VectorSpaceModel
            return buildVectorSpaceModel(modelUniqueVectors, dimensions, mean, stdDev, min, max);

        } catch (Exception e) {
            logger.error("Error reading vector file", e);
            throw new RuntimeException("Failed to profile vectors: " + e.getMessage(), e);
        }
    }

    /**
     * Loads vectors and transposes them for efficient per-dimension SIMD processing.
     */
    private float[][] loadAndTranspose(VectorFileArray<float[]> vectorArray, int rangeStart,
                                        int rangeSize, int sampleSize, int dimensions) {
        float[][] transposed = new float[dimensions][sampleSize];

        if (sampleSize == rangeSize) {
            // Load all vectors in range
            for (int i = 0; i < rangeSize; i++) {
                float[] vector = vectorArray.get(rangeStart + i);
                for (int d = 0; d < dimensions; d++) {
                    transposed[d][i] = vector[d];
                }
                if ((i + 1) % 100000 == 0) {
                    System.out.printf("    Loaded %d / %d vectors%n", i + 1, rangeSize);
                }
            }
        } else {
            // Random sampling with reservoir sampling
            Random random = new Random(seed);
            int[] sampleIndices = new int[sampleSize];

            for (int i = 0; i < sampleSize; i++) {
                sampleIndices[i] = i;
            }
            for (int i = sampleSize; i < rangeSize; i++) {
                int j = random.nextInt(i + 1);
                if (j < sampleSize) {
                    sampleIndices[j] = i;
                }
            }

            for (int i = 0; i < sampleSize; i++) {
                float[] vector = vectorArray.get(rangeStart + sampleIndices[i]);
                for (int d = 0; d < dimensions; d++) {
                    transposed[d][i] = vector[d];
                }
                if ((i + 1) % 100000 == 0) {
                    System.out.printf("    Sampled %d / %d vectors%n", i + 1, sampleSize);
                }
            }
        }

        return transposed;
    }

    private void printStatisticsSummary(double[] mean, double[] stdDev,
                                         double[] min, double[] max, int dimensions) {
        // Compute overall statistics
        double overallMeanOfMeans = 0;
        double overallMeanOfStdDevs = 0;
        double globalMin = Double.MAX_VALUE;
        double globalMax = Double.MIN_VALUE;

        for (int d = 0; d < dimensions; d++) {
            overallMeanOfMeans += mean[d];
            overallMeanOfStdDevs += stdDev[d];
            if (min[d] < globalMin) globalMin = min[d];
            if (max[d] > globalMax) globalMax = max[d];
        }

        overallMeanOfMeans /= dimensions;
        overallMeanOfStdDevs /= dimensions;

        System.out.println("\nStatistics Summary:");
        System.out.printf("  Mean of means:    %.6f%n", overallMeanOfMeans);
        System.out.printf("  Mean of stdDevs:  %.6f%n", overallMeanOfStdDevs);
        System.out.printf("  Global min:       %.6f%n", globalMin);
        System.out.printf("  Global max:       %.6f%n", globalMax);

        // Check variance across dimensions
        double meanVariance = 0;
        double stdDevVariance = 0;
        for (int d = 0; d < dimensions; d++) {
            double meanDiff = mean[d] - overallMeanOfMeans;
            double stdDevDiff = stdDev[d] - overallMeanOfStdDevs;
            meanVariance += meanDiff * meanDiff;
            stdDevVariance += stdDevDiff * stdDevDiff;
        }
        meanVariance /= dimensions;
        stdDevVariance /= dimensions;

        System.out.printf("  Cross-dim mean variance:   %.6f%n", meanVariance);
        System.out.printf("  Cross-dim stdDev variance: %.6f%n", stdDevVariance);

        boolean isUniform = meanVariance < tolerance && stdDevVariance < tolerance;
        System.out.printf("  Dimensions uniform: %s%n", isUniform ? "YES" : "NO");
    }

    /**
     * Runs model equivalence analysis to identify simplification opportunities.
     */
    private VectorSpaceModel runEquivalenceAnalysis(VectorSpaceModel model) {
        System.out.println("\nModel Equivalence Analysis:");
        System.out.printf("  Equivalence threshold: %.4f%n", equivalenceThreshold);

        ModelEquivalenceAnalyzer analyzer = new ModelEquivalenceAnalyzer(1000, equivalenceThreshold);
        ModelEquivalenceAnalyzer.VectorSimplificationSummary summary = analyzer.summarizeVector(model);

        System.out.println(summary);

        if (!applySimplifications) {
            System.out.println("\n  Use --apply-simplifications to apply recommended changes.");
            return model;
        }

        if (summary.canSimplifyCount() == 0) {
            System.out.println("  No simplifications applicable.");
            return model;
        }

        System.out.printf("\n  Applying %d simplifications...%n", summary.canSimplifyCount());

        // Build new model with simplifications applied
        java.util.Map<Integer, ModelEquivalenceAnalyzer.EquivalenceReport> reports =
            analyzer.analyzeVector(model);

        ScalarModel[] newScalars = new ScalarModel[model.dimensions()];
        int appliedCount = 0;

        for (int d = 0; d < model.dimensions(); d++) {
            ModelEquivalenceAnalyzer.EquivalenceReport report = reports.get(d);
            if (report.canSimplify()) {
                newScalars[d] = report.getRecommendedSimplification();
                appliedCount++;
            } else {
                newScalars[d] = model.scalarModel(d);
            }
        }

        System.out.printf("  Applied %d simplifications.%n", appliedCount);

        return new VectorSpaceModel(model.uniqueVectors(), newScalars);
    }

    private VectorSpaceModel buildVectorSpaceModel(long uniqueVectors, int dimensions,
                                                    double[] mean, double[] stdDev,
                                                    double[] min, double[] max) {
        // Check if dimensions are uniform
        double overallMean = 0;
        double overallStdDev = 0;
        double globalMin = Double.MAX_VALUE;
        double globalMax = Double.MIN_VALUE;

        for (int d = 0; d < dimensions; d++) {
            overallMean += mean[d];
            overallStdDev += stdDev[d];
            if (min[d] < globalMin) globalMin = min[d];
            if (max[d] > globalMax) globalMax = max[d];
        }

        overallMean /= dimensions;
        overallStdDev /= dimensions;

        // Check if all dimensions have similar statistics
        boolean isUniform = true;
        if (!perDimension) {
            for (int d = 0; d < dimensions; d++) {
                if (Math.abs(mean[d] - overallMean) > tolerance ||
                    Math.abs(stdDev[d] - overallStdDev) > tolerance) {
                    isUniform = false;
                    break;
                }
            }
        } else {
            isUniform = false;
        }

        if (isUniform) {
            System.out.println("\nBuilding uniform VectorSpaceModel...");
            if (truncated) {
                System.out.printf("  Using truncation bounds: [%.6f, %.6f]%n", globalMin, globalMax);
                return new VectorSpaceModel(uniqueVectors, dimensions,
                    overallMean, overallStdDev, globalMin, globalMax);
            } else {
                return new VectorSpaceModel(uniqueVectors, dimensions, overallMean, overallStdDev);
            }
        } else {
            System.out.println("\nBuilding per-dimension VectorSpaceModel...");
            NormalScalarModel[] components = new NormalScalarModel[dimensions];

            for (int d = 0; d < dimensions; d++) {
                if (truncated) {
                    components[d] = new NormalScalarModel(mean[d], stdDev[d], min[d], max[d]);
                } else {
                    components[d] = new NormalScalarModel(mean[d], stdDev[d]);
                }
            }

            return new VectorSpaceModel(uniqueVectors, components);
        }
    }
}
