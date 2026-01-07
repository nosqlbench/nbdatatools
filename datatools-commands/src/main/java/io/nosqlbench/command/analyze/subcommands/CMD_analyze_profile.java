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
import io.nosqlbench.datatools.virtdata.DimensionDistributionGenerator;
import io.nosqlbench.vshapes.ComputeMode;
import io.nosqlbench.vshapes.extract.BestFitSelector;
import io.nosqlbench.vshapes.extract.ConvergentDatasetModelExtractor;
import io.nosqlbench.vshapes.extract.ConvergentDimensionEstimator;
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
import io.nosqlbench.vshapes.checkpoint.CheckpointManager;
import io.nosqlbench.vshapes.checkpoint.CheckpointState;
import io.nosqlbench.vshapes.trace.NdjsonTraceObserver;
import io.nosqlbench.vshapes.trace.StateObserver;
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
 *   <li><b>OPTIMIZED</b>: Virtual threads + SIMD + microbatching (default, recommended for large datasets)</li>
 *   <li><b>SIMD</b>: Single-threaded with convergent parameter estimation and early stopping.
 *       Processes data incrementally and stops when parameters converge, potentially
 *       saving significant time on large datasets. Use {@code --no-convergence} to disable.</li>
 *   <li><b>PARALLEL</b>: Multi-threaded with SIMD acceleration (ForkJoinPool)</li>
 *   <li><b>NUMA</b>: NUMA-aware multi-threaded for multi-socket systems</li>
 *   <li><b>FAST</b>: Fast Gaussian-only statistics (mean/stdDev only, SIMD)</li>
 * </ul>
 *
 * <h2>Convergent Extraction (--compute SIMD)</h2>
 *
 * <p>The SIMD strategy uses convergent parameter estimation by default. This monitors
 * each dimension's statistical parameters (mean, variance, skewness, kurtosis) and
 * stops early when all dimensions have converged. This can save significant computation
 * on large datasets where convergence is achieved with only a fraction of the data.
 *
 * <ul>
 *   <li><b>--no-convergence</b>: Disable early stopping (process all samples)</li>
 *   <li><b>--convergence-threshold</b>: Parameter change threshold (default: 0.05 = 5% of SE)</li>
 *   <li><b>--convergence-checkpoint</b>: Samples between convergence checks (default: 1000)</li>
 * </ul>
 *
 * <h2>Model Types (for OPTIMIZED, SIMD, PARALLEL, NUMA strategies)</h2>
 * <ul>
 *   <li><b>auto</b>: Best-fit from bounded distributions (Normal, Beta, Uniform, Empirical).
 *       This is appropriate for vector embeddings which typically have bounded value ranges.
 *       Heavy-tailed distributions are excluded as they are indistinguishable in bounded data.</li>
 *   <li><b>pearson</b>: Full Pearson family - Normal, Beta, Gamma, StudentT, PearsonIV,
 *       InverseGamma, BetaPrime, Uniform. Use this for unbounded or semi-bounded data where
 *       heavy-tailed distributions may be appropriate.</li>
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
 *
 * # Use convergent extraction with early stopping (SIMD mode)
 * nbvectors analyze profile -b base_vectors.fvec -o model.json --compute SIMD
 *
 * # Convergent extraction with custom threshold (stricter convergence)
 * nbvectors analyze profile -b base_vectors.fvec -o model.json --compute SIMD --convergence-threshold 0.02
 *
 * # Disable convergent early stopping (process all samples)
 * nbvectors analyze profile -b base_vectors.fvec -o model.json --compute SIMD --no-convergence
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
        description = "Model type: auto (Normal/Beta/Uniform/Empirical - bounded distributions), pearson (full Pearson family including heavy-tailed), normal, uniform, empirical, parametric (default: auto)")
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

    @CommandLine.Option(names = {"--no-convergence"},
        description = "Disable convergent parameter estimation (process all samples without early stopping)")
    private boolean noConvergence = false;

    @CommandLine.Option(names = {"--convergence-threshold"},
        description = "Convergence threshold as fraction of standard error (default: 0.05 = 5%)")
    private double convergenceThreshold = ConvergentDimensionEstimator.DEFAULT_THRESHOLD;

    @CommandLine.Option(names = {"--convergence-checkpoint"},
        description = "Samples between convergence checks (default: 1000)")
    private int convergenceCheckpoint = ConvergentDimensionEstimator.DEFAULT_CHECKPOINT_INTERVAL;

    @CommandLine.Option(names = {"--verify"},
        description = "Perform round-trip verification: generate synthetic data from extracted model, " +
            "re-extract model from synthetic, and compare")
    private boolean verify = false;

    @CommandLine.Option(names = {"--verify-count"},
        description = "Number of synthetic vectors to generate for verification (default: 100000)")
    private int verifyCount = 100_000;

    @CommandLine.Option(names = {"--verbose", "-v"},
        description = "Show detailed progress and output")
    private boolean verbose = false;

    @CommandLine.Option(names = {"--empirical-dimensions"},
        description = "Force empirical (histogram) model for specific dimensions (comma-separated list, e.g., '23,47,89')",
        split = ",")
    private java.util.List<Integer> empiricalDimensions = new java.util.ArrayList<>();

    @CommandLine.Option(names = {"--multimodal"},
        description = "Enable multimodal detection and composite model fitting. " +
            "When enabled, the profile command detects multi-modal distributions and fits " +
            "2-3 component mixture models instead of falling back to empirical histograms.")
    private boolean enableMultimodal = false;

    @CommandLine.Option(names = {"--max-modes"},
        description = "Maximum number of modes to detect per dimension (default: 3). " +
            "Only used when --multimodal is enabled.")
    private int maxModes = 3;

    @CommandLine.Option(names = {"--checkpoint-dir"},
        description = "Directory for checkpoint files. When specified, progress is saved " +
            "periodically so that processing can be resumed if interrupted.")
    private Path checkpointDir;

    @CommandLine.Option(names = {"--checkpoint-interval"},
        description = "Number of dimensions to process between checkpoints (default: 100). " +
            "Lower values provide more frequent saves but may impact performance.")
    private int checkpointInterval = 100;

    @CommandLine.Option(names = {"--resume"},
        description = "Resume from the latest checkpoint in the checkpoint directory. " +
            "Requires --checkpoint-dir to be specified.")
    private boolean resumeFromCheckpoint = false;

    @CommandLine.Option(names = {"--trace-state"},
        description = "Write extraction state trace to NDJSON file. Each line is a JSON object " +
            "representing an event (dimension_start, accumulator_update, dimension_complete).")
    private Path traceStatePath;

    @CommandLine.Option(names = {"--max-memory"},
        description = "Maximum memory to use for vector data in bytes (e.g., 4g, 2048m). " +
            "Overrides auto-detection. Used to partition large datasets that exceed heap size.")
    private String maxMemorySpec;

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

            // Validate checkpoint options
            if (resumeFromCheckpoint && checkpointDir == null) {
                logger.error("--resume requires --checkpoint-dir to be specified");
                System.err.println("Error: --resume requires --checkpoint-dir to be specified");
                return 1;
            }

            // Handle checkpoint resume
            CheckpointState resumeState = null;
            if (resumeFromCheckpoint && checkpointDir != null) {
                try {
                    Path latestCheckpoint = CheckpointManager.findLatestCheckpoint(checkpointDir);
                    if (latestCheckpoint != null) {
                        resumeState = CheckpointManager.load(latestCheckpoint);
                        System.out.printf("Resuming from checkpoint: %s%n", latestCheckpoint.getFileName());
                        System.out.printf("  Progress: %d/%d dimensions (%.1f%%)%n",
                            resumeState.completedDimensions(),
                            resumeState.totalDimensions(),
                            resumeState.progressPercent());
                    } else {
                        System.out.println("No checkpoint found in directory, starting fresh.");
                    }
                } catch (CheckpointManager.CheckpointException e) {
                    logger.error("Failed to load checkpoint: {}", e.getMessage());
                    System.err.println("Error loading checkpoint: " + e.getMessage());
                    return 1;
                }
            }

            // Create checkpoint directory if needed
            if (checkpointDir != null && !Files.exists(checkpointDir)) {
                Files.createDirectories(checkpointDir);
                System.out.printf("Created checkpoint directory: %s%n", checkpointDir);
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
            if (checkpointDir != null) {
                System.out.printf("  Checkpoint directory: %s%n", checkpointDir);
                System.out.printf("  Checkpoint interval: every %d dimensions%n", checkpointInterval);
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

            // Run round-trip verification if requested
            if (verify) {
                boolean passed = runRoundTripVerification(model);
                return passed ? 0 : 1;
            }

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
     *
     * <p>This method also validates that Panama Vector API is enabled when it should be.
     * If the CPU supports AVX instructions and the Java version supports Panama (25+),
     * but the incubator module is not enabled, an error is thrown with instructions
     * on how to enable it.
     *
     * @throws IllegalStateException if Panama should be available but isn't enabled
     */
    private void printComputeCapabilities() {
        // Validate Panama is enabled when it should be - fail fast with helpful error
        ComputeMode.validatePanamaEnabled();

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

            // Set up state observer if trace file is specified
            NdjsonTraceObserver traceObserver = null;
            if (traceStatePath != null) {
                try {
                    traceObserver = new NdjsonTraceObserver(traceStatePath);
                    extractor.setObserver(traceObserver);
                    System.out.printf("  State tracing enabled: %s%n", traceStatePath);
                } catch (java.io.IOException e) {
                    logger.error("Failed to open trace file", e);
                    System.err.println("Warning: Could not open trace file: " + e.getMessage());
                }
            }

            // Extract model with progress display
            long startTime = System.currentTimeMillis();
            System.out.printf("  Fitting %d dimensions...%n", dimensions);

            ModelExtractor.ExtractionResult result = extractWithProgressDisplay(extractor, data, dimensions);
            VectorSpaceModel model = result.model();

            long elapsed = System.currentTimeMillis() - startTime;
            System.out.printf("  Extraction completed in %d ms%n", elapsed);

            // Print convergence summary if using convergent extractor
            if (extractor instanceof ConvergentDatasetModelExtractor convergentExtractor) {
                printConvergenceSummary(convergentExtractor, actualSampleSize);
            }

            // Apply truncation bounds if requested
            if (truncated) {
                model = applyTruncationBounds(model, result.dimensionStats());
            }

            // Apply empirical overrides for specified dimensions
            if (!empiricalDimensions.isEmpty()) {
                model = applyEmpiricalOverrides(model, data, dimensions);
            }

            // Shutdown parallel extractors and close trace observer
            shutdownExtractor(extractor);
            if (traceObserver != null) {
                try {
                    traceObserver.close();
                    System.out.printf("  State trace written to: %s%n", traceStatePath);
                } catch (java.io.IOException e) {
                    logger.warn("Failed to close trace file", e);
                }
            }

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
                if (noConvergence) {
                    System.out.println("  Using single-threaded SIMD extractor (non-convergent)");
                    DatasetModelExtractor simdExtractor = createDatasetExtractor(selector);
                    return showFitTable ? simdExtractor.withAllFitsCollection() : simdExtractor;
                } else {
                    System.out.printf("  Using single-threaded convergent extractor (threshold=%.2f%%, checkpoint=%d)%n",
                        convergenceThreshold * 100, convergenceCheckpoint);
                    return createConvergentExtractor(selector);
                }

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
     * Creates a ConvergentDatasetModelExtractor with per-dimension convergence detection.
     *
     * <p>This extractor processes data incrementally and can stop early when all
     * dimensions have converged to stable parameter estimates, potentially saving
     * significant computation time on large datasets.
     */
    private ConvergentDatasetModelExtractor createConvergentExtractor(BestFitSelector selector) {
        return ConvergentDatasetModelExtractor.builder()
            .convergenceThreshold(convergenceThreshold)
            .checkpointInterval(convergenceCheckpoint)
            .earlyStoppingEnabled(true)  // Enable early stopping by default
            .selector(selector)
            .uniqueVectors(uniqueVectors != null ? uniqueVectors : 1_000_000)
            .build();
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
     *
     * <p>By default, uses bounded data selector (Normal, Beta, Uniform) which is appropriate
     * for vector embeddings that typically have bounded value ranges. Heavy-tailed distributions
     * (Gamma, StudentT, InverseGamma, BetaPrime) are excluded because their distinguishing
     * features are truncated in bounded ranges, making them indistinguishable from bounded types.
     */
    private BestFitSelector createSelector() {
        switch (modelType) {
            case pearson:
                // Use multimodal selector if enabled
                return enableMultimodal
                    ? BestFitSelector.pearsonMultimodalSelector()
                    : BestFitSelector.fullPearsonSelector();
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
                if (showFitTable) {
                    return enableMultimodal
                        ? BestFitSelector.pearsonMultimodalSelector()
                        : BestFitSelector.fullPearsonSelector();
                }
                // Use bounded data selector (Normal, Beta, Uniform, Empirical) for typical
                // vector embeddings. With multimodal enabled, also tries composite models.
                return enableMultimodal
                    ? BestFitSelector.multimodalAwareSelector()
                    : BestFitSelector.boundedDataWithEmpirical();
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
     * Applies empirical model overrides for specified dimensions.
     *
     * <p>This allows users to force empirical (histogram) models for dimensions
     * that don't fit well with parametric distributions, improving round-trip
     * verification accuracy.
     */
    private VectorSpaceModel applyEmpiricalOverrides(VectorSpaceModel model, float[][] data, int dimensions) {
        java.util.Set<Integer> overrideDims = new java.util.HashSet<>(empiricalDimensions);

        // Validate dimension indices
        for (int dim : overrideDims) {
            if (dim < 0 || dim >= dimensions) {
                System.err.printf("Warning: Ignoring invalid dimension %d (valid range: 0-%d)%n",
                    dim, dimensions - 1);
            }
        }

        // Count valid overrides
        int validOverrides = (int) overrideDims.stream()
            .filter(d -> d >= 0 && d < dimensions)
            .count();

        if (validOverrides == 0) {
            return model;
        }

        System.out.printf("  Applying empirical overrides for %d dimensions: %s%n",
            validOverrides,
            overrideDims.stream()
                .filter(d -> d >= 0 && d < dimensions)
                .limit(10)
                .map(String::valueOf)
                .collect(java.util.stream.Collectors.joining(", ")) +
                (validOverrides > 10 ? ", ..." : ""));

        // Extract dimension data and create empirical models
        EmpiricalModelFitter empiricalFitter = new EmpiricalModelFitter();
        ScalarModel[] updatedModels = new ScalarModel[dimensions];

        for (int d = 0; d < dimensions; d++) {
            if (overrideDims.contains(d)) {
                // Extract values for this dimension
                float[] dimValues = new float[data.length];
                for (int v = 0; v < data.length; v++) {
                    dimValues[v] = data[v][d];
                }

                // Compute statistics and fit empirical model
                DimensionStatistics stats = DimensionStatistics.compute(d, dimValues);
                updatedModels[d] = empiricalFitter.fit(stats, dimValues).model();
            } else {
                updatedModels[d] = model.scalarModel(d);
            }
        }

        return new VectorSpaceModel(model.uniqueVectors(), updatedModels);
    }

    /**
     * Applies empirical model overrides for a specified list of dimensions.
     * This overload is used for round-trip verification where we want to preserve
     * empirical dimensions from the original model.
     */
    private VectorSpaceModel applyEmpiricalOverrides(VectorSpaceModel model, float[][] data,
            int dimensions, java.util.List<Integer> dimensionsToOverride) {
        if (dimensionsToOverride.isEmpty()) {
            return model;
        }

        java.util.Set<Integer> overrideDims = new java.util.HashSet<>(dimensionsToOverride);

        EmpiricalModelFitter empiricalFitter = new EmpiricalModelFitter();
        ScalarModel[] updatedModels = new ScalarModel[dimensions];

        for (int d = 0; d < dimensions; d++) {
            if (overrideDims.contains(d)) {
                // Extract values for this dimension
                float[] dimValues = new float[data.length];
                for (int v = 0; v < data.length; v++) {
                    dimValues[v] = data[v][d];
                }

                // Compute statistics and fit empirical model
                DimensionStatistics stats = DimensionStatistics.compute(d, dimValues);
                updatedModels[d] = empiricalFitter.fit(stats, dimValues).model();
            } else {
                updatedModels[d] = model.scalarModel(d);
            }
        }

        return new VectorSpaceModel(model.uniqueVectors(), updatedModels);
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

        // If composite models are present, show breakdown by component count
        if (typeCounts.containsKey("composite")) {
            printCompositeBreakdown(model);
        }
    }

    /**
     * Prints a breakdown of composite (mixture) models by component count.
     */
    private void printCompositeBreakdown(VectorSpaceModel model) {
        java.util.Map<Integer, Integer> componentCounts = new java.util.HashMap<>();

        for (int d = 0; d < model.dimensions(); d++) {
            if (model.scalarModel(d) instanceof io.nosqlbench.vshapes.model.CompositeScalarModel composite) {
                int components = composite.getComponentCount();
                componentCounts.merge(components, 1, Integer::sum);
            }
        }

        if (!componentCounts.isEmpty()) {
            System.out.println("  Composite breakdown:");
            componentCounts.entrySet().stream()
                .sorted(java.util.Map.Entry.comparingByKey())
                .forEach(entry ->
                    System.out.printf("    %d-component: %d dimensions%n",
                        entry.getKey(), entry.getValue()));
        }
    }

    /**
     * Prints a summary of the convergence status from the convergent extractor.
     */
    private void printConvergenceSummary(ConvergentDatasetModelExtractor extractor, int totalSamples) {
        ConvergentDatasetModelExtractor.ConvergenceSummary summary = extractor.getConvergenceSummary();
        if (summary == null) {
            return;
        }

        System.out.println("\nConvergence Summary:");
        System.out.printf("  Samples processed: %d / %d (%.1f%%)%n",
            summary.totalSamplesProcessed(), totalSamples,
            100.0 * summary.totalSamplesProcessed() / totalSamples);
        System.out.printf("  Dimensions converged: %d / %d (%.1f%%)%n",
            summary.convergedDimensions(), summary.totalDimensions(),
            summary.convergenceRate() * 100);

        if (summary.allConverged()) {
            System.out.printf("  All dimensions converged! Early stopping saved %.1f%% of samples.%n",
                100.0 * (1.0 - (double) summary.totalSamplesProcessed() / totalSamples));
        }

        if (summary.convergedDimensions() > 0) {
            System.out.printf("  Convergence range: %d - %d samples (avg: %.0f)%n",
                summary.minConvergenceSamples(), summary.maxConvergenceSamples(),
                summary.averageConvergenceSamples());
        }
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

    /**
     * Performs round-trip verification of the extracted model.
     *
     * <p>This method:
     * <ol>
     *   <li>Generates synthetic data from the extracted model</li>
     *   <li>Re-extracts a model from the synthetic data</li>
     *   <li>Compares the two models for type matches and parameter drift</li>
     *   <li>Reports pass/fail status with dimension-by-dimension details</li>
     * </ol>
     *
     * @param originalModel the model extracted from the original data
     * @return true if verification passed, false otherwise
     */
    private boolean runRoundTripVerification(VectorSpaceModel originalModel) {
        System.out.println();
        System.out.println("═══════════════════════════════════════════════════════════════════════════════");
        System.out.println("                         ROUND-TRIP VERIFICATION                               ");
        System.out.println("═══════════════════════════════════════════════════════════════════════════════");
        System.out.println();

        // Show the pipeline flow
        printPipelineFlow();

        int dimensions = originalModel.dimensions();

        // Step 1: Generate synthetic data
        System.out.printf("Step 1/3: Generating %,d synthetic vectors...%n", verifyCount);
        long genStart = System.currentTimeMillis();

        DimensionDistributionGenerator generator = new DimensionDistributionGenerator(originalModel);
        float[][] syntheticData = new float[verifyCount][];

        int progressInterval = Math.max(verifyCount / 10, 1000);
        for (int i = 0; i < verifyCount; i++) {
            syntheticData[i] = generator.apply(i);
            if (verbose && i > 0 && i % progressInterval == 0) {
                System.out.printf("  [%3d%%] Generated %,d vectors%n",
                    (i * 100) / verifyCount, i);
            }
        }

        long genElapsed = System.currentTimeMillis() - genStart;
        System.out.printf("  ✓ Synthetic data generated (%,d ms)%n%n", genElapsed);

        // Step 2: Re-extract model from synthetic data
        System.out.println("Step 2/3: Extracting model from synthetic data...");
        long extractStart = System.currentTimeMillis();

        BestFitSelector selector = createSelector();
        DatasetModelExtractor extractor = new DatasetModelExtractor(selector, verifyCount);
        VectorSpaceModel roundTripModel = extractor.extractVectorModel(syntheticData);

        // Apply the same empirical overrides to round-trip model - if the original
        // model uses empirical for a dimension, the round-trip should too for
        // meaningful comparison
        java.util.List<Integer> originalEmpiricalDims = new java.util.ArrayList<>();
        for (int d = 0; d < dimensions; d++) {
            if (originalModel.scalarModel(d).getModelType().equals("empirical")) {
                originalEmpiricalDims.add(d);
            }
        }
        if (!originalEmpiricalDims.isEmpty()) {
            if (verbose) {
                System.out.printf("  Preserving empirical type for %d dimensions in round-trip%n",
                    originalEmpiricalDims.size());
            }
            roundTripModel = applyEmpiricalOverrides(roundTripModel, syntheticData, dimensions,
                originalEmpiricalDims);
        }

        long extractElapsed = System.currentTimeMillis() - extractStart;
        System.out.printf("  ✓ Round-trip model extracted (%,d ms)%n%n", extractElapsed);

        // Step 3: Compare models
        System.out.println("Step 3/3: Comparing models...");
        System.out.println();

        // Collect comparison data
        java.util.List<DimensionComparison> comparisons = new java.util.ArrayList<>();
        int typeMatches = 0;
        double totalDrift = 0;
        double maxDrift = 0;
        int maxDriftDim = 0;

        for (int d = 0; d < dimensions; d++) {
            ScalarModel orig = originalModel.scalarModel(d);
            ScalarModel roundTrip = roundTripModel.scalarModel(d);

            boolean typeMatch = orig.getModelType().equals(roundTrip.getModelType());
            if (typeMatch) typeMatches++;

            double drift = calculateParameterDrift(orig, roundTrip);
            totalDrift += drift;
            if (drift > maxDrift) {
                maxDrift = drift;
                maxDriftDim = d;
            }

            comparisons.add(new DimensionComparison(d, orig, roundTrip, typeMatch, drift));
        }

        double avgDrift = totalDrift / dimensions;
        double typeMatchPct = 100.0 * typeMatches / dimensions;

        // Print dimension-by-dimension comparison table
        printDimensionComparisonTable(comparisons, dimensions);

        // Print summary statistics
        System.out.println();
        System.out.println("───────────────────────────────────────────────────────────────────────────────");
        System.out.println("                              SUMMARY                                          ");
        System.out.println("───────────────────────────────────────────────────────────────────────────────");

        String typeStatus = typeMatches == dimensions ? "✓" : "⚠";
        String avgDriftStatus = avgDrift < 1.0 ? "✓" : "⚠";
        String maxDriftStatus = maxDrift < 2.0 ? "✓" : "⚠";

        System.out.printf("  %s Type matches:      %d/%d (%.1f%%)%n",
            typeStatus, typeMatches, dimensions, typeMatchPct);
        System.out.printf("  %s Parameter drift:   %.2f%% avg (threshold: 1.0%%)%n",
            avgDriftStatus, avgDrift);
        System.out.printf("  %s Max drift:         %.2f%% (dim %d, threshold: 2.0%%)%n",
            maxDriftStatus, maxDrift, maxDriftDim);

        // Determine overall pass/fail
        boolean passed = typeMatches == dimensions && avgDrift < 1.0 && maxDrift < 2.0;
        boolean warning = !passed && (typeMatchPct >= 95 || avgDrift < 2.0);

        // Collect mismatched dimensions
        java.util.List<Integer> mismatchedDims = comparisons.stream()
            .filter(c -> !c.typeMatch || c.drift > 2.0)
            .map(c -> c.dimension)
            .collect(java.util.stream.Collectors.toList());

        System.out.println();
        System.out.println("═══════════════════════════════════════════════════════════════════════════════");
        System.out.println();

        if (passed) {
            System.out.println("╔═══════════════════════════════════════════════════════════════════════════════╗");
            System.out.println("║                         ✓ VERIFICATION PASSED                                 ║");
            System.out.println("║                                                                               ║");
            System.out.printf("║  Model saved to: %-58s ║%n", truncatePath(outputFile.toString(), 58));
            System.out.println("║                                                                               ║");
            System.out.println("║  The extraction-generation pipeline is accurate. All dimensions round-trip    ║");
            System.out.println("║  with stable distribution types and parameters within tolerance.              ║");
            System.out.println("╚═══════════════════════════════════════════════════════════════════════════════╝");
        } else if (warning) {
            printVerificationWarning(typeMatches, dimensions, typeMatchPct, mismatchedDims);
        } else {
            printVerificationFailed(typeMatches, dimensions, typeMatchPct, avgDrift, maxDrift, mismatchedDims);
        }

        return passed;
    }

    /**
     * Prints the pipeline flow diagram.
     */
    private void printPipelineFlow() {
        System.out.println("Pipeline Flow:");
        System.out.println("┌─────────────────────────────────────────────────────────────────────────────┐");
        System.out.println("│                                                                             │");
        System.out.println("│  Input      ┌─────────┐     Extracted     ┌──────────┐     Synthetic       │");
        System.out.println("│  Vectors ──►│ Extract │────► Model ──────►│ Generate │────► Vectors        │");
        System.out.println("│             └─────────┘        │          └──────────┘        │            │");
        System.out.println("│                                │                              │            │");
        System.out.println("│                                │          ┌──────────┐        │            │");
        System.out.println("│                                │          │ Extract  │◄───────┘            │");
        System.out.println("│                                │          └──────────┘                     │");
        System.out.println("│                                │               │                           │");
        System.out.println("│                                │          Round-Trip                       │");
        System.out.println("│                                │            Model                          │");
        System.out.println("│                                │               │                           │");
        System.out.println("│                                ▼               ▼                           │");
        System.out.println("│                           ┌─────────────────────────┐                      │");
        System.out.println("│                           │   Compare Parameters    │                      │");
        System.out.println("│                           │   (type match + drift)  │                      │");
        System.out.println("│                           └─────────────────────────┘                      │");
        System.out.println("│                                                                             │");
        System.out.println("└─────────────────────────────────────────────────────────────────────────────┘");
        System.out.println();
    }

    /**
     * Record for holding dimension comparison data.
     */
    private record DimensionComparison(
        int dimension,
        ScalarModel extracted,
        ScalarModel roundTrip,
        boolean typeMatch,
        double drift
    ) {}

    /**
     * Prints the dimension-by-dimension comparison table.
     */
    private void printDimensionComparisonTable(java.util.List<DimensionComparison> comparisons, int dimensions) {
        System.out.println("Dimension-by-Dimension Comparison:");
        System.out.println("┌──────┬────────────┬────────────────────────────────┬────────────────────────────────┬────────┬────────┐");
        System.out.println("│ Dim  │ Type       │ Extracted Model                │ Round-Trip Model               │ Drift  │ Status │");
        System.out.println("├──────┼────────────┼────────────────────────────────┼────────────────────────────────┼────────┼────────┤");

        // Determine which dimensions to show
        int maxToShow = verbose ? dimensions : Math.min(20, dimensions);
        int shown = 0;
        int problematicCount = 0;

        // First pass: count problematic dimensions
        for (DimensionComparison c : comparisons) {
            if (!c.typeMatch || c.drift > 1.0) {
                problematicCount++;
            }
        }

        // If not verbose and there are many dimensions, prioritize showing problems
        boolean showAllProblems = !verbose && problematicCount > 0;

        for (DimensionComparison c : comparisons) {
            boolean isProblematic = !c.typeMatch || c.drift > 1.0;

            // In non-verbose mode, show first few + all problems
            if (!verbose && shown >= 10 && !isProblematic) {
                continue;
            }
            if (shown >= maxToShow && !isProblematic) {
                continue;
            }

            String typeCol;
            if (c.typeMatch) {
                typeCol = c.extracted.getModelType();
            } else {
                typeCol = c.extracted.getModelType() + "→" + c.roundTrip.getModelType();
            }

            String extractedParams = formatModelParams(c.extracted);
            String roundTripParams = formatModelParams(c.roundTrip);
            String driftStr = c.typeMatch ? String.format("%.2f%%", c.drift) : "TYPE";
            String status = (c.typeMatch && c.drift < 1.0) ? "✓" : (c.drift < 2.0 ? "⚠" : "✗");

            System.out.printf("│ %4d │ %-10s │ %-30s │ %-30s │ %6s │   %s    │%n",
                c.dimension,
                truncateString(typeCol, 10),
                truncateString(extractedParams, 30),
                truncateString(roundTripParams, 30),
                driftStr,
                status);

            shown++;
        }

        // Show summary if we truncated
        int notShown = dimensions - shown;
        if (notShown > 0) {
            System.out.println("├──────┴────────────┴────────────────────────────────┴────────────────────────────────┴────────┴────────┤");
            System.out.printf("│ ... %d more dimensions not shown (use --verbose to see all)                                           │%n", notShown);
        }

        System.out.println("└──────────────────────────────────────────────────────────────────────────────────────────────────────────┘");
    }

    /**
     * Formats model parameters for display.
     */
    private String formatModelParams(ScalarModel model) {
        if (model instanceof NormalScalarModel normal) {
            if (normal.isTruncated()) {
                return String.format("μ=%.3f, σ=%.3f [%.2f,%.2f]",
                    normal.getMean(), normal.getStdDev(), normal.lower(), normal.upper());
            }
            return String.format("μ=%.4f, σ=%.4f", normal.getMean(), normal.getStdDev());
        }

        if (model instanceof io.nosqlbench.vshapes.model.BetaScalarModel beta) {
            return String.format("α=%.3f, β=%.3f [%.2f,%.2f]",
                beta.getAlpha(), beta.getBeta(), beta.getLower(), beta.getUpper());
        }

        if (model instanceof io.nosqlbench.vshapes.model.UniformScalarModel uniform) {
            return String.format("[%.4f, %.4f]", uniform.getLower(), uniform.getUpper());
        }

        if (model instanceof io.nosqlbench.vshapes.model.EmpiricalScalarModel) {
            return "empirical (histogram)";
        }

        // Fallback for other types
        return model.getModelType();
    }

    /**
     * Truncates a string for table display.
     */
    private String truncateString(String str, int maxLen) {
        if (str.length() <= maxLen) {
            return str;
        }
        return str.substring(0, maxLen - 2) + "..";
    }

    /**
     * Prints the verification warning message.
     */
    private void printVerificationWarning(int typeMatches, int dimensions, double typeMatchPct,
                                          java.util.List<Integer> mismatchedDims) {
        System.out.println("╔═══════════════════════════════════════════════════════════════════════════════╗");
        System.out.println("║                         ⚠ VERIFICATION WARNING                                ║");
        System.out.println("╠═══════════════════════════════════════════════════════════════════════════════╣");
        System.out.printf("║  Type matches: %d/%d (%.1f%%)                                                  ║%n",
            typeMatches, dimensions, typeMatchPct);
        if (!mismatchedDims.isEmpty()) {
            String dims = mismatchedDims.size() <= 8
                ? mismatchedDims.toString()
                : mismatchedDims.subList(0, 8) + "...";
            System.out.printf("║  Unstable dimensions: %-53s ║%n", dims);
        }
        System.out.println("╠═══════════════════════════════════════════════════════════════════════════════╣");
        System.out.println("║  WHAT THIS MEANS:                                                             ║");
        System.out.println("║  Some dimensions diverged during the round-trip verification pipeline:        ║");
        System.out.println("║                                                                               ║");
        System.out.println("║    Extract ──► Model ──► Generate ──► Re-Extract ──► Different Model          ║");
        System.out.println("║                 │                                          │                  ║");
        System.out.println("║                 └──────── Should Match ─────────┘         ✗                  ║");
        System.out.println("║                                                                               ║");
        System.out.println("║  The distribution type or parameters changed, meaning the parametric fit      ║");
        System.out.println("║  doesn't fully capture the data's shape (multimodal, heavy tails, etc.).      ║");
        System.out.println("╠═══════════════════════════════════════════════════════════════════════════════╣");
        System.out.println("║  RECOMMENDED FIX:                                                             ║");
        System.out.println("║  Force empirical (histogram) models for unstable dimensions. These capture    ║");
        System.out.println("║  exact quantile distributions and always round-trip accurately:               ║");
        System.out.println("║                                                                               ║");
        if (!mismatchedDims.isEmpty()) {
            String dimList = mismatchedDims.stream()
                .limit(10)
                .map(String::valueOf)
                .collect(java.util.stream.Collectors.joining(","));
            if (mismatchedDims.size() > 10) dimList += ",...";
            System.out.printf("║    nbvectors analyze profile -b <input> -o <output> \\                        ║%n");
            System.out.printf("║        --empirical-dimensions %-45s ║%n", dimList);
            System.out.println("║                                                                               ║");
        }
        System.out.println("║  The model has been saved and may still work for your use case.               ║");
        System.out.println("╚═══════════════════════════════════════════════════════════════════════════════╝");
    }

    /**
     * Prints the verification failed message.
     */
    private void printVerificationFailed(int typeMatches, int dimensions, double typeMatchPct,
                                         double avgDrift, double maxDrift,
                                         java.util.List<Integer> mismatchedDims) {
        System.out.println("╔═══════════════════════════════════════════════════════════════════════════════╗");
        System.out.println("║                         ✗ VERIFICATION FAILED                                 ║");
        System.out.println("╠═══════════════════════════════════════════════════════════════════════════════╣");
        System.out.printf("║  Type matches:   %d/%d (%.1f%%)                                                ║%n",
            typeMatches, dimensions, typeMatchPct);
        System.out.printf("║  Average drift:  %.2f%% (threshold: 1.0%%)                                      ║%n", avgDrift);
        System.out.printf("║  Max drift:      %.2f%% (threshold: 2.0%%)                                      ║%n", maxDrift);
        System.out.println("╠═══════════════════════════════════════════════════════════════════════════════╣");
        System.out.println("║  The extraction-generation pipeline shows significant deviation.              ║");
        System.out.println("║                                                                               ║");
        System.out.println("║  Possible causes:                                                             ║");
        System.out.println("║    • Data has complex distributions not captured by parametric models         ║");
        System.out.println("║    • Insufficient sample size for accurate parameter estimation               ║");
        System.out.println("║    • Data contains outliers or multimodal distributions                       ║");
        System.out.println("║                                                                               ║");
        System.out.println("║  Recommended actions:                                                         ║");
        System.out.println("║    1. Use --model-type empirical for all dimensions                           ║");
        System.out.println("║    2. Increase --verify-count for more stable estimation                      ║");
        System.out.println("║    3. See troubleshooting guide for more solutions                            ║");
        System.out.println("╚═══════════════════════════════════════════════════════════════════════════════╝");
    }

    /**
     * Calculates parameter drift between two scalar models as a percentage.
     *
     * <p>Uses type-specific parameter comparison. For models of different types,
     * compares based on common properties like bounds.
     */
    private double calculateParameterDrift(ScalarModel orig, ScalarModel roundTrip) {
        // Type-specific comparison
        if (orig instanceof NormalScalarModel origNormal && roundTrip instanceof NormalScalarModel rtNormal) {
            double meanDrift = relativeDrift(origNormal.getMean(), rtNormal.getMean(), origNormal.getStdDev());
            double stdDevDrift = relativeDrift(origNormal.getStdDev(), rtNormal.getStdDev(), origNormal.getStdDev());
            return (meanDrift + stdDevDrift) * 50;
        }

        if (orig instanceof io.nosqlbench.vshapes.model.BetaScalarModel origBeta &&
            roundTrip instanceof io.nosqlbench.vshapes.model.BetaScalarModel rtBeta) {
            double alphaDrift = relativeDrift(origBeta.getAlpha(), rtBeta.getAlpha(), origBeta.getAlpha());
            double betaDrift = relativeDrift(origBeta.getBeta(), rtBeta.getBeta(), origBeta.getBeta());
            return (alphaDrift + betaDrift) * 50;
        }

        if (orig instanceof io.nosqlbench.vshapes.model.UniformScalarModel origUniform &&
            roundTrip instanceof io.nosqlbench.vshapes.model.UniformScalarModel rtUniform) {
            double range = origUniform.getUpper() - origUniform.getLower();
            double lowerDrift = range > 0 ? Math.abs(origUniform.getLower() - rtUniform.getLower()) / range : 0;
            double upperDrift = range > 0 ? Math.abs(origUniform.getUpper() - rtUniform.getUpper()) / range : 0;
            return (lowerDrift + upperDrift) * 50;
        }

        // For empirical or mismatched types, use a simple heuristic
        return 0.5; // Report small drift for empirical-to-empirical (hard to compare exactly)
    }

    /**
     * Calculates relative drift between two values.
     */
    private double relativeDrift(double orig, double roundTrip, double scale) {
        if (scale == 0 || Math.abs(scale) < 1e-10) {
            return Math.abs(orig - roundTrip) < 1e-10 ? 0 : 1.0;
        }
        return Math.abs(orig - roundTrip) / Math.abs(scale);
    }

    /**
     * Truncates a path for display.
     */
    private String truncatePath(String path, int maxLen) {
        if (path.length() <= maxLen) {
            return path;
        }
        return "..." + path.substring(path.length() - maxLen + 3);
    }
}
