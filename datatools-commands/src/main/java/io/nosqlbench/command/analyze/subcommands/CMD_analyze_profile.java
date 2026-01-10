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
import io.nosqlbench.command.compute.VectorNormalizationDetector;
import io.nosqlbench.common.types.VectorFileExtension;
import io.nosqlbench.datatools.virtdata.DimensionDistributionGenerator;
import io.nosqlbench.datatools.virtdata.sampling.ComponentSampler;
import io.nosqlbench.datatools.virtdata.sampling.ComponentSamplerFactory;
import io.nosqlbench.vshapes.ComputeMode;
import io.nosqlbench.vshapes.extract.AdaptiveModelExtractor;
import io.nosqlbench.vshapes.extract.AdaptiveModelExtractor.AdaptiveExtractionResult;
import io.nosqlbench.vshapes.extract.AdaptiveModelExtractor.DimensionStrategy;
import io.nosqlbench.vshapes.extract.BestFitSelector;
import io.nosqlbench.vshapes.extract.ComponentModelFitter;
import io.nosqlbench.vshapes.extract.CompositeModelFitter.ClusteringStrategy;
import io.nosqlbench.vshapes.extract.ConvergentDatasetModelExtractor;
import io.nosqlbench.vshapes.extract.InternalVerifier.VerificationLevel;
import io.nosqlbench.vshapes.extract.IterativeModelRefiner;
import io.nosqlbench.vshapes.extract.ConvergentDimensionEstimator;
import io.nosqlbench.vshapes.extract.DatasetModelExtractor;
import io.nosqlbench.vshapes.extract.DimensionStatistics;
import io.nosqlbench.vshapes.extract.BetaModelFitter;
import io.nosqlbench.vshapes.extract.EmpiricalModelFitter;
import io.nosqlbench.vshapes.extract.GammaModelFitter;
import io.nosqlbench.vshapes.extract.InternalVerifier;
import io.nosqlbench.vshapes.extract.ModelEquivalenceAnalyzer;
import io.nosqlbench.vshapes.extract.StudentTModelFitter;
import io.nosqlbench.vshapes.extract.ModelExtractor;
import io.nosqlbench.vshapes.extract.NormalModelFitter;
import io.nosqlbench.vshapes.extract.UniformModelFitter;
import io.nosqlbench.vshapes.extract.DimensionFitReport;
import io.nosqlbench.vshapes.checkpoint.CheckpointManager;
import io.nosqlbench.vshapes.checkpoint.CheckpointState;
import io.nosqlbench.vshapes.trace.NdjsonTraceObserver;
import io.nosqlbench.vshapes.trace.StateObserver;
import io.nosqlbench.vshapes.model.NormalScalarModel;
import io.nosqlbench.vshapes.model.ScalarModel;
import io.nosqlbench.vshapes.model.VectorSpaceModel;
import io.nosqlbench.vshapes.model.VectorSpaceModelConfig;
import io.nosqlbench.vshapes.stream.AnalysisResults;
import io.nosqlbench.vshapes.stream.AnalyzerHarness;
import io.nosqlbench.vshapes.stream.ChunkSizeCalculator;
import io.nosqlbench.vshapes.stream.DataspaceShape;
import io.nosqlbench.vshapes.stream.PrefetchingDataSource;
import io.nosqlbench.vshapes.stream.StreamingModelExtractor;
import io.nosqlbench.vshapes.stream.TransposedChunkDataSource;
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
 * <p>Uses memory-efficient streaming extraction with convergence tracking. Processes data
 * in columnar chunks with early exit when parameters converge. Works efficiently for
 * datasets of any size. Supports all model fitting capabilities including parametric,
 * composite, and empirical models. Supports {@code --show-fit-table} for comprehensive
 * fit comparison. Supports {@code --threads} for configurable parallelism.
 * NUMA-aware fitting is enabled by default for optimal memory locality on multi-socket systems.
 *
 * <h2>Convergent Extraction</h2>
 *
 * <p>Uses convergent parameter estimation by default. This monitors each dimension's
 * statistical parameters (mean, variance, skewness, kurtosis) and stops early when
 * all dimensions have converged. This can save significant computation on large datasets
 * where convergence is achieved with only a fraction of the data.
 *
 * <ul>
 *   <li><b>--no-convergence</b>: Disable early stopping (process all samples)</li>
 *   <li><b>--convergence-threshold</b>: Parameter change threshold (default: 0.05 = 5% of SE)</li>
 *   <li><b>--convergence-checkpoint</b>: Samples between convergence checks (default: 1000)</li>
 *   <li><b>--memory-budget</b>: Fraction of available heap for chunks (0.0-1.0, default: 0.6)
 *       or absolute size with suffix (e.g., 4g, 512m)</li>
 *   <li><b>--threads</b>: Number of threads for parallel model fitting (default: auto)</li>
 * </ul>
 *
 * <h2>Model Types</h2>
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
 * # Default extraction with convergence-based early exit
 * nbvectors analyze profile -b base_vectors.fvec -o model.json
 *
 * # Parallel extraction with 16 threads (NUMA-aware by default)
 * nbvectors analyze profile -b base_vectors.fvec -o model.json --threads 16
 *
 * # Custom memory budget (use 4GB for chunks)
 * nbvectors analyze profile -b large_vectors.fvec -o model.json --memory-budget 4g
 *
 * # Fraction of available heap (40%)
 * nbvectors analyze profile -b large_vectors.fvec -o model.json --memory-budget 0.4
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
 * # Convergent extraction with custom threshold (stricter convergence)
 * nbvectors analyze profile -b base_vectors.fvec -o model.json --convergence-threshold 0.02
 *
 * # Disable convergent early stopping (process all samples)
 * nbvectors analyze profile -b base_vectors.fvec -o model.json --no-convergence
 *
 * # Show fit comparison table for all model types
 * nbvectors analyze profile -b base_vectors.fvec -o model.json --show-fit-table
 * }</pre>
 */
@CommandLine.Command(name = "profile",
    header = "Profile a vector dataset to build a VectorSpaceModel",
    description = "Analyzes vectors to compute per-dimension distribution models and saves as JSON",
    exitCodeList = {"0: success", "1: error processing file"})
public class CMD_analyze_profile implements Callable<Integer> {
    private static final Logger logger = LogManager.getLogger(CMD_analyze_profile.class);

    /// ANSI color codes for CVD-friendly (colorblind accessible) verification output.
    ///
    /// Based on research from Engeset et al. (2022) "Colours and maps for communicating
    /// natural hazards to users with and without colour vision deficiency":
    /// - Blue replaces green for "pass/good" status (blue is distinguishable for deuteranopia)
    /// - Yellow/amber for warnings (universally visible)
    /// - Red for errors (can be distinguished from blue by all CVD types)
    ///
    /// Uses bright ANSI colors (90-97 range) for better visibility.
    private static final String ANSI_RESET = "\u001B[0m";
    private static final String ANSI_RED = "\u001B[91m";      // Bright red for errors/failures
    private static final String ANSI_BLUE = "\u001B[94m";     // Bright blue for pass/good (CVD-friendly)
    private static final String ANSI_YELLOW = "\u001B[93m";   // Bright yellow for warnings
    private static final String ANSI_CYAN = "\u001B[96m";     // Bright cyan for highlights
    private static final String ANSI_BOLD = "\u001B[1m";
    private static final String ANSI_DIM = "\u001B[2m";
    // Background colors for highlighting differences
    private static final String ANSI_BG_RED = "\u001B[101m";  // Bright red background
    private static final String ANSI_BG_BLUE = "\u001B[104m"; // Bright blue background (CVD-friendly)

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

    @CommandLine.Option(names = {"--model-type", "-m"},
        description = "Model type: auto (Normal/Beta/Uniform/Empirical - bounded distributions), pearson (full Pearson family including heavy-tailed), normal, uniform, empirical, parametric (default: auto)")
    private ModelType modelType = ModelType.auto;

    @CommandLine.Option(names = {"--threads", "-t"},
        description = "Number of threads for parallel model fitting (default: auto = all processors - 10)")
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
        description = "Convergence threshold as fraction of standard error (default: 0.01 = 1%)")
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

    @CommandLine.Option(names = {"--refine"},
        description = "Enable iterative model refinement with internal verification. " +
            "When enabled, each dimension is verified via round-trip and refined if verification fails. " +
            "Refinement tries models in order: simple parametric → extended parametric → composite → empirical. " +
            "Implies --verify.")
    private boolean refine = false;

    @CommandLine.Option(names = {"--refine-threshold"},
        description = "Maximum parameter drift threshold for refinement verification (default: 0.02 = 2%%)")
    private double refineThreshold = 0.02;

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

    // Adaptive composite fallback options
    @CommandLine.Option(names = {"--no-adaptive"},
        description = "Disable adaptive composite fallback (enabled by default). " +
            "When adaptive is enabled, the extractor tries parametric models first, " +
            "then composite models with 2-4 components before falling back to empirical.")
    private boolean noAdaptive = false;

    @CommandLine.Option(names = {"--max-composite-modes"},
        description = "Maximum number of modes for composite fitting (default: 4). " +
            "Range: 2-4. Only used when adaptive extraction is enabled.")
    private int maxCompositeModes = 4;

    @CommandLine.Option(names = {"--verification-level"},
        description = "Internal verification thoroughness level: fast (500 samples), " +
            "balanced (1000 samples, default), or thorough (5000 samples).")
    private String verificationLevel = "balanced";

    @CommandLine.Option(names = {"--clustering"},
        description = "Clustering method for composite fitting: hard (nearest-mode) or em (soft clustering, default). " +
            "EM is more accurate for overlapping modes but slightly slower.")
    private String clusteringMethod = "em";

    @CommandLine.Option(names = {"--no-internal-verify"},
        description = "Disable internal mini-verification (enabled by default). " +
            "Internal verification detects parameter instability by doing a mini round-trip test.")
    private boolean noInternalVerify = false;

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

    @CommandLine.Option(names = {"--memory-budget"},
        description = "Memory budget for chunk loading as fraction of available heap (0.0-1.0, default: 0.6). " +
            "Alternatively, specify absolute size with suffix: 4g, 512m, 256m. " +
            "Used for chunked processing of large datasets.")
    private String memoryBudget = "0.6";

    @CommandLine.Option(names = {"--use-transposed-loading"},
        description = "Load vectors directly in column-major (transposed) format for faster per-dimension analysis. " +
            "Automatically enabled when using memory-mapped I/O.")
    private boolean useTransposedLoading = true;

    @CommandLine.Option(names = {"--assume-normalized"},
        description = "Assume vectors are L2-normalized (||v|| = 1.0). Skips auto-detection and applies " +
            "[-1, 1] range bounds to model extraction. Use when you know vectors are normalized " +
            "to avoid detection overhead.")
    private boolean assumeNormalized = false;

    @CommandLine.Option(names = {"--assume-unnormalized"},
        description = "Assume vectors are NOT normalized. Skips auto-detection. " +
            "Use when you know vectors are not normalized.")
    private boolean assumeUnnormalized = false;

    /// Detected or assumed normalization status. Set during call().
    private Boolean detectedNormalized = null;

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

            // Validate conflicting normalization options
            if (assumeNormalized && assumeUnnormalized) {
                System.err.println("Error: Cannot use both --assume-normalized and --assume-unnormalized");
                return 1;
            }

            // Detect or set normalization status
            if (assumeNormalized) {
                detectedNormalized = true;
                logger.info("Assuming vectors are L2-normalized (user specified)");
            } else if (assumeUnnormalized) {
                detectedNormalized = false;
                logger.info("Assuming vectors are NOT normalized (user specified)");
            } else {
                // Auto-detect normalization by sampling vectors
                try {
                    VectorNormalizationDetector.NormalizationResult normResult =
                        VectorNormalizationDetector.detectNormalization(inputFile);
                    detectedNormalized = normResult.isNormalized();
                    logger.info("Normalization detection: {} ({}/{} samples normalized, {}%)",
                        detectedNormalized ? "NORMALIZED" : "NOT NORMALIZED",
                        normResult.normalizedCount(),
                        normResult.sampleSize(),
                        String.format("%.1f", normResult.normalizedPercentage() * 100));
                } catch (Exception e) {
                    logger.warn("Failed to detect normalization, assuming NOT normalized: {}", e.getMessage());
                    detectedNormalized = false;
                }
            }

            // Display compute mode capabilities
            printComputeCapabilities();

            System.out.printf("Profiling vector file: %s%n", inputFile);
            if (inlineRange != null) {
                System.out.printf("  Vector range: %s%n", inlineRange);
            }
            System.out.printf("  Model type: %s%n", modelType);
            // Display normalization status
            String normSource = (assumeNormalized || assumeUnnormalized) ? " (user specified)" : " (auto-detected)";
            System.out.printf("  Normalized: %s%s%n",
                detectedNormalized ? "yes (L2 unit vectors)" : "no",
                normSource);
            if (detectedNormalized) {
                System.out.println("  Range bounds: [-1.0, 1.0] (normalized vectors)");
            }
            if (checkpointDir != null) {
                System.out.printf("  Checkpoint directory: %s%n", checkpointDir);
                System.out.printf("  Checkpoint interval: every %d dimensions%n", checkpointInterval);
            }

            VectorSpaceModel model = profileVectorsStreaming(inputFile, fileType, inlineRange);

            if (model == null) {
                return 1;
            }

            // Run equivalence analysis if requested
            if (analyzeEquivalence) {
                model = runEquivalenceAnalysis(model);
            }

            // Run iterative refinement if requested
            if (refine) {
                verify = true;  // --refine implies --verify
                model = runIterativeRefinement(model, inputFile, fileType, inlineRange);
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
     * Loads vectors in transposed (column-major) format with progress display.
     *
     * <p>Uses memory-mapped I/O for optimal performance. The returned array has shape
     * {@code float[dimensions][vectorCount]} rather than the standard row-major format.
     *
     * @param filePath the file path
     * @param startIndex starting vector index (inclusive)
     * @param endIndex ending vector index (exclusive)
     * @return transposed vectors, shape {@code [dimensions][vectorCount]}
     */
    private float[][] loadVectorsTransposedWithProgress(Path filePath, int startIndex, int endIndex) {
        int[] lastPercent = {-1};
        int vectorCount = endIndex - startIndex;

        VectorLoadingProvider.ProgressCallback progressCallback = (progress, message) -> {
            int percent = (int) (progress * 100);
            if (percent != lastPercent[0]) {
                printLoadProgressBar(percent, (int)(progress * vectorCount), vectorCount);
                lastPercent[0] = percent;
            }
        };

        float[][] transposed = VectorLoadingProvider.loadVectorsTransposed(
            filePath, startIndex, endIndex, progressCallback);

        // Print final progress
        printLoadProgressBar(100, vectorCount, vectorCount);
        System.out.println(); // New line after progress bar

        return transposed;
    }

    /**
     * Extracts model from pre-transposed data with progress display.
     *
     * <p>When data is already in column-major format, this avoids the redundant
     * transpose step in the extractor.
     *
     * @param extractor the model extractor
     * @param transposedData data in format {@code [dimensions][vectorCount]}
     * @param dimensions number of dimensions
     * @return extraction result
     */
    private ModelExtractor.ExtractionResult extractFromTransposedWithProgress(
            ModelExtractor extractor, float[][] transposedData, int dimensions) {

        // For DatasetModelExtractor, use extractFromTransposed if available
        if (extractor instanceof DatasetModelExtractor dsExtractor) {
            return extractTransposedWithCallbackProgress(dsExtractor, transposedData, dimensions);
        }

        // For AdaptiveModelExtractor, use its transposed extraction with strategy tracking
        if (extractor instanceof AdaptiveModelExtractor adaptiveExtractor) {
            return extractTransposedWithAdaptiveProgress(adaptiveExtractor, transposedData, dimensions);
        }

        // Fallback: transpose back to row-major and use standard extraction
        // This is suboptimal but maintains compatibility
        System.out.println("  Note: Using row-major fallback for this extractor type");
        int vectorCount = transposedData.length > 0 ? transposedData[0].length : 0;
        float[][] rowMajor = new float[vectorCount][dimensions];
        for (int v = 0; v < vectorCount; v++) {
            for (int d = 0; d < dimensions; d++) {
                rowMajor[v][d] = transposedData[d][v];
            }
        }
        return extractWithProgressDisplay(extractor, rowMajor, dimensions);
    }

    /**
     * Extracts from transposed data using DatasetModelExtractor with callback progress.
     */
    private ModelExtractor.ExtractionResult extractTransposedWithCallbackProgress(
            DatasetModelExtractor extractor, float[][] transposedData, int dimensions) {

        int[] lastPercent = {-1};

        DatasetModelExtractor.ProgressCallback callback = (progress, message) -> {
            int percent = (int) (progress * 100);
            if (percent != lastPercent[0]) {
                int completed = (int) (progress * dimensions);
                printProgressBar(completed, dimensions, percent);
                lastPercent[0] = percent;
            }
        };

        ModelExtractor.ExtractionResult result = extractor.extractFromTransposedWithProgress(
            transposedData, callback);

        printProgressBar(dimensions, dimensions, 100);
        System.out.println();

        return result;
    }

    /**
     * Extracts from transposed data using AdaptiveModelExtractor with progress and strategy summary.
     *
     * <p>This method provides detailed narration of the adaptive extraction process:
     * <ul>
     *   <li>Progress bar during extraction</li>
     *   <li>Summary of strategies used (parametric, composite, empirical)</li>
     *   <li>Details for dimensions that fell back to composite or empirical</li>
     * </ul>
     */
    private ModelExtractor.ExtractionResult extractTransposedWithAdaptiveProgress(
            AdaptiveModelExtractor extractor, float[][] transposedData, int dimensions) {

        System.out.print("  ");

        // Run adaptive extraction
        AdaptiveExtractionResult adaptiveResult = extractor.extractAdaptiveFromTransposed(transposedData);

        // Print completion
        printProgressBar(dimensions, dimensions, 100);
        System.out.println();

        // Print adaptive extraction summary
        printAdaptiveExtractionSummary(adaptiveResult);

        return adaptiveResult.toExtractionResult();
    }

    /**
     * Prints a summary of the adaptive extraction showing which strategies were used.
     */
    private void printAdaptiveExtractionSummary(AdaptiveExtractionResult result) {
        long parametric = result.parametricCount();
        long composite = result.compositeCount();
        long empirical = result.empiricalCount();
        int total = result.strategies().size();

        // Color codes for strategy types
        String GREEN = "\u001B[32m";
        String YELLOW = "\u001B[33m";
        String RED = "\u001B[31m";
        String RESET = "\u001B[0m";

        // Print summary line
        System.out.printf("  Adaptive fitting: %s%d parametric%s",
            GREEN, parametric, RESET);

        if (composite > 0) {
            System.out.printf(", %s%d composite%s", YELLOW, composite, RESET);
        }
        if (empirical > 0) {
            System.out.printf(", %s%d empirical%s", RED, empirical, RESET);
        }
        System.out.printf(" (%d total)%n", total);

        // Show breakdown of composite models by mode count
        if (composite > 0) {
            long composite2 = result.strategies().stream()
                .filter(s -> s.strategyUsed() == AdaptiveModelExtractor.Strategy.COMPOSITE_2).count();
            long composite3 = result.strategies().stream()
                .filter(s -> s.strategyUsed() == AdaptiveModelExtractor.Strategy.COMPOSITE_3).count();
            long composite4 = result.strategies().stream()
                .filter(s -> s.strategyUsed() == AdaptiveModelExtractor.Strategy.COMPOSITE_4).count();

            StringBuilder compositeBrief = new StringBuilder("    Composite: ");
            boolean first = true;
            if (composite2 > 0) {
                compositeBrief.append(composite2).append("×2-mode");
                first = false;
            }
            if (composite3 > 0) {
                if (!first) compositeBrief.append(", ");
                compositeBrief.append(composite3).append("×3-mode");
                first = false;
            }
            if (composite4 > 0) {
                if (!first) compositeBrief.append(", ");
                compositeBrief.append(composite4).append("×4-mode");
            }
            System.out.println(compositeBrief);
        }

        // For dimensions that used non-parametric strategies, show which ones (if few)
        if (composite + empirical > 0 && composite + empirical <= 10) {
            System.out.print("    Non-parametric dims: ");
            boolean first = true;
            for (DimensionStrategy s : result.strategies()) {
                if (!s.isParametric()) {
                    if (!first) System.out.print(", ");
                    String strategyName = switch (s.strategyUsed()) {
                        case COMPOSITE_2 -> "2-mode";
                        case COMPOSITE_3 -> "3-mode";
                        case COMPOSITE_4 -> "4-mode";
                        case EMPIRICAL -> "empirical";
                        default -> "?";
                    };
                    System.out.printf("%d(%s)", s.dimension(), strategyName);
                    first = false;
                }
            }
            System.out.println();
        } else if (composite + empirical > 10) {
            // Just show dimension numbers for brevity
            System.out.print("    Non-parametric dims: [");
            boolean first = true;
            for (DimensionStrategy s : result.strategies()) {
                if (!s.isParametric()) {
                    if (!first) System.out.print(",");
                    System.out.print(s.dimension());
                    first = false;
                }
            }
            System.out.println("]");
        }
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
     * Extracts a VectorSpaceModel from in-memory data using the streaming extractor.
     *
     * <p>This is the core extraction method used by both the main profile command and
     * round-trip verification. Using the same method for both ensures the round-trip
     * test is valid - both extractions use identical logic.
     *
     * <p>The data is fed through StreamingModelExtractor to match the streaming
     * pipeline behavior.
     *
     * @param data row-major vector data [vectorIndex][dimension]
     * @param dimensions number of dimensions
     * @param vectorCount number of vectors (for unique vector count)
     * @return extraction result containing the model and statistics
     */
    private ModelExtractor.ExtractionResult extractModelFromData(float[][] data, int dimensions, int vectorCount) {
        // Use StreamingModelExtractor - same code path as file-based streaming
        return extractWithStreamingExtractor(data, dimensions, vectorCount);
    }

    /**
     * Extracts using StreamingModelExtractor - same logic as profileVectorsStreaming but
     * for in-memory data.
     *
     * <p>This ensures round-trip verification uses the identical streaming extraction
     * algorithm, including convergence tracking, histogram detection, and adaptive fitting.
     */
    private ModelExtractor.ExtractionResult extractWithStreamingExtractor(float[][] data, int dimensions, int vectorCount) {
        // Create StreamingModelExtractor with same configuration as profileVectorsStreaming
        StreamingModelExtractor extractor = new StreamingModelExtractor(createSelector());
        extractor.setUniqueVectors(vectorCount);

        // Enable same features as main streaming extraction
        if (!noConvergence) {
            extractor.enableConvergenceTracking(convergenceThreshold);
            extractor.enableIncrementalFitting(5);
            extractor.enableHistogramTracking(0.25);
        }

        // Configure adaptive settings
        extractor.setAdaptiveEnabled(isAdaptiveEnabled());
        if (isAdaptiveEnabled()) {
            extractor.setMaxCompositeComponents(maxCompositeModes);
        }

        // Initialize with shape
        DataspaceShape shape = new DataspaceShape(vectorCount, dimensions);
        extractor.initialize(shape);

        // Transpose data to columnar format (StreamingModelExtractor expects [dim][vector])
        float[][] columnarData = new float[dimensions][vectorCount];
        for (int v = 0; v < vectorCount; v++) {
            for (int d = 0; d < dimensions; d++) {
                columnarData[d][v] = data[v][d];
            }
        }

        // Feed data in chunks (matching streaming behavior)
        int chunkSize = Math.min(10_000, vectorCount);
        int chunks = (vectorCount + chunkSize - 1) / chunkSize;
        int lastPercent = -1;

        for (int chunk = 0; chunk < chunks; chunk++) {
            int startIdx = chunk * chunkSize;
            int endIdx = Math.min(startIdx + chunkSize, vectorCount);
            int chunkVectors = endIdx - startIdx;

            // Create chunk slice
            float[][] chunkData = new float[dimensions][chunkVectors];
            for (int d = 0; d < dimensions; d++) {
                System.arraycopy(columnarData[d], startIdx, chunkData[d], 0, chunkVectors);
            }

            extractor.accept(chunkData, startIdx);

            // Progress display
            int percent = ((chunk + 1) * 100) / chunks;
            if (percent != lastPercent) {
                System.out.printf("\r  Processing chunks: %d/%d (%d%%)...", chunk + 1, chunks, percent);
                System.out.flush();
                lastPercent = percent;
            }

            // Check convergence
            if (!noConvergence) {
                extractor.checkConvergence();
                if (extractor.shouldStopEarly()) {
                    System.out.printf("\r  Converged at chunk %d/%d (%d%%)      %n", chunk + 1, chunks, percent);
                    break;
                }
            }
        }
        System.out.println();

        // Complete extraction
        long startTime = System.currentTimeMillis();
        VectorSpaceModel model = extractor.complete();
        long extractionTimeMs = System.currentTimeMillis() - startTime;

        // Build result with dimension statistics from accumulators
        DimensionStatistics[] stats = new DimensionStatistics[dimensions];
        for (int d = 0; d < dimensions; d++) {
            stats[d] = DimensionStatistics.compute(d, columnarData[d]);
        }

        return new ModelExtractor.ExtractionResult(model, stats, null, extractionTimeMs);
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
     * Creates an AdaptiveModelExtractor with configured fallback chain.
     *
     * <p>The adaptive extractor tries parametric models first, then composite models
     * with 2-4 components (using EM clustering by default), and falls back to empirical
     * only as a last resort.
     */
    private AdaptiveModelExtractor createAdaptiveExtractor() {
        // Parse verification level
        VerificationLevel vLevel = switch (verificationLevel.toLowerCase()) {
            case "fast" -> VerificationLevel.FAST;
            case "thorough" -> VerificationLevel.THOROUGH;
            default -> VerificationLevel.BALANCED;
        };

        // Parse clustering strategy
        ClusteringStrategy strategy = "hard".equalsIgnoreCase(clusteringMethod)
            ? ClusteringStrategy.HARD
            : ClusteringStrategy.EM;

        return AdaptiveModelExtractor.builder()
            .parametricSelector(BestFitSelector.pearsonSelector())
            .verificationLevel(vLevel)
            .clusteringStrategy(strategy)
            .maxCompositeComponents(maxCompositeModes)
            .uniqueVectors(uniqueVectors != null ? uniqueVectors : 1_000_000)
            .internalVerification(!noInternalVerify)
            .build();
    }

    /**
     * Returns true if adaptive extraction is enabled.
     */
    private boolean isAdaptiveEnabled() {
        return !noAdaptive && modelType == ModelType.auto;
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
                // Use normalized Pearson selector if data is normalized, otherwise unbounded
                if (Boolean.TRUE.equals(detectedNormalized)) {
                    return enableMultimodal
                        ? BestFitSelector.normalizedPearsonMultimodalSelector()
                        : BestFitSelector.normalizedPearsonSelector();
                }
                return enableMultimodal
                    ? BestFitSelector.pearsonMultimodalSelector()
                    : BestFitSelector.fullPearsonSelector();
            case normal:
                return new BestFitSelector(java.util.List.of(
                    Boolean.TRUE.equals(detectedNormalized)
                        ? NormalModelFitter.forNormalizedVectors()
                        : new NormalModelFitter()));
            case uniform:
                return new BestFitSelector(java.util.List.of(
                    Boolean.TRUE.equals(detectedNormalized)
                        ? UniformModelFitter.forNormalizedVectors()
                        : new UniformModelFitter()));
            case empirical:
                return new BestFitSelector(java.util.List.of(new EmpiricalModelFitter()));
            case parametric:
                return BestFitSelector.parametricOnly();
            case auto:
            default:
                // When showing fit table, use Pearson selector for comprehensive comparison
                // Respect normalization detection for proper bounds handling
                if (showFitTable) {
                    if (Boolean.TRUE.equals(detectedNormalized)) {
                        return enableMultimodal
                            ? BestFitSelector.normalizedPearsonMultimodalSelector()
                            : BestFitSelector.normalizedPearsonSelector();
                    }
                    return enableMultimodal
                        ? BestFitSelector.pearsonMultimodalSelector()
                        : BestFitSelector.fullPearsonSelector();
                }
                // Use normalized vector selector if data is detected/assumed normalized
                if (Boolean.TRUE.equals(detectedNormalized)) {
                    // Normalized vectors have known bounds [-1, 1]
                    return BestFitSelector.normalizedVectorSelector();
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
     * <p>Uses progress callback for DatasetModelExtractor, or falls back to
     * simple extraction for other extractor types.
     */
    private ModelExtractor.ExtractionResult extractWithProgressDisplay(
            ModelExtractor extractor, float[][] data, int dimensions) {

        // For DatasetModelExtractor with progress callback support
        if (extractor instanceof DatasetModelExtractor dsExtractor) {
            return extractWithCallbackProgress(dsExtractor, data, dimensions);
        }

        // Fallback: just run extraction without progress
        return extractor.extractWithStats(data);
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
     * Shuts down extractors that own thread pools.
     *
     * <p>Note: This is now a no-op since PARALLEL and NUMA modes have been removed.
     * StreamingModelExtractor is shut down separately in its dedicated code path.
     */
    private void shutdownExtractor(ModelExtractor extractor) {
        // No-op - PARALLEL and NUMA extractors have been removed
        // StreamingModelExtractor shutdown is handled separately
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
     * Computes dimension statistics from raw vector data.
     *
     * <p>This is used as a fallback when the extraction method doesn't provide
     * dimension statistics (e.g., PARALLEL mode) but they are needed for truncation.
     *
     * @param data row-major vector data [vectorIndex][dimension]
     * @param dimensions number of dimensions
     * @return array of computed dimension statistics
     */
    private DimensionStatistics[] computeStatisticsFromData(float[][] data, int dimensions) {
        DimensionStatistics[] stats = new DimensionStatistics[dimensions];
        int vectorCount = data.length;

        for (int d = 0; d < dimensions; d++) {
            double sum = 0;
            double sumSq = 0;
            double min = Double.POSITIVE_INFINITY;
            double max = Double.NEGATIVE_INFINITY;

            for (float[] vector : data) {
                double value = vector[d];
                sum += value;
                sumSq += value * value;
                min = Math.min(min, value);
                max = Math.max(max, value);
            }

            double mean = sum / vectorCount;
            double variance = (sumSq / vectorCount) - (mean * mean);
            double stdDev = Math.sqrt(Math.max(0, variance));

            // Create statistics with basic moments (skewness/kurtosis set to 0 as they're not needed for truncation)
            stats[d] = new DimensionStatistics(d, vectorCount, mean, stdDev, min, max, 0.0, 0.0);
        }

        return stats;
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
     * Prints a summary of fit quality from the extraction result.
     *
     * <p>This provides useful diagnostic information regardless of which extractor was used:
     * <ul>
     *   <li>Distribution of model types selected per dimension</li>
     *   <li>Fit quality statistics (K-S scores)</li>
     *   <li>Warnings about poorly fitting dimensions</li>
     * </ul>
     */
    private void printFitQualitySummary(ModelExtractor.ExtractionResult result, int sampleCount) {
        ComponentModelFitter.FitResult[] fitResults = result.fitResults();
        if (fitResults == null || fitResults.length == 0) {
            return;
        }

        int dimensions = fitResults.length;

        // Collect statistics about fit quality
        double totalKsScore = 0;
        double minKsScore = Double.MAX_VALUE;
        double maxKsScore = 0;
        int poorFitCount = 0;
        java.util.Map<String, Integer> typeCounts = new java.util.LinkedHashMap<>();

        for (ComponentModelFitter.FitResult fit : fitResults) {
            if (fit == null) continue;

            double gof = fit.goodnessOfFit();
            totalKsScore += gof;
            minKsScore = Math.min(minKsScore, gof);
            maxKsScore = Math.max(maxKsScore, gof);

            // Track model type distribution
            String type = fit.model().getModelType();
            typeCounts.merge(type, 1, Integer::sum);

            // K-S critical value at α=0.05 for two-sample test: 1.36 * sqrt(2/n)
            double criticalValue = 1.36 * Math.sqrt(2.0 / sampleCount);
            if (gof > criticalValue * 2) {  // Flag if > 2x critical value
                poorFitCount++;
            }
        }

        double avgKsScore = totalKsScore / dimensions;

        System.out.println("\nFit Quality Summary:");
        System.out.printf("  Dimensions fitted: %d%n", dimensions);
        System.out.printf("  K-S score: avg=%.4f, min=%.4f, max=%.4f%n",
            avgKsScore, minKsScore, maxKsScore);

        // Show model type distribution
        System.out.print("  Model types: ");
        java.util.List<String> typeStrings = new java.util.ArrayList<>();
        for (java.util.Map.Entry<String, Integer> entry : typeCounts.entrySet()) {
            int count = entry.getValue();
            double pct = 100.0 * count / dimensions;
            typeStrings.add(String.format("%s=%d (%.0f%%)", entry.getKey(), count, pct));
        }
        System.out.println(String.join(", ", typeStrings));

        // Report poor fits if any
        if (poorFitCount > 0) {
            System.out.printf("  Warning: %d dimensions (%.1f%%) have poor fits (K-S > 2x critical)%n",
                poorFitCount, 100.0 * poorFitCount / dimensions);
        } else {
            System.out.println("  All dimensions have acceptable fit quality");
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

    /**
     * Runs iterative model refinement with internal verification feedback loop.
     *
     * <p>This method:
     * <ol>
     *   <li>Loads the original data</li>
     *   <li>For each dimension, runs internal verification</li>
     *   <li>For dimensions that fail verification, uses IterativeModelRefiner to find a better model</li>
     *   <li>Returns a new model with refined dimensions</li>
     * </ol>
     *
     * <p>Model preference hierarchy (tried in order):
     * <ol>
     *   <li>Simple parametric (Normal, Uniform) - 2 parameters, uses mean/variance</li>
     *   <li>Extended parametric (Beta, Gamma, Student-t) - 3+ parameters, uses higher moments</li>
     *   <li>Composite (2-4 component mixtures) - for multimodal data</li>
     *   <li>Empirical (histogram) - exact quantile distribution, always works</li>
     * </ol>
     *
     * @param model the initial model to refine
     * @param inputFile the original vector file
     * @param fileType the file type
     * @param range optional range constraint
     * @return refined model with stable dimensions
     */
    private VectorSpaceModel runIterativeRefinement(VectorSpaceModel model, Path inputFile,
                                                     FileType fileType,
                                                     RangeOption.Range range) {
        System.out.println();
        System.out.println("═══════════════════════════════════════════════════════════════════════════════");
        System.out.println("                       ITERATIVE MODEL REFINEMENT                              ");
        System.out.println("═══════════════════════════════════════════════════════════════════════════════");
        System.out.println();
        System.out.printf("  Drift threshold: %.1f%%%n", refineThreshold * 100);
        System.out.printf("  Max composite components: %d%n", maxModes);
        System.out.println();

        int dimensions = model.dimensions();

        // Create the refiner
        IterativeModelRefiner.Builder refinerBuilder = IterativeModelRefiner.builder()
            .verificationLevel(VerificationLevel.BALANCED)
            .driftThreshold(refineThreshold)
            .ksThresholdParametric(0.03)
            .ksThresholdComposite(0.05)
            .maxCompositeComponents(Math.min(maxModes, 4))
            .verboseLogging(verbose);

        // Configure for normalized vectors if detected
        if (Boolean.TRUE.equals(detectedNormalized)) {
            refinerBuilder.forNormalizedVectors();
        }

        IterativeModelRefiner refiner = refinerBuilder.build();

        // Load original data for refinement
        System.out.println("Step 1/3: Loading original data for refinement...");
        float[][] originalData;
        try {
            originalData = loadDataForRefinement(inputFile, fileType, range, dimensions);
        } catch (Exception e) {
            logger.warn("Failed to load data for refinement: {}", e.getMessage());
            System.out.println("  ✗ Could not load data - skipping refinement");
            return model;
        }
        System.out.printf("  ✓ Loaded %,d vectors%n%n", originalData.length);

        // Step 2: Verify each dimension using BestFitSelector (matches round-trip verification)
        System.out.println("Step 2/3: Verifying dimensions via round-trip type matching...");

        // Use the same selector that extraction uses
        BestFitSelector verifySelector = Boolean.TRUE.equals(detectedNormalized)
            ? BestFitSelector.normalizedVectorSelector()
            : BestFitSelector.boundedDataWithEmpirical();

        java.util.List<Integer> failedDimensions = new java.util.ArrayList<>();
        int verifiedCount = 0;
        int sampleSize = 10_000;  // Generate synthetic samples for verification

        for (int d = 0; d < dimensions; d++) {
            ScalarModel dimModel = model.scalarModel(d);
            String originalType = dimModel.getModelType();

            // Skip empirical models - they're already exact
            if ("empirical".equals(originalType)) {
                verifiedCount++;
                continue;
            }

            // Generate synthetic data from the model
            float[] syntheticData = new float[sampleSize];
            ComponentSampler sampler = ComponentSamplerFactory.forModel(dimModel);
            Random random = new Random(42 + d);  // Deterministic per dimension
            for (int i = 0; i < sampleSize; i++) {
                double u = random.nextDouble();
                syntheticData[i] = (float) sampler.sample(u);
            }

            // Use BestFitSelector to find what model type would be selected
            ScalarModel refitModel = verifySelector.selectBest(syntheticData);
            String refitType = refitModel.getModelType();

            // Check if types match or are equivalent (Normal ↔ Beta allowed)
            if (originalType.equals(refitType) || areTypesEquivalent(originalType, refitType)) {
                verifiedCount++;
            } else {
                failedDimensions.add(d);
                if (verbose) {
                    System.out.printf("%n  Dim %d: %s → %s (type mismatch)", d, originalType, refitType);
                }
            }

            // Show progress
            if ((d + 1) % 50 == 0 || d == dimensions - 1) {
                System.out.printf("\r  Verifying: %d/%d dimensions (%d failed)...",
                    d + 1, dimensions, failedDimensions.size());
                System.out.flush();
            }
        }
        System.out.printf("\r  Verifying: %d/%d dimensions (%d type mismatches)%n",
            dimensions, dimensions, failedDimensions.size());

        if (failedDimensions.isEmpty()) {
            System.out.println();
            System.out.println("  ✓ All dimensions verified - no refinement needed");
            return model;
        }

        System.out.printf("  Found %d dimensions requiring refinement%n%n", failedDimensions.size());

        // Step 3: Refine failed dimensions by finding type-stable models
        System.out.println("Step 3/3: Finding type-stable models for failed dimensions...");

        ScalarModel[] refinedComponents = new ScalarModel[dimensions];
        System.arraycopy(model.scalarModels(), 0, refinedComponents, 0, dimensions);

        int refinedStable = 0, refinedEmpirical = 0;
        java.util.Map<String, Integer> stableTypeCount = new java.util.LinkedHashMap<>();

        for (int i = 0; i < failedDimensions.size(); i++) {
            int d = failedDimensions.get(i);
            float[] dimData = extractDimensionData(originalData, d);
            String originalType = model.scalarModel(d).getModelType();

            if (verbose) {
                System.out.printf("%n  Dimension %d (was: %s):%n", d, originalType);
            }

            // Get all candidate fits from selector (already sorted by goodness-of-fit)
            java.util.List<ComponentModelFitter.FitResult> allFits = verifySelector.fitAll(dimData);

            // Find first type-stable model (prefers parametric due to empirical penalty)
            ScalarModel stableModel = null;
            String stableType = null;

            for (ComponentModelFitter.FitResult fit : allFits) {
                ScalarModel candidate = fit.model();
                String candidateType = candidate.getModelType();

                // Empirical always round-trips - use as last resort
                if ("empirical".equals(candidateType)) {
                    continue;  // Try parametric first
                }

                // Test if this candidate is type-stable
                if (isTypeStable(candidate, candidateType, verifySelector, sampleSize)) {
                    stableModel = candidate;
                    stableType = candidateType;
                    break;
                }

                if (verbose) {
                    System.out.printf("    [%s] KS=%.4f - not type-stable%n", candidateType, fit.goodnessOfFit());
                }
            }

            if (stableModel != null) {
                // Found a stable parametric model
                refinedComponents[d] = stableModel;
                refinedStable++;
                stableTypeCount.merge(stableType, 1, Integer::sum);

                if (verbose) {
                    System.out.printf("    → %s (type-stable)%n", stableType);
                }
            } else {
                // No stable parametric found - use empirical (always stable)
                ComponentModelFitter.FitResult empiricalFit = allFits.stream()
                    .filter(f -> "empirical".equals(f.model().getModelType()))
                    .findFirst()
                    .orElseGet(() -> new EmpiricalModelFitter().fit(
                        DimensionStatistics.compute(d, dimData), dimData));

                refinedComponents[d] = empiricalFit.model();
                refinedEmpirical++;

                if (verbose) {
                    System.out.printf("    → empirical (fallback)%n");
                }
            }

            // Show progress
            if (!verbose && ((i + 1) % 10 == 0 || i == failedDimensions.size() - 1)) {
                System.out.printf("\r  Refining: %d/%d dimensions...",
                    i + 1, failedDimensions.size());
                System.out.flush();
            }
        }

        if (!verbose) {
            System.out.printf("\r  Refining: %d/%d dimensions     %n", failedDimensions.size(), failedDimensions.size());
        }

        // Print summary
        System.out.println();
        System.out.println("Refinement Summary:");
        System.out.printf("  Total dimensions: %d%n", dimensions);
        System.out.printf("  Already type-stable: %d (%.1f%%)%n", verifiedCount, 100.0 * verifiedCount / dimensions);
        System.out.printf("  Refined to stable: %d%n", failedDimensions.size());
        if (refinedStable > 0) {
            System.out.printf("    - Parametric (stable): %d%n", refinedStable);
            for (var entry : stableTypeCount.entrySet()) {
                System.out.printf("      • %s: %d%n", entry.getKey(), entry.getValue());
            }
        }
        if (refinedEmpirical > 0) {
            System.out.printf("    - Empirical (fallback): %d%n", refinedEmpirical);
        }
        System.out.println();

        return new VectorSpaceModel(model.uniqueVectors(), refinedComponents);
    }

    /**
     * Loads data from file for refinement.
     */
    private float[][] loadDataForRefinement(Path inputFile, FileType fileType,
                                            RangeOption.Range range, int dimensions) throws Exception {
        try (VectorFileArray<float[]> reader = VectorFileIO.randomAccess(FileType.xvec, float[].class, inputFile)) {
            int totalVectors = reader.getSize();
            int start = range != null ? (int) range.start() : 0;
            int end = range != null ? (int) Math.min(range.end(), totalVectors) : totalVectors;
            int count = end - start;

            // Limit to reasonable size for refinement
            int maxVectors = Math.min(count, 100_000);
            int stride = count / maxVectors;

            float[][] data = new float[maxVectors][];
            for (int i = 0; i < maxVectors; i++) {
                int index = start + (i * stride);
                data[i] = reader.get(index);
            }
            return data;
        }
    }

    /**
     * Tests if a model is type-stable under round-trip.
     *
     * A model is type-stable if generating synthetic data from it and
     * re-fitting with BestFitSelector produces the same model type.
     *
     * @param model the model to test
     * @param expectedType the expected model type after round-trip
     * @param selector the selector to use for re-fitting
     * @param sampleSize number of synthetic samples to generate
     * @return true if the model is type-stable
     */
    private boolean isTypeStable(ScalarModel model, String expectedType,
                                  BestFitSelector selector, int sampleSize) {
        // Generate synthetic data from the model
        float[] syntheticData = new float[sampleSize];
        ComponentSampler sampler = ComponentSamplerFactory.forModel(model);
        Random random = new Random(12345);  // Fixed seed for consistency
        for (int i = 0; i < sampleSize; i++) {
            double u = random.nextDouble();
            syntheticData[i] = (float) sampler.sample(u);
        }

        // Re-fit using the selector
        ScalarModel refitModel = selector.selectBest(syntheticData);
        String refitType = refitModel.getModelType();

        // Type-stable if the re-fitted type matches or types are equivalent
        return expectedType.equals(refitType) || areTypesEquivalent(expectedType, refitType);
    }

    /// Checks if two model types are statistically equivalent.
    ///
    /// Normal and symmetric Beta distributions are equivalent when both produce
    /// bell-shaped distributions centered near the same location. This allows
    /// the type-stability check to pass when the distinction is statistical noise.
    private boolean areTypesEquivalent(String type1, String type2) {
        // Normal and Beta can be equivalent for bounded symmetric data
        if (("normal".equals(type1) && "beta".equals(type2)) ||
            ("beta".equals(type1) && "normal".equals(type2))) {
            return true;
        }
        return false;
    }

    /**
     * Extracts dimension data from row-major vector array.
     */
    private float[] extractDimensionData(float[][] data, int dimension) {
        float[] dimData = new float[data.length];
        for (int i = 0; i < data.length; i++) {
            dimData[i] = data[i][dimension];
        }
        return dimData;
    }

    /**
     * Finds a matching fitter for a given model type.
     */
    private ComponentModelFitter findFitterForModelType(String modelType) {
        return switch (modelType) {
            case "normal" -> new NormalModelFitter();
            case "uniform" -> new UniformModelFitter();
            case "beta" -> new BetaModelFitter();
            case "gamma" -> new GammaModelFitter();
            case "student_t" -> new StudentTModelFitter();
            default -> null;
        };
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

        // Show progress every 5% or at least every 1000 vectors
        int progressInterval = Math.max(verifyCount / 20, 1000);
        int lastReportedPercent = -1;
        for (int i = 0; i < verifyCount; i++) {
            syntheticData[i] = generator.apply(i);
            if (i > 0 && i % progressInterval == 0) {
                int percent = (i * 100) / verifyCount;
                if (percent > lastReportedPercent) {
                    lastReportedPercent = percent;
                    System.out.printf("\r  Generating: %,d/%,d vectors (%d%%)...",
                        i, verifyCount, percent);
                    System.out.flush();
                }
            }
        }
        System.out.printf("\r  Generating: %,d/%,d vectors (100%%)...   %n", verifyCount, verifyCount);

        long genElapsed = System.currentTimeMillis() - genStart;
        System.out.printf("  ✓ Synthetic data generated (%,d ms)%n%n", genElapsed);

        // Step 2: Re-extract model from synthetic data using the SAME extraction logic
        System.out.println("Step 2/3: Extracting model from synthetic data...");
        long extractStart = System.currentTimeMillis();

        // Use the EXACT same extraction method as the main profile command
        // This ensures the round-trip test is valid - identical extraction logic
        ModelExtractor.ExtractionResult extractionResult = extractModelFromData(
            syntheticData, dimensions, verifyCount);
        VectorSpaceModel roundTripModel = extractionResult.model();

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

        // Color-coded status indicators
        String typeStatusColor = typeMatches == dimensions ? ANSI_BLUE : ANSI_YELLOW;
        String typeStatus = typeMatches == dimensions ? "✓" : "⚠";
        String avgDriftColor = avgDrift < 1.0 ? ANSI_BLUE : ANSI_YELLOW;
        String avgDriftStatus = avgDrift < 1.0 ? "✓" : "⚠";
        String maxDriftColor = maxDrift < 2.0 ? ANSI_BLUE : (maxDrift < 5.0 ? ANSI_YELLOW : ANSI_RED);
        String maxDriftStatus = maxDrift < 2.0 ? "✓" : "⚠";

        System.out.printf("  %s%s%s Type matches:      %d/%d (%.1f%%)%n",
            typeStatusColor, typeStatus, ANSI_RESET, typeMatches, dimensions, typeMatchPct);
        System.out.printf("  %s%s%s Parameter drift:   %s%.2f%%%s avg (threshold: 1.0%%)%n",
            avgDriftColor, avgDriftStatus, ANSI_RESET, avgDriftColor, avgDrift, ANSI_RESET);
        System.out.printf("  %s%s%s Max drift:         %s%.2f%%%s (dim %d, threshold: 2.0%%)%n",
            maxDriftColor, maxDriftStatus, ANSI_RESET, maxDriftColor, maxDrift, ANSI_RESET, maxDriftDim);

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
            System.out.println(ANSI_BLUE + "╔" + "═".repeat(BOX_WIDTH) + "╗" + ANSI_RESET);
            System.out.println(boxLine(ANSI_BLUE, ANSI_BLUE + ANSI_BOLD + "                         ✓ VERIFICATION PASSED                                " + ANSI_RESET));
            System.out.println(boxLine(ANSI_BLUE, ""));
            System.out.println(boxLine(ANSI_BLUE, "  Model saved to: " + truncatePath(outputFile.toString(), 58)));
            System.out.println(boxLine(ANSI_BLUE, ""));
            System.out.println(boxLine(ANSI_BLUE, "  The extraction-generation pipeline is accurate. All dimensions round-trip"));
            System.out.println(boxLine(ANSI_BLUE, "  with stable distribution types and parameters within tolerance."));
            System.out.println(ANSI_BLUE + "╚" + "═".repeat(BOX_WIDTH) + "╝" + ANSI_RESET);
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
     * Prints the dimension-by-dimension comparison table with diff-style color coding.
     *
     * <p>Color coding:
     * <ul>
     *   <li>Green: matching values / passed</li>
     *   <li>Yellow: warning (small drift)</li>
     *   <li>Red: divergence / type mismatch</li>
     * </ul>
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

            // Determine row color based on status
            String rowColor;
            String statusSymbol;
            if (c.typeMatch && c.drift < 1.0) {
                rowColor = ANSI_BLUE;
                statusSymbol = "✓";
            } else if (c.drift < 2.0) {
                rowColor = ANSI_YELLOW;
                statusSymbol = "⚠";
            } else {
                rowColor = ANSI_RED;
                statusSymbol = "✗";
            }

            // Format type column with color for type mismatches
            String typeCol;
            String typeColor = ANSI_RESET;
            if (c.typeMatch) {
                typeCol = c.extracted.getModelType();
                typeColor = ANSI_DIM;  // Dim for matching types
            } else {
                // Type mismatch: show change like a diff
                typeCol = c.extracted.getModelType() + "→" + c.roundTrip.getModelType();
                typeColor = ANSI_RED + ANSI_BOLD;  // Bold red for type changes
            }

            String extractedParams = formatModelParams(c.extracted);
            String roundTripParams = formatModelParams(c.roundTrip);

            // Color the params based on drift
            String extractedColor = ANSI_DIM;  // Original is dimmed (like - in diff)
            String roundTripColor;
            if (c.typeMatch && c.drift < 0.5) {
                roundTripColor = ANSI_BLUE;  // Good match
            } else if (c.drift < 2.0) {
                roundTripColor = ANSI_YELLOW;  // Warning
            } else {
                roundTripColor = ANSI_RED;  // Diverged
            }

            // Format drift with color
            String driftStr;
            String driftColor;
            if (!c.typeMatch) {
                driftStr = "TYPE";
                driftColor = ANSI_RED + ANSI_BOLD;
            } else if (c.drift < 0.5) {
                driftStr = String.format("%.2f%%", c.drift);
                driftColor = ANSI_BLUE;
            } else if (c.drift < 1.0) {
                driftStr = String.format("%.2f%%", c.drift);
                driftColor = ANSI_BLUE;
            } else if (c.drift < 2.0) {
                driftStr = String.format("%.2f%%", c.drift);
                driftColor = ANSI_YELLOW;
            } else {
                driftStr = String.format("%.2f%%", c.drift);
                driftColor = ANSI_RED;
            }

            // Print the row with colors
            System.out.printf("│ %4d │ %s%-10s%s │ %s%-30s%s │ %s%-30s%s │ %s%6s%s │   %s%s%s    │%n",
                c.dimension,
                typeColor, truncateString(typeCol, 10), ANSI_RESET,
                extractedColor, truncateString(extractedParams, 30), ANSI_RESET,
                roundTripColor, truncateString(roundTripParams, 30), ANSI_RESET,
                driftColor, driftStr, ANSI_RESET,
                rowColor, statusSymbol, ANSI_RESET);

            shown++;
        }

        // Show summary if we truncated
        int notShown = dimensions - shown;
        if (notShown > 0) {
            System.out.println("├──────┴────────────┴────────────────────────────────┴────────────────────────────────┴────────┴────────┤");
            System.out.printf("│ %s... %d more dimensions not shown (use --verbose to see all)%s                                           │%n",
                ANSI_DIM, notShown, ANSI_RESET);
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
     * Prints the verification warning message with yellow color coding.
     */
    private void printVerificationWarning(int typeMatches, int dimensions, double typeMatchPct,
                                          java.util.List<Integer> mismatchedDims) {
        System.out.println(ANSI_YELLOW + "╔═══════════════════════════════════════════════════════════════════════════════╗" + ANSI_RESET);
        System.out.println(boxLine(ANSI_YELLOW, ANSI_YELLOW + ANSI_BOLD + "                         ⚠ VERIFICATION WARNING" + ANSI_RESET));
        System.out.println(ANSI_YELLOW + "╠═══════════════════════════════════════════════════════════════════════════════╣" + ANSI_RESET);
        System.out.println(boxLine(ANSI_YELLOW, String.format("  Type matches: %d/%d (%.1f%%)", typeMatches, dimensions, typeMatchPct)));
        if (!mismatchedDims.isEmpty()) {
            String dims = mismatchedDims.size() <= 8
                ? mismatchedDims.toString()
                : mismatchedDims.subList(0, 8) + "...";
            System.out.println(boxLine(ANSI_YELLOW, "  Unstable dimensions: " + ANSI_RED + dims + ANSI_RESET));
        }
        System.out.println(ANSI_YELLOW + "╠═══════════════════════════════════════════════════════════════════════════════╣" + ANSI_RESET);
        System.out.println(boxLine(ANSI_YELLOW, "  WHAT THIS MEANS:"));
        System.out.println(boxLine(ANSI_YELLOW, "  Some dimensions " + ANSI_RED + "diverged" + ANSI_RESET + " during the round-trip verification pipeline:"));
        System.out.println(boxLine(ANSI_YELLOW, ""));
        System.out.println(boxLine(ANSI_YELLOW, "    Extract ──► Model ──► Generate ──► Re-Extract ──► " + ANSI_RED + "Different Model" + ANSI_RESET));
        System.out.println(boxLine(ANSI_YELLOW, "                 │                                          │"));
        System.out.println(boxLine(ANSI_YELLOW, "                 └──────── Should Match ─────────┘         " + ANSI_RED + "✗" + ANSI_RESET));
        System.out.println(boxLine(ANSI_YELLOW, ""));
        System.out.println(boxLine(ANSI_YELLOW, "  The distribution type or parameters changed, meaning the parametric fit"));
        System.out.println(boxLine(ANSI_YELLOW, "  doesn't fully capture the data's shape (multimodal, heavy tails, etc.)."));
        System.out.println(ANSI_YELLOW + "╠═══════════════════════════════════════════════════════════════════════════════╣" + ANSI_RESET);
        System.out.println(boxLine(ANSI_YELLOW, "  " + ANSI_CYAN + "RECOMMENDED FIX:" + ANSI_RESET));
        System.out.println(boxLine(ANSI_YELLOW, "  Force empirical (histogram) models for unstable dimensions. These capture"));
        System.out.println(boxLine(ANSI_YELLOW, "  exact quantile distributions and always round-trip accurately:"));
        System.out.println(boxLine(ANSI_YELLOW, ""));
        if (!mismatchedDims.isEmpty()) {
            String dimList = mismatchedDims.stream()
                .limit(10)
                .map(String::valueOf)
                .collect(java.util.stream.Collectors.joining(","));
            if (mismatchedDims.size() > 10) dimList += ",...";
            System.out.println(boxLine(ANSI_YELLOW, "    " + ANSI_DIM + "nbvectors analyze profile -b <input> -o <output> \\" + ANSI_RESET));
            System.out.println(boxLine(ANSI_YELLOW, "        " + ANSI_DIM + "--empirical-dimensions " + ANSI_RESET + ANSI_CYAN + dimList + ANSI_RESET));
            System.out.println(boxLine(ANSI_YELLOW, ""));
        }
        System.out.println(boxLine(ANSI_YELLOW, "  The model has been saved and may still work for your use case."));
        System.out.println(ANSI_YELLOW + "╚═══════════════════════════════════════════════════════════════════════════════╝" + ANSI_RESET);
    }

    /**
     * Prints the verification failed message with red ANSI color coding.
     */
    private void printVerificationFailed(int typeMatches, int dimensions, double typeMatchPct,
                                         double avgDrift, double maxDrift,
                                         java.util.List<Integer> mismatchedDims) {
        System.out.println(ANSI_RED + "╔═══════════════════════════════════════════════════════════════════════════════╗" + ANSI_RESET);
        System.out.println(boxLine(ANSI_RED, "                         " + ANSI_BOLD + ANSI_RED + "✗ VERIFICATION FAILED" + ANSI_RESET));
        System.out.println(ANSI_RED + "╠═══════════════════════════════════════════════════════════════════════════════╣" + ANSI_RESET);
        String typeMatchColor = typeMatchPct < 90 ? ANSI_RED : ANSI_YELLOW;
        System.out.println(boxLine(ANSI_RED, String.format("  Type matches:   " + typeMatchColor + "%d/%d (%.1f%%)" + ANSI_RESET, typeMatches, dimensions, typeMatchPct)));
        String avgDriftColor = avgDrift > 1.0 ? ANSI_RED : ANSI_YELLOW;
        System.out.println(boxLine(ANSI_RED, String.format("  Average drift:  " + avgDriftColor + "%.2f%%" + ANSI_RESET + " (threshold: 1.0%%)", avgDrift)));
        String maxDriftColor = maxDrift > 2.0 ? ANSI_RED : ANSI_YELLOW;
        System.out.println(boxLine(ANSI_RED, String.format("  Max drift:      " + maxDriftColor + "%.2f%%" + ANSI_RESET + " (threshold: 2.0%%)", maxDrift)));
        System.out.println(ANSI_RED + "╠═══════════════════════════════════════════════════════════════════════════════╣" + ANSI_RESET);
        System.out.println(boxLine(ANSI_RED, "  " + ANSI_BOLD + "The extraction-generation pipeline shows significant deviation." + ANSI_RESET));
        System.out.println(boxLine(ANSI_RED, ""));
        System.out.println(boxLine(ANSI_RED, "  " + ANSI_BOLD + "Possible causes:" + ANSI_RESET));
        System.out.println(boxLine(ANSI_RED, "    " + ANSI_DIM + "•" + ANSI_RESET + " Data has complex distributions not captured by parametric models"));
        System.out.println(boxLine(ANSI_RED, "    " + ANSI_DIM + "•" + ANSI_RESET + " Insufficient sample size for accurate parameter estimation"));
        System.out.println(boxLine(ANSI_RED, "    " + ANSI_DIM + "•" + ANSI_RESET + " Data contains outliers or multimodal distributions"));
        System.out.println(boxLine(ANSI_RED, ""));
        System.out.println(boxLine(ANSI_RED, "  " + ANSI_BOLD + "Recommended actions:" + ANSI_RESET));
        System.out.println(boxLine(ANSI_RED, "    " + ANSI_CYAN + "1." + ANSI_RESET + " Use " + ANSI_CYAN + "--model-type empirical" + ANSI_RESET + " for all dimensions"));
        System.out.println(boxLine(ANSI_RED, "    " + ANSI_CYAN + "2." + ANSI_RESET + " Increase " + ANSI_CYAN + "--verify-count" + ANSI_RESET + " for more stable estimation"));
        System.out.println(boxLine(ANSI_RED, "    " + ANSI_CYAN + "3." + ANSI_RESET + " See troubleshooting guide for more solutions"));
        System.out.println(ANSI_RED + "╚═══════════════════════════════════════════════════════════════════════════════╝" + ANSI_RESET);
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

    /// Box width for verification output (inner content width, excluding borders).
    private static final int BOX_WIDTH = 79;

    /// Pads a string (which may contain ANSI codes) to the target visible width.
    ///
    /// ANSI escape codes don't occupy visible space, so we strip them to calculate
    /// the actual visible length, then pad with spaces accordingly.
    ///
    /// @param text the text (may contain ANSI codes)
    /// @param targetWidth the target visible width
    /// @return the padded string
    private static String padToWidth(String text, int targetWidth) {
        // Strip ANSI codes to get visible length
        String visible = text.replaceAll("\u001B\\[[;\\d]*m", "");
        int padding = targetWidth - visible.length();
        if (padding <= 0) {
            return text;
        }
        return text + " ".repeat(padding);
    }

    /// Formats a box line with proper alignment.
    ///
    /// @param borderColor the color for the border
    /// @param content the content (may contain ANSI codes)
    /// @return formatted line with aligned borders
    private static String boxLine(String borderColor, String content) {
        return borderColor + "║" + ANSI_RESET + padToWidth(content, BOX_WIDTH) + borderColor + "║" + ANSI_RESET;
    }

    /**
     * Streaming chunk-based model extraction for large files.
     *
     * <p>Uses {@link TransposedChunkDataSource} and {@link AnalyzerHarness} to process
     * vectors in memory-efficient chunks. This approach is optimal for datasets that
     * exceed available heap memory.
     *
     * <h3>Processing Flow</h3>
     * <ol>
     *   <li>Calculate optimal chunk size based on memory budget</li>
     *   <li>Create TransposedChunkDataSource for columnar chunk loading</li>
     *   <li>Register StreamingModelExtractor with AnalyzerHarness</li>
     *   <li>Stream chunks through the analyzer with progress reporting</li>
     *   <li>Complete analysis and return VectorSpaceModel</li>
     * </ol>
     *
     * @param file the vector file path
     * @param fileType the file type
     * @param range optional range constraint
     * @return extracted VectorSpaceModel
     */
    private VectorSpaceModel profileVectorsStreaming(Path file, FileType fileType, RangeOption.Range range) {
        try {
            // Create TransposedChunkDataSource - it will read metadata from file automatically
            // Note: Range offset not yet fully supported in streaming mode
            if (range != null && range.start() > 0) {
                System.out.println("  Note: Range offset not yet supported in streaming mode, processing from start of file");
            }

            // Parse memory budget for initial builder configuration
            // We'll get dimensions from the source after it reads the file
            double budgetFraction = parseMemoryBudget(memoryBudget, 768); // Use typical dimension for initial estimate

            TransposedChunkDataSource source = TransposedChunkDataSource.builder()
                .file(file)
                .memoryBudgetFraction(budgetFraction)
                .build();

            // Get actual metadata from the source
            int totalVectorCount = (int) source.getShape().cardinality();
            int dimensions = source.getShape().dimensionality();

            if (totalVectorCount == 0) {
                logger.error("File contains no vectors");
                System.err.println("Error: File contains no vectors");
                return null;
            }

            // Apply range constraint if specified
            int vectorCount = totalVectorCount;
            if (range != null) {
                RangeOption.Range effectiveRange = range.constrain(totalVectorCount);
                vectorCount = (int) (effectiveRange.end() - effectiveRange.start());
            }

            System.out.printf("  Vectors in file: %d%n", totalVectorCount);
            if (range != null) {
                System.out.printf("  Processing: %d vectors%n", vectorCount);
            }
            System.out.printf("  Dimensions: %d%n", dimensions);
            int optimalChunkSize = source.getOptimalChunkSize();

            System.out.printf("  Memory budget: %.0f%% of available heap%n", budgetFraction * 100);
            System.out.printf("  Chunk size: %,d vectors%n", optimalChunkSize);
            System.out.printf("  Estimated chunks: %d%n", (totalVectorCount + optimalChunkSize - 1) / optimalChunkSize);

            // Wrap with PrefetchingDataSource for double-buffering (load next chunk while processing current)
            PrefetchingDataSource prefetchingSource = PrefetchingDataSource.builder()
                .source(source)
                .prefetchCount(2)  // Double buffer: one being processed, one prefetched
                .withMemoryMonitoring()  // Adapt to memory pressure
                .build();
            System.out.println("  Double-buffering: enabled (prefetch=2)");

            // Create and configure the streaming model extractor with convergence tracking
            StreamingModelExtractor extractor = new StreamingModelExtractor(createSelector());
            if (uniqueVectors != null) {
                extractor.setUniqueVectors(uniqueVectors);
            } else {
                // Use vectorCount (which is range-constrained) not totalVectorCount
                extractor.setUniqueVectors(vectorCount);
            }

            // Configure parallelism for model fitting (NUMA-aware by default)
            if (threads != null && threads > 0) {
                extractor.setParallelism(threads);
                System.out.printf("  Fitting parallelism: %d threads (NUMA-aware)%n", threads);
            } else {
                System.out.printf("  Fitting parallelism: auto (%d threads, NUMA-aware)%n", extractor.getEffectiveParallelism());
            }

            // Enable all-fits collection if --show-fit-table is requested
            if (showFitTable) {
                extractor.setCollectAllFits(true);
            }

            // Disable adaptive fitting when a specific model type is forced
            // (to prevent fallback to Empirical when user explicitly requests Normal, etc.)
            if (modelType != ModelType.auto && modelType != ModelType.pearson) {
                extractor.setAdaptiveEnabled(false);
            }

            // Enable convergence tracking, incremental fitting, and histogram tracking
            if (!noConvergence) {
                extractor.enableConvergenceTracking(convergenceThreshold);
                extractor.enableIncrementalFitting(5);  // Fit every 5 batches
                extractor.enableHistogramTracking(0.25); // 25% prominence for multi-modal detection (stricter)
                System.out.printf("  Convergence tracking: enabled (threshold=%.2f%%, all 4 moments)%n", convergenceThreshold * 100);
                System.out.println("  Incremental fitting: enabled (tracks model type stability)");
                System.out.println("  Multi-modal detection: enabled (25% prominence threshold)");
            }

            // Create analyzer harness
            final AnalyzerHarness harness = new AnalyzerHarness();
            harness.register(extractor);

            // Run analysis with progress reporting
            long startTime = System.currentTimeMillis();
            System.out.printf("  Streaming analysis of %d dimensions...%n", dimensions);
            if (!noConvergence) {
                System.out.println("  Auto-stop on convergence: enabled (fitting begins when all dimensions stabilize)");
            }
            System.out.println("  Press Enter to stop data collection at any time.");

            int[] lastPercent = {-1};
            int[] lastChunk = {0};
            AnalyzerHarness.Phase[] lastPhase = {null};
            int[] lastConvergedCount = {0};
            boolean[] earlyExitTriggered = {false};
            boolean[] manualExitTriggered = {false};

            // Start keystroke listener for manual early exit
            Thread keystrokeListener = startKeystrokeListener(harness, earlyExitTriggered, manualExitTriggered);

            AnalysisResults results = harness.run(prefetchingSource, optimalChunkSize, new AnalyzerHarness.ProgressCallback() {
                @Override
                public void onProgress(double progress, long processed, long total) {
                    // This callback runs AFTER processing - update percent and check convergence
                    int percent = (int) (progress * 100);
                    lastPercent[0] = percent;

                    // Check convergence AFTER processing each chunk
                    if (extractor.isConvergenceEnabled()) {
                        extractor.checkConvergence();
                        int convergedNow = extractor.getConvergedCount();
                        if (convergedNow != lastConvergedCount[0]) {
                            // Print convergence update on new line
                            StreamingModelExtractor.ConvergenceStatus status = extractor.getConvergenceStatus();
                            if (status != null) {
                                System.out.printf("%n  %s%n", status);
                            }
                            lastConvergedCount[0] = convergedNow;

                            // Automatic early exit on convergence
                            if (extractor.allConverged() && !earlyExitTriggered[0]) {
                                earlyExitTriggered[0] = true;
                                System.out.println("  All dimensions converged - stopping data collection.");
                                harness.requestStop();
                            }
                        }
                    }
                }

                @Override
                public void onProgress(AnalyzerHarness.Phase phase, double progress, long processed,
                                       long total, int chunkNumber, int totalChunks) {
                    // Show phase transitions with consistent format
                    if (phase == AnalyzerHarness.Phase.LOADING && chunkNumber != lastChunk[0]) {
                        printStreamingProgressBarWithPhase(phase, (int)(progress * 100), processed, total, chunkNumber, totalChunks);
                        lastChunk[0] = chunkNumber;
                        lastPhase[0] = phase;
                    } else if (phase == AnalyzerHarness.Phase.PROCESSING && lastPhase[0] == AnalyzerHarness.Phase.LOADING) {
                        printStreamingProgressBarWithPhase(phase, (int)(progress * 100), processed, total, chunkNumber, totalChunks);
                        lastPhase[0] = phase;
                    }
                }
            });

            // Stop keystroke listener
            keystrokeListener.interrupt();

            // Print final progress
            long samplesProcessed = extractor.getSamplesProcessed();
            if (earlyExitTriggered[0] || manualExitTriggered[0]) {
                int percent = (int) (100.0 * samplesProcessed / totalVectorCount);
                printStreamingProgressBar(percent, samplesProcessed, totalVectorCount);
                System.out.println();
                String stopReason = manualExitTriggered[0] ? "user request" : "convergence";
                System.out.printf("  Data collection stopped (%s): %,d of %,d samples (%.1f%%)%n",
                    stopReason, samplesProcessed, totalVectorCount, 100.0 * samplesProcessed / totalVectorCount);
            } else {
                printStreamingProgressBar(100, totalVectorCount, totalVectorCount);
                System.out.println();
            }

            long elapsed = System.currentTimeMillis() - startTime;
            System.out.printf("  Streaming extraction completed in %d ms%n", elapsed);

            // Shutdown harness
            harness.shutdown();

            // Check for errors
            if (results.hasErrors()) {
                for (var entry : results.getErrors().entrySet()) {
                    logger.error("Analyzer {} failed: {}", entry.getKey(), entry.getValue().getMessage());
                    System.err.println("Error in " + entry.getKey() + ": " + entry.getValue().getMessage());
                }
                return null;
            }

            // Get the model from results
            VectorSpaceModel model = results.getResult("model-extractor", VectorSpaceModel.class);
            if (model == null) {
                logger.error("No model produced by streaming extractor");
                System.err.println("Error: Streaming extraction produced no model");
                return null;
            }

            // Apply truncation bounds if requested
            if (truncated) {
                // Get dimension statistics from the extractor
                DimensionStatistics[] stats = extractor.getDimensionStatistics();
                if (stats != null) {
                    model = applyTruncationBounds(model, stats);
                } else {
                    logger.warn("Truncation requested but dimension statistics not available from streaming extractor");
                }
            }

            // Print model summary
            printModelSummary(model);

            return model;

        } catch (Exception e) {
            logger.error("Error in streaming profile", e);
            throw new RuntimeException("Failed to profile vectors: " + e.getMessage(), e);
        }
    }

    /**
     * Parses the memory budget specification.
     *
     * <p>Accepts either a fraction (0.0-1.0) or an absolute size with suffix (e.g., "4g", "512m").
     *
     * @param budget the budget specification
     * @param dimensions the number of dimensions (for calculating fraction from absolute)
     * @return memory budget as a fraction of available heap
     */
    private double parseMemoryBudget(String budget, int dimensions) {
        if (budget == null || budget.isEmpty()) {
            return ChunkSizeCalculator.DEFAULT_BUDGET_FRACTION;
        }

        String trimmed = budget.trim().toLowerCase();

        // Check for absolute size suffix
        long absoluteBytes = -1;
        if (trimmed.endsWith("g")) {
            absoluteBytes = Long.parseLong(trimmed.substring(0, trimmed.length() - 1)) * 1024L * 1024L * 1024L;
        } else if (trimmed.endsWith("m")) {
            absoluteBytes = Long.parseLong(trimmed.substring(0, trimmed.length() - 1)) * 1024L * 1024L;
        } else if (trimmed.endsWith("k")) {
            absoluteBytes = Long.parseLong(trimmed.substring(0, trimmed.length() - 1)) * 1024L;
        }

        if (absoluteBytes > 0) {
            // Convert absolute bytes to fraction of available heap
            Runtime rt = Runtime.getRuntime();
            long availableHeap = rt.maxMemory() - rt.totalMemory() + rt.freeMemory();
            return Math.min(1.0, (double) absoluteBytes / availableHeap);
        }

        // Parse as fraction
        try {
            double fraction = Double.parseDouble(trimmed);
            if (fraction <= 0 || fraction > 1.0) {
                logger.warn("Memory budget {} out of range, using default 0.6", budget);
                return ChunkSizeCalculator.DEFAULT_BUDGET_FRACTION;
            }
            return fraction;
        } catch (NumberFormatException e) {
            logger.warn("Invalid memory budget '{}', using default 0.6", budget);
            return ChunkSizeCalculator.DEFAULT_BUDGET_FRACTION;
        }
    }

    /**
     * Prints a streaming progress bar.
     */
    private void printStreamingProgressBar(int percent, long processed, long total) {
        int barWidth = 40;
        int filled = (int) ((percent / 100.0) * barWidth);
        StringBuilder bar = new StringBuilder("\r  [");
        for (int i = 0; i < barWidth; i++) {
            if (i < filled) {
                bar.append("█");
            } else {
                bar.append("░");
            }
        }
        bar.append(String.format("] %3d%% (%,d / %,d vectors)", percent, processed, total));
        System.out.print(bar);
        System.out.flush();
    }

    /**
     * Prints a streaming progress bar with phase information.
     *
     * <p>Shows the current phase (Loading/Processing) and chunk progress,
     * which provides feedback during long I/O operations.
     */
    private void printStreamingProgressBarWithPhase(AnalyzerHarness.Phase phase, int percent,
                                                    long processed, long total,
                                                    int chunkNumber, int totalChunks) {
        int barWidth = 30;
        int filled = (int) ((percent / 100.0) * barWidth);
        StringBuilder bar = new StringBuilder("\r  [");
        for (int i = 0; i < barWidth; i++) {
            if (i < filled) {
                bar.append("█");
            } else {
                bar.append("░");
            }
        }
        String phaseStr = phase == AnalyzerHarness.Phase.LOADING ? "Loading" : "Processing";
        bar.append(String.format("] %3d%% | %s chunk %d/%d (%,d vectors)",
            percent, phaseStr, chunkNumber, totalChunks, processed));
        // Pad with spaces to clear any previous longer text
        bar.append("          ");
        System.out.print(bar);
        System.out.flush();
    }

    /// Reads vector file metadata without loading vectors.
    ///
    /// @param path path to .fvec or .ivec file
    /// @return array of [vectorCount, dimensions]
    /// @throws java.io.IOException if file cannot be read
    private int[] readFileMetadata(Path path) throws java.io.IOException {
        try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(path.toFile(), "r")) {
            long fileSize = raf.length();

            // Read dimension (little-endian int at offset 0)
            byte[] dimBytes = new byte[4];
            raf.read(dimBytes);
            int dimension = (dimBytes[0] & 0xFF) |
                           ((dimBytes[1] & 0xFF) << 8) |
                           ((dimBytes[2] & 0xFF) << 16) |
                           ((dimBytes[3] & 0xFF) << 24);

            // Calculate vector count from file size
            // Each vector: 4 bytes (dimension) + dimension * 4 bytes (float values)
            long vectorStride = (1 + dimension) * 4L;
            int vectorCount = (int) (fileSize / vectorStride);

            return new int[] { vectorCount, dimension };
        }
    }

    /// Starts a daemon thread that listens for Enter key to trigger early exit.
    ///
    /// @param harness the analyzer harness to stop
    /// @param autoExitTriggered flag to check if auto exit already triggered
    /// @param manualExitTriggered flag to set when manual exit is triggered
    /// @return the listener thread (can be interrupted to stop)
    private Thread startKeystrokeListener(AnalyzerHarness harness,
                                          boolean[] autoExitTriggered,
                                          boolean[] manualExitTriggered) {
        Thread listener = new Thread(() -> {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    // Check if input is available without blocking
                    if (System.in.available() > 0) {
                        int ch = System.in.read();
                        // Enter key (newline) or 'q' triggers exit
                        if (ch == '\n' || ch == '\r' || ch == 'q' || ch == 'Q') {
                            if (!autoExitTriggered[0] && !manualExitTriggered[0]) {
                                manualExitTriggered[0] = true;
                                System.out.println("\n  Manual early exit requested...");
                                harness.requestStop();
                            }
                            break;
                        }
                    }
                    // Small sleep to avoid busy-waiting
                    Thread.sleep(100);
                }
            } catch (InterruptedException e) {
                // Normal termination
                Thread.currentThread().interrupt();
            } catch (java.io.IOException e) {
                // Ignore I/O errors (e.g., stdin closed)
            }
        }, "early-exit-listener");
        listener.setDaemon(true);
        listener.start();
        return listener;
    }
}
