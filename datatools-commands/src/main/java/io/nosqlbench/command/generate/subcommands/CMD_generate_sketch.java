package io.nosqlbench.command.generate.subcommands;

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

import io.nosqlbench.command.common.OutputFileOption;
import io.nosqlbench.command.common.RandomSeedOption;
import io.nosqlbench.command.common.VectorSpecOption;
import io.nosqlbench.datatools.virtdata.DimensionDistributionGenerator;
import io.nosqlbench.datatools.virtdata.NormalizingVectorGenerator;
import io.nosqlbench.nbdatatools.api.fileio.VectorFileStreamStore;
import io.nosqlbench.nbdatatools.api.services.FileType;
import io.nosqlbench.nbdatatools.api.services.VectorFileIO;
import io.nosqlbench.vshapes.model.*;
import io.nosqlbench.vshapes.model.CompositeScalarModel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;

/**
 * Command to generate a "sketch" vector dataset with known distribution properties.
 *
 * <p>This command creates a reference dataset from CLI-specified distribution types,
 * enabling users to follow the verification walkthrough without needing an existing
 * dataset. The distribution parameters are deterministic based on the dimension index,
 * making the generated model reproducible and verifiable.
 *
 * <h2>Distribution Mix Options</h2>
 *
 * <p>The {@code --mix} option specifies which distribution types to include:
 * <ul>
 *   <li>{@code bounded} (default) - Mix of Normal, Beta, Uniform (for bounded data like embeddings)</li>
 *   <li>{@code normal-only} - All dimensions use truncated Normal distribution</li>
 *   <li>{@code beta-only} - All dimensions use Beta distribution</li>
 *   <li>{@code uniform-only} - All dimensions use Uniform distribution</li>
 *   <li>{@code mixed} - Rotating mix of Normal, Beta, and Uniform</li>
 *   <li>{@code full} - All distribution types including multimodal composites (2-4 modes)</li>
 * </ul>
 *
 * <h2>Multimodal Support</h2>
 *
 * <p>When using {@code --mix=full}, the dataset includes composite (multimodal) distributions
 * with 2, 3, and 4 modes. Use {@code --max-modes} to control the maximum number of modes
 * (default: 4). This is useful for testing empirical fallback thresholds in the
 * {@code analyze profile --multimodal} command.
 *
 * <h2>Example Usage</h2>
 * <pre>{@code
 * # Generate bounded dataset with ground truth model
 * nbvectors generate sketch -d 128 -n 100000 -o reference.fvec --format FVEC --model-out ground_truth.json
 *
 * # Generate with specific mix
 * nbvectors generate sketch -d 64 -n 50000 -o test.fvec --format FVEC --mix mixed
 *
 * # Generate full mix with multimodal distributions (up to 4 modes)
 * nbvectors generate sketch -d 128 -n 100000 -o full_test.fvec --format FVEC --mix full --model-out ground_truth.json
 *
 * # Generate with limited modes (max 3)
 * nbvectors generate sketch -d 128 -n 100000 -o test.fvec --format FVEC --mix full --max-modes 3
 * }</pre>
 */
@CommandLine.Command(name = "sketch",
    description = "Generate a reference vector dataset with known distribution properties for verification testing")
public class CMD_generate_sketch implements Callable<Integer> {
    private static final Logger logger = LogManager.getLogger(CMD_generate_sketch.class);

    private static final int EXIT_SUCCESS = 0;
    private static final int EXIT_FILE_EXISTS = 1;
    private static final int EXIT_ERROR = 2;

    @CommandLine.Mixin
    private OutputFileOption outputFileOption = new OutputFileOption();

    @CommandLine.Mixin
    private VectorSpecOption vectorSpecOption = new VectorSpecOption();

    @CommandLine.Mixin
    private RandomSeedOption randomSeedOption = new RandomSeedOption();

    @CommandLine.Option(names = {"--format"},
        description = "Output file format (${COMPLETION-CANDIDATES})",
        required = true)
    private FileType format;

    @CommandLine.Option(names = {"--mix"},
        description = "Distribution mix type: bounded (default), normal-only, beta-only, uniform-only, mixed, full",
        defaultValue = "bounded")
    private String distributionMix = "bounded";

    @CommandLine.Option(names = {"--max-modes"},
        description = "Maximum number of modes for multimodal distributions (default: 4, used with --mix=full)",
        defaultValue = "4")
    private int maxModes = 4;

    @CommandLine.Option(names = {"--model-out"},
        description = "Optional path to write the ground-truth model JSON")
    private Path modelOutputPath;

    @CommandLine.Option(names = {"--lower"},
        description = "Lower bound for all distributions (default: -1.0)",
        defaultValue = "-1.0")
    private double lowerBound = -1.0;

    @CommandLine.Option(names = {"--upper"},
        description = "Upper bound for all distributions (default: 1.0)",
        defaultValue = "1.0")
    private double upperBound = 1.0;

    @CommandLine.Option(names = {"--verbose", "-v"},
        description = "Show detailed progress information")
    private boolean verbose = false;

    @CommandLine.Option(names = {"--normalize", "--normalized"},
        description = "Normalize vectors to unit length (L2 norm = 1.0). Default: true",
        defaultValue = "true",
        negatable = true)
    private boolean normalize = true;

    @CommandLine.Spec
    private CommandLine.Model.CommandSpec spec;

    /**
     * Distribution mix strategies.
     */
    public enum DistributionMix {
        BOUNDED,       // Mix of Normal, Beta, Uniform - good for bounded embeddings
        NORMAL_ONLY,   // All truncated Normal
        BETA_ONLY,     // All Beta
        UNIFORM_ONLY,  // All Uniform
        MIXED,         // Rotating mix for verification testing
        FULL           // All distribution types including multimodal (1-4 modes)
    }

    @Override
    public Integer call() throws Exception {
        // Validate output path
        Path outputPath = outputFileOption.getNormalizedOutputPath();
        if (outputPath == null) {
            System.err.println("Error: No output path provided");
            return EXIT_ERROR;
        }

        if (outputFileOption.outputExistsWithoutForce()) {
            System.err.println("Error: Output file already exists. Use --force to overwrite.");
            return EXIT_FILE_EXISTS;
        }

        // Parse distribution mix
        DistributionMix mix;
        try {
            mix = parseDistributionMix(distributionMix);
        } catch (IllegalArgumentException e) {
            System.err.println("Error: " + e.getMessage());
            return EXIT_ERROR;
        }

        // Validate vector spec
        try {
            vectorSpecOption.validate();
        } catch (IllegalArgumentException e) {
            System.err.println("Error: " + e.getMessage());
            return EXIT_ERROR;
        }

        int dimension = vectorSpecOption.getDimension();
        int count = vectorSpecOption.getCount();
        long seed = randomSeedOption.getSeed();

        if (verbose) {
            System.out.println("Generating sketch dataset:");
            System.out.println("  Dimensions: " + dimension);
            System.out.println("  Vectors: " + count);
            System.out.println("  Distribution mix: " + mix);
            if (mix == DistributionMix.FULL) {
                System.out.println("  Max modes: " + maxModes);
            }
            System.out.println("  Bounds: [" + lowerBound + ", " + upperBound + "]");
            System.out.println("  Normalize: " + normalize);
            System.out.println("  Seed: " + seed);
            System.out.println();
        }

        try {
            // Create parent directories if needed
            Path parent = outputPath.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }

            // Build the scalar models based on mix strategy
            ScalarModel[] scalarModels = buildScalarModels(dimension, mix, seed);

            // Create the VectorSpaceModel
            VectorSpaceModel model = new VectorSpaceModel(count, scalarModels);

            // Create the generator, with optional L2 normalization
            java.util.function.LongFunction<float[]> generator;
            DimensionDistributionGenerator baseGenerator = new DimensionDistributionGenerator(model);
            if (normalize) {
                generator = new NormalizingVectorGenerator(baseGenerator);
            } else {
                generator = baseGenerator;
            }

            // Write the ground truth model if requested
            if (modelOutputPath != null) {
                if (verbose) {
                    System.out.println("Writing ground-truth model to: " + modelOutputPath);
                }
                VectorSpaceModelConfig.saveToFile(model, modelOutputPath);
            }

            // Generate and write vectors
            if (verbose) {
                System.out.println("Generating vectors...");
            }

            try (VectorFileStreamStore<float[]> store = (VectorFileStreamStore<float[]>)
                    VectorFileIO.streamOut(format, float[].class, outputPath)
                    .orElseThrow(() -> new RuntimeException("Could not create vector file store for format: " + format))) {

                int progressInterval = Math.max(count / 10, 1000);
                for (int i = 0; i < count; i++) {
                    float[] vector = generator.apply(i);
                    store.write(vector);

                    if (verbose && i > 0 && i % progressInterval == 0) {
                        double percent = (double) i / count * 100;
                        System.out.printf("  Generated %,d of %,d vectors (%.1f%%)%n", i, count, percent);
                    }
                }
            }

            // Print summary
            System.out.println();
            System.out.println("╔═══════════════════════════════════════════════════════════════════╗");
            System.out.println("║                    SKETCH DATASET GENERATED                       ║");
            System.out.println("╠═══════════════════════════════════════════════════════════════════╣");
            System.out.printf("║  Output file: %-52s ║%n", truncatePath(outputPath.toString(), 52));
            System.out.printf("║  Vectors: %,15d                                        ║%n", count);
            System.out.printf("║  Dimensions: %,12d                                        ║%n", dimension);
            System.out.printf("║  Format: %-56s ║%n", format);
            System.out.printf("║  Distribution mix: %-46s ║%n", mix);
            System.out.printf("║  Bounds: [%.2f, %.2f]                                           ║%n", lowerBound, upperBound);
            System.out.printf("║  Normalized: %-52s ║%n", normalize ? "yes (L2 unit vectors)" : "no");
            System.out.printf("║  Seed: %-58d ║%n", seed);
            if (modelOutputPath != null) {
                System.out.println("╠═══════════════════════════════════════════════════════════════════╣");
                System.out.printf("║  Ground-truth model: %-44s ║%n", truncatePath(modelOutputPath.toString(), 44));
            }
            System.out.println("╚═══════════════════════════════════════════════════════════════════╝");

            printDistributionSummary(scalarModels);

            return EXIT_SUCCESS;

        } catch (IOException e) {
            System.err.println("Error: I/O problem when writing to file - " + e.getMessage());
            if (verbose) {
                e.printStackTrace();
            }
            return EXIT_ERROR;
        } catch (Exception e) {
            System.err.println("Error generating sketch dataset: " + e.getMessage());
            if (verbose) {
                e.printStackTrace();
            }
            return EXIT_ERROR;
        }
    }

    /**
     * Parses the distribution mix string into an enum.
     */
    private DistributionMix parseDistributionMix(String mixStr) {
        return switch (mixStr.toLowerCase().replace("-", "_").replace(" ", "_")) {
            case "bounded" -> DistributionMix.BOUNDED;
            case "normal_only", "normal" -> DistributionMix.NORMAL_ONLY;
            case "beta_only", "beta" -> DistributionMix.BETA_ONLY;
            case "uniform_only", "uniform" -> DistributionMix.UNIFORM_ONLY;
            case "mixed" -> DistributionMix.MIXED;
            case "full", "comprehensive", "all" -> DistributionMix.FULL;
            default -> throw new IllegalArgumentException(
                "Unknown distribution mix: " + mixStr +
                ". Valid options: bounded, normal-only, beta-only, uniform-only, mixed, full");
        };
    }

    /**
     * Builds scalar models for each dimension based on the mix strategy.
     *
     * <p>Parameters are derived deterministically from the dimension index
     * to ensure reproducibility.
     */
    private ScalarModel[] buildScalarModels(int dimensions, DistributionMix mix, long seed) {
        ScalarModel[] models = new ScalarModel[dimensions];
        Random paramRng = new Random(seed);  // For deterministic parameter variation

        for (int d = 0; d < dimensions; d++) {
            models[d] = switch (mix) {
                case NORMAL_ONLY -> createNormalModel(d, paramRng);
                case BETA_ONLY -> createBetaModel(d, paramRng);
                case UNIFORM_ONLY -> createUniformModel(d, paramRng);
                case BOUNDED -> createBoundedMixModel(d, paramRng);
                case MIXED -> createMixedModel(d, paramRng);
                case FULL -> createFullMixModel(d, paramRng);
            };
        }

        return models;
    }

    /**
     * Creates a truncated Normal model with parameters varying by dimension.
     */
    private ScalarModel createNormalModel(int dimension, Random rng) {
        // Mean varies around center of bounds
        double center = (lowerBound + upperBound) / 2.0;
        double range = upperBound - lowerBound;

        // Vary mean within central 50% of range
        double mean = center + (rng.nextDouble() - 0.5) * range * 0.5;

        // StdDev varies from 10% to 40% of range
        double stdDev = range * (0.1 + rng.nextDouble() * 0.3);

        return new NormalScalarModel(mean, stdDev, lowerBound, upperBound);
    }

    /**
     * Creates a Beta model with parameters varying by dimension.
     */
    private ScalarModel createBetaModel(int dimension, Random rng) {
        // Alpha and Beta parameters between 1.0 and 5.0 (unimodal, varied shapes)
        double alpha = 1.0 + rng.nextDouble() * 4.0;
        double beta = 1.0 + rng.nextDouble() * 4.0;

        return new BetaScalarModel(alpha, beta, lowerBound, upperBound);
    }

    /**
     * Creates a Uniform model with parameters varying by dimension.
     */
    private ScalarModel createUniformModel(int dimension, Random rng) {
        // Slightly vary the bounds for each dimension
        double range = upperBound - lowerBound;
        double margin = range * 0.1 * rng.nextDouble();

        double lo = lowerBound + margin * rng.nextDouble();
        double hi = upperBound - margin * rng.nextDouble();

        // Ensure lo < hi
        if (lo >= hi) {
            lo = lowerBound;
            hi = upperBound;
        }

        return new UniformScalarModel(lo, hi);
    }

    /**
     * Creates a mix suitable for bounded data (embeddings).
     * Uses 50% Normal, 30% Beta, 20% Uniform.
     */
    private ScalarModel createBoundedMixModel(int dimension, Random rng) {
        int choice = dimension % 10;
        if (choice < 5) {
            // 50% Normal
            return createNormalModel(dimension, rng);
        } else if (choice < 8) {
            // 30% Beta
            return createBetaModel(dimension, rng);
        } else {
            // 20% Uniform
            return createUniformModel(dimension, rng);
        }
    }

    /**
     * Creates a rotating mix of all distribution types for verification.
     * Equal distribution: 33% each.
     */
    private ScalarModel createMixedModel(int dimension, Random rng) {
        int choice = dimension % 3;
        return switch (choice) {
            case 0 -> createNormalModel(dimension, rng);
            case 1 -> createBetaModel(dimension, rng);
            case 2 -> createUniformModel(dimension, rng);
            default -> createNormalModel(dimension, rng);
        };
    }

    /**
     * Creates a comprehensive mix including all distribution types and multimodal.
     *
     * <p>Distribution breakdown designed to test empirical fallback thresholds:
     * <ul>
     *   <li>~25% Normal (unimodal)</li>
     *   <li>~20% Beta (unimodal)</li>
     *   <li>~15% Uniform (unimodal)</li>
     *   <li>~15% 2-mode composite</li>
     *   <li>~15% 3-mode composite</li>
     *   <li>~10% 4-mode composite (or up to maxModes)</li>
     * </ul>
     */
    private ScalarModel createFullMixModel(int dimension, Random rng) {
        int choice = dimension % 20;
        if (choice < 5) {
            // 25% Normal
            return createNormalModel(dimension, rng);
        } else if (choice < 9) {
            // 20% Beta
            return createBetaModel(dimension, rng);
        } else if (choice < 12) {
            // 15% Uniform
            return createUniformModel(dimension, rng);
        } else if (choice < 15) {
            // 15% 2-mode composite
            return createCompositeModel(dimension, rng, 2);
        } else if (choice < 18) {
            // 15% 3-mode composite
            return createCompositeModel(dimension, rng, Math.min(3, maxModes));
        } else {
            // 10% 4-mode composite (or maxModes)
            return createCompositeModel(dimension, rng, Math.min(4, maxModes));
        }
    }

    /**
     * Creates a composite (multimodal) model with the specified number of modes.
     *
     * <p>Each mode is a different distribution type (Normal, Beta, or Uniform)
     * with well-separated peaks to ensure clear multimodality.
     */
    private ScalarModel createCompositeModel(int dimension, Random rng, int numModes) {
        if (numModes < 2) {
            // Fallback to unimodal
            return createNormalModel(dimension, rng);
        }

        ScalarModel[] components = new ScalarModel[numModes];
        double[] weights = new double[numModes];

        double range = upperBound - lowerBound;
        double modeSpacing = range / (numModes + 1);  // Space modes evenly

        for (int m = 0; m < numModes; m++) {
            // Position each mode at evenly spaced intervals
            double modeCenter = lowerBound + modeSpacing * (m + 1);

            // Vary the distribution type for each mode
            int modeType = (dimension + m) % 3;

            // Mode width - narrow enough to show separation
            double modeWidth = modeSpacing * 0.3;

            components[m] = switch (modeType) {
                case 0 -> {
                    // Normal mode
                    double stdDev = modeWidth * (0.3 + rng.nextDouble() * 0.2);
                    yield new NormalScalarModel(modeCenter, stdDev, lowerBound, upperBound);
                }
                case 1 -> {
                    // Beta mode - map to local region
                    double localLower = Math.max(lowerBound, modeCenter - modeWidth);
                    double localUpper = Math.min(upperBound, modeCenter + modeWidth);
                    // Use beta parameters that create a peak
                    double alpha = 2.0 + rng.nextDouble() * 3.0;
                    double beta = 2.0 + rng.nextDouble() * 3.0;
                    yield new BetaScalarModel(alpha, beta, localLower, localUpper);
                }
                case 2 -> {
                    // Uniform mode - narrow band around center
                    double localLower = Math.max(lowerBound, modeCenter - modeWidth * 0.5);
                    double localUpper = Math.min(upperBound, modeCenter + modeWidth * 0.5);
                    yield new UniformScalarModel(localLower, localUpper);
                }
                default -> new NormalScalarModel(modeCenter, modeWidth * 0.4, lowerBound, upperBound);
            };

            // Vary weights slightly (but keep roughly equal)
            weights[m] = 0.8 + rng.nextDouble() * 0.4;
        }

        return new CompositeScalarModel(List.of(components), weights);
    }

    /**
     * Prints a summary of the distribution types used.
     */
    private void printDistributionSummary(ScalarModel[] models) {
        int normalCount = 0;
        int betaCount = 0;
        int uniformCount = 0;
        int compositeCount = 0;
        int[] modeCountBreakdown = new int[5];  // Index = number of modes (2, 3, 4)
        int otherCount = 0;

        for (ScalarModel model : models) {
            if (model instanceof NormalScalarModel) {
                normalCount++;
            } else if (model instanceof BetaScalarModel) {
                betaCount++;
            } else if (model instanceof UniformScalarModel) {
                uniformCount++;
            } else if (model instanceof CompositeScalarModel composite) {
                compositeCount++;
                int modes = composite.getComponentCount();
                if (modes >= 2 && modes <= 4) {
                    modeCountBreakdown[modes]++;
                }
            } else {
                otherCount++;
            }
        }

        System.out.println();
        System.out.println("Distribution Summary:");
        if (normalCount > 0) {
            System.out.printf("  Normal (truncated): %d dimensions (%.1f%%)%n",
                normalCount, 100.0 * normalCount / models.length);
        }
        if (betaCount > 0) {
            System.out.printf("  Beta:               %d dimensions (%.1f%%)%n",
                betaCount, 100.0 * betaCount / models.length);
        }
        if (uniformCount > 0) {
            System.out.printf("  Uniform:            %d dimensions (%.1f%%)%n",
                uniformCount, 100.0 * uniformCount / models.length);
        }
        if (compositeCount > 0) {
            System.out.printf("  Composite:          %d dimensions (%.1f%%)%n",
                compositeCount, 100.0 * compositeCount / models.length);
            // Show mode breakdown
            for (int modes = 2; modes <= 4; modes++) {
                if (modeCountBreakdown[modes] > 0) {
                    System.out.printf("    %d-mode:          %d dimensions%n",
                        modes, modeCountBreakdown[modes]);
                }
            }
        }
        if (otherCount > 0) {
            System.out.printf("  Other:              %d dimensions (%.1f%%)%n",
                otherCount, 100.0 * otherCount / models.length);
        }
        System.out.println();
        System.out.println("You can now use this dataset with 'nbvectors analyze profile' to");
        System.out.println("extract and verify the model, or use it directly for testing.");
        if (compositeCount > 0) {
            System.out.println();
            System.out.println("Tip: Use 'nbvectors analyze profile --multimodal' to enable");
            System.out.println("     composite model detection for the multimodal dimensions.");
        }
    }

    /**
     * Truncates a path string for display.
     */
    private String truncatePath(String path, int maxLen) {
        if (path.length() <= maxLen) {
            return path;
        }
        return "..." + path.substring(path.length() - maxLen + 3);
    }

    public static void main(String[] args) {
        CMD_generate_sketch cmd = new CMD_generate_sketch();
        int exitCode = new CommandLine(cmd).execute(args);
        System.exit(exitCode);
    }
}
