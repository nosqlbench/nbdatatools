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
import io.nosqlbench.datatools.virtdata.DimensionDistributionGenerator;
import io.nosqlbench.nbdatatools.api.fileio.VectorFileStreamStore;
import io.nosqlbench.nbdatatools.api.services.FileType;
import io.nosqlbench.nbdatatools.api.services.VectorFileIO;
import io.nosqlbench.vshapes.model.VectorSpaceModel;
import io.nosqlbench.vshapes.model.VectorSpaceModelConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Callable;

/**
 * Command to generate vectors from an existing VectorSpaceModel JSON file.
 *
 * <p>This command loads a model that was previously extracted using
 * {@code analyze profile} and generates synthetic vectors that follow
 * the same statistical distributions.
 *
 * <h2>Usage Examples</h2>
 * <pre>{@code
 * # Generate 100,000 vectors from an extracted model
 * nbvectors generate from-model -m extracted_model.json -o synthetic.fvec -n 100000 --format xvec
 *
 * # Generate with specific seed for reproducibility
 * nbvectors generate from-model -m model.json -o output.fvec -n 50000 --format xvec --seed 42
 * }</pre>
 */
@CommandLine.Command(name = "from-model",
    description = "Generate vectors from a VectorSpaceModel JSON file")
public class CMD_generate_from_model implements Callable<Integer> {
    private static final Logger logger = LogManager.getLogger(CMD_generate_from_model.class);

    private static final int EXIT_SUCCESS = 0;
    private static final int EXIT_FILE_EXISTS = 1;
    private static final int EXIT_ERROR = 2;

    @CommandLine.Option(names = {"-m", "--model"},
        description = "Input VectorSpaceModel JSON file",
        required = true)
    private Path modelFile;

    @CommandLine.Mixin
    private OutputFileOption outputFileOption = new OutputFileOption();

    @CommandLine.Mixin
    private RandomSeedOption randomSeedOption = new RandomSeedOption();

    @CommandLine.Option(names = {"-n", "--count"},
        description = "Number of vectors to generate",
        required = true)
    private int count;

    @CommandLine.Option(names = {"--format"},
        description = "Output file format (${COMPLETION-CANDIDATES})",
        required = true)
    private FileType format;

    @CommandLine.Option(names = {"--verbose", "-v"},
        description = "Show detailed progress information")
    private boolean verbose = false;

    @CommandLine.Spec
    private CommandLine.Model.CommandSpec spec;

    @Override
    public Integer call() throws Exception {
        // Validate model file
        if (!Files.exists(modelFile)) {
            System.err.println("Error: Model file does not exist: " + modelFile);
            return EXIT_ERROR;
        }

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

        try {
            // Load the model
            if (verbose) {
                System.out.println("Loading model from: " + modelFile);
            }
            VectorSpaceModel model = VectorSpaceModelConfig.loadFromFile(modelFile);

            int dimensions = model.dimensions();
            if (verbose) {
                System.out.println("  Dimensions: " + dimensions);
                System.out.println("  Target vectors: " + count);
                System.out.println("  Seed: " + randomSeedOption.getSeed());
            }

            // Create parent directories if needed
            Path parent = outputPath.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }

            // Create the generator
            DimensionDistributionGenerator generator = new DimensionDistributionGenerator(model);

            // Generate and write vectors
            if (verbose) {
                System.out.println("Generating vectors...");
            }

            try (VectorFileStreamStore<float[]> store = (VectorFileStreamStore<float[]>)
                    VectorFileIO.streamOut(format, float[].class, outputPath)
                    .orElseThrow(() -> new RuntimeException("Could not create vector file store for format: " + format))) {

                int progressInterval = Math.max(count / 10, 1000);
                long startTime = System.currentTimeMillis();

                for (int i = 0; i < count; i++) {
                    float[] vector = generator.apply(i);
                    store.write(vector);

                    if (verbose && i > 0 && i % progressInterval == 0) {
                        double percent = (double) i / count * 100;
                        System.out.printf("  [%3.0f%%] Generated %,d of %,d vectors%n", percent, i, count);
                    }
                }

                long elapsed = System.currentTimeMillis() - startTime;
                double throughput = count * 1000.0 / elapsed;

                // Print summary
                System.out.println();
                System.out.println("Generation complete:");
                System.out.printf("  Output file: %s%n", outputPath);
                System.out.printf("  Vectors: %,d%n", count);
                System.out.printf("  Dimensions: %d%n", dimensions);
                System.out.printf("  Time: %,d ms%n", elapsed);
                System.out.printf("  Throughput: %,.0f vectors/sec%n", throughput);
            }

            return EXIT_SUCCESS;

        } catch (Exception e) {
            System.err.println("Error generating vectors: " + e.getMessage());
            if (verbose) {
                e.printStackTrace();
            }
            return EXIT_ERROR;
        }
    }

    public static void main(String[] args) {
        CMD_generate_from_model cmd = new CMD_generate_from_model();
        int exitCode = new CommandLine(cmd).execute(args);
        System.exit(exitCode);
    }
}
