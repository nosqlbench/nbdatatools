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

import io.nosqlbench.nbdatatools.api.fileio.VectorFileStreamStore;
import io.nosqlbench.nbdatatools.api.services.FileType;
import io.nosqlbench.nbdatatools.api.services.VectorFileIO;
import org.apache.commons.rng.RestorableUniformRandomProvider;
import org.apache.commons.rng.sampling.distribution.ContinuousSampler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Callable;

/// Command to generate a file of random vectors with specified type, dimensionality, and count.
/// Supports various vector types like "int[]", "float[]", "double[]", etc.
@CommandLine.Command(name = "vectors",
    description = "Generate a file of random vectors with specified type, dimensionality, and count")
public class CMD_generate_vectors implements Callable<Integer> {
    private static final Logger logger = LogManager.getLogger(CMD_generate_vectors.class);

    private static final int EXIT_SUCCESS = 0;
    private static final int EXIT_FILE_EXISTS = 1;
    private static final int EXIT_ERROR = 2;

    @CommandLine.Option(names = {"-o", "--output"}, description = "The output file", required = true)
    private Path outputPath;

    @CommandLine.Option(names = {"-t", "--type"}, 
        description = "Vector type as Java class signature (e.g., \"int[]\", \"float[]\", \"double[]\")", 
        required = true)
    private String vectorType;

    @CommandLine.Option(names = {"-d", "--dimension"}, 
        description = "Dimensionality of each vector", 
        required = true)
    private int dimension;

    @CommandLine.Option(names = {"-n", "--count"}, 
        description = "Number of vectors to generate", 
        required = true)
    private int count;

    @CommandLine.Option(names = {"-f", "--format"}, 
        description = "Output file format (${COMPLETION-CANDIDATES})", 
        required = true)
    private FileType format;

    @CommandLine.Option(names = {"-s", "--seed"}, 
        description = "Random seed for reproducible generation (0 for non-deterministic)", 
        defaultValue = "0")
    private long seed;

    @CommandLine.Option(names = {"--force"}, 
        description = "Force overwrite if output file already exists")
    private boolean force = false;

    @CommandLine.Option(names = {"-a", "--algorithm"}, 
        description = "PRNG algorithm to use (${COMPLETION-CANDIDATES})", 
        defaultValue = "XO_SHI_RO_256_PP")
    private RandomGenerators.Algorithm algorithm = RandomGenerators.Algorithm.XO_SHI_RO_256_PP;

    @CommandLine.Option(names = {"--min"}, 
        description = "Minimum value for random numbers (for float/double types)", 
        defaultValue = "0.0")
    private double min = 0.0;

    @CommandLine.Option(names = {"--max"}, 
        description = "Maximum value for random numbers (for float/double types)", 
        defaultValue = "1.0")
    private double max = 1.0;

    @CommandLine.Option(names = {"--int-min"}, 
        description = "Minimum value for random integers (for int types)", 
        defaultValue = "0")
    private int intMin = 0;

    @CommandLine.Option(names = {"--int-max"}, 
        description = "Maximum value for random integers (for int types)", 
        defaultValue = "100")
    private int intMax = 100;

    @CommandLine.Spec
    private CommandLine.Model.CommandSpec spec;

    /**
     * Validates the output path before execution.
     */
    private void validateOutputPath() {
        if (outputPath == null) {
            throw new CommandLine.ParameterException(spec.commandLine(),
                "Error: No output path provided");
        }

        // Normalize the path to resolve any "." or ".." components
        outputPath = outputPath.normalize();
    }

    /**
     * Parse the vector type string into a Class object.
     * 
     * @param vectorTypeStr The vector type string (e.g., "int[]", "float[]", "double[]")
     * @return The Class object representing the vector type
     * @throws IllegalArgumentException if the vector type is not supported
     */
    private Class<?> parseVectorType(String vectorTypeStr) {
        switch (vectorTypeStr.toLowerCase()) {
            case "int[]":
                return int[].class;
            case "float[]":
                return float[].class;
            case "double[]":
                return double[].class;
            case "byte[]":
                return byte[].class;
            case "short[]":
                return short[].class;
            case "long[]":
                return long[].class;
            default:
                throw new IllegalArgumentException("Unsupported vector type: " + vectorTypeStr);
        }
    }

    /**
     * Generate a random vector of the specified type and dimension.
     * 
     * @param vectorClass The class of the vector to generate
     * @param dimension The dimension of the vector
     * @param rng The random number generator to use
     * @return A random vector of the specified type and dimension
     */
    private Object generateRandomVector(Class<?> vectorClass, int dimension, RestorableUniformRandomProvider rng) {
        if (vectorClass == int[].class) {
            int[] vector = new int[dimension];
            for (int i = 0; i < dimension; i++) {
                vector[i] = intMin + rng.nextInt(intMax - intMin + 1);
            }
            return vector;
        } else if (vectorClass == float[].class) {
            float[] vector = new float[dimension];
            ContinuousSampler sampler = RandomGenerators.createUniformSampler(rng, min, max);
            for (int i = 0; i < dimension; i++) {
                vector[i] = (float) sampler.sample();
            }
            return vector;
        } else if (vectorClass == double[].class) {
            double[] vector = new double[dimension];
            ContinuousSampler sampler = RandomGenerators.createUniformSampler(rng, min, max);
            for (int i = 0; i < dimension; i++) {
                vector[i] = sampler.sample();
            }
            return vector;
        } else if (vectorClass == byte[].class) {
            byte[] vector = new byte[dimension];
            for (int i = 0; i < dimension; i++) {
                vector[i] = (byte) (intMin + rng.nextInt(intMax - intMin + 1));
            }
            return vector;
        } else if (vectorClass == short[].class) {
            short[] vector = new short[dimension];
            for (int i = 0; i < dimension; i++) {
                vector[i] = (short) (intMin + rng.nextInt(intMax - intMin + 1));
            }
            return vector;
        } else if (vectorClass == long[].class) {
            long[] vector = new long[dimension];
            for (int i = 0; i < dimension; i++) {
                vector[i] = intMin + rng.nextInt(intMax - intMin + 1);
            }
            return vector;
        } else {
            throw new IllegalArgumentException("Unsupported vector type: " + vectorClass.getName());
        }
    }

    public static void main(String[] args) {
        CMD_generate_vectors cmd = new CMD_generate_vectors();
        int exitCode = new CommandLine(cmd).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() throws Exception {
        // Validate the output path
        try {
            validateOutputPath();
        } catch (CommandLine.ParameterException e) {
            System.err.println(e.getMessage());
            return EXIT_ERROR;
        }

        // Check if file exists and handle force option
        if (Files.exists(outputPath) && !force) {
            System.err.println("Error: Output file already exists. Use --force to overwrite.");
            return EXIT_FILE_EXISTS;
        }

        try {
            // Create parent directories if they don't exist and there is a parent path
            Path parent = outputPath.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }

            // Parse the vector type
            Class<?> vectorClass;
            try {
                vectorClass = parseVectorType(vectorType);
            } catch (IllegalArgumentException e) {
                System.err.println("Error: " + e.getMessage());
                return EXIT_ERROR;
            }

            // Validate dimension
            if (dimension <= 0) {
                System.err.println("Error: Dimension must be positive");
                return EXIT_ERROR;
            }

            // Validate count
            if (count <= 0) {
                System.err.println("Error: Count must be positive");
                return EXIT_ERROR;
            }

            // Determine effective seed: use provided seed, or generate a new one if seed <= 0
            long effectiveSeed = seed;
            if (seed <= 0) {
                effectiveSeed = System.nanoTime() ^ System.currentTimeMillis();
            }
            RestorableUniformRandomProvider rng = RandomGenerators.create(algorithm, effectiveSeed);

            // Create the vector file store
            try (VectorFileStreamStore<?> store = VectorFileIO.streamOut(format, vectorClass, outputPath)
                .orElseThrow(() -> new RuntimeException("Could not create vector file store for format: " + format))) {
                // Use reflection to handle different vector types
                for (int i = 0; i < count; i++) {
                    Object vector = generateRandomVector(vectorClass, dimension, rng);

                    // Use reflection to write the vector
                    try {
                        java.lang.reflect.Method writeMethod = store.getClass().getMethod("write", Array.newInstance(vectorClass.getComponentType(), 0).getClass());
                        writeMethod.invoke(store, vector);
                    } catch (Exception e) {
                        System.err.println("Error writing vector: " + e.getMessage());
                        e.printStackTrace();
                        return EXIT_ERROR;
                    }

                    // Print progress every 10% or every 1000 vectors, whichever is more frequent
                    if (i % Math.max(count / 10, 1000) == 0 && i > 0) {
                        System.out.printf("Generated %,d of %,d vectors (%.1f%%)%n", 
                            i, count, (double) i / count * 100);
                    }
                }
            }

            System.out.println("Successfully generated vector file: " + outputPath);
            System.out.println("Type: " + vectorType + ", Dimension: " + dimension + 
                ", Count: " + count + ", Format: " + format);
            System.out.println("Seed: " + effectiveSeed + ", Algorithm: " + algorithm);

            return EXIT_SUCCESS;
        } catch (IOException e) {
            System.err.println("Error: I/O problem when writing to file - " + e.getMessage());
            e.printStackTrace();
            return EXIT_ERROR;
        } catch (Exception e) {
            System.err.println("Error generating vector file: " + e.getMessage());
            e.printStackTrace();
            return EXIT_ERROR;
        }
    }
}
