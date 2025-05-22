package io.nosqlbench.command.generate;

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

import io.nosqlbench.nbvectors.api.commands.BundledCommand;
import io.nosqlbench.nbvectors.api.commands.VectorFileIO;
import io.nosqlbench.nbvectors.api.fileio.VectorFileStore;
import io.nosqlbench.nbvectors.api.services.FileType;
import org.apache.commons.rng.RestorableUniformRandomProvider;
import org.apache.commons.rng.sampling.distribution.ContinuousSampler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.Callable;

/// Command to generate a file of random vectors with specified type, dimensionality, and count.
/// Supports various vector types like "int[]", "float[]", "double[]", etc.
@CommandLine.Command(name = "vector-generate",
    description = "Generate a file of random vectors with specified type, dimensionality, and count")
public class VectorGenerate implements Callable<Integer>, BundledCommand {
    private static final Logger logger = LogManager.getLogger(VectorGenerate.class);

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
        defaultValue = "1000")
    private int intMax = 1000;

    @CommandLine.Spec
    private CommandLine.Model.CommandSpec spec;

    /// Validates the output path before execution.
    private void validateOutputPath() {
        if (outputPath == null) {
            throw new CommandLine.ParameterException(spec.commandLine(),
                "Error: No output path provided");
        }

        // Normalize the path to resolve any "." or ".." components
        outputPath = outputPath.normalize();
    }

    /// Parses the vector type string to determine the actual Java class to use.
    /// @param vectorTypeStr The vector type as a Java class signature (e.g., "int[]", "float[]")
    /// @return The Class object representing the vector type
    /// @throws IllegalArgumentException if the vector type is not supported
    private Class<?> parseVectorType(String vectorTypeStr) {
        switch (vectorTypeStr.trim()) {
            case "int[]":
                return int[].class;
            case "float[]":
                return float[].class;
            case "double[]":
                return double[].class;
            case "long[]":
                return long[].class;
            case "byte[]":
                return byte[].class;
            case "short[]":
                return short[].class;
            default:
                throw new IllegalArgumentException("Unsupported vector type: " + vectorTypeStr +
                    ". Supported types are: int[], float[], double[], long[], byte[], short[]");
        }
    }

    /// Generates a random vector of the specified type and dimension.
    /// @param vectorClass The Class object representing the vector type
    /// @param dimension The dimensionality of the vector
    /// @param rng The random number generator
    /// @return A random vector of the specified type and dimension
    private Object generateRandomVector(Class<?> vectorClass, int dimension, RestorableUniformRandomProvider rng) {
        if (vectorClass.equals(int[].class)) {
            int[] vector = new int[dimension];
            for (int i = 0; i < dimension; i++) {
                vector[i] = intMin + rng.nextInt(intMax - intMin + 1);
            }
            return vector;
        } else if (vectorClass.equals(float[].class)) {
            float[] vector = new float[dimension];
            ContinuousSampler sampler = RandomGenerators.createUniformSampler(rng, min, max);
            for (int i = 0; i < dimension; i++) {
                vector[i] = (float) sampler.sample();
            }
            return vector;
        } else if (vectorClass.equals(double[].class)) {
            double[] vector = new double[dimension];
            ContinuousSampler sampler = RandomGenerators.createUniformSampler(rng, min, max);
            for (int i = 0; i < dimension; i++) {
                vector[i] = sampler.sample();
            }
            return vector;
        } else if (vectorClass.equals(long[].class)) {
            long[] vector = new long[dimension];
            for (int i = 0; i < dimension; i++) {
                vector[i] = intMin + rng.nextLong((long)intMax - intMin + 1);
            }
            return vector;
        } else if (vectorClass.equals(byte[].class)) {
            byte[] vector = new byte[dimension];
            for (int i = 0; i < dimension; i++) {
                vector[i] = (byte) (intMin + rng.nextInt(Math.min(intMax, 255) - intMin + 1));
            }
            return vector;
        } else if (vectorClass.equals(short[].class)) {
            short[] vector = new short[dimension];
            for (int i = 0; i < dimension; i++) {
                vector[i] = (short) (intMin + rng.nextInt(Math.min(intMax, 32767) - intMin + 1));
            }
            return vector;
        } else {
            throw new IllegalArgumentException("Unsupported vector type: " + vectorClass.getName());
        }
    }

    /// Main method for running the command directly.
    public static void main(String[] args) {
        VectorGenerate cmd = new VectorGenerate();
        int exitCode = new CommandLine(cmd).execute(args);
        System.exit(exitCode);
    }

    /// Executes the vector generation command.
    /// @return Exit code (0 for success, 1 for file exists without force, 2 for other errors)
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
                System.err.println(e.getMessage());
                return EXIT_ERROR;
            }

            // Validate dimension and count
            if (dimension <= 0) {
                System.err.println("Error: Dimension must be positive");
                return EXIT_ERROR;
            }
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

            // Get the vector file store for writing
            Optional<?> storeOptRaw = VectorFileIO.vectorFileStore(format, vectorClass, outputPath);
            if (storeOptRaw.isEmpty()) {
                System.err.println("Error: Could not create vector file store for format " + format + 
                    " and type " + vectorType);
                return EXIT_ERROR;
            }

            // Cast to the correct type
            @SuppressWarnings("unchecked")
            VectorFileStore<Object> store = (VectorFileStore<Object>) storeOptRaw.get();

            // Generate and write vectors
            try (VectorFileStore<Object> vectorStore = store) {
                System.out.println("Generating " + count + " vectors of type " + vectorType + 
                    " with dimension " + dimension + "...");

                // Use batch writing for better performance
                int batchSize = Math.min(1000, count);
                Object batchArray = Array.newInstance(vectorClass, batchSize);

                for (int i = 0; i < count; i++) {
                    // Generate a random vector
                    Object vector = generateRandomVector(vectorClass, dimension, rng);

                    // Add to batch
                    int batchIndex = i % batchSize;
                    Array.set(batchArray, batchIndex, vector);

                    // Write batch when full or at the end
                    if (batchIndex == batchSize - 1 || i == count - 1) {
                        int actualBatchSize = (i == count - 1) ? (i % batchSize) + 1 : batchSize;

                        // If the batch isn't full, create a properly sized array
                        Object vectorsToWrite;
                        if (actualBatchSize < batchSize) {
                            vectorsToWrite = Array.newInstance(vectorClass, actualBatchSize);
                            for (int j = 0; j < actualBatchSize; j++) {
                                Array.set(vectorsToWrite, j, Array.get(batchArray, j));
                            }
                        } else {
                            vectorsToWrite = batchArray;
                        }

                        // Write batch - handle different array types
                        if (vectorClass.equals(int[].class)) {
                            @SuppressWarnings("unchecked")
                            int[][] typedArray = (int[][]) vectorsToWrite;
                            vectorStore.writeBulk(typedArray);
                        } else if (vectorClass.equals(float[].class)) {
                            @SuppressWarnings("unchecked")
                            float[][] typedArray = (float[][]) vectorsToWrite;
                            vectorStore.writeBulk(typedArray);
                        } else if (vectorClass.equals(double[].class)) {
                            @SuppressWarnings("unchecked")
                            double[][] typedArray = (double[][]) vectorsToWrite;
                            vectorStore.writeBulk(typedArray);
                        } else if (vectorClass.equals(long[].class)) {
                            @SuppressWarnings("unchecked")
                            long[][] typedArray = (long[][]) vectorsToWrite;
                            vectorStore.writeBulk(typedArray);
                        } else if (vectorClass.equals(byte[].class)) {
                            @SuppressWarnings("unchecked")
                            byte[][] typedArray = (byte[][]) vectorsToWrite;
                            vectorStore.writeBulk(typedArray);
                        } else if (vectorClass.equals(short[].class)) {
                            @SuppressWarnings("unchecked")
                            short[][] typedArray = (short[][]) vectorsToWrite;
                            vectorStore.writeBulk(typedArray);
                        }

                        // Report progress periodically
                        if (i % 10000 == 0 || i == count - 1) {
                            System.out.println("Progress: Generated " + (i + 1) + " of " + count + 
                                " vectors (" + String.format("%.1f%%", (100.0 * (i + 1) / count)) + ")");
                        }
                    }
                }

                // Ensure all data is written
                vectorStore.flush();
            }

            System.out.println("Successfully generated vector file: " + outputPath);
            System.out.println("Type: " + vectorType + ", Dimension: " + dimension + 
                ", Count: " + count + ", Format: " + format + ", Seed: " + effectiveSeed);

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
