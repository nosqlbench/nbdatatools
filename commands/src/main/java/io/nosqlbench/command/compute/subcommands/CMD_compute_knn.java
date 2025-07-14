package io.nosqlbench.command.compute.subcommands;

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

import io.nosqlbench.nbdatatools.api.fileio.BoundedVectorFileStream;
import io.nosqlbench.nbdatatools.api.fileio.VectorFileStreamStore;
import io.nosqlbench.nbdatatools.api.services.FileType;
import io.nosqlbench.nbdatatools.api.services.VectorFileIO;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

/// Command to compute k-nearest neighbors ground truth dataset from base and query vectors
@CommandLine.Command(name = "knn",
    description = "Compute k-nearest neighbors ground truth dataset from base and query vectors")
public class CMD_compute_knn implements Callable<Integer> {
    private static final Logger logger = LogManager.getLogger(CMD_compute_knn.class);

    private static final int EXIT_SUCCESS = 0;
    private static final int EXIT_FILE_EXISTS = 1;
    private static final int EXIT_ERROR = 2;

    @CommandLine.Option(names = {"-b", "--base"}, description = "The base vectors file", required = true)
    private Path baseVectorsPath;

    @CommandLine.Option(names = {"-q", "--query"}, description = "The query vectors file", required = true)
    private Path queryVectorsPath;

    @CommandLine.Option(names = {"-o", "--output"}, description = "The output ground truth file", required = true)
    private Path outputPath;

    @CommandLine.Option(names = {"-k", "--neighbors"}, description = "Number of nearest neighbors to find", required = true)
    private int k;

    @CommandLine.Option(names = {"-d", "--distance"}, 
        description = "Distance metric to use (L2, L1, COSINE)", 
        defaultValue = "L2")
    private DistanceMetric distanceMetric = DistanceMetric.L2;

    @CommandLine.Option(names = {"--force"}, description = "Force overwrite if output file already exists")
    private boolean force = false;

    @CommandLine.Spec
    private CommandLine.Model.CommandSpec spec;

    /**
     * Enum for supported distance metrics
     */
    public enum DistanceMetric {
        L2,     // Euclidean distance
        L1,     // Manhattan distance
        COSINE  // Cosine similarity
    }

    /**
     * Validates the input and output paths before execution.
     */
    private void validatePaths() {
        if (baseVectorsPath == null) {
            throw new CommandLine.ParameterException(spec.commandLine(),
                "Error: No base vectors path provided");
        }

        if (queryVectorsPath == null) {
            throw new CommandLine.ParameterException(spec.commandLine(),
                "Error: No query vectors path provided");
        }

        if (outputPath == null) {
            throw new CommandLine.ParameterException(spec.commandLine(),
                "Error: No output path provided");
        }

        // Check if input files exist
        if (!Files.exists(baseVectorsPath)) {
            throw new CommandLine.ParameterException(spec.commandLine(),
                "Error: Base vectors file does not exist: " + baseVectorsPath);
        }

        if (!Files.exists(queryVectorsPath)) {
            throw new CommandLine.ParameterException(spec.commandLine(),
                "Error: Query vectors file does not exist: " + queryVectorsPath);
        }

        // Normalize the paths to resolve any "." or ".." components
        baseVectorsPath = baseVectorsPath.normalize();
        queryVectorsPath = queryVectorsPath.normalize();
        outputPath = outputPath.normalize();
    }

    /**
     * Calculate distance between two vectors based on the selected distance metric
     * 
     * @param vec1 First vector
     * @param vec2 Second vector
     * @return Distance between the vectors
     */
    private double calculateDistance(float[] vec1, float[] vec2) {
        if (vec1.length != vec2.length) {
            throw new IllegalArgumentException("Vectors must have the same dimension");
        }

        switch (distanceMetric) {
            case L2:
                return calculateL2Distance(vec1, vec2);
            case L1:
                return calculateL1Distance(vec1, vec2);
            case COSINE:
                return calculateCosineDistance(vec1, vec2);
            default:
                throw new IllegalArgumentException("Unsupported distance metric: " + distanceMetric);
        }
    }

    /**
     * Calculate Euclidean (L2) distance between two vectors
     */
    private double calculateL2Distance(float[] vec1, float[] vec2) {
        double sum = 0.0;
        for (int i = 0; i < vec1.length; i++) {
            double diff = vec1[i] - vec2[i];
            sum += diff * diff;
        }
        return Math.sqrt(sum);
    }

    /**
     * Calculate Manhattan (L1) distance between two vectors
     */
    private double calculateL1Distance(float[] vec1, float[] vec2) {
        double sum = 0.0;
        for (int i = 0; i < vec1.length; i++) {
            sum += Math.abs(vec1[i] - vec2[i]);
        }
        return sum;
    }

    /**
     * Calculate Cosine distance between two vectors
     */
    private double calculateCosineDistance(float[] vec1, float[] vec2) {
        double dotProduct = 0.0;
        double norm1 = 0.0;
        double norm2 = 0.0;

        for (int i = 0; i < vec1.length; i++) {
            dotProduct += vec1[i] * vec2[i];
            norm1 += vec1[i] * vec1[i];
            norm2 += vec2[i] * vec2[i];
        }

        // Prevent division by zero
        if (norm1 == 0.0 || norm2 == 0.0) {
            return 1.0; // Maximum distance
        }

        // Cosine similarity is dot product divided by the product of the norms
        double cosineSimilarity = dotProduct / (Math.sqrt(norm1) * Math.sqrt(norm2));

        // Convert to distance (1 - similarity)
        return 1.0 - cosineSimilarity;
    }

    /**
     * Find k nearest neighbors for a query vector from the base vectors
     * 
     * @param queryVector Query vector
     * @param baseVectors List of base vectors
     * @param k Number of nearest neighbors to find
     * @return Array of indices of the k nearest neighbors
     */
    private int[] findKNearestNeighbors(float[] queryVector, List<float[]> baseVectors, int k) {
        // Create array of (index, distance) pairs
        int numBaseVectors = baseVectors.size();
        IndexDistancePair[] pairs = new IndexDistancePair[numBaseVectors];

        // Calculate distances
        for (int i = 0; i < numBaseVectors; i++) {
            double distance = calculateDistance(queryVector, baseVectors.get(i));
            pairs[i] = new IndexDistancePair(i, distance);
        }

        // Sort by distance (ascending)
        Arrays.sort(pairs);

        // Take k nearest
        int[] neighbors = new int[Math.min(k, numBaseVectors)];
        for (int i = 0; i < neighbors.length; i++) {
            neighbors[i] = pairs[i].index;
        }

        return neighbors;
    }

    /**
     * Helper class to store index and distance pairs for sorting
     */
    private static class IndexDistancePair implements Comparable<IndexDistancePair> {
        int index;
        double distance;

        IndexDistancePair(int index, double distance) {
            this.index = index;
            this.distance = distance;
        }

        @Override
        public int compareTo(IndexDistancePair other) {
            return Double.compare(this.distance, other.distance);
        }
    }

    public static void main(String[] args) {
        CMD_compute_knn cmd = new CMD_compute_knn();
        int exitCode = new CommandLine(cmd).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() throws Exception {
        // Validate the paths
        try {
            validatePaths();
        } catch (CommandLine.ParameterException e) {
            System.err.println(e.getMessage());
            return EXIT_ERROR;
        }

        // Check if output file exists and handle force option
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

            // Validate k
            if (k <= 0) {
                System.err.println("Error: Number of neighbors (k) must be positive");
                return EXIT_ERROR;
            }

            // Load base vectors
            List<float[]> baseVectors = new ArrayList<>();
            BoundedVectorFileStream<float[]> baseStream = VectorFileIO.streamIn(FileType.xvec, float[].class, baseVectorsPath)
                .orElseThrow(() -> new RuntimeException("Could not open base vectors file: " + baseVectorsPath));

            for (float[] vector : baseStream) {
                baseVectors.add(vector);
            }

            System.out.println("Loaded " + baseVectors.size() + " base vectors");

            if (baseVectors.isEmpty()) {
                System.err.println("Error: No base vectors found in file");
                return EXIT_ERROR;
            }

            // Prepare output file for ground truth (int[] arrays for each query)
            try (VectorFileStreamStore<int[]> outputStore = VectorFileIO.streamOut(FileType.xvec, int[].class, outputPath)
                .orElseThrow(() -> new RuntimeException("Could not create output file: " + outputPath))) {

                // Process query vectors and find k nearest neighbors for each
                int queryCount = 0;
                BoundedVectorFileStream<float[]> queryStream = VectorFileIO.streamIn(FileType.xvec, float[].class, queryVectorsPath)
                    .orElseThrow(() -> new RuntimeException("Could not open query vectors file: " + queryVectorsPath));

                for (float[] queryVector : queryStream) {
                    // Find k nearest neighbors
                    int[] neighbors = findKNearestNeighbors(queryVector, baseVectors, k);

                    // Write to output file
                    outputStore.write(neighbors);

                    queryCount++;

                    // Print progress every 100 queries
                    if (queryCount % 100 == 0) {
                        System.out.println("Processed " + queryCount + " query vectors");
                    }
                }

                System.out.println("Successfully computed KNN ground truth for " + queryCount + " query vectors");
                System.out.println("Base vectors: " + baseVectors.size() + ", k: " + k + ", distance metric: " + distanceMetric);
                System.out.println("Output file: " + outputPath);
            }

            return EXIT_SUCCESS;
        } catch (IOException e) {
            System.err.println("Error: I/O problem - " + e.getMessage());
            e.printStackTrace();
            return EXIT_ERROR;
        } catch (Exception e) {
            System.err.println("Error computing KNN: " + e.getMessage());
            e.printStackTrace();
            return EXIT_ERROR;
        }
    }
}
