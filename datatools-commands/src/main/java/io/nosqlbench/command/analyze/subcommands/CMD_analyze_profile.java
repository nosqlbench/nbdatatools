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

import io.nosqlbench.common.types.VectorFileExtension;
import io.nosqlbench.vshapes.model.GaussianComponentModel;
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

/**
 * Analyze a vector dataset and build a VectorSpaceModel configuration.
 *
 * <p>This command samples vectors from a dataset file, computes per-dimension
 * Gaussian statistics (mean and standard deviation), and saves the resulting
 * VectorSpaceModel configuration to a JSON file.
 *
 * <p>The resulting model can be used to generate synthetic vectors with similar
 * statistical properties to the input dataset.
 *
 * <h2>Usage Examples</h2>
 * <pre>{@code
 * # Profile all vectors in a file
 * nbvectors analyze profile base_vectors.fvec -o model.json
 *
 * # Sample 10000 vectors with truncation bounds
 * nbvectors analyze profile base_vectors.fvec -o model.json --sample 10000 --truncated
 *
 * # Custom unique vector count for the model
 * nbvectors analyze profile base_vectors.fvec -o model.json -n 1000000
 * }</pre>
 */
@CommandLine.Command(name = "profile",
    header = "Profile a vector dataset to build a VectorSpaceModel",
    description = "Analyzes vectors to compute per-dimension Gaussian statistics and saves as JSON",
    exitCodeList = {"0: success", "1: error processing file"})
public class CMD_analyze_profile implements Callable<Integer> {
    private static final Logger logger = LogManager.getLogger(CMD_analyze_profile.class);

    @CommandLine.Parameters(index = "0", description = "Vector file to analyze")
    private Path inputFile;

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

    @Override
    public Integer call() {
        try {
            if (!Files.exists(inputFile)) {
                logger.error("File not found: {}", inputFile);
                System.err.println("Error: File not found: " + inputFile);
                return 1;
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

            System.out.printf("Profiling vector file: %s%n", inputFile);

            VectorSpaceModel model = profileVectors(inputFile, fileType);
            if (model == null) {
                return 1;
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

    private VectorSpaceModel profileVectors(Path file, FileType fileType) {
        try (VectorFileArray<float[]> vectorArray = VectorFileIO.randomAccess(fileType, float[].class, file)) {
            int vectorCount = vectorArray.getSize();

            if (vectorCount == 0) {
                logger.error("File contains no vectors");
                System.err.println("Error: File contains no vectors");
                return null;
            }

            // Get dimensions from first vector
            float[] first = vectorArray.get(0);
            int dimensions = first.length;

            System.out.printf("  Vectors in file: %d%n", vectorCount);
            System.out.printf("  Dimensions: %d%n", dimensions);

            // Determine sample size
            int actualSampleSize = sampleSize != null ? Math.min(sampleSize, vectorCount) : vectorCount;
            System.out.printf("  Sampling: %d vectors%n", actualSampleSize);

            // Initialize per-dimension statistics
            double[] sum = new double[dimensions];
            double[] sumSq = new double[dimensions];
            double[] min = new double[dimensions];
            double[] max = new double[dimensions];

            for (int d = 0; d < dimensions; d++) {
                min[d] = Double.MAX_VALUE;
                max[d] = Double.MIN_VALUE;
            }

            // Sample vectors
            if (actualSampleSize == vectorCount) {
                // Use all vectors
                for (int i = 0; i < vectorCount; i++) {
                    float[] vector = vectorArray.get(i);
                    accumulateStats(vector, sum, sumSq, min, max);

                    if ((i + 1) % 100000 == 0) {
                        System.out.printf("  Processed %d / %d vectors%n", i + 1, vectorCount);
                    }
                }
            } else {
                // Random sampling with reservoir sampling approach
                Random random = new Random(seed);
                int[] sampleIndices = new int[actualSampleSize];

                // Generate unique random indices using reservoir sampling
                for (int i = 0; i < actualSampleSize; i++) {
                    sampleIndices[i] = i;
                }
                for (int i = actualSampleSize; i < vectorCount; i++) {
                    int j = random.nextInt(i + 1);
                    if (j < actualSampleSize) {
                        sampleIndices[j] = i;
                    }
                }

                for (int i = 0; i < actualSampleSize; i++) {
                    float[] vector = vectorArray.get(sampleIndices[i]);
                    accumulateStats(vector, sum, sumSq, min, max);

                    if ((i + 1) % 100000 == 0) {
                        System.out.printf("  Sampled %d / %d vectors%n", i + 1, actualSampleSize);
                    }
                }
            }

            // Compute mean and stdDev for each dimension
            double[] mean = new double[dimensions];
            double[] stdDev = new double[dimensions];

            for (int d = 0; d < dimensions; d++) {
                mean[d] = sum[d] / actualSampleSize;
                double variance = (sumSq[d] / actualSampleSize) - (mean[d] * mean[d]);
                stdDev[d] = Math.sqrt(Math.max(0, variance)); // Ensure non-negative due to floating point errors
            }

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

    private void accumulateStats(float[] vector, double[] sum, double[] sumSq,
                                  double[] min, double[] max) {
        for (int d = 0; d < vector.length; d++) {
            double v = vector[d];
            sum[d] += v;
            sumSq[d] += v * v;
            if (v < min[d]) min[d] = v;
            if (v > max[d]) max[d] = v;
        }
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
            GaussianComponentModel[] components = new GaussianComponentModel[dimensions];

            for (int d = 0; d < dimensions; d++) {
                if (truncated) {
                    components[d] = new GaussianComponentModel(mean[d], stdDev[d], min[d], max[d]);
                } else {
                    components[d] = new GaussianComponentModel(mean[d], stdDev[d]);
                }
            }

            return new VectorSpaceModel(uniqueVectors, components);
        }
    }
}
