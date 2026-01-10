package io.nosqlbench.command.compute;

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

import io.nosqlbench.nbdatatools.api.fileio.VectorFileArray;
import io.nosqlbench.nbdatatools.api.services.FileType;
import io.nosqlbench.nbdatatools.api.services.VectorFileIO;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Detects whether vectors in a file are normalized using sparse sampling.
 * Normalized vectors have ||v|| = 1.0, which is required for some distance metrics.
 */
public class VectorNormalizationDetector {

    /**
     * Detect if vectors are normalized by sampling.
     * Samples up to 100 vectors spaced throughout the file.
     *
     * @param vectorPath path to .fvec or .ivec file
     * @return true if vectors appear to be normalized (||v|| ≈ 1.0)
     */
    public static boolean areVectorsNormalized(Path vectorPath) throws IOException {
        try (VectorFileArray<float[]> reader = VectorFileIO.randomAccess(FileType.xvec, float[].class, vectorPath)) {
            int totalVectors = reader.getSize();
            if (totalVectors == 0) {
                return false;
            }

            // Sample up to 100 vectors, evenly spaced
            int sampleSize = Math.min(100, totalVectors);
            int stride = totalVectors / sampleSize;

            int normalizedCount = 0;
            double tolerance = 0.01;  // 1% tolerance for floating point

            for (int i = 0; i < sampleSize; i++) {
                int index = i * stride;
                float[] vector = reader.get(index);

                // Compute L2 norm
                double normSquared = 0.0;
                for (float v : vector) {
                    normSquared += v * v;
                }
                double norm = Math.sqrt(normSquared);

                // Check if normalized (||v|| ≈ 1.0)
                if (Math.abs(norm - 1.0) < tolerance) {
                    normalizedCount++;
                }
            }

            // Consider normalized if >90% of samples are normalized
            return ((double) normalizedCount / sampleSize) > 0.9;
        }
    }

    /**
     * Get recommendation for distance metric based on normalization.
     *
     * @param isNormalized whether vectors are normalized
     * @return recommended distance metric name
     */
    public static String getRecommendedMetric(boolean isNormalized) {
        return isNormalized ? "COSINE (or DOT_PRODUCT for normalized vectors)" : "L2 or L1";
    }

    /**
     * Check if distance metric is appropriate for vector normalization.
     *
     * @param metricName distance metric name
     * @param isNormalized whether vectors are normalized
     * @return true if metric is appropriate
     */
    public static boolean isMetricAppropriate(String metricName, boolean isNormalized) {
        String metric = metricName.toUpperCase();

        if (isNormalized) {
            // Normalized vectors: DOT_PRODUCT is optimal, COSINE also works
            return metric.equals("DOT_PRODUCT") || metric.equals("COSINE");
        } else {
            // Non-normalized: L2 and L1 are meaningful, DOT_PRODUCT is NOT
            boolean isDotProduct = metric.equals("DOT_PRODUCT");
            return !isDotProduct && (metric.equals("L2") || metric.equals("EUCLIDEAN") || metric.equals("L1") || metric.equals("COSINE"));
        }
    }

    /**
     * Check specifically if DOT_PRODUCT is being used with non-normalized vectors.
     * This is a serious error - results will be meaningless.
     */
    public static boolean isDotProductWithNonNormalized(String metricName, boolean isNormalized) {
        return metricName.toUpperCase().equals("DOT_PRODUCT") && !isNormalized;
    }

    /**
     * Detect if vectors are normalized from in-memory data.
     * Samples up to 100 vectors from the provided array.
     *
     * @param vectors array of vectors (row-major: vectors[vectorIndex][dimension])
     * @return true if vectors appear to be normalized (||v|| ≈ 1.0)
     */
    public static boolean areVectorsNormalized(float[][] vectors) {
        if (vectors == null || vectors.length == 0) {
            return false;
        }

        // Sample up to 100 vectors, evenly spaced
        int totalVectors = vectors.length;
        int sampleSize = Math.min(100, totalVectors);
        int stride = totalVectors / sampleSize;

        int normalizedCount = 0;
        double tolerance = 0.01;  // 1% tolerance for floating point

        for (int i = 0; i < sampleSize; i++) {
            int index = i * stride;
            float[] vector = vectors[index];

            // Compute L2 norm
            double normSquared = 0.0;
            for (float v : vector) {
                normSquared += v * v;
            }
            double norm = Math.sqrt(normSquared);

            // Check if normalized (||v|| ≈ 1.0)
            if (Math.abs(norm - 1.0) < tolerance) {
                normalizedCount++;
            }
        }

        // Consider normalized if >90% of samples are normalized
        return ((double) normalizedCount / sampleSize) > 0.9;
    }

    /**
     * Detect if vectors are normalized from transposed (column-major) data.
     * Data format: transposed[dimension][vectorIndex]
     * Samples up to 100 vectors from the provided array.
     *
     * @param transposed transposed vector data (column-major)
     * @return true if vectors appear to be normalized (||v|| ≈ 1.0)
     */
    public static boolean areVectorsNormalizedTransposed(float[][] transposed) {
        if (transposed == null || transposed.length == 0 || transposed[0].length == 0) {
            return false;
        }

        int dimensions = transposed.length;
        int totalVectors = transposed[0].length;

        // Sample up to 100 vectors, evenly spaced
        int sampleSize = Math.min(100, totalVectors);
        int stride = totalVectors / sampleSize;

        int normalizedCount = 0;
        double tolerance = 0.01;  // 1% tolerance for floating point

        for (int i = 0; i < sampleSize; i++) {
            int vectorIndex = i * stride;

            // Compute L2 norm from transposed data
            double normSquared = 0.0;
            for (int d = 0; d < dimensions; d++) {
                float v = transposed[d][vectorIndex];
                normSquared += v * v;
            }
            double norm = Math.sqrt(normSquared);

            // Check if normalized (||v|| ≈ 1.0)
            if (Math.abs(norm - 1.0) < tolerance) {
                normalizedCount++;
            }
        }

        // Consider normalized if >90% of samples are normalized
        return ((double) normalizedCount / sampleSize) > 0.9;
    }

    /**
     * Result of normalization detection with additional metadata.
     *
     * @param isNormalized true if vectors are normalized
     * @param sampleSize number of vectors sampled
     * @param normalizedCount number of samples that were normalized
     * @param normalizedPercentage percentage of samples that were normalized
     */
    public record NormalizationResult(
        boolean isNormalized,
        int sampleSize,
        int normalizedCount,
        double normalizedPercentage
    ) {
        /// Threshold for considering vectors normalized (90%)
        public static final double NORMALIZED_THRESHOLD = 0.9;
    }

    /**
     * Detect normalization with detailed results.
     *
     * @param vectorPath path to vector file
     * @return detailed normalization result
     */
    public static NormalizationResult detectNormalization(Path vectorPath) throws IOException {
        try (VectorFileArray<float[]> reader = VectorFileIO.randomAccess(FileType.xvec, float[].class, vectorPath)) {
            int totalVectors = reader.getSize();
            if (totalVectors == 0) {
                return new NormalizationResult(false, 0, 0, 0.0);
            }

            // Sample up to 100 vectors, evenly spaced
            int sampleSize = Math.min(100, totalVectors);
            int stride = totalVectors / sampleSize;

            int normalizedCount = 0;
            double tolerance = 0.01;  // 1% tolerance for floating point

            for (int i = 0; i < sampleSize; i++) {
                int index = i * stride;
                float[] vector = reader.get(index);

                // Compute L2 norm
                double normSquared = 0.0;
                for (float v : vector) {
                    normSquared += v * v;
                }
                double norm = Math.sqrt(normSquared);

                // Check if normalized (||v|| ≈ 1.0)
                if (Math.abs(norm - 1.0) < tolerance) {
                    normalizedCount++;
                }
            }

            double percentage = (double) normalizedCount / sampleSize;
            boolean isNormalized = percentage > NormalizationResult.NORMALIZED_THRESHOLD;

            return new NormalizationResult(isNormalized, sampleSize, normalizedCount, percentage);
        }
    }
}
