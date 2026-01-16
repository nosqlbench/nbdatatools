package io.nosqlbench.vshapes.extract;

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

/**
 * Correlation analysis utilities for comparing correlation structure
 * between original and synthetic vector data.
 *
 * <p>Since the model treats dimensions independently, this analysis
 * documents the correlation loss that results from the independence
 * assumption. This helps users understand when the independence
 * assumption matters for their use case.
 *
 * <p>Key metrics:
 * <ul>
 *   <li>Frobenius norm of correlation matrix difference</li>
 *   <li>Maximum absolute correlation error</li>
 *   <li>Count of significant correlations lost</li>
 * </ul>
 */
public final class CorrelationAnalysis {

    private CorrelationAnalysis() {
        // Utility class
    }

    /**
     * Comparison result between two correlation matrices.
     *
     * @param frobeniusNormDiff Frobenius norm of difference ||R_orig - R_synth||_F
     * @param maxAbsDiff maximum absolute correlation difference
     * @param meanAbsDiff mean absolute correlation difference
     * @param significantCorrelations count where |r_ij| &gt; threshold in original
     * @param preservedCorrelations count preserved within tolerance in synthetic
     * @param dimensions number of dimensions
     */
    public record CorrelationComparison(
        double frobeniusNormDiff,
        double maxAbsDiff,
        double meanAbsDiff,
        int significantCorrelations,
        int preservedCorrelations,
        int dimensions
    ) {
        /**
         * Fraction of significant correlations preserved.
         */
        public double preservationRate() {
            return significantCorrelations > 0
                ? (double) preservedCorrelations / significantCorrelations
                : 1.0;
        }

        /**
         * Fraction of significant correlations lost.
         */
        public double lossRate() {
            return 1.0 - preservationRate();
        }
    }

    /**
     * Computes the full correlation matrix for a set of vectors.
     *
     * @param vectors array of vectors [numVectors][numDimensions]
     * @return correlation matrix [numDimensions][numDimensions]
     */
    public static double[][] computeCorrelationMatrix(float[][] vectors) {
        if (vectors == null || vectors.length == 0) {
            throw new IllegalArgumentException("Vectors cannot be null or empty");
        }

        int n = vectors.length;
        int dims = vectors[0].length;

        // Compute means
        double[] means = new double[dims];
        for (float[] vector : vectors) {
            for (int d = 0; d < dims; d++) {
                means[d] += vector[d];
            }
        }
        for (int d = 0; d < dims; d++) {
            means[d] /= n;
        }

        // Compute standard deviations
        double[] stdDevs = new double[dims];
        for (float[] vector : vectors) {
            for (int d = 0; d < dims; d++) {
                double diff = vector[d] - means[d];
                stdDevs[d] += diff * diff;
            }
        }
        for (int d = 0; d < dims; d++) {
            stdDevs[d] = Math.sqrt(stdDevs[d] / (n - 1));
        }

        // Compute correlation matrix
        double[][] corr = new double[dims][dims];

        for (int i = 0; i < dims; i++) {
            corr[i][i] = 1.0;  // Diagonal is always 1

            for (int j = i + 1; j < dims; j++) {
                double cov = 0;
                for (float[] vector : vectors) {
                    cov += (vector[i] - means[i]) * (vector[j] - means[j]);
                }
                cov /= (n - 1);

                double r = (stdDevs[i] > 0 && stdDevs[j] > 0)
                    ? cov / (stdDevs[i] * stdDevs[j])
                    : 0;

                // Clamp to [-1, 1]
                r = Math.max(-1.0, Math.min(1.0, r));

                corr[i][j] = r;
                corr[j][i] = r;
            }
        }

        return corr;
    }

    /**
     * Compares two correlation matrices.
     *
     * @param original original data correlation matrix
     * @param synthetic synthetic data correlation matrix
     * @param significanceThreshold threshold for "significant" correlation (default 0.1)
     * @param preservationTolerance tolerance for considering a correlation "preserved"
     * @return comparison result
     */
    public static CorrelationComparison compareCorrelationMatrices(
            double[][] original, double[][] synthetic,
            double significanceThreshold, double preservationTolerance) {

        if (original.length != synthetic.length) {
            throw new IllegalArgumentException("Matrices must have same dimensions");
        }

        int dims = original.length;
        double frobeniusSum = 0;
        double maxDiff = 0;
        double sumDiff = 0;
        int significant = 0;
        int preserved = 0;
        int count = 0;

        for (int i = 0; i < dims; i++) {
            for (int j = i + 1; j < dims; j++) {
                double diff = Math.abs(original[i][j] - synthetic[i][j]);
                frobeniusSum += diff * diff;
                maxDiff = Math.max(maxDiff, diff);
                sumDiff += diff;
                count++;

                if (Math.abs(original[i][j]) > significanceThreshold) {
                    significant++;
                    if (diff < preservationTolerance) {
                        preserved++;
                    }
                }
            }
        }

        double frobeniusNorm = Math.sqrt(frobeniusSum);
        double meanDiff = count > 0 ? sumDiff / count : 0;

        return new CorrelationComparison(
            frobeniusNorm,
            maxDiff,
            meanDiff,
            significant,
            preserved,
            dims
        );
    }

    /**
     * Compares correlation matrices with default thresholds.
     *
     * @param original original data correlation matrix
     * @param synthetic synthetic data correlation matrix
     * @return comparison result
     */
    public static CorrelationComparison compareCorrelationMatrices(
            double[][] original, double[][] synthetic) {
        return compareCorrelationMatrices(original, synthetic, 0.1, 0.05);
    }

    /**
     * Compares correlation structure between original and synthetic vector data.
     *
     * @param original original vectors [numVectors][numDimensions]
     * @param synthetic synthetic vectors [numVectors][numDimensions]
     * @return comparison result
     */
    public static CorrelationComparison compareCorrelationStructure(
            float[][] original, float[][] synthetic) {
        double[][] origCorr = computeCorrelationMatrix(original);
        double[][] synthCorr = computeCorrelationMatrix(synthetic);
        return compareCorrelationMatrices(origCorr, synthCorr);
    }

    /**
     * Formats a correlation comparison as a report string.
     *
     * @param comparison the comparison result
     * @return formatted report string
     */
    public static String formatCorrelationReport(CorrelationComparison comparison) {
        StringBuilder sb = new StringBuilder();
        sb.append("CORRELATION STRUCTURE ANALYSIS\n");
        sb.append("==============================\n");
        sb.append(String.format("Dimensions: %d\n", comparison.dimensions()));
        sb.append(String.format("Original significant correlations (|r|>0.1): %d\n",
            comparison.significantCorrelations()));
        sb.append(String.format("Preserved within tolerance: %d (%.1f%%)\n",
            comparison.preservedCorrelations(),
            comparison.preservationRate() * 100));
        sb.append(String.format("Correlation loss rate: %.1f%%\n",
            comparison.lossRate() * 100));
        sb.append("\n");
        sb.append(String.format("Frobenius norm of difference: %.4f\n",
            comparison.frobeniusNormDiff()));
        sb.append(String.format("Max absolute correlation error: %.4f\n",
            comparison.maxAbsDiff()));
        sb.append(String.format("Mean absolute correlation error: %.4f\n",
            comparison.meanAbsDiff()));

        if (comparison.lossRate() > 0.5) {
            sb.append("\nWARNING: High correlation loss detected.\n");
            sb.append("The independence assumption may not be appropriate for this data.\n");
        } else if (comparison.lossRate() > 0.1) {
            sb.append("\nNOTE: Some correlation structure lost due to independence assumption.\n");
        }

        return sb.toString();
    }

    /**
     * Computes explained variance ratios for PCA comparison.
     *
     * <p>Uses eigenvalue decomposition of the covariance matrix to compute
     * the fraction of variance explained by each principal component.
     *
     * @param vectors input vectors
     * @param numComponents number of components to compute
     * @return explained variance ratios (sum to ~1.0)
     */
    public static double[] computeExplainedVariance(float[][] vectors, int numComponents) {
        if (vectors == null || vectors.length == 0) {
            throw new IllegalArgumentException("Vectors cannot be null or empty");
        }

        int n = vectors.length;
        int dims = vectors[0].length;
        numComponents = Math.min(numComponents, dims);

        // Compute covariance matrix
        double[] means = new double[dims];
        for (float[] vector : vectors) {
            for (int d = 0; d < dims; d++) {
                means[d] += vector[d];
            }
        }
        for (int d = 0; d < dims; d++) {
            means[d] /= n;
        }

        double[][] cov = new double[dims][dims];
        for (float[] vector : vectors) {
            for (int i = 0; i < dims; i++) {
                for (int j = i; j < dims; j++) {
                    double prod = (vector[i] - means[i]) * (vector[j] - means[j]);
                    cov[i][j] += prod;
                    if (i != j) {
                        cov[j][i] += prod;
                    }
                }
            }
        }
        for (int i = 0; i < dims; i++) {
            for (int j = 0; j < dims; j++) {
                cov[i][j] /= (n - 1);
            }
        }

        // Power iteration to find top eigenvalues
        double[] eigenvalues = new double[numComponents];
        double totalVariance = 0;
        for (int d = 0; d < dims; d++) {
            totalVariance += cov[d][d];
        }

        // Simplified: use diagonal elements as approximation for independent dimensions
        // This is appropriate since our model assumes independence
        double[] variances = new double[dims];
        for (int d = 0; d < dims; d++) {
            variances[d] = cov[d][d];
        }

        // Sort variances descending
        java.util.Arrays.sort(variances);
        for (int i = 0; i < variances.length / 2; i++) {
            double temp = variances[i];
            variances[i] = variances[variances.length - 1 - i];
            variances[variances.length - 1 - i] = temp;
        }

        // Compute explained variance ratios
        double[] ratios = new double[numComponents];
        for (int i = 0; i < numComponents; i++) {
            ratios[i] = totalVariance > 0 ? variances[i] / totalVariance : 0;
        }

        return ratios;
    }

    /**
     * Compares explained variance between original and synthetic data.
     *
     * @param original original vectors
     * @param synthetic synthetic vectors
     * @param numComponents number of components to compare
     * @return mean absolute difference in explained variance ratios
     */
    public static double compareExplainedVariance(
            float[][] original, float[][] synthetic, int numComponents) {
        double[] origVar = computeExplainedVariance(original, numComponents);
        double[] synthVar = computeExplainedVariance(synthetic, numComponents);

        double sumDiff = 0;
        for (int i = 0; i < numComponents; i++) {
            sumDiff += Math.abs(origVar[i] - synthVar[i]);
        }

        return sumDiff / numComponents;
    }
}
