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

package io.nosqlbench.vshapes.measures;

import io.nosqlbench.vshapes.AbstractAnalysisMeasure;
import io.nosqlbench.vshapes.VectorSpace;
import io.nosqlbench.vshapes.VectorUtils;

import java.nio.file.Path;
import java.util.Map;

/**
 * Hubness measure based on reverse k-nearest neighbor in-degree distribution.
 * 
 * Hubness measures the tendency of certain points to appear frequently in the 
 * k-nearest neighbor lists of other points. This phenomenon is more pronounced
 * in high-dimensional spaces and can affect the performance of similarity-based
 * algorithms.
 * 
 * The measure computes:
 * - In-degree: How many times each point appears as a k-NN of other points
 * - Hubness score: Standardized in-degree values
 * - Skewness: Distribution skewness indicating hubness concentration
 */
public class HubnessMeasure extends AbstractAnalysisMeasure<HubnessMeasure.HubnessResult> {

    private final int k;

    /// Creates a new HubnessMeasure with specified k parameter.
    /// 
    /// @param k number of nearest neighbors to consider for hubness analysis
    public HubnessMeasure(int k) {
        this.k = k;
    }

    /// Creates a new HubnessMeasure with default k=10.
    public HubnessMeasure() {
        this(10); // Default k=10
    }

    @Override
    public String getMnemonic() {
        return "Hubness";
    }

    @Override
    public String[] getDependencies() {
        return new String[0]; // Hubness computation doesn't depend on other measures
    }

    @Override
    protected Class<HubnessResult> getResultClass() {
        return HubnessResult.class;
    }

    @Override
    public String getCacheFilename(VectorSpace vectorSpace) {
        return getMnemonic().toLowerCase() + "_k" + k + "_" + vectorSpace.getId() + ".json";
    }

    @Override
    protected HubnessResult computeImpl(VectorSpace vectorSpace, Path cacheDir, Map<String, Object> dependencyResults) {
        int n = vectorSpace.getVectorCount();
        
        // Compute k-NN for all points
        int[][] knn = VectorUtils.computeAllKNN(vectorSpace, k);
        
        // Count in-degrees (how often each point appears in others' k-NN lists)
        int[] inDegrees = new int[n];
        for (int i = 0; i < n; i++) {
            for (int neighbor : knn[i]) {
                inDegrees[neighbor]++;
            }
        }
        
        // Convert to double for statistics computation
        double[] inDegreesDouble = new double[n];
        for (int i = 0; i < n; i++) {
            inDegreesDouble[i] = inDegrees[i];
        }
        
        // Compute statistics
        VectorUtils.Statistics inDegreeStats = VectorUtils.computeStatistics(inDegreesDouble);
        
        // Compute hubness scores (standardized in-degrees)
        double[] hubnessScores = new double[n];
        if (inDegreeStats.stdDev > 0) {
            for (int i = 0; i < n; i++) {
                hubnessScores[i] = (inDegrees[i] - inDegreeStats.mean) / inDegreeStats.stdDev;
            }
        } else {
            // All points have same in-degree, no hubness
            for (int i = 0; i < n; i++) {
                hubnessScores[i] = 0.0;
            }
        }
        
        // Compute skewness of in-degree distribution
        double skewness = computeSkewness(inDegreesDouble, inDegreeStats);
        
        // Identify hubs (high positive hubness) and anti-hubs (high negative hubness)
        double hubThreshold = 2.0; // Points with hubness > 2.0 standard deviations
        double antiHubThreshold = -2.0; // Points with hubness < -2.0 standard deviations
        
        int hubCount = 0;
        int antiHubCount = 0;
        for (double score : hubnessScores) {
            if (score > hubThreshold) hubCount++;
            if (score < antiHubThreshold) antiHubCount++;
        }
        
        VectorUtils.Statistics hubnessStats = VectorUtils.computeStatistics(hubnessScores);
        
        return new HubnessResult(inDegrees, hubnessScores, inDegreeStats, hubnessStats, 
                                skewness, hubCount, antiHubCount, k);
    }

    /**
     * Computes skewness of a distribution.
     * @param values the data values
     * @param stats pre-computed statistics
     * @return skewness value
     */
    private double computeSkewness(double[] values, VectorUtils.Statistics stats) {
        if (stats.stdDev == 0 || values.length < 3) {
            return 0.0; // No skewness for constant values or insufficient data
        }
        
        double sum = 0.0;
        for (double value : values) {
            double standardized = (value - stats.mean) / stats.stdDev;
            sum += standardized * standardized * standardized;
        }
        
        return sum / values.length;
    }

    /**
     * Result container for Hubness analysis.
     */
    public static class HubnessResult {
        /// In-degree count for each vector (how many times it appears in k-NN lists)
        public final int[] inDegrees;
        /// Standardized hubness scores for each vector
        public final double[] hubnessScores;
        /// Statistics for the in-degree distribution
        public final VectorUtils.Statistics inDegreeStats;
        /// Statistics for the hubness score distribution
        public final VectorUtils.Statistics hubnessStats;
        /// Skewness of the hubness score distribution
        public final double skewness;
        /// Number of vectors classified as hubs (score > 2.0)
        public final int hubCount;
        /// Number of vectors classified as anti-hubs (score < -2.0)
        public final int antiHubCount;
        /// The k parameter used for analysis
        public final int k;

        /// Default constructor for JSON deserialization.
        public HubnessResult() {
            this(new int[0], new double[0], 
                 new VectorUtils.Statistics(0, 0, 0, 0),
                 new VectorUtils.Statistics(0, 0, 0, 0),
                 0.0, 0, 0, 0);
        }

        /// Creates a complete hubness analysis result.
        /// 
        /// @param inDegrees in-degree count for each vector
        /// @param hubnessScores standardized hubness scores
        /// @param inDegreeStats statistics for in-degree distribution
        /// @param hubnessStats statistics for hubness score distribution
        /// @param skewness distribution skewness
        /// @param hubCount number of hub vectors
        /// @param antiHubCount number of anti-hub vectors
        /// @param k the k parameter used
        public HubnessResult(int[] inDegrees, double[] hubnessScores, 
                           VectorUtils.Statistics inDegreeStats, VectorUtils.Statistics hubnessStats,
                           double skewness, int hubCount, int antiHubCount, int k) {
            this.inDegrees = inDegrees;
            this.hubnessScores = hubnessScores;
            this.inDegreeStats = inDegreeStats;
            this.hubnessStats = hubnessStats;
            this.skewness = skewness;
            this.hubCount = hubCount;
            this.antiHubCount = antiHubCount;
            this.k = k;
        }

        /**
         * Gets the in-degree (reverse k-NN count) for a specific vector.
         * @param index vector index
         * @return in-degree count
         */
        public int getInDegree(int index) {
            return inDegrees[index];
        }

        /**
         * Gets the hubness score for a specific vector.
         * @param index vector index
         * @return hubness score (standardized in-degree)
         */
        public double getHubnessScore(int index) {
            return hubnessScores[index];
        }

        /**
         * Checks if a vector is a hub (hubness score > 2.0).
         * @param index vector index
         * @return true if vector is a hub
         */
        public boolean isHub(int index) {
            return hubnessScores[index] > 2.0;
        }

        /**
         * Checks if a vector is an anti-hub (hubness score less than -2.0).
         * @param index vector index
         * @return true if vector is an anti-hub
         */
        public boolean isAntiHub(int index) {
            return hubnessScores[index] < -2.0;
        }

        /**
         * Gets the number of vectors analyzed.
         * @return vector count
         */
        public int getVectorCount() {
            return inDegrees.length;
        }

        /**
         * Gets the skewness of the in-degree distribution.
         * Positive skewness indicates hubness.
         * @return skewness value
         */
        public double getSkewness() {
            return skewness;
        }

        /**
         * Gets the fraction of vectors that are hubs.
         * @return hub fraction (0.0 to 1.0)
         */
        public double getHubFraction() {
            return inDegrees.length > 0 ? (double) hubCount / inDegrees.length : 0.0;
        }

        /**
         * Gets the fraction of vectors that are anti-hubs.
         * @return anti-hub fraction (0.0 to 1.0)
         */
        public double getAntiHubFraction() {
            return inDegrees.length > 0 ? (double) antiHubCount / inDegrees.length : 0.0;
        }

        @Override
        public String toString() {
            return String.format("HubnessResult{k=%d, vectors=%d, skewness=%.3f, hubs=%d (%.1f%%), anti-hubs=%d (%.1f%%)}", 
                               k, inDegrees.length, skewness, hubCount, getHubFraction() * 100,
                               antiHubCount, getAntiHubFraction() * 100);
        }
    }
}