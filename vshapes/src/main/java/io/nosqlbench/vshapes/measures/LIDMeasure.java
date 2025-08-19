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
 * Local Intrinsic Dimensionality (LID) measure using Maximum Likelihood Estimation.
 * 
 * LID estimates the intrinsic dimensionality of data around each point using 
 * the distances to its k nearest neighbors. It's based on the principle that
 * in a d-dimensional space, the ratio of distances follows a specific distribution.
 * 
 * The MLE formula is: LID = -1 / (1/k * Σ(ln(r_i / r_k))) where r_i are the 
 * distances to the k nearest neighbors, sorted in ascending order.
 */
public class LIDMeasure extends AbstractAnalysisMeasure<LIDMeasure.LIDResult> {

    private final int k;

    /// Creates a new LIDMeasure with specified k parameter.
    /// 
    /// @param k number of nearest neighbors to consider for LID estimation
    public LIDMeasure(int k) {
        this.k = k;
    }

    /// Creates a new LIDMeasure with default k=20 (commonly used in literature).
    public LIDMeasure() {
        this(20); // Default k=20 as commonly used in literature
    }

    @Override
    public String getMnemonic() {
        return "LID";
    }

    @Override
    public String[] getDependencies() {
        return new String[0]; // LID computation doesn't depend on other measures
    }

    @Override
    protected Class<LIDResult> getResultClass() {
        return LIDResult.class;
    }

    @Override
    public String getCacheFilename(VectorSpace vectorSpace) {
        return getMnemonic().toLowerCase() + "_k" + k + "_" + vectorSpace.getId() + ".json";
    }

    @Override
    protected LIDResult computeImpl(VectorSpace vectorSpace, Path cacheDir, Map<String, Object> dependencyResults) {
        int n = vectorSpace.getVectorCount();
        double[] lidValues = new double[n];

        for (int i = 0; i < n; i++) {
            float[] queryVector = vectorSpace.getVector(i);
            
            // Find k nearest neighbors (excluding self)
            int[] neighbors = VectorUtils.findKNearestNeighbors(queryVector, vectorSpace, k, i);
            
            // Compute distances to neighbors
            double[] distances = new double[neighbors.length];
            for (int j = 0; j < neighbors.length; j++) {
                float[] neighborVector = vectorSpace.getVector(neighbors[j]);
                distances[j] = VectorUtils.euclideanDistance(queryVector, neighborVector);
            }

            // Compute LID using MLE formula
            lidValues[i] = computeLID(distances);
        }

        // Compute summary statistics
        VectorUtils.Statistics stats = VectorUtils.computeStatistics(lidValues);

        return new LIDResult(lidValues, stats, k);
    }

    /**
     * Computes LID using Maximum Likelihood Estimation.
     * @param distances sorted distances to k nearest neighbors
     * @return estimated local intrinsic dimensionality
     */
    private double computeLID(double[] distances) {
        if (distances.length < 2) {
            return Double.NaN; // Cannot estimate with less than 2 neighbors
        }

        // Sort distances (should already be sorted from kNN, but ensure)
        java.util.Arrays.sort(distances);

        // Use MLE formula: LID = -1 / (1/k * Σ(ln(r_i / r_k)))
        double rk = distances[distances.length - 1]; // Distance to k-th neighbor
        
        if (rk <= 0) {
            return Double.NaN; // Invalid distance
        }

        double sum = 0.0;
        int validCount = 0;

        for (int i = 0; i < distances.length - 1; i++) {
            if (distances[i] > 0) {
                sum += Math.log(distances[i] / rk);
                validCount++;
            }
        }

        if (validCount == 0) {
            return Double.NaN;
        }

        double meanLogRatio = sum / validCount;
        
        // Handle edge cases
        if (meanLogRatio >= 0) {
            return Double.POSITIVE_INFINITY; // Degenerate case
        }

        return -1.0 / meanLogRatio;
    }

    /**
     * Result container for LID analysis.
     */
    public static class LIDResult {
        /// Local Intrinsic Dimensionality values for each vector
        public final double[] lidValues;
        /// Statistics for the LID value distribution
        public final VectorUtils.Statistics statistics;
        /// The k parameter used for LID estimation
        public final int k;

        /// Default constructor for JSON deserialization.
        public LIDResult() {
            this(new double[0], new VectorUtils.Statistics(0, 0, 0, 0), 0);
        }

        /// Creates a complete LID analysis result.
        /// 
        /// @param lidValues LID values for each vector
        /// @param statistics distribution statistics
        /// @param k the k parameter used
        public LIDResult(double[] lidValues, VectorUtils.Statistics statistics, int k) {
            this.lidValues = lidValues;
            this.statistics = statistics;
            this.k = k;
        }

        /**
         * Gets the LID value for a specific vector.
         * @param index vector index
         * @return LID value
         */
        public double getLID(int index) {
            return lidValues[index];
        }

        /**
         * Gets the number of vectors analyzed.
         * @return vector count
         */
        public int getVectorCount() {
            return lidValues.length;
        }

        /**
         * Gets summary statistics for all LID values.
         * @return statistics summary
         */
        public VectorUtils.Statistics getStatistics() {
            return statistics;
        }

        @Override
        public String toString() {
            return String.format("LIDResult{k=%d, vectors=%d, stats=%s}", 
                               k, lidValues.length, statistics);
        }
    }
}