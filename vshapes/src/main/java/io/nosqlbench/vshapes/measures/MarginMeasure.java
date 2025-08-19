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
import java.util.Optional;

/**
 * Nearest-Neighbor Margin measure for analyzing class separability.
 * 
 * The margin is defined as the ratio of the distance to the nearest neighbor
 * of a different class versus the distance to the nearest neighbor of the same class.
 * Higher margin values indicate better class separability.
 * 
 * This measure requires class labels to be available in the vector space.
 */
public class MarginMeasure extends AbstractAnalysisMeasure<MarginMeasure.MarginResult> {

    /// Creates a new MarginMeasure.
    public MarginMeasure() {
        // Default constructor
    }

    @Override
    public String getMnemonic() {
        return "Margin";
    }

    @Override
    public String[] getDependencies() {
        return new String[0]; // Margin computation doesn't depend on other measures
    }

    @Override
    protected Class<MarginResult> getResultClass() {
        return MarginResult.class;
    }

    @Override
    protected MarginResult computeImpl(VectorSpace vectorSpace, Path cacheDir, Map<String, Object> dependencyResults) {
        if (!vectorSpace.hasClassLabels()) {
            throw new IllegalArgumentException("Margin measure requires class labels");
        }

        int n = vectorSpace.getVectorCount();
        double[] marginValues = new double[n];
        int validMargins = 0;

        for (int i = 0; i < n; i++) {
            Optional<Integer> classLabelOpt = vectorSpace.getClassLabel(i);
            if (!classLabelOpt.isPresent()) {
                marginValues[i] = Double.NaN;
                continue;
            }

            int classLabel = classLabelOpt.get();
            float[] queryVector = vectorSpace.getVector(i);

            // Find nearest neighbor of same class and nearest neighbor of different class
            double nearestSameClassDistance = Double.POSITIVE_INFINITY;
            double nearestDifferentClassDistance = Double.POSITIVE_INFINITY;

            for (int j = 0; j < n; j++) {
                if (i == j) continue; // Skip self

                Optional<Integer> otherClassLabelOpt = vectorSpace.getClassLabel(j);
                if (!otherClassLabelOpt.isPresent()) continue;

                int otherClassLabel = otherClassLabelOpt.get();
                float[] otherVector = vectorSpace.getVector(j);
                double distance = VectorUtils.euclideanDistance(queryVector, otherVector);

                if (otherClassLabel == classLabel) {
                    // Same class
                    if (distance < nearestSameClassDistance) {
                        nearestSameClassDistance = distance;
                    }
                } else {
                    // Different class
                    if (distance < nearestDifferentClassDistance) {
                        nearestDifferentClassDistance = distance;
                    }
                }
            }

            // Compute margin as ratio of distances
            if (nearestSameClassDistance > 0 && nearestSameClassDistance < Double.POSITIVE_INFINITY) {
                double margin = nearestDifferentClassDistance / nearestSameClassDistance;
                marginValues[i] = margin;
                validMargins++;
            } else {
                // No same-class neighbors found, or zero distance
                marginValues[i] = Double.NaN;
            }
        }

        // Compute statistics for valid margins only
        double[] validMarginArray = new double[validMargins];
        int validIndex = 0;
        for (double margin : marginValues) {
            if (!Double.isNaN(margin) && !Double.isInfinite(margin)) {
                validMarginArray[validIndex++] = margin;
            }
        }

        VectorUtils.Statistics stats = VectorUtils.computeStatistics(validMarginArray);

        return new MarginResult(marginValues, stats, validMargins);
    }

    /**
     * Result container for Margin analysis.
     */
    public static class MarginResult {
        /// Margin values for each vector (NaN for vectors without valid margins)
        public final double[] marginValues;
        /// Statistics for the valid margin values
        public final VectorUtils.Statistics statistics;
        /// Number of vectors with valid margin calculations
        public final int validCount;

        /// Default constructor for JSON deserialization.
        public MarginResult() {
            this(new double[0], new VectorUtils.Statistics(0, 0, 0, 0), 0);
        }

        /// Creates a complete margin analysis result.
        /// 
        /// @param marginValues margin values for each vector
        /// @param statistics statistics for valid margins
        /// @param validCount number of vectors with valid margins
        public MarginResult(double[] marginValues, VectorUtils.Statistics statistics, int validCount) {
            this.marginValues = marginValues;
            this.statistics = statistics;
            this.validCount = validCount;
        }

        /**
         * Gets the margin value for a specific vector.
         * @param index vector index
         * @return margin value (may be NaN or infinite)
         */
        public double getMargin(int index) {
            return marginValues[index];
        }

        /**
         * Gets the number of vectors analyzed.
         * @return vector count
         */
        public int getVectorCount() {
            return marginValues.length;
        }

        /**
         * Gets the number of valid (finite, non-NaN) margin values.
         * @return valid margin count
         */
        public int getValidCount() {
            return validCount;
        }

        /**
         * Gets summary statistics for valid margin values.
         * @return statistics summary
         */
        public VectorUtils.Statistics getStatistics() {
            return statistics;
        }

        /**
         * Checks if a margin value is valid (finite and non-NaN).
         * @param index vector index
         * @return true if margin is valid
         */
        public boolean isValidMargin(int index) {
            double margin = marginValues[index];
            return !Double.isNaN(margin) && !Double.isInfinite(margin);
        }

        /**
         * Gets the fraction of vectors with valid margins.
         * @return valid fraction (0.0 to 1.0)
         */
        public double getValidFraction() {
            return marginValues.length > 0 ? (double) validCount / marginValues.length : 0.0;
        }

        @Override
        public String toString() {
            return String.format("MarginResult{vectors=%d, valid=%d (%.1f%%), stats=%s}", 
                               marginValues.length, validCount, 
                               getValidFraction() * 100, statistics);
        }
    }
}