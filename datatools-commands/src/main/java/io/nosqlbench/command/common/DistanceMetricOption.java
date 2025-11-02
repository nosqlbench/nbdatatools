/*
 * Copyright (c) nosqlbench
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.nosqlbench.command.common;

import picocli.CommandLine;

/**
 * Shared distance metric option for k-NN and similarity operations.
 * All supporting types are inner classes for self-contained encapsulation.
 */
public class DistanceMetricOption {

    /**
     * Enum for supported distance/similarity metrics in vector operations.
     */
    public enum DistanceMetric {
        /**
         * Euclidean distance (L2 norm).
         * Measures the straight-line distance between two points.
         */
        L2,

        /**
         * Manhattan distance (L1 norm).
         * Measures the sum of absolute differences between coordinates.
         */
        L1,

        /**
         * Cosine similarity.
         * Measures the cosine of the angle between two vectors.
         * Often converted to distance as (1 - similarity).
         */
        COSINE,

        /**
         * Dot product similarity (FASTEST for normalized vectors!).
         * For normalized vectors (||v||=1), this is equivalent to cosine but 3x faster.
         * WARNING: Only meaningful for normalized vectors!
         */
        DOT_PRODUCT
    }

    @CommandLine.Option(
        names = {"-m", "--metric"},
        description = {
            "Distance metric:",
            "  DOT_PRODUCT: Fastest for normalized vectors (default)",
            "  COSINE: Works for both normalized and non-normalized",
            "  L2: Euclidean distance",
            "  L1: Manhattan distance",
            "Valid values: ${COMPLETION-CANDIDATES}"
        },
        defaultValue = "DOT_PRODUCT"
    )
    private DistanceMetric distanceMetric = DistanceMetric.DOT_PRODUCT;

    /**
     * Gets the selected distance metric.
     *
     * @return the distance metric
     */
    public DistanceMetric getDistanceMetric() {
        return distanceMetric;
    }

    /**
     * Checks if L2 (Euclidean) distance is selected.
     *
     * @return true if L2 is selected
     */
    public boolean isL2() {
        return distanceMetric == DistanceMetric.L2;
    }

    /**
     * Checks if L1 (Manhattan) distance is selected.
     *
     * @return true if L1 is selected
     */
    public boolean isL1() {
        return distanceMetric == DistanceMetric.L1;
    }

    /**
     * Checks if Cosine similarity is selected.
     *
     * @return true if COSINE is selected
     */
    public boolean isCosine() {
        return distanceMetric == DistanceMetric.COSINE;
    }

    /**
     * Sets the distance metric programmatically.
     *
     * @param distanceMetric the distance metric to set
     */
    public void setDistanceMetric(DistanceMetric distanceMetric) {
        this.distanceMetric = distanceMetric;
    }
}
