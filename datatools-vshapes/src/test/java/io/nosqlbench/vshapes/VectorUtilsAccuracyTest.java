package io.nosqlbench.vshapes;

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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Accuracy tests for VectorUtils operations.
 *
 * <p>These tests verify numerical accuracy of vector operations against
 * known analytical solutions and high-precision reference implementations.
 */
@Tag("accuracy")
@Tag("unit")
public class VectorUtilsAccuracyTest {

    private static final double TOLERANCE = 1e-10;
    private static final long SEED = 42L;

    // ==================== Euclidean Distance Tests ====================

    @Test
    void euclideanDistance_unitVectors() {
        // Distance between unit vectors along axes should be sqrt(2)
        float[] x_axis = {1, 0, 0};
        float[] y_axis = {0, 1, 0};
        float[] z_axis = {0, 0, 1};

        double expected = Math.sqrt(2);

        assertEquals(expected, VectorUtils.euclideanDistance(x_axis, y_axis), TOLERANCE,
            "Distance between orthogonal unit vectors should be sqrt(2)");
        assertEquals(expected, VectorUtils.euclideanDistance(y_axis, z_axis), TOLERANCE,
            "Distance between orthogonal unit vectors should be sqrt(2)");
        assertEquals(expected, VectorUtils.euclideanDistance(x_axis, z_axis), TOLERANCE,
            "Distance between orthogonal unit vectors should be sqrt(2)");
    }

    @Test
    void euclideanDistance_oppositeVectors() {
        // Distance between opposite unit vectors should be 2
        float[] positive = {1, 0, 0};
        float[] negative = {-1, 0, 0};

        assertEquals(2.0, VectorUtils.euclideanDistance(positive, negative), TOLERANCE,
            "Distance between opposite unit vectors should be 2");
    }

    @Test
    void euclideanDistance_sameVector() {
        float[] v = {1, 2, 3, 4, 5};

        assertEquals(0.0, VectorUtils.euclideanDistance(v, v), TOLERANCE,
            "Distance from a vector to itself should be 0");
    }

    @Test
    void euclideanDistance_knownDistance() {
        // 3-4-5 right triangle
        float[] origin = {0, 0};
        float[] point = {3, 4};

        assertEquals(5.0, VectorUtils.euclideanDistance(origin, point), TOLERANCE,
            "Distance for 3-4-5 triangle should be 5");
    }

    @Test
    void euclideanDistance_highDimensional() {
        // In n dimensions, distance from origin to (1,1,...,1) is sqrt(n)
        for (int n : new int[]{10, 100, 1000}) {
            float[] origin = new float[n];
            float[] ones = new float[n];
            java.util.Arrays.fill(ones, 1.0f);

            double expected = Math.sqrt(n);
            double actual = VectorUtils.euclideanDistance(origin, ones);

            assertEquals(expected, actual, TOLERANCE * n,
                "High-dimensional distance to all-ones vector should be sqrt(" + n + ")");
        }
    }

    @Test
    void squaredEuclideanDistance_consistency() {
        Random rng = new Random(SEED);

        for (int trial = 0; trial < 100; trial++) {
            float[] a = randomVector(rng, 50);
            float[] b = randomVector(rng, 50);

            double euclidean = VectorUtils.euclideanDistance(a, b);
            double squared = VectorUtils.squaredEuclideanDistance(a, b);

            assertEquals(euclidean * euclidean, squared, TOLERANCE,
                "Squared distance should equal distance squared");
        }
    }

    // ==================== Statistics Accuracy Tests ====================

    @Test
    void statistics_knownValues() {
        // Simple case: values 1,2,3,4,5
        double[] values = {1, 2, 3, 4, 5};

        VectorUtils.Statistics stats = VectorUtils.computeStatistics(values);

        assertEquals(3.0, stats.mean, TOLERANCE, "Mean of 1,2,3,4,5 should be 3");
        assertEquals(1.0, stats.min, TOLERANCE, "Min should be 1");
        assertEquals(5.0, stats.max, TOLERANCE, "Max should be 5");

        // Population std dev of 1,2,3,4,5 is sqrt(2)
        double expectedStdDev = Math.sqrt(2);
        assertEquals(expectedStdDev, stats.stdDev, TOLERANCE,
            "StdDev of 1,2,3,4,5 should be sqrt(2)");
    }

    @Test
    void statistics_uniformDistribution() {
        // For uniform [0,1], mean=0.5, variance=1/12
        int n = 100000;
        double[] values = new double[n];
        for (int i = 0; i < n; i++) {
            values[i] = (double) i / (n - 1);  // 0.0 to 1.0
        }

        VectorUtils.Statistics stats = VectorUtils.computeStatistics(values);

        assertEquals(0.5, stats.mean, 1e-4, "Mean of uniform [0,1] should be 0.5");
        assertEquals(0.0, stats.min, TOLERANCE, "Min should be 0");
        assertEquals(1.0, stats.max, TOLERANCE, "Max should be 1");

        // Variance of uniform [0,1] is 1/12, so stdDev = 1/sqrt(12)
        double expectedStdDev = 1.0 / Math.sqrt(12);
        assertEquals(expectedStdDev, stats.stdDev, 1e-4,
            "StdDev of uniform [0,1] should be 1/sqrt(12)");
    }

    @Test
    void statistics_constantValues() {
        double[] values = {42, 42, 42, 42, 42};

        VectorUtils.Statistics stats = VectorUtils.computeStatistics(values);

        assertEquals(42.0, stats.mean, TOLERANCE, "Mean of constant values should be that constant");
        assertEquals(0.0, stats.stdDev, TOLERANCE, "StdDev of constant values should be 0");
        assertEquals(42.0, stats.min, TOLERANCE);
        assertEquals(42.0, stats.max, TOLERANCE);
    }

    @Test
    void statistics_emptyArray() {
        double[] values = {};

        VectorUtils.Statistics stats = VectorUtils.computeStatistics(values);

        assertEquals(0.0, stats.mean, TOLERANCE, "Mean of empty array should be 0");
        assertEquals(0.0, stats.stdDev, TOLERANCE, "StdDev of empty array should be 0");
    }

    // ==================== k-NN Accuracy Tests ====================

    @Test
    void knn_orderedByDistance() {
        // Create a simple vector space where distances are predictable
        SimpleVectorSpace space = new SimpleVectorSpace(new float[][]{
            {0, 0},    // index 0
            {1, 0},    // index 1, distance 1 from origin
            {0, 2},    // index 2, distance 2 from origin
            {3, 0},    // index 3, distance 3 from origin
            {0, 4},    // index 4, distance 4 from origin
        });

        float[] query = {0, 0};

        int[] nearest = VectorUtils.findKNearestNeighbors(query, space, 3, -1);

        // Should return indices 0, 1, 2 (distances 0, 1, 2)
        assertEquals(0, nearest[0], "Closest should be index 0");
        assertEquals(1, nearest[1], "Second closest should be index 1");
        assertEquals(2, nearest[2], "Third closest should be index 2");
    }

    @Test
    void knn_excludeSelf() {
        SimpleVectorSpace space = new SimpleVectorSpace(new float[][]{
            {0, 0},
            {1, 0},
            {2, 0},
        });

        float[] query = {0, 0};

        // Exclude index 0 (the query point itself)
        int[] nearest = VectorUtils.findKNearestNeighbors(query, space, 2, 0);

        // Should return indices 1, 2 (not 0)
        assertFalse(java.util.Arrays.stream(nearest).anyMatch(i -> i == 0),
            "Excluded index should not appear in results");
        assertEquals(1, nearest[0], "Closest after exclusion should be index 1");
        assertEquals(2, nearest[1], "Second closest after exclusion should be index 2");
    }

    // ==================== Helper Methods ====================

    private float[] randomVector(Random rng, int dimension) {
        float[] v = new float[dimension];
        for (int i = 0; i < dimension; i++) {
            v[i] = (float) rng.nextGaussian();
        }
        return v;
    }

    // Simple VectorSpace implementation for testing
    private static class SimpleVectorSpace implements VectorSpace {
        private final float[][] vectors;

        SimpleVectorSpace(float[][] vectors) {
            this.vectors = vectors;
        }

        @Override
        public int getVectorCount() {
            return vectors.length;
        }

        @Override
        public int getDimension() {
            return vectors.length > 0 ? vectors[0].length : 0;
        }

        @Override
        public float[] getVector(int index) {
            return vectors[index];
        }

        @Override
        public float[][] getAllVectors() {
            return vectors;
        }

        @Override
        public java.util.Optional<Integer> getClassLabel(int index) {
            return java.util.Optional.empty();
        }

        @Override
        public String getId() {
            return "test";
        }
    }
}
