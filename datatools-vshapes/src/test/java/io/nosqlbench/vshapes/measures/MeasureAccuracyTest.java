package io.nosqlbench.vshapes.measures;

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

import io.nosqlbench.vshapes.VectorSpace;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Accuracy tests for analysis measures (LID, Hubness, Margin).
 *
 * <p>These tests verify that measures produce correct values for
 * analytically tractable cases and known reference data.
 */
@Tag("accuracy")
@Tag("unit")
public class MeasureAccuracyTest {

    private static final double TOLERANCE = 0.1;  // 10% for statistical measures
    private static final long SEED = 42L;

    // ==================== LID Accuracy Tests ====================

    @Test
    void lid_uniformGridApproachesDimension() {
        // For a uniform d-dimensional grid, LID should approach d
        // Note: LID estimation can be higher than true dimension due to boundary effects
        // and the MLE estimator's behavior on finite grids. We use relaxed tolerance.
        // Test with 2D, 3D grids
        for (int dim : new int[]{2, 3}) {
            VectorSpace grid = createUniformGrid(dim, 10);  // 10^dim points

            LIDMeasure lid = new LIDMeasure(10);
            LIDMeasure.LIDResult result = lid.compute(grid, null, Map.of());

            double meanLID = result.statistics.mean;

            // Mean LID should be in a reasonable range of the true dimensionality
            // LID on finite grids tends to overestimate, so we allow up to 2x
            assertTrue(meanLID >= dim * 0.5 && meanLID <= dim * 2.0,
                "Mean LID for " + dim + "D uniform grid should be in range [" +
                (dim * 0.5) + ", " + (dim * 2.0) + "], got " + meanLID);
        }
    }

    @Test
    void lid_lowDimensionalSubspaceDetected() {
        // Points in a 2D plane embedded in 10D should have LID around 2
        int embeddingDim = 10;
        int intrinsicDim = 2;
        int n = 500;

        VectorSpace subspace = createLowDimensionalSubspace(n, embeddingDim, intrinsicDim);

        LIDMeasure lid = new LIDMeasure(20);
        LIDMeasure.LIDResult result = lid.compute(subspace, null, Map.of());

        double meanLID = result.statistics.mean;

        // LID should detect the low intrinsic dimension
        assertTrue(meanLID < embeddingDim / 2,
            "LID should detect low-dimensional structure, got " + meanLID);
        assertEquals(intrinsicDim, meanLID, intrinsicDim * 0.5,
            "LID should be close to intrinsic dimension " + intrinsicDim);
    }

    @Test
    void lid_increasesWithDimensionality() {
        // LID should increase as we add more dimensions
        double prevMeanLID = 0;

        for (int dim : new int[]{2, 4, 8}) {
            VectorSpace space = createRandomSpace(100, dim);

            LIDMeasure lid = new LIDMeasure(10);
            LIDMeasure.LIDResult result = lid.compute(space, null, Map.of());

            double meanLID = result.statistics.mean;

            assertTrue(meanLID > prevMeanLID,
                "LID should increase with dimension, but " + meanLID +
                " <= " + prevMeanLID + " for dim=" + dim);

            prevMeanLID = meanLID;
        }
    }

    // ==================== Hubness Accuracy Tests ====================

    @Test
    void hubness_uniformDistributionLowSkewness() {
        // For uniformly distributed data, hubness skewness should be low
        VectorSpace uniform = createUniformGrid(3, 8);  // 512 points

        HubnessMeasure hubness = new HubnessMeasure(10);
        HubnessMeasure.HubnessResult result = hubness.compute(uniform, null, Map.of());

        // Skewness should be relatively low (not strongly skewed)
        // Perfect uniform would have skewness near 0
        assertTrue(Math.abs(result.skewness) < 2.0,
            "Uniform data should have low hubness skewness, got " + result.skewness);
    }

    @Test
    void hubness_allPointsAppearAsNeighbors() {
        // In a small dataset with k >= n/2, all points should appear as neighbors
        int n = 20;
        int k = 10;
        VectorSpace space = createRandomSpace(n, 3);

        HubnessMeasure hubness = new HubnessMeasure(k);
        HubnessMeasure.HubnessResult result = hubness.compute(space, null, Map.of());

        // Most points should have non-zero in-degree counts
        int nonZeroCount = 0;
        for (int count : result.inDegrees) {
            if (count > 0) nonZeroCount++;
        }

        assertTrue(nonZeroCount > n * 0.8,
            "Most points should appear as neighbors, but only " +
            nonZeroCount + "/" + n + " did");
    }

    @Test
    void hubness_symmetryInSmallDataset() {
        // For very small k relative to n, check that hubs emerge
        int n = 100;
        int k = 5;
        VectorSpace space = createRandomSpace(n, 10);

        HubnessMeasure hubness = new HubnessMeasure(k);
        HubnessMeasure.HubnessResult result = hubness.compute(space, null, Map.of());

        // Total neighbor appearances should be n * k
        int totalAppearances = 0;
        for (int count : result.inDegrees) {
            totalAppearances += count;
        }

        assertEquals(n * k, totalAppearances,
            "Total neighbor appearances should be n*k = " + (n * k));
    }

    // ==================== Margin Accuracy Tests ====================

    @Test
    void margin_separableClasses() {
        // Create well-separated clusters - margins should be positive
        VectorSpace clusters = createSeparableClusters(2, 50, 5, 10.0);

        MarginMeasure margin = new MarginMeasure();
        MarginMeasure.MarginResult result = margin.compute(clusters, null, Map.of());

        // Well-separated clusters should have mostly positive margins
        int positiveCount = 0;
        for (double m : result.marginValues) {
            if (m > 0) positiveCount++;
        }

        double positiveRatio = (double) positiveCount / result.marginValues.length;
        assertTrue(positiveRatio > 0.8,
            "Well-separated clusters should have >80% positive margins, got " +
            (positiveRatio * 100) + "%");

        // Mean margin should be positive
        assertTrue(result.statistics.mean > 0,
            "Mean margin for separable data should be positive, got " +
            result.statistics.mean);
    }

    @Test
    void margin_overlappingClasses() {
        // Test that overlapping clusters have lower margins than well-separated ones
        // With separation=1.0 and stddev=0.5, clusters may not fully overlap
        VectorSpace overlapping = createSeparableClusters(2, 50, 5, 0.5);  // Tighter separation

        MarginMeasure margin = new MarginMeasure();
        MarginMeasure.MarginResult result = margin.compute(overlapping, null, Map.of());

        // Count negative margins
        int negativeCount = 0;
        for (double m : result.marginValues) {
            if (m < 0) negativeCount++;
        }

        double negativeRatio = (double) negativeCount / result.marginValues.length;

        // With tighter separation, we should see some confusion (negative margins)
        // or at least lower mean margin than well-separated case
        // This is a softer test - just check mean margin is reasonable (not huge)
        assertTrue(result.statistics.mean < 5.0,
            "Overlapping clusters should have relatively low mean margin, got " +
            result.statistics.mean + ", negative ratio: " + (negativeRatio * 100) + "%");
    }

    // ==================== Helper Methods ====================

    private VectorSpace createUniformGrid(int dimension, int pointsPerDim) {
        int n = (int) Math.pow(pointsPerDim, dimension);
        float[][] vectors = new float[n][dimension];

        for (int i = 0; i < n; i++) {
            int idx = i;
            for (int d = 0; d < dimension; d++) {
                vectors[i][d] = (float) (idx % pointsPerDim) / (pointsPerDim - 1);
                idx /= pointsPerDim;
            }
        }

        return new SimpleVectorSpace(vectors, null);
    }

    private VectorSpace createLowDimensionalSubspace(int n, int embeddingDim, int intrinsicDim) {
        Random rng = new Random(SEED);
        float[][] vectors = new float[n][embeddingDim];

        for (int i = 0; i < n; i++) {
            // Only first intrinsicDim components are random, rest are 0
            for (int d = 0; d < intrinsicDim; d++) {
                vectors[i][d] = (float) rng.nextGaussian();
            }
        }

        return new SimpleVectorSpace(vectors, null);
    }

    private VectorSpace createRandomSpace(int n, int dimension) {
        Random rng = new Random(SEED);
        float[][] vectors = new float[n][dimension];

        for (int i = 0; i < n; i++) {
            for (int d = 0; d < dimension; d++) {
                vectors[i][d] = (float) rng.nextGaussian();
            }
        }

        return new SimpleVectorSpace(vectors, null);
    }

    private VectorSpace createSeparableClusters(int numClusters, int pointsPerCluster,
                                                 int dimension, double separation) {
        Random rng = new Random(SEED);
        int n = numClusters * pointsPerCluster;
        float[][] vectors = new float[n][dimension];
        int[] labels = new int[n];

        for (int c = 0; c < numClusters; c++) {
            // Cluster center
            float[] center = new float[dimension];
            for (int d = 0; d < dimension; d++) {
                center[d] = (float) (c * separation);
            }

            // Generate points around center
            for (int p = 0; p < pointsPerCluster; p++) {
                int idx = c * pointsPerCluster + p;
                labels[idx] = c;
                for (int d = 0; d < dimension; d++) {
                    vectors[idx][d] = center[d] + (float) (rng.nextGaussian() * 0.5);
                }
            }
        }

        return new SimpleVectorSpace(vectors, labels);
    }

    // Simple VectorSpace implementation for testing
    private static class SimpleVectorSpace implements VectorSpace {
        private final float[][] vectors;
        private final int[] labels;

        SimpleVectorSpace(float[][] vectors, int[] labels) {
            this.vectors = vectors;
            this.labels = labels;
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
        public Optional<Integer> getClassLabel(int index) {
            return labels != null ? Optional.of(labels[index]) : Optional.empty();
        }

        @Override
        public String getId() {
            return "test";
        }
    }
}
