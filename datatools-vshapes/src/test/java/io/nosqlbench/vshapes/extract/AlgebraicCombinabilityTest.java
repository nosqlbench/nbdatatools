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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for algebraic combinability of accumulators and statistics.
 *
 * <p>These tests verify that combining partial results produces numerically
 * equivalent results to processing all data in a single pass.
 *
 * <h2>Properties Tested</h2>
 *
 * <ol>
 *   <li><b>Equivalence:</b> sequential([a,b,c,d]) == combine(sequential([a,b]), sequential([c,d]))</li>
 *   <li><b>Associativity:</b> combine(A, combine(B, C)) == combine(combine(A, B), C)</li>
 *   <li><b>Numerical stability:</b> Results consistent under different orderings</li>
 * </ol>
 */
@Tag("accuracy")
@Tag("unit")
public class AlgebraicCombinabilityTest {

    private static final long SEED = 42L;
    private static final double TOLERANCE = 1e-10;
    private static final int SAMPLE_SIZE = 10000;

    // ==================== DimensionStatistics Tests ====================

    @Test
    void dimensionStatistics_combineEquivalentToSequential() {
        Random rng = new Random(SEED);
        float[] data = generateRandomData(rng, SAMPLE_SIZE);

        // Split data in half
        float[] firstHalf = new float[SAMPLE_SIZE / 2];
        float[] secondHalf = new float[SAMPLE_SIZE / 2];
        System.arraycopy(data, 0, firstHalf, 0, SAMPLE_SIZE / 2);
        System.arraycopy(data, SAMPLE_SIZE / 2, secondHalf, 0, SAMPLE_SIZE / 2);

        // Compute statistics sequentially over all data
        DimensionStatistics sequential = DimensionStatistics.compute(0, data);

        // Compute statistics for each half and combine
        DimensionStatistics statsA = DimensionStatistics.compute(0, firstHalf);
        DimensionStatistics statsB = DimensionStatistics.compute(0, secondHalf);
        DimensionStatistics combined = statsA.combine(statsB);

        // Verify equivalence
        assertEquals(sequential.count(), combined.count(),
            "Count should be identical");
        assertEquals(sequential.min(), combined.min(), TOLERANCE,
            "Min should be identical");
        assertEquals(sequential.max(), combined.max(), TOLERANCE,
            "Max should be identical");
        assertEquals(sequential.mean(), combined.mean(), TOLERANCE,
            "Mean should be numerically equivalent");
        assertEquals(sequential.variance(), combined.variance(), TOLERANCE,
            "Variance should be numerically equivalent");
        assertEquals(sequential.skewness(), combined.skewness(), 1e-8,
            "Skewness should be numerically equivalent");
        assertEquals(sequential.kurtosis(), combined.kurtosis(), 1e-6,
            "Kurtosis should be numerically equivalent");
    }

    @Test
    void dimensionStatistics_associativity() {
        Random rng = new Random(SEED);
        float[] dataA = generateRandomData(rng, 1000);
        float[] dataB = generateRandomData(rng, 1500);
        float[] dataC = generateRandomData(rng, 2000);

        DimensionStatistics statsA = DimensionStatistics.compute(0, dataA);
        DimensionStatistics statsB = DimensionStatistics.compute(0, dataB);
        DimensionStatistics statsC = DimensionStatistics.compute(0, dataC);

        // combine(A, combine(B, C))
        DimensionStatistics leftAssoc = statsA.combine(statsB.combine(statsC));

        // combine(combine(A, B), C)
        DimensionStatistics rightAssoc = statsA.combine(statsB).combine(statsC);

        // Verify associativity
        assertEquals(leftAssoc.count(), rightAssoc.count(),
            "Count should be identical under associativity");
        assertEquals(leftAssoc.mean(), rightAssoc.mean(), TOLERANCE,
            "Mean should be identical under associativity");
        assertEquals(leftAssoc.variance(), rightAssoc.variance(), TOLERANCE,
            "Variance should be identical under associativity");
        assertEquals(leftAssoc.skewness(), rightAssoc.skewness(), 1e-8,
            "Skewness should be identical under associativity");
        assertEquals(leftAssoc.kurtosis(), rightAssoc.kurtosis(), 1e-6,
            "Kurtosis should be identical under associativity");
    }

    @Test
    void dimensionStatistics_numericalStabilityUnderRandomOrderings() {
        Random rng = new Random(SEED);
        float[] data = generateRandomData(rng, SAMPLE_SIZE);

        // Compute baseline (sequential)
        DimensionStatistics baseline = DimensionStatistics.compute(0, data);

        // Test 10 different random partitionings
        for (int trial = 0; trial < 10; trial++) {
            int splitPoint = rng.nextInt(SAMPLE_SIZE - 100) + 50;

            float[] partA = new float[splitPoint];
            float[] partB = new float[SAMPLE_SIZE - splitPoint];
            System.arraycopy(data, 0, partA, 0, splitPoint);
            System.arraycopy(data, splitPoint, partB, 0, SAMPLE_SIZE - splitPoint);

            DimensionStatistics statsA = DimensionStatistics.compute(0, partA);
            DimensionStatistics statsB = DimensionStatistics.compute(0, partB);
            DimensionStatistics combined = statsA.combine(statsB);

            assertEquals(baseline.mean(), combined.mean(), TOLERANCE,
                "Mean should be stable under random partitioning (trial " + trial + ")");
            assertEquals(baseline.variance(), combined.variance(), TOLERANCE,
                "Variance should be stable under random partitioning (trial " + trial + ")");
        }
    }

    @Test
    void dimensionStatistics_commutativity() {
        Random rng = new Random(SEED);
        float[] dataA = generateRandomData(rng, 2000);
        float[] dataB = generateRandomData(rng, 3000);

        DimensionStatistics statsA = DimensionStatistics.compute(0, dataA);
        DimensionStatistics statsB = DimensionStatistics.compute(0, dataB);

        // combine(A, B) vs combine(B, A)
        DimensionStatistics ab = statsA.combine(statsB);
        DimensionStatistics ba = statsB.combine(statsA);

        assertEquals(ab.count(), ba.count(),
            "Count should be commutative");
        assertEquals(ab.mean(), ba.mean(), TOLERANCE,
            "Mean should be commutative");
        assertEquals(ab.variance(), ba.variance(), TOLERANCE,
            "Variance should be commutative");
        assertEquals(ab.skewness(), ba.skewness(), 1e-8,
            "Skewness should be commutative");
        assertEquals(ab.kurtosis(), ba.kurtosis(), 1e-6,
            "Kurtosis should be commutative");
    }

    @Test
    void dimensionStatistics_combineWithEmpty() {
        Random rng = new Random(SEED);
        float[] data = generateRandomData(rng, 1000);

        DimensionStatistics stats = DimensionStatistics.compute(0, data);
        DimensionStatistics empty = new DimensionStatistics(0, 0, 0, 0, 0, 0, 0, 3);

        // Combining with empty should return original
        DimensionStatistics combined = stats.combine(empty);

        assertEquals(stats.count(), combined.count());
        assertEquals(stats.mean(), combined.mean(), TOLERANCE);
        assertEquals(stats.variance(), combined.variance(), TOLERANCE);
    }

    @Test
    void dimensionStatistics_multiWayCombine() {
        Random rng = new Random(SEED);

        // Split data into 5 parts
        DimensionStatistics[] parts = new DimensionStatistics[5];
        float[] allData = new float[5000];

        for (int i = 0; i < 5; i++) {
            float[] partData = generateRandomData(rng, 1000);
            System.arraycopy(partData, 0, allData, i * 1000, 1000);
            parts[i] = DimensionStatistics.compute(0, partData);
        }

        // Compute baseline
        DimensionStatistics baseline = DimensionStatistics.compute(0, allData);

        // Combine all parts
        DimensionStatistics combined = parts[0];
        for (int i = 1; i < 5; i++) {
            combined = combined.combine(parts[i]);
        }

        assertEquals(baseline.count(), combined.count());
        assertEquals(baseline.mean(), combined.mean(), TOLERANCE);
        assertEquals(baseline.variance(), combined.variance(), TOLERANCE);
        assertEquals(baseline.skewness(), combined.skewness(), 1e-6,
            "Skewness should be stable in multi-way combine");
    }

    // ==================== Edge Cases ====================

    @Test
    void dimensionStatistics_extremeValues() {
        // Test with extreme values that might cause numerical instability
        float[] data1 = {1e6f, 1e6f + 0.001f, 1e6f + 0.002f, 1e6f + 0.003f};
        float[] data2 = {1e6f + 0.004f, 1e6f + 0.005f, 1e6f + 0.006f, 1e6f + 0.007f};
        float[] allData = new float[8];
        System.arraycopy(data1, 0, allData, 0, 4);
        System.arraycopy(data2, 0, allData, 4, 4);

        DimensionStatistics stats1 = DimensionStatistics.compute(0, data1);
        DimensionStatistics stats2 = DimensionStatistics.compute(0, data2);
        DimensionStatistics combined = stats1.combine(stats2);
        DimensionStatistics baseline = DimensionStatistics.compute(0, allData);

        // With extreme values, we use slightly looser tolerance
        assertEquals(baseline.mean(), combined.mean(), 1e-6,
            "Mean should be stable for extreme values");
        assertEquals(baseline.variance(), combined.variance(), 1e-12,
            "Variance should be stable for extreme values");
    }

    @Test
    void dimensionStatistics_differentDimensionsThrows() {
        float[] data1 = {1f, 2f, 3f};
        float[] data2 = {4f, 5f, 6f};

        DimensionStatistics stats1 = DimensionStatistics.compute(0, data1);
        DimensionStatistics stats2 = DimensionStatistics.compute(1, data2);

        assertThrows(IllegalArgumentException.class, () -> stats1.combine(stats2),
            "Should throw when combining different dimensions");
    }

    // ==================== Helper Methods ====================

    private float[] generateRandomData(Random rng, int size) {
        float[] data = new float[size];
        for (int i = 0; i < size; i++) {
            data[i] = (float) rng.nextGaussian();
        }
        return data;
    }
}
