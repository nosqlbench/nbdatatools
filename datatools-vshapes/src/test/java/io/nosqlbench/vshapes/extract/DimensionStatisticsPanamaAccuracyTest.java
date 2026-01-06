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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Accuracy tests for DimensionStatistics Panama implementation.
 *
 * <h2>Purpose</h2>
 *
 * <p>Verifies that the Panama Vector API implementation produces numerically
 * equivalent results to the scalar implementation within acceptable tolerance.
 *
 * <h2>Test Coverage</h2>
 *
 * <ul>
 *   <li>Various data sizes (aligned and unaligned with vector width)</li>
 *   <li>Different distribution types (normal, uniform, skewed)</li>
 *   <li>Edge cases (small arrays, extreme values)</li>
 *   <li>Both float and double inputs</li>
 * </ul>
 *
 * <h2>Running</h2>
 *
 * <pre>{@code
 * mvn test -pl datatools-vshapes -Paccuracy -Dtest=DimensionStatisticsPanamaAccuracyTest
 * }</pre>
 */
@Tag("accuracy")
public class DimensionStatisticsPanamaAccuracyTest {

    private static final double TOLERANCE = 1e-10;

    /**
     * Tests statistics computation across various data sizes.
     * Includes both vector-aligned and unaligned sizes.
     */
    @ParameterizedTest(name = "dataSize={0}")
    @ValueSource(ints = {7, 8, 15, 16, 31, 32, 63, 64, 100, 1000, 10000, 65536, 65537})
    void testStatisticsAccuracy(int dataSize) {
        Random random = new Random(dataSize);  // Deterministic

        // Generate test data
        double[] data = new double[dataSize];
        for (int i = 0; i < dataSize; i++) {
            data[i] = random.nextGaussian() * 2.0 + 5.0;  // N(5, 2)
        }

        // Compute statistics
        DimensionStatistics stats = DimensionStatistics.compute(0, data);

        // Compute expected values using naive implementation
        double expectedMean = computeMean(data);
        double[] moments = computeMoments(data, expectedMean);
        double expectedVariance = moments[0];
        double expectedSkewness = moments[1];
        double expectedKurtosis = moments[2];

        // Verify results
        assertEquals(expectedMean, stats.mean(), TOLERANCE * Math.abs(expectedMean) + 1e-15,
            "Mean mismatch for size " + dataSize);
        assertEquals(expectedVariance, stats.variance(), TOLERANCE * Math.abs(expectedVariance) + 1e-15,
            "Variance mismatch for size " + dataSize);

        // Skewness and kurtosis have higher numerical sensitivity
        assertEquals(expectedSkewness, stats.skewness(), 1e-8,
            "Skewness mismatch for size " + dataSize);
        assertEquals(expectedKurtosis, stats.kurtosis(), 1e-8,
            "Kurtosis mismatch for size " + dataSize);

        // Verify min/max
        double expectedMin = Double.MAX_VALUE;
        double expectedMax = -Double.MAX_VALUE;
        for (double v : data) {
            if (v < expectedMin) expectedMin = v;
            if (v > expectedMax) expectedMax = v;
        }
        assertEquals(expectedMin, stats.min(), 1e-15, "Min mismatch");
        assertEquals(expectedMax, stats.max(), 1e-15, "Max mismatch");
    }

    /**
     * Tests with float input data.
     */
    @Test
    void testFloatInput() {
        Random random = new Random(42);
        int size = 10000;

        float[] floatData = new float[size];
        double[] doubleData = new double[size];

        for (int i = 0; i < size; i++) {
            float value = (float) (random.nextGaussian() * 0.5 + 0.5);
            floatData[i] = value;
            doubleData[i] = value;  // Same values for comparison
        }

        DimensionStatistics floatStats = DimensionStatistics.compute(0, floatData);
        DimensionStatistics doubleStats = DimensionStatistics.compute(0, doubleData);

        // Float and double versions should produce very similar results
        assertEquals(doubleStats.mean(), floatStats.mean(), 1e-5,
            "Mean differs between float and double input");
        assertEquals(doubleStats.variance(), floatStats.variance(), 1e-5,
            "Variance differs between float and double input");
    }

    /**
     * Tests with uniform distribution data.
     */
    @Test
    void testUniformDistribution() {
        int size = 100000;
        double[] data = new double[size];

        for (int i = 0; i < size; i++) {
            data[i] = (double) i / size;  // Uniform [0, 1)
        }

        DimensionStatistics stats = DimensionStatistics.compute(0, data);

        // Uniform [0,1]: mean ≈ 0.5, variance ≈ 1/12 ≈ 0.0833, skewness ≈ 0, kurtosis ≈ 1.8
        assertEquals(0.5, stats.mean(), 0.01, "Uniform mean");
        assertEquals(1.0 / 12.0, stats.variance(), 0.01, "Uniform variance");
        assertEquals(0.0, stats.skewness(), 0.01, "Uniform skewness");
        assertEquals(1.8, stats.kurtosis(), 0.1, "Uniform kurtosis");
    }

    /**
     * Tests with skewed distribution data.
     */
    @Test
    void testSkewedDistribution() {
        Random random = new Random(123);
        int size = 100000;
        double[] data = new double[size];

        // Exponential-like distribution (right-skewed)
        for (int i = 0; i < size; i++) {
            data[i] = -Math.log(1.0 - random.nextDouble());
        }

        DimensionStatistics stats = DimensionStatistics.compute(0, data);

        // Exponential: mean ≈ 1, skewness ≈ 2 (right-skewed)
        assertTrue(stats.skewness() > 1.5, "Expected positive skewness");
        assertTrue(stats.kurtosis() > 6, "Expected high kurtosis");
    }

    /**
     * Tests edge case with very small array.
     */
    @Test
    void testSmallArray() {
        double[] data = {1.0, 2.0, 3.0};

        DimensionStatistics stats = DimensionStatistics.compute(0, data);

        assertEquals(2.0, stats.mean(), 1e-15, "Mean of [1,2,3]");
        assertEquals(1.0, stats.min(), 1e-15, "Min");
        assertEquals(3.0, stats.max(), 1e-15, "Max");
        assertEquals(3, stats.count(), "Count");
    }

    /**
     * Tests edge case with single element.
     */
    @Test
    void testSingleElement() {
        double[] data = {42.0};

        DimensionStatistics stats = DimensionStatistics.compute(0, data);

        assertEquals(42.0, stats.mean(), 1e-15, "Mean of single element");
        assertEquals(42.0, stats.min(), 1e-15, "Min");
        assertEquals(42.0, stats.max(), 1e-15, "Max");
        assertEquals(0.0, stats.variance(), 1e-15, "Variance of single element");
    }

    /**
     * Tests that appearsNormal() works correctly for normal data.
     */
    @Test
    void testAppearsNormal() {
        Random random = new Random(42);
        int size = 100000;
        double[] data = new double[size];

        for (int i = 0; i < size; i++) {
            data[i] = random.nextGaussian();
        }

        DimensionStatistics stats = DimensionStatistics.compute(0, data);

        assertTrue(stats.appearsNormal(), "Normal data should be detected as normal");
        assertFalse(stats.appearsUniform(), "Normal data should not appear uniform");
    }

    /**
     * Tests that appearsUniform() works correctly for uniform data.
     */
    @Test
    void testAppearsUniform() {
        Random random = new Random(42);
        int size = 100000;
        double[] data = new double[size];

        for (int i = 0; i < size; i++) {
            data[i] = random.nextDouble();  // Uniform [0, 1)
        }

        DimensionStatistics stats = DimensionStatistics.compute(0, data);

        assertTrue(stats.appearsUniform(), "Uniform data should be detected as uniform");
        assertFalse(stats.appearsNormal(), "Uniform data should not appear normal");
    }

    // ═══════════════════════════════════════════════════════════════════
    // Helper methods for expected value computation
    // ═══════════════════════════════════════════════════════════════════

    private double computeMean(double[] data) {
        double sum = 0;
        for (double v : data) {
            sum += v;
        }
        return sum / data.length;
    }

    private double[] computeMoments(double[] data, double mean) {
        double m2 = 0, m3 = 0, m4 = 0;
        for (double v : data) {
            double diff = v - mean;
            double diff2 = diff * diff;
            m2 += diff2;
            m3 += diff2 * diff;
            m4 += diff2 * diff2;
        }

        double variance = m2 / data.length;
        double stdDev = Math.sqrt(variance);

        double skewness = 0;
        double kurtosis = 3;

        if (stdDev > 0) {
            skewness = (m3 / data.length) / (stdDev * stdDev * stdDev);
            kurtosis = (m4 / data.length) / (variance * variance);
        }

        return new double[] {variance, skewness, kurtosis};
    }
}
