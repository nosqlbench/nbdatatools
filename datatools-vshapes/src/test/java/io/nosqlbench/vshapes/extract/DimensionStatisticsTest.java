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

import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

public class DimensionStatisticsTest {

    @Test
    void testBasicStatistics() {
        float[] values = {1f, 2f, 3f, 4f, 5f};
        DimensionStatistics stats = DimensionStatistics.compute(0, values);

        assertEquals(5, stats.count());
        assertEquals(1f, stats.min(), 1e-6);
        assertEquals(5f, stats.max(), 1e-6);
        assertEquals(3f, stats.mean(), 1e-6);
        assertEquals(2f, stats.variance(), 1e-6);  // Sample variance
        assertEquals(Math.sqrt(2), stats.stdDev(), 1e-6);
    }

    @Test
    void testConstantValues() {
        float[] values = {5f, 5f, 5f, 5f, 5f};
        DimensionStatistics stats = DimensionStatistics.compute(0, values);

        assertEquals(5, stats.count());
        assertEquals(5f, stats.min(), 1e-6);
        assertEquals(5f, stats.max(), 1e-6);
        assertEquals(5f, stats.mean(), 1e-6);
        assertEquals(0f, stats.variance(), 1e-6);
        assertEquals(0f, stats.stdDev(), 1e-6);
    }

    @Test
    void testSingleValue() {
        float[] values = {42f};
        DimensionStatistics stats = DimensionStatistics.compute(0, values);

        assertEquals(1, stats.count());
        assertEquals(42f, stats.min(), 1e-6);
        assertEquals(42f, stats.max(), 1e-6);
        assertEquals(42f, stats.mean(), 1e-6);
    }

    @Test
    void testGaussianData() {
        Random random = new Random(12345);
        float[] values = new float[10000];
        for (int i = 0; i < values.length; i++) {
            values[i] = (float) random.nextGaussian();
        }

        DimensionStatistics stats = DimensionStatistics.compute(0, values);

        // Mean should be close to 0
        assertEquals(0, stats.mean(), 0.05);

        // StdDev should be close to 1
        assertEquals(1, stats.stdDev(), 0.05);

        // Should appear normal
        assertTrue(stats.appearsNormal(), "Gaussian data should appear normal");
    }

    @Test
    void testUniformData() {
        float[] values = new float[10000];
        for (int i = 0; i < values.length; i++) {
            values[i] = (float) i / values.length;
        }

        DimensionStatistics stats = DimensionStatistics.compute(0, values);

        // Mean should be close to 0.5
        assertEquals(0.5, stats.mean(), 0.01);

        // Min should be 0, max should be close to 1
        assertEquals(0, stats.min(), 1e-6);
        assertTrue(stats.max() > 0.99);

        // Should appear bounded
        assertTrue(stats.appearsBounded(), "Uniform data should appear bounded");
    }

    @Test
    void testNullThrows() {
        float[] nullArray = null;
        assertThrows(NullPointerException.class, () -> DimensionStatistics.compute(0, nullArray));
    }

    @Test
    void testEmptyThrows() {
        assertThrows(IllegalArgumentException.class, () -> DimensionStatistics.compute(0, new float[0]));
    }

    @Test
    void testDimension() {
        float[] values = {1f, 2f, 3f};
        DimensionStatistics stats = DimensionStatistics.compute(42, values);
        assertEquals(42, stats.dimension());
    }
}
