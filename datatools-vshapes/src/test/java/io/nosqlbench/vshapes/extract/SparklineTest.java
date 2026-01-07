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
 * Tests for {@link Sparkline} utility class.
 */
@Tag("unit")
public class SparklineTest {

    @Test
    void testBasicGeneration() {
        // Create data with a normal-like distribution (values concentrated in middle)
        float[] data = {0.1f, 0.2f, 0.3f, 0.5f, 0.5f, 0.5f, 0.5f, 0.5f, 0.7f, 0.8f, 0.9f};

        String sparkline = Sparkline.generate(data, 10);

        System.out.println("Basic sparkline: " + sparkline);
        assertNotNull(sparkline);
        assertEquals(10, sparkline.length());
    }

    @Test
    void testUniformDistribution() {
        // Uniformly distributed data should have roughly equal bar heights
        float[] data = new float[1000];
        for (int i = 0; i < data.length; i++) {
            data[i] = i / 1000.0f;
        }

        String sparkline = Sparkline.generate(data, 12);
        System.out.println("Uniform distribution: " + sparkline);

        assertEquals(12, sparkline.length());
        // All bars should be approximately the same height (within a block or two)
    }

    @Test
    void testNormalDistribution() {
        Random rng = new Random(12345);
        float[] data = new float[10000];
        for (int i = 0; i < data.length; i++) {
            data[i] = (float) rng.nextGaussian();
        }

        String sparkline = Sparkline.generate(data, 12);
        System.out.println("Normal distribution: " + sparkline);

        assertEquals(12, sparkline.length());
        // Should show bell-curve shape - middle bars higher than edges
    }

    @Test
    void testEmptyData() {
        String sparkline = Sparkline.generate(new float[0], 8);
        assertEquals("        ", sparkline);
        assertEquals(8, sparkline.length());
    }

    @Test
    void testNullData() {
        String sparkline = Sparkline.generate((float[]) null, 8);
        assertEquals("        ", sparkline);
    }

    @Test
    void testAllSameValue() {
        float[] data = {5.0f, 5.0f, 5.0f, 5.0f, 5.0f};

        String sparkline = Sparkline.generate(data, 8);
        System.out.println("All same value: " + sparkline);

        assertEquals(8, sparkline.length());
        // All bars should be middle height (▄)
        assertTrue(sparkline.chars().allMatch(c -> c == '\u2584'));
    }

    @Test
    void testFromHistogram() {
        int[] bins = {1, 2, 4, 8, 16, 8, 4, 2, 1};

        String sparkline = Sparkline.fromHistogram(bins);
        System.out.println("From histogram: " + sparkline);

        assertEquals(9, sparkline.length());
        // Middle should be highest (█), edges lowest (▁)
    }

    @Test
    void testFromNormalized() {
        double[] normalized = {0.0, 0.125, 0.25, 0.375, 0.5, 0.625, 0.75, 0.875, 1.0};

        String sparkline = Sparkline.fromNormalized(normalized);
        System.out.println("From normalized: " + sparkline);

        assertEquals(9, sparkline.length());
        // Should show ascending bar heights
    }

    @Test
    void testDefaultWidth() {
        float[] data = {1.0f, 2.0f, 3.0f, 4.0f, 5.0f};
        String sparkline = Sparkline.generate(data);

        assertEquals(Sparkline.DEFAULT_WIDTH, sparkline.length());
    }

    @Test
    void testDoubleOverload() {
        double[] data = {0.1, 0.5, 0.9, 0.5, 0.1};

        String sparkline = Sparkline.generate(data, 5);
        System.out.println("Double data: " + sparkline);

        assertEquals(5, sparkline.length());
    }

    @Test
    void testWithInfiniteValues() {
        float[] data = {1.0f, 2.0f, Float.POSITIVE_INFINITY, 3.0f, Float.NEGATIVE_INFINITY, 4.0f};

        String sparkline = Sparkline.generate(data, 6);
        System.out.println("With infinite values: " + sparkline);

        assertEquals(6, sparkline.length());
        // Infinite values should be ignored in min/max calculation
    }

    @Test
    void testWithNaNValues() {
        float[] data = {1.0f, 2.0f, Float.NaN, 3.0f, Float.NaN, 4.0f};

        String sparkline = Sparkline.generate(data, 6);
        System.out.println("With NaN values: " + sparkline);

        assertEquals(6, sparkline.length());
        // NaN values should be ignored
    }
}
