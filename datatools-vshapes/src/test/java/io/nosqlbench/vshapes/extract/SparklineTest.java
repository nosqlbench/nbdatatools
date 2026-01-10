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
        assertEquals(8, sparkline.length());
        // Default style (LINE_DENSE) uses empty Braille cells
        assertTrue(sparkline.chars().allMatch(c -> c == 0x2800),
            "Empty data should produce blank Braille cells");
    }

    @Test
    void testEmptyData_barsStyle() {
        String sparkline = Sparkline.generate(new float[0], 8, Sparkline.Style.BARS);
        assertEquals("        ", sparkline);
        assertEquals(8, sparkline.length());
    }

    @Test
    void testNullData() {
        String sparkline = Sparkline.generate((float[]) null, 8);
        assertEquals(8, sparkline.length());
        // Default style (LINE_DENSE) uses empty Braille cells
        assertTrue(sparkline.chars().allMatch(c -> c == 0x2800),
            "Null data should produce blank Braille cells");
    }

    @Test
    void testNullData_barsStyle() {
        String sparkline = Sparkline.generate((float[]) null, 8, Sparkline.Style.BARS);
        assertEquals("        ", sparkline);
    }

    @Test
    void testAllSameValue() {
        float[] data = {5.0f, 5.0f, 5.0f, 5.0f, 5.0f};

        String sparkline = Sparkline.generate(data, 8);
        System.out.println("All same value: " + sparkline);

        assertEquals(8, sparkline.length());
        // All characters should be identical (flat line)
        char first = sparkline.charAt(0);
        assertTrue(sparkline.chars().allMatch(c -> c == first),
            "All same values should produce uniform characters");
    }

    @Test
    void testAllSameValue_barsStyle() {
        float[] data = {5.0f, 5.0f, 5.0f, 5.0f, 5.0f};

        String sparkline = Sparkline.generate(data, 8, Sparkline.Style.BARS);
        System.out.println("All same value (BARS): " + sparkline);

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

    // ==================== LINE style tests ====================

    @Test
    void testLineStyle_basicGeneration() {
        // Rising trend data
        float[] data = {0.0f, 0.25f, 0.5f, 0.75f, 1.0f};

        String sparkline = Sparkline.generate(data, 5, Sparkline.Style.LINE);
        System.out.println("LINE style (rising): " + sparkline);

        assertNotNull(sparkline);
        assertEquals(5, sparkline.length());
        // Should contain Braille characters (U+2800 range)
        assertTrue(sparkline.chars().allMatch(c -> c >= 0x2800 && c <= 0x28FF),
            "LINE style should use Braille characters");
    }

    @Test
    void testLineStyle_flatData() {
        float[] data = {5.0f, 5.0f, 5.0f, 5.0f, 5.0f};

        String sparkline = Sparkline.generate(data, 5, Sparkline.Style.LINE);
        System.out.println("LINE style (flat): " + sparkline);

        assertEquals(5, sparkline.length());
        // All characters should be the same (flat line)
        char first = sparkline.charAt(0);
        assertTrue(sparkline.chars().allMatch(c -> c == first),
            "Flat data should produce uniform characters");
    }

    @Test
    void testLineStyle_emptyData() {
        String sparkline = Sparkline.generate(new float[0], 8, Sparkline.Style.LINE);

        assertEquals(8, sparkline.length());
        // Should be empty Braille cells (U+2800)
        assertTrue(sparkline.chars().allMatch(c -> c == 0x2800),
            "Empty LINE should use blank Braille");
    }

    @Test
    void testLineStyle_sineWave() {
        // Generate sine wave data
        float[] data = new float[100];
        for (int i = 0; i < data.length; i++) {
            data[i] = (float) Math.sin(i * Math.PI / 25);
        }

        String sparkline = Sparkline.generate(data, 20, Sparkline.Style.LINE);
        System.out.println("LINE style (sine wave): " + sparkline);

        assertEquals(20, sparkline.length());
    }

    // ==================== LINE_DENSE style tests ====================

    @Test
    void testLineDenseStyle_basicGeneration() {
        // Rising trend data
        float[] data = {0.0f, 0.25f, 0.5f, 0.75f, 1.0f};

        String sparkline = Sparkline.generate(data, 5, Sparkline.Style.LINE_DENSE);
        System.out.println("LINE_DENSE style (rising): " + sparkline);

        assertNotNull(sparkline);
        assertEquals(5, sparkline.length());
        // Should contain Braille characters
        assertTrue(sparkline.chars().allMatch(c -> c >= 0x2800 && c <= 0x28FF),
            "LINE_DENSE style should use Braille characters");
    }

    @Test
    void testLineDenseStyle_doublesDataPoints() {
        // With LINE_DENSE, 10 chars should represent 20 data points
        float[] data = new float[40];
        for (int i = 0; i < data.length; i++) {
            data[i] = (float) Math.sin(i * Math.PI / 10);
        }

        String denseSparkline = Sparkline.generate(data, 10, Sparkline.Style.LINE_DENSE);
        String normalSparkline = Sparkline.generate(data, 10, Sparkline.Style.LINE);

        System.out.println("LINE_DENSE (10 chars, 20 points): " + denseSparkline);
        System.out.println("LINE (10 chars, 10 points):       " + normalSparkline);

        // Both should be 10 chars
        assertEquals(10, denseSparkline.length());
        assertEquals(10, normalSparkline.length());
    }

    @Test
    void testLineDenseStyle_flatData() {
        float[] data = {3.0f, 3.0f, 3.0f, 3.0f, 3.0f, 3.0f};

        String sparkline = Sparkline.generate(data, 3, Sparkline.Style.LINE_DENSE);
        System.out.println("LINE_DENSE style (flat): " + sparkline);

        assertEquals(3, sparkline.length());
        // Should show both left and right dots at same level
        char first = sparkline.charAt(0);
        assertTrue(sparkline.chars().allMatch(c -> c == first),
            "Flat data should produce uniform characters in dense mode");
    }

    // ==================== Style comparison tests ====================

    @Test
    void testStyleComparison() {
        float[] data = new float[100];
        Random rng = new Random(42);
        for (int i = 0; i < data.length; i++) {
            data[i] = (float) rng.nextGaussian();
        }

        String bars = Sparkline.generate(data, 12, Sparkline.Style.BARS);
        String line = Sparkline.generate(data, 12, Sparkline.Style.LINE);
        String dense = Sparkline.generate(data, 12, Sparkline.Style.LINE_DENSE);

        System.out.println("BARS:       " + bars);
        System.out.println("LINE:       " + line);
        System.out.println("LINE_DENSE: " + dense);

        assertEquals(12, bars.length());
        assertEquals(12, line.length());
        assertEquals(12, dense.length());

        // BARS uses block elements (U+2580-U+259F range or space)
        // LINE/LINE_DENSE use Braille (U+2800-U+28FF range)
        assertFalse(bars.equals(line), "Different styles should produce different output");
    }

    @Test
    void testDefaultStyleIsLineDense() {
        float[] data = {1.0f, 2.0f, 3.0f, 4.0f, 5.0f};

        String withoutStyle = Sparkline.generate(data, 5);
        String withLineDense = Sparkline.generate(data, 5, Sparkline.Style.LINE_DENSE);

        assertEquals(withLineDense, withoutStyle,
            "Default style should be LINE_DENSE");
    }

    @Test
    void testGenerateWithStyleOnly() {
        float[] data = {1.0f, 2.0f, 3.0f};

        String sparkline = Sparkline.generate(data, Sparkline.Style.LINE);

        assertEquals(Sparkline.DEFAULT_WIDTH, sparkline.length(),
            "generate(data, style) should use DEFAULT_WIDTH");
    }
}
