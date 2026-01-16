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
 * Accuracy tests for Sparkline generation.
 *
 * <p>These tests verify that sparklines correctly represent the shape
 * of various distributions and handle edge cases properly.
 */
@Tag("accuracy")
@Tag("unit")
public class SparklineAccuracyTest {

    // ==================== Distribution Shape Tests ====================

    @Test
    void uniformDistribution_producesFlattish() {
        // Uniform data should produce relatively even heights
        int n = 10000;
        float[] uniformData = new float[n];
        for (int i = 0; i < n; i++) {
            uniformData[i] = (float) i / n;  // Evenly spaced 0 to 1
        }

        String sparkline = Sparkline.generate(uniformData, 10, Sparkline.Style.BARS);

        // All characters should be similar height (within 2 levels)
        int minLevel = Integer.MAX_VALUE;
        int maxLevel = Integer.MIN_VALUE;

        for (char c : sparkline.toCharArray()) {
            int level = getBlockLevel(c);
            minLevel = Math.min(minLevel, level);
            maxLevel = Math.max(maxLevel, level);
        }

        assertTrue(maxLevel - minLevel <= 2,
            "Uniform distribution should have relatively even heights, " +
            "but diff was " + (maxLevel - minLevel));
    }

    @Test
    void normalDistribution_producesBellShape() {
        // Normal data should have peak in middle
        Random rng = new Random(42);
        int n = 10000;
        float[] normalData = new float[n];
        for (int i = 0; i < n; i++) {
            normalData[i] = (float) (rng.nextGaussian() * 0.15 + 0.5);
        }

        String sparkline = Sparkline.generate(normalData, 12, Sparkline.Style.BARS);

        // Find the peak (highest block)
        int peakIndex = -1;
        int maxLevel = -1;
        for (int i = 0; i < sparkline.length(); i++) {
            int level = getBlockLevel(sparkline.charAt(i));
            if (level > maxLevel) {
                maxLevel = level;
                peakIndex = i;
            }
        }

        // Peak should be near the center (within 40% of edges)
        int center = sparkline.length() / 2;
        int tolerance = sparkline.length() * 2 / 5;  // 40%

        assertTrue(Math.abs(peakIndex - center) <= tolerance,
            "Normal distribution peak should be near center, " +
            "but peak at " + peakIndex + " vs center " + center);
    }

    @Test
    void bimodalDistribution_producesTwoPeaks() {
        // Bimodal data should show two peaks
        Random rng = new Random(42);
        int n = 10000;
        float[] bimodalData = new float[n];
        for (int i = 0; i < n; i++) {
            if (i < n / 2) {
                bimodalData[i] = (float) (rng.nextGaussian() * 0.05 + 0.25);
            } else {
                bimodalData[i] = (float) (rng.nextGaussian() * 0.05 + 0.75);
            }
        }

        String sparkline = Sparkline.generate(bimodalData, 16, Sparkline.Style.BARS);

        // Count local maxima (peaks)
        int peaks = countPeaks(sparkline);

        assertTrue(peaks >= 2,
            "Bimodal distribution should show at least 2 peaks, found " + peaks);
    }

    @Test
    void skewedDistribution_producesAsymmetry() {
        // Right-skewed data should have peak on left
        Random rng = new Random(42);
        int n = 10000;
        float[] skewedData = new float[n];
        for (int i = 0; i < n; i++) {
            // Exponential-like distribution (right skewed)
            skewedData[i] = (float) (-Math.log(1 - rng.nextFloat()) * 0.2);
        }

        String sparkline = Sparkline.generate(skewedData, 12, Sparkline.Style.BARS);

        // Find peak
        int peakIndex = findPeakIndex(sparkline);

        // Peak should be in left half (right-skewed means more mass on left)
        assertTrue(peakIndex < sparkline.length() / 2,
            "Right-skewed distribution should peak on left, peak at " + peakIndex);
    }

    // ==================== Edge Case Tests ====================

    @Test
    void constantValues_producesMiddleBlocks() {
        float[] constant = {0.5f, 0.5f, 0.5f, 0.5f, 0.5f};

        String sparkline = Sparkline.generate(constant, 4, Sparkline.Style.BARS);

        // All should be the same middle block (▄)
        for (char c : sparkline.toCharArray()) {
            assertEquals('\u2584', c,
                "Constant values should produce middle blocks");
        }
    }

    @Test
    void emptyArray_producesSpaces() {
        String sparkline = Sparkline.generate(new float[0], 5, Sparkline.Style.BARS);

        assertEquals("     ", sparkline,
            "Empty array should produce spaces");
    }

    @Test
    void nullData_producesSpaces() {
        String sparkline = Sparkline.generate((float[]) null, 5, Sparkline.Style.BARS);

        assertEquals("     ", sparkline,
            "Null data should produce spaces");
    }

    @Test
    void singleValue_producesMiddleBlocks() {
        float[] single = {42.0f};

        String sparkline = Sparkline.generate(single, 3, Sparkline.Style.BARS);

        // Single value means min == max, should produce middle blocks
        for (char c : sparkline.toCharArray()) {
            assertEquals('\u2584', c,
                "Single value should produce middle blocks");
        }
    }

    // ==================== Histogram Accuracy Tests ====================

    @Test
    void fromHistogram_respectsBinCounts() {
        int[] bins = {0, 10, 20, 40, 80, 40, 20, 10, 0};

        String sparkline = Sparkline.fromHistogram(bins);

        // The peak (80) should be the highest block
        int maxBin = 4;  // Index of 80
        assertEquals('\u2588', sparkline.charAt(maxBin),
            "Highest bin should be full block");

        // Zeros should be spaces
        assertEquals(' ', sparkline.charAt(0),
            "Zero bin should be space");
        assertEquals(' ', sparkline.charAt(8),
            "Zero bin should be space");
    }

    @Test
    void fromNormalized_mapsCorrectly() {
        double[] normalized = {0.0, 0.25, 0.5, 0.75, 1.0};

        String sparkline = Sparkline.fromNormalized(normalized);

        assertEquals(5, sparkline.length());

        // 0.0 -> level 0 (space)
        assertEquals(' ', sparkline.charAt(0));

        // 0.5 -> level 4 (▄)
        assertEquals('\u2584', sparkline.charAt(2));

        // 1.0 -> level 8 (█)
        assertEquals('\u2588', sparkline.charAt(4));
    }

    // ==================== Width Tests ====================

    @Test
    void differentWidths_maintainShape() {
        Random rng = new Random(42);
        float[] data = new float[1000];
        for (int i = 0; i < data.length; i++) {
            data[i] = (float) rng.nextGaussian();
        }

        String narrow = Sparkline.generate(data, 6, Sparkline.Style.BARS);
        String wide = Sparkline.generate(data, 24, Sparkline.Style.BARS);

        assertEquals(6, narrow.length());
        assertEquals(24, wide.length());

        // Both should have peak near center
        int narrowPeak = findPeakIndex(narrow);
        int widePeak = findPeakIndex(wide);

        // Normalized positions should be similar (within 20%)
        double narrowPos = (double) narrowPeak / narrow.length();
        double widePos = (double) widePeak / wide.length();

        assertEquals(narrowPos, widePos, 0.2,
            "Peak position should be consistent across widths");
    }

    // ==================== Helper Methods ====================

    private int getBlockLevel(char c) {
        return switch (c) {
            case ' ' -> 0;
            case '\u2581' -> 1;
            case '\u2582' -> 2;
            case '\u2583' -> 3;
            case '\u2584' -> 4;
            case '\u2585' -> 5;
            case '\u2586' -> 6;
            case '\u2587' -> 7;
            case '\u2588' -> 8;
            default -> -1;
        };
    }

    private int findPeakIndex(String sparkline) {
        int peakIndex = 0;
        int maxLevel = -1;
        for (int i = 0; i < sparkline.length(); i++) {
            int level = getBlockLevel(sparkline.charAt(i));
            if (level > maxLevel) {
                maxLevel = level;
                peakIndex = i;
            }
        }
        return peakIndex;
    }

    private int countPeaks(String sparkline) {
        int peaks = 0;
        for (int i = 1; i < sparkline.length() - 1; i++) {
            int prev = getBlockLevel(sparkline.charAt(i - 1));
            int curr = getBlockLevel(sparkline.charAt(i));
            int next = getBlockLevel(sparkline.charAt(i + 1));

            if (curr > prev && curr >= next) {
                peaks++;
            }
        }
        // Check edges
        if (sparkline.length() >= 2) {
            if (getBlockLevel(sparkline.charAt(0)) > getBlockLevel(sparkline.charAt(1))) {
                peaks++;
            }
            int last = sparkline.length() - 1;
            if (getBlockLevel(sparkline.charAt(last)) > getBlockLevel(sparkline.charAt(last - 1))) {
                peaks++;
            }
        }
        return peaks;
    }
}
