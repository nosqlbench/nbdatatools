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

/**
 * Generates Unicode sparkline representations of data distributions.
 *
 * <p>Uses Unicode block elements (▁▂▃▄▅▆▇█) to create compact
 * visual histograms that fit in a single line of text.
 *
 * <h2>Block Characters</h2>
 * <pre>
 * ▁ U+2581 LOWER ONE EIGHTH BLOCK
 * ▂ U+2582 LOWER ONE QUARTER BLOCK
 * ▃ U+2583 LOWER THREE EIGHTHS BLOCK
 * ▄ U+2584 LOWER HALF BLOCK
 * ▅ U+2585 LOWER FIVE EIGHTHS BLOCK
 * ▆ U+2586 LOWER THREE QUARTERS BLOCK
 * ▇ U+2587 LOWER SEVEN EIGHTHS BLOCK
 * █ U+2588 FULL BLOCK
 * </pre>
 *
 * <h2>Usage</h2>
 * <pre>{@code
 * float[] data = ...;
 * String sparkline = Sparkline.generate(data, 12); // 12-char wide
 * // Output: "▁▂▄▇█▇▄▂▁▁▁▁" (normal distribution)
 * // Output: "▄▄▄▄▄▄▄▄▄▄▄▄" (uniform distribution)
 * }</pre>
 */
public final class Sparkline {

    /** Unicode block characters from lowest to highest */
    private static final char[] BLOCKS = {
        ' ',      // 0/8 - empty (for zero counts)
        '\u2581', // 1/8 ▁
        '\u2582', // 2/8 ▂
        '\u2583', // 3/8 ▃
        '\u2584', // 4/8 ▄
        '\u2585', // 5/8 ▅
        '\u2586', // 6/8 ▆
        '\u2587', // 7/8 ▇
        '\u2588'  // 8/8 █
    };

    /** Default sparkline width */
    public static final int DEFAULT_WIDTH = 12;

    private Sparkline() {}

    /**
     * Generates a sparkline histogram from float data.
     *
     * @param data the data values
     * @param width number of bins/characters in the sparkline
     * @return Unicode sparkline string
     */
    public static String generate(float[] data, int width) {
        if (data == null || data.length == 0) {
            return " ".repeat(width);
        }

        // Find min/max for binning
        float min = Float.MAX_VALUE;
        float max = Float.MIN_VALUE;
        for (float v : data) {
            if (Float.isFinite(v)) {
                min = Math.min(min, v);
                max = Math.max(max, v);
            }
        }

        if (min == max) {
            // All values identical - show flat line at middle height
            return String.valueOf(BLOCKS[4]).repeat(width);
        }

        // Compute histogram
        int[] bins = new int[width];
        float range = max - min;
        float binWidth = range / width;

        for (float v : data) {
            if (Float.isFinite(v)) {
                int bin = (int) ((v - min) / binWidth);
                if (bin >= width) bin = width - 1; // Handle max value
                bins[bin]++;
            }
        }

        // Find max bin count for normalization
        int maxCount = 0;
        for (int count : bins) {
            maxCount = Math.max(maxCount, count);
        }

        // Generate sparkline
        StringBuilder sb = new StringBuilder(width);
        for (int count : bins) {
            int level = (maxCount > 0) ? (count * 8 / maxCount) : 0;
            sb.append(BLOCKS[level]);
        }

        return sb.toString();
    }

    /**
     * Generates a sparkline with default width.
     *
     * @param data the data values
     * @return Unicode sparkline string
     */
    public static String generate(float[] data) {
        return generate(data, DEFAULT_WIDTH);
    }

    /**
     * Generates a sparkline from double data.
     *
     * @param data the data values
     * @param width number of bins/characters
     * @return Unicode sparkline string
     */
    public static String generate(double[] data, int width) {
        if (data == null) return " ".repeat(width);
        float[] floatData = new float[data.length];
        for (int i = 0; i < data.length; i++) {
            floatData[i] = (float) data[i];
        }
        return generate(floatData, width);
    }

    /**
     * Generates a sparkline from pre-computed histogram bins.
     *
     * @param bins histogram bin counts
     * @return Unicode sparkline string
     */
    public static String fromHistogram(int[] bins) {
        if (bins == null || bins.length == 0) {
            return "";
        }

        int maxCount = 0;
        for (int count : bins) {
            maxCount = Math.max(maxCount, count);
        }

        StringBuilder sb = new StringBuilder(bins.length);
        for (int count : bins) {
            int level = (maxCount > 0) ? (count * 8 / maxCount) : 0;
            sb.append(BLOCKS[level]);
        }

        return sb.toString();
    }

    /**
     * Generates a sparkline from normalized values (0.0 to 1.0).
     *
     * @param normalizedValues values between 0 and 1
     * @return Unicode sparkline string
     */
    public static String fromNormalized(double[] normalizedValues) {
        if (normalizedValues == null || normalizedValues.length == 0) {
            return "";
        }

        StringBuilder sb = new StringBuilder(normalizedValues.length);
        for (double v : normalizedValues) {
            int level = (int) Math.round(v * 8);
            level = Math.max(0, Math.min(8, level));
            sb.append(BLOCKS[level]);
        }

        return sb.toString();
    }
}
