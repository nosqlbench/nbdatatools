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
 * <p>Supports multiple rendering styles via {@link Style}:
 * <ul>
 *   <li>{@link Style#BARS} - Block elements for histogram bars (8 vertical levels)</li>
 *   <li>{@link Style#LINE} - Braille dots for line plots (4 vertical levels)</li>
 *   <li>{@link Style#LINE_DENSE} - Braille with 2 data points per character</li>
 * </ul>
 *
 * <h2>Block Characters (BARS style)</h2>
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
 * <h2>Braille Characters (LINE styles)</h2>
 * <pre>
 * Braille cell layout (dot positions):
 *   1 4
 *   2 5
 *   3 6
 *   7 8
 *
 * LINE style uses left column (1,2,3,7) for 4 vertical levels.
 * LINE_DENSE uses both columns for 2× horizontal density.
 * </pre>
 *
 * <h2>Usage</h2>
 * <pre>{@code
 * float[] data = ...;
 *
 * // Histogram bars (default)
 * String bars = Sparkline.generate(data, 12);
 * // Output: "▁▂▄▇█▇▄▂▁▁▁▁"
 *
 * // Line plot with Braille
 * String line = Sparkline.generate(data, 12, Style.LINE);
 * // Output: "⡀⠄⠂⠁⠁⠂⠄⡀⡀⡀⡀⡀"
 *
 * // Dense line plot (2× data points)
 * String dense = Sparkline.generate(data, 12, Style.LINE_DENSE);
 * // Output: "⡠⠔⠊⠑⠢⡀⡀⡀⡀⡀⡀⡀" (24 data points in 12 chars)
 * }</pre>
 */
public final class Sparkline {

    /**
     * Rendering style for sparklines.
     */
    public enum Style {
        /**
         * Block element bars with 8 vertical levels.
         *
         * <p>Characters: ▁▂▃▄▅▆▇█
         * <p>Best for: Histograms, showing distribution shape
         */
        BARS,

        /**
         * Braille dot line plot with 4 vertical levels.
         *
         * <p>Characters: ⡀⠄⠂⠁ (bottom to top)
         * <p>Best for: Time series, trend visualization
         */
        LINE,

        /**
         * Dense Braille line plot with 2 data points per character.
         *
         * <p>Uses both left and right Braille columns, providing
         * 2× horizontal density at the cost of reduced clarity.
         * <p>Best for: High-density data in limited space
         */
        LINE_DENSE
    }

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

    /**
     * Braille dot patterns for left column (positions 1,2,3,7 from top to bottom).
     * Index 0 = bottom (row 4/position 7), Index 3 = top (row 1/position 1).
     */
    private static final int[] BRAILLE_LEFT = {
        0x40,  // ⡀ position 7 (bottom)
        0x04,  // ⠄ position 3
        0x02,  // ⠂ position 2
        0x01   // ⠁ position 1 (top)
    };

    /**
     * Braille dot patterns for right column (positions 4,5,6,8 from top to bottom).
     */
    private static final int[] BRAILLE_RIGHT = {
        0x80,  // ⢀ position 8 (bottom)
        0x20,  // ⠠ position 6
        0x10,  // ⠐ position 5
        0x08   // ⠈ position 4 (top)
    };

    /** Braille pattern base (empty cell) */
    private static final int BRAILLE_BASE = 0x2800;

    /** Default sparkline width */
    public static final int DEFAULT_WIDTH = 12;

    /** Default rendering style */
    public static final Style DEFAULT_STYLE = Style.LINE_DENSE;

    private Sparkline() {}

    /**
     * Generates a sparkline histogram from float data using default BARS style.
     *
     * @param data the data values
     * @param width number of bins/characters in the sparkline
     * @return Unicode sparkline string
     */
    public static String generate(float[] data, int width) {
        return generate(data, width, DEFAULT_STYLE);
    }

    /**
     * Generates a sparkline from float data with the specified style.
     *
     * @param data the data values
     * @param width number of characters in the output
     * @param style the rendering style
     * @return Unicode sparkline string
     */
    public static String generate(float[] data, int width, Style style) {
        if (data == null || data.length == 0) {
            return emptySparkline(width, style);
        }

        return switch (style) {
            case BARS -> generateBars(data, width);
            case LINE -> generateLine(data, width);
            case LINE_DENSE -> generateLineDense(data, width);
        };
    }

    /**
     * Generates empty sparkline for the given style.
     */
    private static String emptySparkline(int width, Style style) {
        return switch (style) {
            case BARS -> " ".repeat(width);
            case LINE, LINE_DENSE -> String.valueOf((char) BRAILLE_BASE).repeat(width);
        };
    }

    /**
     * Generates bar-style sparkline using block elements.
     */
    private static String generateBars(float[] data, int width) {
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
     * Generates line-style sparkline using Braille dots (1 point per character).
     *
     * <p>This treats the data as a series of values to plot, not a histogram.
     * Each value maps to a vertical position (0-3) shown as a Braille dot.
     */
    private static String generateLine(float[] data, int width) {
        // Resample data to fit width
        float[] resampled = resample(data, width);

        // Find min/max for normalization
        float min = Float.MAX_VALUE;
        float max = Float.MIN_VALUE;
        for (float v : resampled) {
            if (Float.isFinite(v)) {
                min = Math.min(min, v);
                max = Math.max(max, v);
            }
        }

        if (min == max) {
            // Flat line at middle height
            char midChar = (char) (BRAILLE_BASE | BRAILLE_LEFT[2]);
            return String.valueOf(midChar).repeat(width);
        }

        float range = max - min;
        StringBuilder sb = new StringBuilder(width);

        for (float v : resampled) {
            int level;
            if (!Float.isFinite(v)) {
                level = 0;
            } else {
                // Map to 0-3 range
                level = (int) ((v - min) / range * 3.99f);
                level = Math.max(0, Math.min(3, level));
            }
            sb.append((char) (BRAILLE_BASE | BRAILLE_LEFT[level]));
        }

        return sb.toString();
    }

    /**
     * Generates dense line-style sparkline using both Braille columns (2 points per character).
     *
     * <p>Each character contains two data points: left column for odd indices,
     * right column for even indices. This provides 2× horizontal density.
     */
    private static String generateLineDense(float[] data, int width) {
        // Each character holds 2 data points
        int dataPoints = width * 2;
        float[] resampled = resample(data, dataPoints);

        // Find min/max for normalization
        float min = Float.MAX_VALUE;
        float max = Float.MIN_VALUE;
        for (float v : resampled) {
            if (Float.isFinite(v)) {
                min = Math.min(min, v);
                max = Math.max(max, v);
            }
        }

        if (min == max) {
            // Flat line at middle height (both columns at level 2)
            char midChar = (char) (BRAILLE_BASE | BRAILLE_LEFT[2] | BRAILLE_RIGHT[2]);
            return String.valueOf(midChar).repeat(width);
        }

        float range = max - min;
        StringBuilder sb = new StringBuilder(width);

        for (int i = 0; i < dataPoints; i += 2) {
            int pattern = 0;

            // Left column (first point)
            float v1 = resampled[i];
            if (Float.isFinite(v1)) {
                int level1 = (int) ((v1 - min) / range * 3.99f);
                level1 = Math.max(0, Math.min(3, level1));
                pattern |= BRAILLE_LEFT[level1];
            }

            // Right column (second point, if available)
            if (i + 1 < dataPoints) {
                float v2 = resampled[i + 1];
                if (Float.isFinite(v2)) {
                    int level2 = (int) ((v2 - min) / range * 3.99f);
                    level2 = Math.max(0, Math.min(3, level2));
                    pattern |= BRAILLE_RIGHT[level2];
                }
            }

            sb.append((char) (BRAILLE_BASE | pattern));
        }

        return sb.toString();
    }

    /**
     * Resamples data array to target size using linear interpolation.
     */
    private static float[] resample(float[] data, int targetSize) {
        if (data.length == targetSize) {
            return data;
        }

        float[] result = new float[targetSize];
        float scale = (float) (data.length - 1) / (targetSize - 1);

        for (int i = 0; i < targetSize; i++) {
            float srcIndex = i * scale;
            int srcLow = (int) srcIndex;
            int srcHigh = Math.min(srcLow + 1, data.length - 1);
            float fraction = srcIndex - srcLow;

            result[i] = data[srcLow] * (1 - fraction) + data[srcHigh] * fraction;
        }

        return result;
    }

    /**
     * Generates a sparkline with default width and style.
     *
     * @param data the data values
     * @return Unicode sparkline string
     */
    public static String generate(float[] data) {
        return generate(data, DEFAULT_WIDTH, DEFAULT_STYLE);
    }

    /**
     * Generates a sparkline with default width.
     *
     * @param data the data values
     * @param style the rendering style
     * @return Unicode sparkline string
     */
    public static String generate(float[] data, Style style) {
        return generate(data, DEFAULT_WIDTH, style);
    }

    /**
     * Generates a sparkline from double data.
     *
     * @param data the data values
     * @param width number of bins/characters
     * @return Unicode sparkline string
     */
    public static String generate(double[] data, int width) {
        return generate(data, width, DEFAULT_STYLE);
    }

    /**
     * Generates a sparkline from double data with the specified style.
     *
     * @param data the data values
     * @param width number of characters
     * @param style the rendering style
     * @return Unicode sparkline string
     */
    public static String generate(double[] data, int width, Style style) {
        if (data == null) return emptySparkline(width, style);
        float[] floatData = new float[data.length];
        for (int i = 0; i < data.length; i++) {
            floatData[i] = (float) data[i];
        }
        return generate(floatData, width, style);
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
