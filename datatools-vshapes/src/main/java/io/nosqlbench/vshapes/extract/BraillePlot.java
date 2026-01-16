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

import io.nosqlbench.vshapes.model.CompositeScalarModel;
import io.nosqlbench.vshapes.model.ScalarModel;
import io.nosqlbench.vshapes.model.NormalScalarModel;
import io.nosqlbench.vshapes.model.UniformScalarModel;
import io.nosqlbench.vshapes.model.BetaScalarModel;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/// Unicode braille-based plotting utility for terminal visualization.
///
/// Braille characters (U+2800 to U+28FF) provide a 2x4 dot matrix per character,
/// enabling high-resolution plots in terminal output. Each character can represent
/// 8 individual dots arranged in a 2-column by 4-row grid:
///
/// ```
/// ┌───┬───┐
/// │ 1 │ 4 │  Bit positions:
/// ├───┼───┤  1=0x01, 2=0x02, 3=0x04, 4=0x08
/// │ 2 │ 5 │  5=0x10, 6=0x20, 7=0x40, 8=0x80
/// ├───┼───┤
/// │ 3 │ 6 │  Character = 0x2800 + (dot bits)
/// ├───┼───┤
/// │ 7 │ 8 │
/// └───┴───┘
/// ```
///
/// This class provides methods for rendering histograms and scatter plots
/// with support for multiple overlaid series in different ANSI colors.
public final class BraillePlot {

    /// ANSI color codes for series differentiation.
    public static final String[] SERIES_COLORS = {
        "\u001B[94m",  // Bright Blue
        "\u001B[91m",  // Bright Red
        "\u001B[92m",  // Bright Green
        "\u001B[93m",  // Bright Yellow
        "\u001B[95m",  // Bright Magenta
        "\u001B[96m",  // Bright Cyan
        "\u001B[34m",  // Blue
        "\u001B[31m",  // Red
    };

    /// ANSI reset code.
    public static final String RESET = "\u001B[0m";

    /// Box drawing characters for axes.
    private static final char VERTICAL_LINE = '│';
    private static final char HORIZONTAL_LINE = '─';
    private static final char CORNER_BL = '└';
    private static final char TICK_LEFT = '┤';

    /// Braille base character (empty cell).
    private static final int BRAILLE_BASE = 0x2800;

    /// Dot bit positions in braille character (column-major: left column 1,2,3,7; right column 4,5,6,8).
    private static final int[] LEFT_COLUMN_BITS = {0x01, 0x02, 0x04, 0x40};   // rows 0,1,2,3
    private static final int[] RIGHT_COLUMN_BITS = {0x08, 0x10, 0x20, 0x80};  // rows 0,1,2,3

    private BraillePlot() {}

    /// Renders a single-series histogram using braille dots.
    ///
    /// @param data the data values to histogram
    /// @param width plot width in characters (each char = 2 x-pixels)
    /// @param height plot height in lines (each line = 4 y-pixels)
    /// @param bins number of histogram bins (auto if <= 0)
    /// @return multi-line string containing the braille plot
    public static String histogram(float[] data, int width, int height, int bins) {
        if (data == null || data.length == 0) {
            return "(no data)";
        }

        // Auto-determine bins if not specified
        if (bins <= 0) {
            bins = width * 2;  // 2 x-pixels per character
        }

        // Compute histogram
        float min = Float.MAX_VALUE, max = Float.MIN_VALUE;
        for (float v : data) {
            if (v < min) min = v;
            if (v > max) max = v;
        }

        // Handle degenerate case
        if (min == max) {
            max = min + 1;
        }

        int[] counts = new int[bins];
        float binWidth = (max - min) / bins;
        for (float v : data) {
            int bin = Math.min((int) ((v - min) / binWidth), bins - 1);
            counts[bin]++;
        }

        // Find max count for normalization
        int maxCount = 0;
        for (int c : counts) {
            if (c > maxCount) maxCount = c;
        }

        // Convert counts to normalized heights (0.0 to 1.0)
        double[] normalized = new double[bins];
        for (int i = 0; i < bins; i++) {
            normalized[i] = maxCount > 0 ? (double) counts[i] / maxCount : 0;
        }

        return renderHistogram(normalized, width, height, min, max, maxCount);
    }

    /// Renders multiple histogram series overlaid with different colors.
    ///
    /// @param seriesData list of data arrays, one per series
    /// @param labels optional labels for each series (can be null)
    /// @param width plot width in characters
    /// @param height plot height in lines
    /// @param bins number of histogram bins (auto if <= 0)
    /// @return multi-line string containing the colored braille plot
    public static String multiHistogram(List<float[]> seriesData, List<String> labels,
                                        int width, int height, int bins) {
        if (seriesData == null || seriesData.isEmpty()) {
            return "(no data)";
        }

        // Auto-determine bins
        if (bins <= 0) {
            bins = width * 2;
        }

        // Find global min/max across all series
        float globalMin = Float.MAX_VALUE, globalMax = Float.MIN_VALUE;
        for (float[] data : seriesData) {
            for (float v : data) {
                if (v < globalMin) globalMin = v;
                if (v > globalMax) globalMax = v;
            }
        }

        if (globalMin == globalMax) {
            globalMax = globalMin + 1;
        }

        float binWidth = (globalMax - globalMin) / bins;

        // Compute histograms for each series
        int[][] allCounts = new int[seriesData.size()][bins];
        int globalMaxCount = 0;

        for (int s = 0; s < seriesData.size(); s++) {
            float[] data = seriesData.get(s);
            for (float v : data) {
                int bin = Math.min((int) ((v - globalMin) / binWidth), bins - 1);
                allCounts[s][bin]++;
            }
            for (int c : allCounts[s]) {
                if (c > globalMaxCount) globalMaxCount = c;
            }
        }

        // Convert to normalized heights
        double[][] normalized = new double[seriesData.size()][bins];
        for (int s = 0; s < seriesData.size(); s++) {
            for (int i = 0; i < bins; i++) {
                normalized[s][i] = globalMaxCount > 0 ? (double) allCounts[s][i] / globalMaxCount : 0;
            }
        }

        return renderMultiHistogram(normalized, labels, width, height, globalMin, globalMax, globalMaxCount);
    }

    /// Renders a scatter plot of two dimensions using braille dots.
    ///
    /// @param x x-coordinate values
    /// @param y y-coordinate values
    /// @param width plot width in characters
    /// @param height plot height in lines
    /// @return multi-line string containing the braille scatter plot
    public static String scatter(float[] x, float[] y, int width, int height) {
        if (x == null || y == null || x.length == 0 || y.length == 0) {
            return "(no data)";
        }

        int n = Math.min(x.length, y.length);

        // Find bounds
        float xMin = Float.MAX_VALUE, xMax = Float.MIN_VALUE;
        float yMin = Float.MAX_VALUE, yMax = Float.MIN_VALUE;
        for (int i = 0; i < n; i++) {
            if (x[i] < xMin) xMin = x[i];
            if (x[i] > xMax) xMax = x[i];
            if (y[i] < yMin) yMin = y[i];
            if (y[i] > yMax) yMax = y[i];
        }

        if (xMin == xMax) xMax = xMin + 1;
        if (yMin == yMax) yMax = yMin + 1;

        // Create dot grid (2 x-pixels per char, 4 y-pixels per line)
        int gridWidth = width * 2;
        int gridHeight = height * 4;
        boolean[][] dots = new boolean[gridHeight][gridWidth];

        // Plot points
        for (int i = 0; i < n; i++) {
            int px = (int) ((x[i] - xMin) / (xMax - xMin) * (gridWidth - 1));
            int py = (int) ((y[i] - yMin) / (yMax - yMin) * (gridHeight - 1));
            px = Math.max(0, Math.min(gridWidth - 1, px));
            py = Math.max(0, Math.min(gridHeight - 1, py));
            // Flip y (terminal y=0 is top)
            dots[gridHeight - 1 - py][px] = true;
        }

        return renderDotGrid(dots, width, height, xMin, xMax, yMin, yMax);
    }

    /// Renders multiple scatter series overlaid with different colors.
    ///
    /// @param xSeries list of x-coordinate arrays
    /// @param ySeries list of y-coordinate arrays
    /// @param labels optional labels for each series
    /// @param width plot width in characters
    /// @param height plot height in lines
    /// @return multi-line string containing the colored scatter plot
    public static String multiScatter(List<float[]> xSeries, List<float[]> ySeries,
                                      List<String> labels, int width, int height) {
        if (xSeries == null || ySeries == null || xSeries.isEmpty()) {
            return "(no data)";
        }

        // Find global bounds
        float xMin = Float.MAX_VALUE, xMax = Float.MIN_VALUE;
        float yMin = Float.MAX_VALUE, yMax = Float.MIN_VALUE;

        for (int s = 0; s < xSeries.size(); s++) {
            float[] x = xSeries.get(s);
            float[] y = ySeries.get(s);
            int n = Math.min(x.length, y.length);
            for (int i = 0; i < n; i++) {
                if (x[i] < xMin) xMin = x[i];
                if (x[i] > xMax) xMax = x[i];
                if (y[i] < yMin) yMin = y[i];
                if (y[i] > yMax) yMax = y[i];
            }
        }

        if (xMin == xMax) xMax = xMin + 1;
        if (yMin == yMax) yMax = yMin + 1;

        // Create colored dot grid (store series index, -1 = empty)
        int gridWidth = width * 2;
        int gridHeight = height * 4;
        int[][] seriesGrid = new int[gridHeight][gridWidth];
        for (int[] row : seriesGrid) {
            Arrays.fill(row, -1);
        }

        // Plot points for each series
        for (int s = 0; s < xSeries.size(); s++) {
            float[] x = xSeries.get(s);
            float[] y = ySeries.get(s);
            int n = Math.min(x.length, y.length);

            for (int i = 0; i < n; i++) {
                int px = (int) ((x[i] - xMin) / (xMax - xMin) * (gridWidth - 1));
                int py = (int) ((y[i] - yMin) / (yMax - yMin) * (gridHeight - 1));
                px = Math.max(0, Math.min(gridWidth - 1, px));
                py = Math.max(0, Math.min(gridHeight - 1, py));
                seriesGrid[gridHeight - 1 - py][px] = s;
            }
        }

        return renderColoredDotGrid(seriesGrid, labels, width, height, xMin, xMax, yMin, yMax);
    }

    // ============ Private rendering methods ============

    private static String renderHistogram(double[] normalized, int width, int height,
                                          float min, float max, int maxCount) {
        StringBuilder sb = new StringBuilder();
        int gridHeight = height * 4;

        // Y-axis label width
        String maxLabel = String.format("%.2f", 1.0);
        int yLabelWidth = maxLabel.length() + 2;

        // Render plot area line by line
        for (int row = 0; row < height; row++) {
            // Y-axis tick/label
            if (row == 0) {
                sb.append(String.format("%" + (yLabelWidth - 1) + ".1f ", 1.0));
            } else if (row == height - 1) {
                sb.append(String.format("%" + (yLabelWidth - 1) + ".1f ", 0.0));
            } else {
                sb.append(" ".repeat(yLabelWidth - 1)).append(VERTICAL_LINE);
            }

            // Render braille characters for this row
            for (int col = 0; col < width; col++) {
                int bits = 0;

                // Each character covers 2 x-bins and 4 y-rows
                for (int dx = 0; dx < 2; dx++) {
                    int binIdx = col * 2 + dx;
                    if (binIdx >= normalized.length) continue;

                    double barHeight = normalized[binIdx];
                    int filledRows = (int) (barHeight * gridHeight);

                    for (int dy = 0; dy < 4; dy++) {
                        int gridRow = row * 4 + dy;
                        int rowFromBottom = gridHeight - 1 - gridRow;

                        if (rowFromBottom < filledRows) {
                            bits |= (dx == 0 ? LEFT_COLUMN_BITS[dy] : RIGHT_COLUMN_BITS[dy]);
                        }
                    }
                }

                sb.append((char) (BRAILLE_BASE + bits));
            }
            sb.append('\n');
        }

        // X-axis
        sb.append(" ".repeat(yLabelWidth - 1)).append(CORNER_BL);
        sb.append(String.valueOf(HORIZONTAL_LINE).repeat(width));
        sb.append('\n');

        // X-axis labels
        sb.append(String.format("%" + yLabelWidth + ".2f", min));
        int midPos = width / 2 - 4;
        sb.append(" ".repeat(Math.max(0, midPos)));
        sb.append(String.format("%.2f", (min + max) / 2));
        int endPos = width - midPos - 8;
        sb.append(" ".repeat(Math.max(0, endPos)));
        sb.append(String.format("%.2f", max));
        sb.append('\n');

        return sb.toString();
    }

    private static String renderMultiHistogram(double[][] normalized, List<String> labels,
                                               int width, int height, float min, float max, int maxCount) {
        StringBuilder sb = new StringBuilder();
        int gridHeight = height * 4;
        int numSeries = normalized.length;
        int bins = normalized[0].length;

        int yLabelWidth = 8;
        String yMaxStr = String.format("%.2f", 1.0);
        String yMinStr = String.format("%.2f", 0.0);

        // Top X-axis labels (show X bounds at top too for shared axis clarity)
        sb.append(" ".repeat(yLabelWidth));
        sb.append(String.format("%-8.3f", min));
        int topMidPos = (width - 16) / 2;
        sb.append(" ".repeat(Math.max(0, topMidPos)));
        sb.append(String.format("%8.3f", max));
        sb.append('\n');

        // Top border
        sb.append(" ".repeat(yLabelWidth - 1)).append('┌');
        sb.append(String.valueOf(HORIZONTAL_LINE).repeat(width));
        sb.append('┐').append('\n');

        // Render plot area
        for (int row = 0; row < height; row++) {
            // Left Y-axis labels (top and bottom rows)
            if (row == 0) {
                sb.append(String.format("%" + (yLabelWidth - 1) + "s ", yMaxStr));
            } else if (row == height - 1) {
                sb.append(String.format("%" + (yLabelWidth - 1) + "s ", yMinStr));
            } else {
                sb.append(" ".repeat(yLabelWidth - 1)).append(VERTICAL_LINE);
            }

            // Track which series is dominant at each position for coloring
            for (int col = 0; col < width; col++) {
                int[] seriesBits = new int[numSeries];

                for (int dx = 0; dx < 2; dx++) {
                    int binIdx = col * 2 + dx;
                    if (binIdx >= bins) continue;

                    for (int s = 0; s < numSeries; s++) {
                        double barHeight = normalized[s][binIdx];
                        int filledRows = (int) (barHeight * gridHeight);

                        for (int dy = 0; dy < 4; dy++) {
                            int gridRow = row * 4 + dy;
                            int rowFromBottom = gridHeight - 1 - gridRow;

                            if (rowFromBottom < filledRows) {
                                seriesBits[s] |= (dx == 0 ? LEFT_COLUMN_BITS[dy] : RIGHT_COLUMN_BITS[dy]);
                            }
                        }
                    }
                }

                // Combine all series bits and find dominant series for color
                int combinedBits = 0;
                int dominantSeries = -1;
                int maxBitCount = 0;

                for (int s = 0; s < numSeries; s++) {
                    combinedBits |= seriesBits[s];
                    int bitCount = Integer.bitCount(seriesBits[s]);
                    if (bitCount > maxBitCount) {
                        maxBitCount = bitCount;
                        dominantSeries = s;
                    }
                }

                if (dominantSeries >= 0 && combinedBits > 0) {
                    sb.append(SERIES_COLORS[dominantSeries % SERIES_COLORS.length]);
                    sb.append((char) (BRAILLE_BASE + combinedBits));
                    sb.append(RESET);
                } else {
                    sb.append((char) (BRAILLE_BASE + combinedBits));
                }
            }

            // Right Y-axis labels (top and bottom rows)
            if (row == 0) {
                sb.append(String.format(" %-6s", yMaxStr));
            } else if (row == height - 1) {
                sb.append(String.format(" %-6s", yMinStr));
            } else {
                sb.append(VERTICAL_LINE);
            }
            sb.append('\n');
        }

        // Bottom X-axis border
        sb.append(" ".repeat(yLabelWidth - 1)).append(CORNER_BL);
        sb.append(String.valueOf(HORIZONTAL_LINE).repeat(width));
        sb.append('┘').append('\n');

        // Bottom X-axis labels
        sb.append(" ".repeat(yLabelWidth));
        sb.append(String.format("%-8.3f", min));
        int midPos = (width - 16) / 2;
        sb.append(" ".repeat(Math.max(0, midPos)));
        sb.append(String.format("%8.3f", max));
        sb.append('\n');

        // Legend
        if (labels != null) {
            sb.append('\n');
            for (int s = 0; s < numSeries; s++) {
                String label = s < labels.size() ? labels.get(s) : "Series " + (s + 1);
                sb.append("   ");
                sb.append(SERIES_COLORS[s % SERIES_COLORS.length]);
                sb.append("\u25CF");  // Filled circle
                sb.append(RESET);
                sb.append(" ").append(label).append('\n');
            }
        }

        return sb.toString();
    }

    private static String renderDotGrid(boolean[][] dots, int width, int height,
                                        float xMin, float xMax, float yMin, float yMax) {
        StringBuilder sb = new StringBuilder();
        int yLabelWidth = 6;

        for (int row = 0; row < height; row++) {
            // Y-axis
            if (row == 0) {
                sb.append(String.format("%" + (yLabelWidth - 1) + ".2f ", yMax));
            } else if (row == height - 1) {
                sb.append(String.format("%" + (yLabelWidth - 1) + ".2f ", yMin));
            } else {
                sb.append(" ".repeat(yLabelWidth - 1)).append(VERTICAL_LINE);
            }

            // Render braille
            for (int col = 0; col < width; col++) {
                int bits = 0;
                for (int dx = 0; dx < 2; dx++) {
                    for (int dy = 0; dy < 4; dy++) {
                        int gx = col * 2 + dx;
                        int gy = row * 4 + dy;
                        if (gx < dots[0].length && gy < dots.length && dots[gy][gx]) {
                            bits |= (dx == 0 ? LEFT_COLUMN_BITS[dy] : RIGHT_COLUMN_BITS[dy]);
                        }
                    }
                }
                sb.append((char) (BRAILLE_BASE + bits));
            }
            sb.append('\n');
        }

        // X-axis
        sb.append(" ".repeat(yLabelWidth - 1)).append(CORNER_BL);
        sb.append(String.valueOf(HORIZONTAL_LINE).repeat(width));
        sb.append('\n');

        // X-axis labels
        sb.append(String.format("%" + yLabelWidth + ".2f", xMin));
        int midPos = width / 2 - 4;
        sb.append(" ".repeat(Math.max(0, midPos)));
        sb.append(String.format("%.2f", (xMin + xMax) / 2));
        int endPos = width - midPos - 8;
        sb.append(" ".repeat(Math.max(0, endPos)));
        sb.append(String.format("%.2f", xMax));
        sb.append('\n');

        return sb.toString();
    }

    private static String renderColoredDotGrid(int[][] seriesGrid, List<String> labels,
                                               int width, int height,
                                               float xMin, float xMax, float yMin, float yMax) {
        StringBuilder sb = new StringBuilder();
        int yLabelWidth = 6;
        int numSeries = 0;
        for (int[] row : seriesGrid) {
            for (int s : row) {
                if (s >= numSeries) numSeries = s + 1;
            }
        }

        for (int row = 0; row < height; row++) {
            // Y-axis
            if (row == 0) {
                sb.append(String.format("%" + (yLabelWidth - 1) + ".2f ", yMax));
            } else if (row == height - 1) {
                sb.append(String.format("%" + (yLabelWidth - 1) + ".2f ", yMin));
            } else {
                sb.append(" ".repeat(yLabelWidth - 1)).append(VERTICAL_LINE);
            }

            // Render braille with colors
            for (int col = 0; col < width; col++) {
                int[] seriesBits = new int[Math.max(numSeries, 1)];
                int combinedBits = 0;

                for (int dx = 0; dx < 2; dx++) {
                    for (int dy = 0; dy < 4; dy++) {
                        int gx = col * 2 + dx;
                        int gy = row * 4 + dy;
                        if (gx < seriesGrid[0].length && gy < seriesGrid.length) {
                            int s = seriesGrid[gy][gx];
                            if (s >= 0) {
                                int bit = (dx == 0 ? LEFT_COLUMN_BITS[dy] : RIGHT_COLUMN_BITS[dy]);
                                seriesBits[s] |= bit;
                                combinedBits |= bit;
                            }
                        }
                    }
                }

                // Find dominant series
                int dominantSeries = -1;
                int maxBits = 0;
                for (int s = 0; s < seriesBits.length; s++) {
                    int count = Integer.bitCount(seriesBits[s]);
                    if (count > maxBits) {
                        maxBits = count;
                        dominantSeries = s;
                    }
                }

                if (dominantSeries >= 0 && combinedBits > 0) {
                    sb.append(SERIES_COLORS[dominantSeries % SERIES_COLORS.length]);
                    sb.append((char) (BRAILLE_BASE + combinedBits));
                    sb.append(RESET);
                } else {
                    sb.append((char) (BRAILLE_BASE + combinedBits));
                }
            }
            sb.append('\n');
        }

        // X-axis
        sb.append(" ".repeat(yLabelWidth - 1)).append(CORNER_BL);
        sb.append(String.valueOf(HORIZONTAL_LINE).repeat(width));
        sb.append('\n');

        // X-axis labels
        sb.append(String.format("%" + yLabelWidth + ".2f", xMin));
        int midPos = width / 2 - 4;
        sb.append(" ".repeat(Math.max(0, midPos)));
        sb.append(String.format("%.2f", (xMin + xMax) / 2));
        int endPos = width - midPos - 8;
        sb.append(" ".repeat(Math.max(0, endPos)));
        sb.append(String.format("%.2f", xMax));
        sb.append('\n');

        // Legend
        if (labels != null && numSeries > 0) {
            sb.append('\n');
            for (int s = 0; s < numSeries; s++) {
                String label = s < labels.size() ? labels.get(s) : "Series " + (s + 1);
                sb.append("   ");
                sb.append(SERIES_COLORS[s % SERIES_COLORS.length]);
                sb.append("\u25CF");
                sb.append(RESET);
                sb.append(" ").append(label).append('\n');
            }
        }

        return sb.toString();
    }

    /// Renders a histogram plot from one or more ScalarModels.
    ///
    /// This method samples from each model to generate histogram data,
    /// uses model bounds to set the display range, and shows bounds
    /// and normalization status on the plot borders.
    ///
    /// @param models list of ScalarModels to plot as separate series
    /// @param labels optional labels for each series
    /// @param width plot width in characters
    /// @param height plot height in lines
    /// @param sampleCount number of samples to generate per model
    /// @param isNormalized whether the data is known to be L2-normalized
    /// @return multi-line string containing the braille plot with annotations
    public static String modelHistogram(List<ScalarModel> models, List<String> labels,
                                        int width, int height, int sampleCount, boolean isNormalized) {
        return modelHistogram(models, labels, width, height, sampleCount, isNormalized, null, null);
    }

    /// Renders a histogram plot from one or more ScalarModels with source metadata.
    ///
    /// Uses inverse CDF sampling to generate values from each model.
    ///
    /// @param models list of ScalarModels to plot as separate series
    /// @param labels optional labels for each series
    /// @param width plot width in characters
    /// @param height plot height in lines
    /// @param sampleCount number of samples to generate per model
    /// @param isNormalized whether the data is known to be L2-normalized
    /// @param dimension optional dimension index (null if unknown)
    /// @param source optional data source description (e.g., filename)
    /// @return multi-line string containing the braille plot with annotations
    public static String modelHistogram(List<ScalarModel> models, List<String> labels,
                                        int width, int height, int sampleCount, boolean isNormalized,
                                        Integer dimension, String source) {
        if (models == null || models.isEmpty()) {
            return "(no models)";
        }

        int numSeries = models.size();
        int bins = width * 2;

        // Sample from each model first to find empirical min/max
        double[][] samples = new double[numSeries][sampleCount];
        Random random = new Random(42);

        for (int s = 0; s < numSeries; s++) {
            ScalarModel model = models.get(s);
            for (int i = 0; i < sampleCount; i++) {
                double u = random.nextDouble();
                // Use inverse CDF sampling: find x such that CDF(x) = u
                samples[s][i] = inverseCdfSample(model, u);
            }
        }

        // Determine global bounds from samples
        float globalMin = Float.MAX_VALUE, globalMax = Float.MIN_VALUE;
        Float[] modelMins = new Float[numSeries];
        Float[] modelMaxes = new Float[numSeries];

        for (int s = 0; s < numSeries; s++) {
            float sMin = Float.MAX_VALUE, sMax = Float.MIN_VALUE;
            for (double v : samples[s]) {
                if ((float)v < sMin) sMin = (float)v;
                if ((float)v > sMax) sMax = (float)v;
            }
            modelMins[s] = sMin;
            modelMaxes[s] = sMax;
            if (sMin < globalMin) globalMin = sMin;
            if (sMax > globalMax) globalMax = sMax;
        }

        // Handle degenerate case
        if (globalMin == globalMax) globalMax = globalMin + 1;

        float binWidth = (globalMax - globalMin) / bins;

        // Build histograms from samples
        int[][] allCounts = new int[numSeries][bins];
        int globalMaxCount = 0;

        for (int s = 0; s < numSeries; s++) {
            for (double value : samples[s]) {
                int bin = Math.min((int) ((value - globalMin) / binWidth), bins - 1);
                if (bin >= 0 && bin < bins) {
                    allCounts[s][bin]++;
                }
            }
            for (int c : allCounts[s]) {
                if (c > globalMaxCount) globalMaxCount = c;
            }
        }

        // Convert to normalized heights
        double[][] normalized = new double[numSeries][bins];
        for (int s = 0; s < numSeries; s++) {
            for (int i = 0; i < bins; i++) {
                normalized[s][i] = globalMaxCount > 0 ? (double) allCounts[s][i] / globalMaxCount : 0;
            }
        }

        return renderModelHistogram(normalized, labels, models, width, height,
            globalMin, globalMax, modelMins, modelMaxes, isNormalized, dimension, source);
    }

    /// Samples from a model using inverse CDF (binary search).
    ///
    /// Given u ∈ (0,1), finds x such that CDF(x) ≈ u.
    private static double inverseCdfSample(ScalarModel model, double u) {
        // Use binary search on the CDF
        // Start with a reasonable range based on model type
        double lo = -10.0, hi = 10.0;

        // Get tighter bounds from known model types
        if (model instanceof NormalScalarModel nm) {
            lo = nm.lower();
            hi = nm.upper();
            if (Double.isInfinite(lo)) lo = nm.getMean() - 6 * nm.getStdDev();
            if (Double.isInfinite(hi)) hi = nm.getMean() + 6 * nm.getStdDev();
        } else if (model instanceof UniformScalarModel um) {
            lo = um.getLower();
            hi = um.getUpper();
        } else if (model instanceof BetaScalarModel bm) {
            lo = bm.getLower();
            hi = bm.getUpper();
        } else if (model instanceof CompositeScalarModel cm) {
            // For composite, sample from component and use component bounds
            ScalarModel[] components = cm.getScalarModels();
            double[] weights = cm.getWeights();
            double r = new Random().nextDouble();
            double cumWeight = 0;
            for (int i = 0; i < components.length; i++) {
                cumWeight += weights[i];
                if (r < cumWeight) {
                    return inverseCdfSample(components[i], u);
                }
            }
            return inverseCdfSample(components[components.length - 1], u);
        }

        // Binary search for inverse CDF
        for (int iter = 0; iter < 100; iter++) {
            double mid = (lo + hi) / 2;
            double cdfMid = model.cdf(mid);
            if (Math.abs(cdfMid - u) < 1e-10) {
                return mid;
            } else if (cdfMid < u) {
                lo = mid;
            } else {
                hi = mid;
            }
        }
        return (lo + hi) / 2;
    }

    /// Renders model histogram with bounds annotations.
    private static String renderModelHistogram(double[][] normalized, List<String> labels,
                                               List<ScalarModel> models, int width, int height,
                                               float globalMin, float globalMax,
                                               Float[] modelMins, Float[] modelMaxes,
                                               boolean isNormalized, Integer dimension, String source) {
        StringBuilder sb = new StringBuilder();
        int gridHeight = height * 4;
        int numSeries = normalized.length;
        int bins = normalized[0].length;

        int yLabelWidth = 8;

        // Header line showing dimension and source
        StringBuilder headerLine = new StringBuilder();
        if (dimension != null) {
            headerLine.append("Dimension ").append(dimension);
        }
        if (source != null && !source.isEmpty()) {
            if (headerLine.length() > 0) headerLine.append(" | ");
            headerLine.append(source);
        }
        if (isNormalized) {
            if (headerLine.length() > 0) headerLine.append(" | ");
            headerLine.append("L2-NORMALIZED");
        }
        if (headerLine.length() > 0) {
            sb.append(" ".repeat(yLabelWidth)).append("[").append(headerLine).append("]");
            sb.append('\n');
        }

        // Top X-axis with bounds
        sb.append(" ".repeat(yLabelWidth));
        sb.append(String.format("%-10.4f", globalMin));
        int topMidPos = width - 20;
        sb.append(" ".repeat(Math.max(0, topMidPos)));
        sb.append(String.format("%10.4f", globalMax));
        sb.append('\n');

        // Top border
        sb.append(" ".repeat(yLabelWidth - 1)).append('┌');
        sb.append(String.valueOf(HORIZONTAL_LINE).repeat(width));
        sb.append('┐').append('\n');

        // Render plot area
        for (int row = 0; row < height; row++) {
            // Left Y-axis labels
            if (row == 0) {
                sb.append(String.format("%" + (yLabelWidth - 1) + ".2f ", 1.0));
            } else if (row == height - 1) {
                sb.append(String.format("%" + (yLabelWidth - 1) + ".2f ", 0.0));
            } else {
                sb.append(" ".repeat(yLabelWidth - 1)).append(VERTICAL_LINE);
            }

            // Render histogram bars with series coloring
            for (int col = 0; col < width; col++) {
                int[] seriesBits = new int[numSeries];

                for (int dx = 0; dx < 2; dx++) {
                    int binIdx = col * 2 + dx;
                    if (binIdx >= bins) continue;

                    for (int s = 0; s < numSeries; s++) {
                        double barHeight = normalized[s][binIdx];
                        int filledRows = (int) (barHeight * gridHeight);

                        for (int dy = 0; dy < 4; dy++) {
                            int gridRow = row * 4 + dy;
                            int rowFromBottom = gridHeight - 1 - gridRow;

                            if (rowFromBottom < filledRows) {
                                seriesBits[s] |= (dx == 0 ? LEFT_COLUMN_BITS[dy] : RIGHT_COLUMN_BITS[dy]);
                            }
                        }
                    }
                }

                int combinedBits = 0;
                int dominantSeries = -1;
                int maxBitCount = 0;

                for (int s = 0; s < numSeries; s++) {
                    combinedBits |= seriesBits[s];
                    int bitCount = Integer.bitCount(seriesBits[s]);
                    if (bitCount > maxBitCount) {
                        maxBitCount = bitCount;
                        dominantSeries = s;
                    }
                }

                if (dominantSeries >= 0 && combinedBits > 0) {
                    sb.append(SERIES_COLORS[dominantSeries % SERIES_COLORS.length]);
                    sb.append((char) (BRAILLE_BASE + combinedBits));
                    sb.append(RESET);
                } else {
                    sb.append((char) (BRAILLE_BASE + combinedBits));
                }
            }

            // Right Y-axis labels
            if (row == 0) {
                sb.append(String.format(" %.2f", 1.0));
            } else if (row == height - 1) {
                sb.append(String.format(" %.2f", 0.0));
            } else {
                sb.append(VERTICAL_LINE);
            }
            sb.append('\n');
        }

        // Bottom X-axis border
        sb.append(" ".repeat(yLabelWidth - 1)).append(CORNER_BL);
        sb.append(String.valueOf(HORIZONTAL_LINE).repeat(width));
        sb.append('┘').append('\n');

        // Bottom X-axis labels with bounds
        sb.append(" ".repeat(yLabelWidth));
        sb.append(String.format("%-10.4f", globalMin));
        int midPos = width - 20;
        sb.append(" ".repeat(Math.max(0, midPos)));
        sb.append(String.format("%10.4f", globalMax));
        sb.append('\n');

        // Legend with model bounds and specifications shown
        sb.append('\n');
        for (int s = 0; s < numSeries; s++) {
            ScalarModel model = models.get(s);
            String label = (labels != null && s < labels.size()) ? labels.get(s) : "Series " + (s + 1);
            sb.append("   ");
            sb.append(SERIES_COLORS[s % SERIES_COLORS.length]);
            sb.append("\u25CF");
            sb.append(RESET);
            sb.append(" ").append(label);

            // Show model bounds if specified
            if (modelMins[s] != null || modelMaxes[s] != null) {
                sb.append(" [");
                if (modelMins[s] != null) {
                    sb.append(String.format("%.3f", modelMins[s]));
                } else {
                    sb.append("-∞");
                }
                sb.append(", ");
                if (modelMaxes[s] != null) {
                    sb.append(String.format("%.3f", modelMaxes[s]));
                } else {
                    sb.append("+∞");
                }
                sb.append("]");
            }
            sb.append('\n');

            // Show model specification on next line
            String modelSpec = formatModelSpec(model);
            sb.append("     Model: ").append(modelSpec).append('\n');
        }

        return sb.toString();
    }

    /// Formats a ScalarModel as a readable specification string.
    private static String formatModelSpec(ScalarModel model) {
        if (model == null) return "(null)";

        String className = model.getClass().getSimpleName();

        // Handle composite models specially
        if (model instanceof CompositeScalarModel composite) {
            ScalarModel[] components = composite.getScalarModels();
            double[] weights = composite.getWeights();
            StringBuilder sb = new StringBuilder();
            sb.append("⊕").append(components.length).append("[");
            for (int i = 0; i < components.length; i++) {
                if (i > 0) sb.append(", ");
                sb.append(formatModelSpec(components[i]));
                sb.append(String.format("*%.2f", weights[i]));
            }
            sb.append("]");
            return sb.toString();
        }

        // Get distribution glyph and parameters
        String glyph = getDistributionGlyph(className);
        String params = getModelParams(model);
        return glyph + params;
    }

    /// Returns the Unicode glyph for a distribution type.
    private static String getDistributionGlyph(String className) {
        return switch (className) {
            case "NormalScalarModel" -> "𝒩";
            case "UniformScalarModel" -> "▭";
            case "BetaScalarModel" -> "β";
            case "GammaScalarModel" -> "Γ";
            case "StudentTScalarModel" -> "t";
            case "EmpiricalScalarModel" -> "H";
            default -> className.replace("ScalarModel", "");
        };
    }

    /// Extracts model parameters as a formatted string.
    private static String getModelParams(ScalarModel model) {
        // Get bounds from specific model types
        if (model instanceof NormalScalarModel nm) {
            double lo = nm.lower();
            double hi = nm.upper();
            String boundsStr = "";
            if (!Double.isInfinite(lo) || !Double.isInfinite(hi)) {
                boundsStr = String.format(":[%.2f,%.2f]",
                    Double.isInfinite(lo) ? -999 : lo,
                    Double.isInfinite(hi) ? 999 : hi);
            }
            return String.format("(%.3f,%.3f)%s", nm.getMean(), nm.getStdDev(), boundsStr);
        } else if (model instanceof UniformScalarModel um) {
            return String.format("(%.3f,%.3f)", um.getLower(), um.getUpper());
        } else if (model instanceof BetaScalarModel bm) {
            return String.format("(%.2f,%.2f):[%.2f,%.2f]",
                bm.getAlpha(), bm.getBeta(), bm.getLower(), bm.getUpper());
        } else {
            return "";
        }
    }

    /// Renders a composite model's components as separate series overlaid.
    ///
    /// Each component of the composite is shown in a different color,
    /// with weights displayed in the legend.
    ///
    /// @param composite the composite model to render
    /// @param width plot width in characters
    /// @param height plot height in lines
    /// @param sampleCount number of samples per component
    /// @param isNormalized whether the data is known to be normalized
    /// @return multi-line string containing the overlaid component plots
    public static String compositeHistogram(CompositeScalarModel composite, int width, int height,
                                            int sampleCount, boolean isNormalized) {
        return compositeHistogram(composite, width, height, sampleCount, isNormalized, null, null);
    }

    /// Renders a composite model's components as separate series overlaid with source metadata.
    ///
    /// @param composite the composite model to render
    /// @param width plot width in characters
    /// @param height plot height in lines
    /// @param sampleCount number of samples per component
    /// @param isNormalized whether the data is known to be normalized
    /// @param dimension optional dimension index
    /// @param source optional data source description
    /// @return multi-line string containing the overlaid component plots
    public static String compositeHistogram(CompositeScalarModel composite, int width, int height,
                                            int sampleCount, boolean isNormalized,
                                            Integer dimension, String source) {
        if (composite == null) {
            return "(no model)";
        }

        ScalarModel[] components = composite.getScalarModels();
        double[] weights = composite.getWeights();
        List<ScalarModel> componentList = Arrays.asList(components);

        // Create labels with weights
        List<String> labels = new java.util.ArrayList<>();
        for (int i = 0; i < components.length; i++) {
            String typeName = components[i].getClass().getSimpleName().replace("ScalarModel", "");
            labels.add(String.format("%s (%.0f%%)", typeName, weights[i] * 100));
        }

        return modelHistogram(componentList, labels, width, height, sampleCount, isNormalized, dimension, source);
    }

    /// Computes basic statistics for a data array.
    ///
    /// @param data the data values
    /// @return array of [min, max, mean, stddev]
    public static double[] computeStats(float[] data) {
        if (data == null || data.length == 0) {
            return new double[]{0, 0, 0, 0};
        }

        double sum = 0, sumSq = 0;
        float min = Float.MAX_VALUE, max = Float.MIN_VALUE;

        for (float v : data) {
            sum += v;
            sumSq += (double) v * v;
            if (v < min) min = v;
            if (v > max) max = v;
        }

        double mean = sum / data.length;
        double variance = (sumSq / data.length) - (mean * mean);
        double stddev = Math.sqrt(Math.max(0, variance));

        return new double[]{min, max, mean, stddev};
    }
}
