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

import java.util.Arrays;

/**
 * Q-Q (Quantile-Quantile) plot generation utilities.
 *
 * <p>Q-Q plots compare the quantiles of two distributions by plotting
 * them against each other. If the distributions match, points lie on
 * a 45-degree line (y=x).
 *
 * <p>Key features:
 * <ul>
 *   <li>Generate Q-Q plot data points</li>
 *   <li>Compute Q-Q correlation coefficient</li>
 *   <li>ASCII/Unicode visualization for terminal output</li>
 * </ul>
 */
public final class QQPlotGenerator {

    private QQPlotGenerator() {
        // Utility class
    }

    /**
     * Q-Q plot data containing quantile pairs and correlation.
     *
     * @param xQuantiles quantiles from first distribution (original)
     * @param yQuantiles quantiles from second distribution (synthetic)
     * @param correlation Pearson correlation coefficient
     * @param probabilities probability points used (0 to 1)
     */
    public record QQPlotData(
        double[] xQuantiles,
        double[] yQuantiles,
        double correlation,
        double[] probabilities
    ) {
        /**
         * Returns the number of quantile points.
         */
        public int size() {
            return xQuantiles.length;
        }

        /**
         * Returns a specific point as [x, y].
         */
        public double[] point(int i) {
            return new double[] { xQuantiles[i], yQuantiles[i] };
        }
    }

    /**
     * Generates Q-Q plot data comparing two samples.
     *
     * @param original first sample (typically original data)
     * @param synthetic second sample (typically synthetic data)
     * @param numQuantiles number of quantile points (default 100)
     * @return Q-Q plot data with quantiles and correlation
     */
    public static QQPlotData generateQQPlot(float[] original, float[] synthetic, int numQuantiles) {
        if (original == null || synthetic == null) {
            throw new IllegalArgumentException("Samples cannot be null");
        }
        if (original.length < 2 || synthetic.length < 2) {
            throw new IllegalArgumentException("Samples must have at least 2 elements");
        }

        // Sort copies
        float[] sortedOrig = original.clone();
        float[] sortedSynth = synthetic.clone();
        Arrays.sort(sortedOrig);
        Arrays.sort(sortedSynth);

        // Generate probability points using median rank formula
        // p_i = (i - 0.375) / (n + 0.25) for better approximation
        double[] probs = new double[numQuantiles];
        for (int i = 0; i < numQuantiles; i++) {
            probs[i] = (i + 0.5) / numQuantiles;
        }

        // Compute quantiles
        double[] xQuantiles = computeQuantiles(sortedOrig, probs);
        double[] yQuantiles = computeQuantiles(sortedSynth, probs);

        // Compute correlation
        double correlation = computeCorrelation(xQuantiles, yQuantiles);

        return new QQPlotData(xQuantiles, yQuantiles, correlation, probs);
    }

    /**
     * Generates Q-Q plot with default 100 quantile points.
     */
    public static QQPlotData generateQQPlot(float[] original, float[] synthetic) {
        return generateQQPlot(original, synthetic, 100);
    }

    /**
     * Computes quantiles at specified probabilities for a sorted sample.
     */
    private static double[] computeQuantiles(float[] sorted, double[] probs) {
        double[] result = new double[probs.length];
        int n = sorted.length;

        for (int i = 0; i < probs.length; i++) {
            double index = probs[i] * (n - 1);
            int lower = (int) Math.floor(index);
            int upper = (int) Math.ceil(index);

            if (lower == upper || upper >= n) {
                result[i] = sorted[Math.min(lower, n - 1)];
            } else {
                double frac = index - lower;
                result[i] = sorted[lower] * (1 - frac) + sorted[upper] * frac;
            }
        }

        return result;
    }

    /**
     * Computes Pearson correlation between two arrays.
     */
    private static double computeCorrelation(double[] x, double[] y) {
        int n = x.length;
        if (n != y.length || n < 2) {
            throw new IllegalArgumentException("Arrays must have same length >= 2");
        }

        double meanX = 0, meanY = 0;
        for (int i = 0; i < n; i++) {
            meanX += x[i];
            meanY += y[i];
        }
        meanX /= n;
        meanY /= n;

        double cov = 0, varX = 0, varY = 0;
        for (int i = 0; i < n; i++) {
            double dx = x[i] - meanX;
            double dy = y[i] - meanY;
            cov += dx * dy;
            varX += dx * dx;
            varY += dy * dy;
        }

        if (varX <= 0 || varY <= 0) {
            return 1.0;  // Constant data
        }

        return cov / Math.sqrt(varX * varY);
    }

    /**
     * Renders an ASCII Q-Q plot for terminal display.
     *
     * @param data Q-Q plot data
     * @param width plot width in characters
     * @param height plot height in lines
     * @return ASCII representation of the Q-Q plot
     */
    public static String renderAsciiQQPlot(QQPlotData data, int width, int height) {
        if (width < 10 || height < 5) {
            throw new IllegalArgumentException("Plot dimensions must be at least 10x5");
        }

        // Find data bounds
        double minX = Double.MAX_VALUE, maxX = -Double.MAX_VALUE;
        double minY = Double.MAX_VALUE, maxY = -Double.MAX_VALUE;
        for (int i = 0; i < data.size(); i++) {
            minX = Math.min(minX, data.xQuantiles()[i]);
            maxX = Math.max(maxX, data.xQuantiles()[i]);
            minY = Math.min(minY, data.yQuantiles()[i]);
            maxY = Math.max(maxY, data.yQuantiles()[i]);
        }

        // Add small margin
        double xMargin = (maxX - minX) * 0.05;
        double yMargin = (maxY - minY) * 0.05;
        minX -= xMargin;
        maxX += xMargin;
        minY -= yMargin;
        maxY += yMargin;

        // Handle edge case of constant data
        if (maxX == minX) {
            maxX = minX + 1;
        }
        if (maxY == minY) {
            maxY = minY + 1;
        }

        // Create character grid
        char[][] grid = new char[height][width];
        for (int row = 0; row < height; row++) {
            Arrays.fill(grid[row], ' ');
        }

        // Draw diagonal reference line (y = x)
        double diagMin = Math.max(minX, minY);
        double diagMax = Math.min(maxX, maxY);
        for (int step = 0; step < width * 2; step++) {
            double v = diagMin + (diagMax - diagMin) * step / (width * 2);
            int col = (int) ((v - minX) / (maxX - minX) * (width - 1));
            int row = height - 1 - (int) ((v - minY) / (maxY - minY) * (height - 1));
            if (row >= 0 && row < height && col >= 0 && col < width) {
                if (grid[row][col] == ' ') {
                    grid[row][col] = '·';
                }
            }
        }

        // Plot data points
        for (int i = 0; i < data.size(); i++) {
            int col = (int) ((data.xQuantiles()[i] - minX) / (maxX - minX) * (width - 1));
            int row = height - 1 - (int) ((data.yQuantiles()[i] - minY) / (maxY - minY) * (height - 1));

            if (row >= 0 && row < height && col >= 0 && col < width) {
                grid[row][col] = '●';
            }
        }

        // Build output string
        StringBuilder sb = new StringBuilder();
        sb.append("Q-Q Plot (r = ").append(String.format("%.4f", data.correlation())).append(")\n");
        sb.append(String.format("Synthetic ↑ [%.2f, %.2f]\n", minY, maxY));

        for (int row = 0; row < height; row++) {
            sb.append("│");
            sb.append(new String(grid[row]));
            sb.append("\n");
        }

        sb.append("└").append("─".repeat(width)).append("\n");
        sb.append(String.format("Original → [%.2f, %.2f]\n", minX, maxX));

        return sb.toString();
    }

    /**
     * Renders a compact Q-Q plot (40x15).
     */
    public static String renderAsciiQQPlot(QQPlotData data) {
        return renderAsciiQQPlot(data, 40, 15);
    }

    /**
     * Generates a Q-Q plot summary with key metrics.
     *
     * @param data Q-Q plot data
     * @return formatted summary string
     */
    public static String formatQQSummary(QQPlotData data) {
        StringBuilder sb = new StringBuilder();
        sb.append("Q-Q PLOT SUMMARY\n");
        sb.append("================\n");
        sb.append(String.format("Quantile points: %d\n", data.size()));
        sb.append(String.format("Correlation: %.6f\n", data.correlation()));

        // Compute deviation from y=x line
        double sumSquaredDev = 0;
        double maxDev = 0;
        for (int i = 0; i < data.size(); i++) {
            double dev = Math.abs(data.xQuantiles()[i] - data.yQuantiles()[i]);
            sumSquaredDev += dev * dev;
            maxDev = Math.max(maxDev, dev);
        }
        double rmsDev = Math.sqrt(sumSquaredDev / data.size());

        sb.append(String.format("RMS deviation from y=x: %.6f\n", rmsDev));
        sb.append(String.format("Max deviation from y=x: %.6f\n", maxDev));

        // Quality assessment
        String quality;
        if (data.correlation() >= 0.999) {
            quality = "EXCELLENT";
        } else if (data.correlation() >= 0.995) {
            quality = "VERY GOOD";
        } else if (data.correlation() >= 0.99) {
            quality = "GOOD";
        } else if (data.correlation() >= 0.95) {
            quality = "FAIR";
        } else {
            quality = "POOR";
        }
        sb.append(String.format("Quality: %s\n", quality));

        return sb.toString();
    }

    /**
     * Computes detailed Q-Q deviation statistics.
     *
     * @param data Q-Q plot data
     * @return deviation statistics
     */
    public static DeviationStats computeDeviationStats(QQPlotData data) {
        int n = data.size();
        double[] deviations = new double[n];

        for (int i = 0; i < n; i++) {
            deviations[i] = data.yQuantiles()[i] - data.xQuantiles()[i];
        }

        // Compute statistics
        double sum = 0, sumSq = 0;
        double min = Double.MAX_VALUE, max = -Double.MAX_VALUE;
        for (double d : deviations) {
            sum += d;
            sumSq += d * d;
            min = Math.min(min, d);
            max = Math.max(max, d);
        }

        double mean = sum / n;
        double variance = (sumSq - sum * sum / n) / (n - 1);
        double stdDev = Math.sqrt(Math.max(0, variance));

        // Absolute deviations
        double sumAbsDev = 0;
        double maxAbsDev = 0;
        for (double d : deviations) {
            double absDev = Math.abs(d);
            sumAbsDev += absDev;
            maxAbsDev = Math.max(maxAbsDev, absDev);
        }
        double meanAbsDev = sumAbsDev / n;

        return new DeviationStats(mean, stdDev, min, max, meanAbsDev, maxAbsDev);
    }

    /**
     * Statistics about deviations from the y=x line.
     *
     * @param meanDeviation mean signed deviation (bias)
     * @param stdDeviation standard deviation of deviations
     * @param minDeviation minimum deviation
     * @param maxDeviation maximum deviation
     * @param meanAbsDeviation mean absolute deviation
     * @param maxAbsDeviation maximum absolute deviation
     */
    public record DeviationStats(
        double meanDeviation,
        double stdDeviation,
        double minDeviation,
        double maxDeviation,
        double meanAbsDeviation,
        double maxAbsDeviation
    ) {
        /**
         * Returns true if deviations are within acceptable bounds.
         */
        public boolean withinTolerance(double tolerance) {
            return maxAbsDeviation < tolerance;
        }
    }
}
