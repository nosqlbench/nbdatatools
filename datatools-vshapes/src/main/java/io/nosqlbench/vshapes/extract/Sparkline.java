package io.nosqlbench.vshapes.extract;

import io.nosqlbench.vshapes.model.CompositeScalarModel;
import io.nosqlbench.vshapes.model.ScalarModel;

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

    /**
     * Generates a sparkline representation of a ScalarModel's distribution shape.
     *
     * <p>This samples the model's PDF at evenly spaced quantile points to create
     * a visual representation of the distribution shape. For models without a PDF
     * method, the PDF is approximated by differentiating the CDF.
     *
     * @param model the scalar model to visualize
     * @param width number of characters in the sparkline
     * @return Unicode sparkline string showing the distribution shape
     */
    public static String forModel(ScalarModel model, int width) {
        if (model == null) {
            return " ".repeat(width);
        }

        // Extract bounds based on model type
        double lower = extractLowerBound(model);
        double upper = extractUpperBound(model);
        double range = upper - lower;

        if (range <= 0 || !Double.isFinite(range)) {
            // Degenerate case - single point or unbounded
            return String.valueOf(BLOCKS[4]).repeat(width);
        }

        // Sample PDF at evenly spaced points across the distribution's range
        // We approximate PDF from CDF: f(x) ≈ (F(x+dx) - F(x-dx)) / (2*dx)
        double[] pdfValues = new double[width];
        double dx = range / width / 10.0;  // Small step for derivative
        double maxPdf = 0;

        for (int i = 0; i < width; i++) {
            double x = lower + (i + 0.5) * range / width;
            double pdf = approximatePdf(model, x, dx);
            if (Double.isFinite(pdf) && pdf >= 0) {
                pdfValues[i] = pdf;
                maxPdf = Math.max(maxPdf, pdf);
            }
        }

        // Normalize and convert to bar heights
        StringBuilder sb = new StringBuilder(width);
        for (double pdf : pdfValues) {
            int level;
            if (maxPdf > 0) {
                level = (int) Math.round(pdf / maxPdf * 8);
                level = Math.max(0, Math.min(8, level));
            } else {
                level = 0;
            }
            sb.append(BLOCKS[level]);
        }

        return sb.toString();
    }

    /**
     * Extracts the lower bound from a ScalarModel by checking its concrete type.
     */
    private static double extractLowerBound(ScalarModel model) {
        if (model instanceof io.nosqlbench.vshapes.model.NormalScalarModel normal) {
            return normal.lower();
        }
        if (model instanceof io.nosqlbench.vshapes.model.UniformScalarModel uniform) {
            return uniform.getLower();
        }
        if (model instanceof io.nosqlbench.vshapes.model.BetaScalarModel beta) {
            return beta.getLower();
        }
        if (model instanceof io.nosqlbench.vshapes.model.GammaScalarModel gamma) {
            return gamma.getLower();
        }
        if (model instanceof io.nosqlbench.vshapes.model.EmpiricalScalarModel empirical) {
            return empirical.getMin();
        }
        if (model instanceof CompositeScalarModel composite) {
            // Use the minimum lower bound across all components
            double minLower = Double.POSITIVE_INFINITY;
            for (ScalarModel comp : composite.getScalarModels()) {
                minLower = Math.min(minLower, extractLowerBound(comp));
            }
            return Double.isFinite(minLower) ? minLower : -1.0;
        }
        // Fallback: use quantile approach to estimate 0.1% percentile
        return estimateQuantile(model, 0.001);
    }

    /**
     * Extracts the upper bound from a ScalarModel by checking its concrete type.
     */
    private static double extractUpperBound(ScalarModel model) {
        if (model instanceof io.nosqlbench.vshapes.model.NormalScalarModel normal) {
            return normal.upper();
        }
        if (model instanceof io.nosqlbench.vshapes.model.UniformScalarModel uniform) {
            return uniform.getUpper();
        }
        if (model instanceof io.nosqlbench.vshapes.model.BetaScalarModel beta) {
            return beta.getUpper();
        }
        if (model instanceof io.nosqlbench.vshapes.model.GammaScalarModel gamma) {
            // Gamma is unbounded above - use quantile for practical upper bound
            return estimateQuantile(model, 0.999);
        }
        if (model instanceof io.nosqlbench.vshapes.model.EmpiricalScalarModel empirical) {
            return empirical.getMax();
        }
        if (model instanceof CompositeScalarModel composite) {
            // Use the maximum upper bound across all components
            double maxUpper = Double.NEGATIVE_INFINITY;
            for (ScalarModel comp : composite.getScalarModels()) {
                maxUpper = Math.max(maxUpper, extractUpperBound(comp));
            }
            return Double.isFinite(maxUpper) ? maxUpper : 1.0;
        }
        // Fallback: use quantile approach to estimate 99.9% percentile
        return estimateQuantile(model, 0.999);
    }

    /**
     * Estimates a quantile by inverting the CDF via binary search.
     */
    private static double estimateQuantile(ScalarModel model, double p) {
        // Binary search for x such that CDF(x) = p
        double lo = -1e6;
        double hi = 1e6;

        // First, find bounds where CDF crosses target
        while (model.cdf(lo) > p && lo > -1e10) lo *= 2;
        while (model.cdf(hi) < p && hi < 1e10) hi *= 2;

        // Binary search
        for (int i = 0; i < 50; i++) {
            double mid = (lo + hi) / 2;
            if (model.cdf(mid) < p) {
                lo = mid;
            } else {
                hi = mid;
            }
        }
        return (lo + hi) / 2;
    }

    /**
     * Approximates PDF by differentiating CDF.
     */
    private static double approximatePdf(ScalarModel model, double x, double dx) {
        double cdfLo = model.cdf(x - dx);
        double cdfHi = model.cdf(x + dx);
        return (cdfHi - cdfLo) / (2 * dx);
    }

    /**
     * Generates a sparkline for a model with default width.
     *
     * @param model the scalar model to visualize
     * @return Unicode sparkline string
     */
    public static String forModel(ScalarModel model) {
        return forModel(model, DEFAULT_WIDTH);
    }

    /// Default number of samples for data-based sparklines
    private static final int DEFAULT_SAMPLE_COUNT = 10000;

    /**
     * Generates a sparkline by sampling from the model and creating a histogram.
     *
     * <p>Unlike {@link #forModel(ScalarModel, int)} which shows the theoretical PDF,
     * this method generates actual samples from the model and shows what the data
     * distribution looks like. This is more useful for comparing models because:
     * <ul>
     *   <li>A composite model with overlapping modes may produce smooth data even
     *       though its theoretical PDF shows distinct peaks</li>
     *   <li>Two statistically equivalent models will produce similar histograms</li>
     * </ul>
     *
     * @param model the scalar model to sample from
     * @param width number of characters in the sparkline (also number of bins)
     * @return Unicode sparkline string showing the data histogram
     */
    public static String forModelSamples(ScalarModel model, int width) {
        return forModelSamples(model, width, DEFAULT_SAMPLE_COUNT);
    }

    /**
     * Generates a sparkline by sampling from the model and creating a histogram.
     *
     * @param model the scalar model to sample from
     * @param width number of characters in the sparkline (also number of bins)
     * @param sampleCount number of samples to generate
     * @return Unicode sparkline string showing the data histogram
     */
    public static String forModelSamples(ScalarModel model, int width, int sampleCount) {
        if (model == null) {
            return " ".repeat(width);
        }

        // Generate samples from the model using stratified sampling
        // Use CDF inversion (quantile) via estimateQuantile()
        float[] samples = new float[sampleCount];
        for (int i = 0; i < sampleCount; i++) {
            // Use stratified sampling for better coverage
            double u = (i + 0.5) / sampleCount;
            samples[i] = (float) estimateQuantile(model, u);
        }

        // Generate histogram sparkline from the samples
        return generateBars(samples, width);
    }

    /**
     * Generates a sparkline by sampling from the model with default width.
     *
     * @param model the scalar model to sample from
     * @return Unicode sparkline string showing the data histogram
     */
    public static String forModelSamples(ScalarModel model) {
        return forModelSamples(model, DEFAULT_WIDTH, DEFAULT_SAMPLE_COUNT);
    }

    /**
     * Result containing sparklines for a composite model and its components.
     *
     * @param overall sparkline for the combined composite distribution
     * @param components sparklines for each component, with weights
     */
    public record CompositeSparklines(
        String overall,
        java.util.List<ComponentSparkline> components
    ) {}

    /**
     * A component sparkline with its weight.
     *
     * @param sparkline the sparkline for this component
     * @param weight the mixture weight (0.0 to 1.0)
     * @param modelType the type of the component model
     * @param params formatted parameter string
     */
    public record ComponentSparkline(
        String sparkline,
        double weight,
        String modelType,
        String params
    ) {}

    /**
     * Generates sparklines for a composite model and all its components.
     *
     * <p>The overall sparkline uses sample-based histogram to show what the
     * actual data looks like, while individual component sparklines use
     * theoretical PDFs to show each component's shape.
     *
     * @param composite the composite model
     * @param width sparkline width
     * @return composite sparklines with component breakdown
     */
    public static CompositeSparklines forComposite(CompositeScalarModel composite, int width) {
        if (composite == null) {
            return new CompositeSparklines(" ".repeat(width), java.util.List.of());
        }

        // Generate overall sparkline using samples (shows actual data shape)
        String overall = forModelSamples(composite, width);

        // Generate sparklines for each component using theoretical PDFs
        // (shows what each component contributes)
        java.util.List<ComponentSparkline> componentSparklines = new java.util.ArrayList<>();
        ScalarModel[] components = composite.getScalarModels();
        double[] weights = composite.getWeights();

        for (int i = 0; i < components.length; i++) {
            ScalarModel comp = components[i];
            double weight = (i < weights.length) ? weights[i] : 1.0 / components.length;
            String sparkline = forModelSamples(comp, width);
            String params = formatComponentParams(comp);
            componentSparklines.add(new ComponentSparkline(sparkline, weight, comp.getModelType(), params));
        }

        return new CompositeSparklines(overall, componentSparklines);
    }

    /**
     * Formats component model parameters for display.
     */
    private static String formatComponentParams(ScalarModel model) {
        if (model instanceof io.nosqlbench.vshapes.model.NormalScalarModel normal) {
            return String.format("μ=%.2f, σ=%.2f", normal.getMean(), normal.getStdDev());
        }
        if (model instanceof io.nosqlbench.vshapes.model.BetaScalarModel beta) {
            return String.format("α=%.2f, β=%.2f", beta.getAlpha(), beta.getBeta());
        }
        if (model instanceof io.nosqlbench.vshapes.model.UniformScalarModel uniform) {
            return String.format("[%.2f,%.2f]", uniform.getLower(), uniform.getUpper());
        }
        if (model instanceof io.nosqlbench.vshapes.model.GammaScalarModel gamma) {
            return String.format("k=%.2f, θ=%.2f", gamma.getShape(), gamma.getScale());
        }
        return model.getModelType();
    }
}
