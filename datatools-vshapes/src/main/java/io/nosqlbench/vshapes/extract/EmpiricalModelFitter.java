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

import io.nosqlbench.vshapes.model.EmpiricalScalarModel;

import java.util.Objects;

/**
 * Fits an empirical (histogram-based) distribution to observed data.
 *
 * <h2>Algorithm</h2>
 *
 * <p>Builds a histogram from the observed data and uses it to define
 * the empirical CDF. Sampling from this distribution reproduces the
 * shape of the observed data exactly (within binning resolution).
 *
 * <h2>Bin Count Selection</h2>
 *
 * <p>The number of histogram bins can be:
 * <ul>
 *   <li>Specified explicitly</li>
 *   <li>Computed using Sturges' rule: ceil(log2(n)) + 1</li>
 *   <li>Computed using Freedman-Diaconis rule for optimal width</li>
 * </ul>
 *
 * <h2>Goodness of Fit</h2>
 *
 * <p>The empirical distribution always has perfect fit to the training data
 * (by construction). The "goodness of fit" returned is based on the
 * histogram smoothness, which indicates how well the empirical model
 * will generalize to new data.
 *
 * <h2>When to Use</h2>
 *
 * <p>Empirical distributions are appropriate when:
 * <ul>
 *   <li>The underlying distribution is unknown or complex</li>
 *   <li>Parametric models don't fit well</li>
 *   <li>Preserving the exact shape of observed data is important</li>
 * </ul>
 *
 * @see EmpiricalScalarModel
 * @see ComponentModelFitter
 */
public final class EmpiricalModelFitter implements ComponentModelFitter {

    private final int binCount;
    private final boolean autoBinCount;

    /**
     * Creates an empirical model fitter with automatic bin count selection.
     */
    public EmpiricalModelFitter() {
        this.binCount = 0;
        this.autoBinCount = true;
    }

    /**
     * Creates an empirical model fitter with a specified bin count.
     *
     * @param binCount the number of histogram bins (must be at least 2)
     */
    public EmpiricalModelFitter(int binCount) {
        if (binCount < 2) {
            throw new IllegalArgumentException("binCount must be at least 2");
        }
        this.binCount = binCount;
        this.autoBinCount = false;
    }

    @Override
    public FitResult fit(float[] values) {
        Objects.requireNonNull(values, "values cannot be null");
        if (values.length == 0) {
            throw new IllegalArgumentException("values cannot be empty");
        }

        int bins = autoBinCount ? computeOptimalBinCount(values.length) : binCount;
        EmpiricalScalarModel model = EmpiricalScalarModel.fromData(values, bins);

        // Compute a smoothness-based score (lower is better)
        double goodnessOfFit = computeSmoothnessScore(values, bins);

        return new FitResult(model, goodnessOfFit, getModelType());
    }

    @Override
    public FitResult fit(DimensionStatistics stats, float[] values) {
        // Empirical model requires raw data
        return fit(values);
    }

    @Override
    public String getModelType() {
        return EmpiricalScalarModel.MODEL_TYPE;
    }

    @Override
    public boolean supportsBoundedData() {
        return true;
    }

    @Override
    public boolean requiresRawData() {
        return true;
    }

    /**
     * Computes optimal bin count using a combination of rules.
     *
     * <p>Uses Sturges' rule as a baseline, but clamps to reasonable bounds
     * to avoid too few or too many bins.
     *
     * <p>Bin count constraints:
     * <ul>
     *   <li>Minimum: 10 bins (ensures minimal fidelity)</li>
     *   <li>Maximum: 100 bins (limits model complexity and JSON size)</li>
     * </ul>
     */
    private int computeOptimalBinCount(int n) {
        // Sturges' rule: ceil(log2(n)) + 1
        int sturges = (int) Math.ceil(Math.log(n) / Math.log(2)) + 1;

        // Clamp to reasonable range [10, 100]
        // - Minimum 10 bins ensures minimal fidelity
        // - Maximum 100 bins limits complexity while maintaining accuracy
        int minBins = 10;
        int maxBins = 100;

        return Math.max(minBins, Math.min(maxBins, sturges));
    }

    /**
     * Computes a smoothness score for the histogram.
     *
     * <p>A smoother histogram (less variation between adjacent bins)
     * indicates a distribution that will generalize better. The score
     * is based on the total variation of the histogram.
     *
     * <p>Lower scores indicate smoother histograms.
     */
    private double computeSmoothnessScore(float[] values, int bins) {
        // Build histogram
        float min = values[0];
        float max = values[0];
        for (float v : values) {
            if (v < min) min = v;
            if (v > max) max = v;
        }

        if (max == min) {
            return 0;  // All values identical, perfectly smooth
        }

        int[] counts = new int[bins];
        float range = max - min;

        for (float v : values) {
            int bin = (int) ((v - min) / range * bins);
            if (bin >= bins) bin = bins - 1;
            if (bin < 0) bin = 0;
            counts[bin]++;
        }

        // Compute total variation (sum of absolute differences between adjacent bins)
        double totalVariation = 0;
        double expectedCount = (double) values.length / bins;

        for (int i = 1; i < bins; i++) {
            totalVariation += Math.abs(counts[i] - counts[i - 1]);
        }

        // Normalize by expected count and number of bins
        double normalizedVariation = totalVariation / (expectedCount * bins);

        // Also penalize sparse histograms (many empty bins)
        int emptyBins = 0;
        for (int count : counts) {
            if (count == 0) emptyBins++;
        }
        double sparsityPenalty = (double) emptyBins / bins;

        return normalizedVariation + sparsityPenalty;
    }
}
