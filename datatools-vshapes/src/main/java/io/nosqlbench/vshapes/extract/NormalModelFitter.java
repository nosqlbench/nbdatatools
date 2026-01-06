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

import io.nosqlbench.vshapes.model.NormalCDF;
import io.nosqlbench.vshapes.model.NormalScalarModel;

import java.util.Objects;

/**
 * Fits a Normal (Gaussian) distribution to observed data - Pearson Type 0.
 *
 * <h2>Algorithm</h2>
 *
 * <p>Uses maximum likelihood estimation (MLE), which for a normal
 * distribution simply means:
 * <ul>
 *   <li>μ = sample mean</li>
 *   <li>σ = sample standard deviation</li>
 * </ul>
 *
 * <h2>Truncation Detection</h2>
 *
 * <p>This fitter can optionally detect if the data appears truncated
 * (bounded within a finite range) and produce a truncated normal model.
 * Truncation is detected by examining the distribution of values near
 * the observed min/max bounds.
 *
 * <h2>Goodness of Fit</h2>
 *
 * <p>Uses the Anderson-Darling test statistic as the goodness-of-fit measure.
 * This test is particularly sensitive to deviations in the tails, making it
 * suitable for detecting non-normality.
 *
 * @see NormalScalarModel
 * @see ComponentModelFitter
 */
public final class NormalModelFitter implements ComponentModelFitter {

    private final boolean detectTruncation;
    private final double truncationThreshold;

    /**
     * Creates a Normal model fitter with default settings.
     */
    public NormalModelFitter() {
        this(true, 0.01);
    }

    /**
     * Creates a Normal model fitter with specified settings.
     *
     * @param detectTruncation whether to detect and model truncation
     * @param truncationThreshold fraction of data near bounds to trigger truncation detection
     */
    public NormalModelFitter(boolean detectTruncation, double truncationThreshold) {
        this.detectTruncation = detectTruncation;
        this.truncationThreshold = truncationThreshold;
    }

    @Override
    public FitResult fit(float[] values) {
        Objects.requireNonNull(values, "values cannot be null");
        if (values.length == 0) {
            throw new IllegalArgumentException("values cannot be empty");
        }

        DimensionStatistics stats = DimensionStatistics.compute(0, values);
        return fit(stats, values);
    }

    @Override
    public FitResult fit(DimensionStatistics stats, float[] values) {
        Objects.requireNonNull(stats, "stats cannot be null");

        double mean = stats.mean();
        double stdDev = stats.stdDev();

        // Ensure stdDev is positive
        if (stdDev <= 0) {
            stdDev = 1e-10;
        }

        NormalScalarModel model;

        if (detectTruncation && detectsTruncation(stats, values)) {
            // Create truncated normal model
            double lower = stats.min();
            double upper = stats.max();
            model = new NormalScalarModel(mean, stdDev, lower, upper);
        } else {
            // Create unbounded normal model
            model = new NormalScalarModel(mean, stdDev);
        }

        // Compute goodness-of-fit using Anderson-Darling statistic
        double goodnessOfFit = computeAndersonDarling(values, mean, stdDev);

        return new FitResult(model, goodnessOfFit, getModelType());
    }

    @Override
    public String getModelType() {
        return NormalScalarModel.MODEL_TYPE;
    }

    @Override
    public boolean supportsBoundedData() {
        return detectTruncation;
    }

    /**
     * Detects if the data appears to be truncated.
     *
     * <p>Heuristics used:
     * <ul>
     *   <li>Check if significant mass is near the boundaries</li>
     *   <li>Check if the data range is much smaller than expected for a normal</li>
     * </ul>
     */
    private boolean detectsTruncation(DimensionStatistics stats, float[] values) {
        if (values == null || values.length < 100) {
            return false;  // Not enough data to reliably detect truncation
        }

        double min = stats.min();
        double max = stats.max();
        double mean = stats.mean();
        double stdDev = stats.stdDev();

        // Check if the observed range is much smaller than expected
        // For an unbounded normal, 99.7% of data is within 3 sigma
        double expectedRange = 6 * stdDev;
        double observedRange = max - min;

        if (observedRange < 0.5 * expectedRange) {
            // Range is suspiciously small, likely truncated
            return true;
        }

        // Check density near boundaries
        double lowerBoundary = min + (max - min) * truncationThreshold;
        double upperBoundary = max - (max - min) * truncationThreshold;

        int nearLower = 0;
        int nearUpper = 0;

        for (float v : values) {
            if (v <= lowerBoundary) nearLower++;
            if (v >= upperBoundary) nearUpper++;
        }

        double lowerFraction = (double) nearLower / values.length;
        double upperFraction = (double) nearUpper / values.length;

        // If significant mass is at both boundaries, likely truncated
        return lowerFraction > 0.02 && upperFraction > 0.02;
    }

    /**
     * Computes the Anderson-Darling test statistic.
     *
     * <p>Lower values indicate better fit to a normal distribution.
     * Critical values:
     * <ul>
     *   <li>&lt; 0.576: very good fit (p &gt; 0.15)</li>
     *   <li>&lt; 0.787: acceptable fit (p &gt; 0.05)</li>
     *   <li>&gt; 1.072: poor fit (p &lt; 0.01)</li>
     * </ul>
     */
    private double computeAndersonDarling(float[] values, double mean, double stdDev) {
        int n = values.length;

        // Sort a copy of the values
        float[] sorted = values.clone();
        java.util.Arrays.sort(sorted);

        // Compute test statistic
        double sum = 0;
        for (int i = 0; i < n; i++) {
            double z = (sorted[i] - mean) / stdDev;
            double cdfZ = NormalCDF.standardNormalCDF(z);

            // Clamp to avoid log(0) or log(1)
            cdfZ = Math.max(1e-10, Math.min(1 - 1e-10, cdfZ));

            double weight = 2.0 * (i + 1) - 1;
            sum += weight * (Math.log(cdfZ) + Math.log(1 - NormalCDF.standardNormalCDF(
                (sorted[n - 1 - i] - mean) / stdDev)));
        }

        double aSq = -n - sum / n;

        // Apply small sample correction
        aSq = aSq * (1 + 4.0 / n - 25.0 / (n * n));

        return aSq;
    }
}
