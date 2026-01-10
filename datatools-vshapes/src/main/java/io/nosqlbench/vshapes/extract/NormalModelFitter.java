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

import io.nosqlbench.vshapes.model.NormalScalarModel;
import io.nosqlbench.vshapes.model.ScalarModel;

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
 * <p>Inherits uniform scoring from {@link AbstractParametricFitter} using
 * the raw Kolmogorov-Smirnov D-statistic via the model's CDF.
 *
 * @see NormalScalarModel
 * @see AbstractParametricFitter
 */
public final class NormalModelFitter extends AbstractParametricFitter {

    private final boolean detectTruncation;
    private final double truncationThreshold;
    private final Double explicitLowerBound;
    private final Double explicitUpperBound;

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
        this.explicitLowerBound = null;
        this.explicitUpperBound = null;
    }

    /**
     * Creates a Normal model fitter with explicit truncation bounds.
     *
     * <p>This is useful for normalized vectors where the bounds are known
     * to be [-1, 1] regardless of the observed data range.
     *
     * @param lowerBound explicit lower bound for truncation
     * @param upperBound explicit upper bound for truncation
     */
    public NormalModelFitter(double lowerBound, double upperBound) {
        this.detectTruncation = true;
        this.truncationThreshold = 0.01;
        this.explicitLowerBound = lowerBound;
        this.explicitUpperBound = upperBound;
    }

    /**
     * Creates a Normal model fitter configured for L2-normalized vectors.
     *
     * <p>Normalized vectors have values bounded in [-1, 1], so this
     * creates a fitter with explicit bounds.
     *
     * @return a fitter configured for normalized vector data
     */
    public static NormalModelFitter forNormalizedVectors() {
        return new NormalModelFitter(-1.0, 1.0);
    }

    @Override
    protected ScalarModel estimateParameters(DimensionStatistics stats, float[] values) {
        double mean = stats.mean();
        double stdDev = stats.stdDev();

        // Ensure stdDev is positive
        if (stdDev <= 0) {
            stdDev = 1e-10;
        }

        // Use explicit bounds if provided (e.g., for normalized vectors)
        if (explicitLowerBound != null && explicitUpperBound != null) {
            return new NormalScalarModel(mean, stdDev, explicitLowerBound, explicitUpperBound);
        }

        if (detectTruncation && detectsTruncation(stats, values)) {
            // Create truncated normal model
            double lower = stats.min();
            double upper = stats.max();
            return new NormalScalarModel(mean, stdDev, lower, upper);
        } else {
            // Create unbounded normal model
            return new NormalScalarModel(mean, stdDev);
        }
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

}
