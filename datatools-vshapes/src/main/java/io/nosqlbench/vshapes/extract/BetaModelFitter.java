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

import io.nosqlbench.vshapes.model.BetaScalarModel;
import io.nosqlbench.vshapes.model.ScalarModel;

/**
 * Fits a Beta distribution to observed data - Pearson Type I/II.
 *
 * <h2>Algorithm</h2>
 *
 * <p>Uses the method of moments estimation:
 * <ol>
 *   <li>Estimate bounds from data range (with optional extension)</li>
 *   <li>Standardize data to [0, 1]</li>
 *   <li>Compute sample mean and variance on standardized data</li>
 *   <li>Estimate α and β from: α = μ(μ(1-μ)/σ² - 1), β = (1-μ)(μ(1-μ)/σ² - 1)</li>
 * </ol>
 *
 * <h2>Goodness of Fit</h2>
 *
 * <p>Inherits uniform scoring from {@link AbstractParametricFitter} using
 * the raw Kolmogorov-Smirnov D-statistic via the model's CDF.
 *
 * @see BetaScalarModel
 * @see AbstractParametricFitter
 */
public final class BetaModelFitter extends AbstractParametricFitter {

    private final double boundaryExtension;
    private final Double explicitLowerBound;
    private final Double explicitUpperBound;

    /**
     * Creates a Beta model fitter with default settings.
     */
    public BetaModelFitter() {
        this(0.0);
    }

    /**
     * Creates a Beta model fitter with boundary extension.
     *
     * @param boundaryExtension fraction of range to extend beyond observed min/max
     */
    public BetaModelFitter(double boundaryExtension) {
        if (boundaryExtension < 0 || boundaryExtension > 0.5) {
            throw new IllegalArgumentException(
                "Boundary extension must be in [0, 0.5], got: " + boundaryExtension);
        }
        this.boundaryExtension = boundaryExtension;
        this.explicitLowerBound = null;
        this.explicitUpperBound = null;
    }

    /**
     * Creates a Beta model fitter with explicit bounds.
     *
     * <p>This is useful for normalized vectors where the bounds are known
     * to be [-1, 1] regardless of the observed data range.
     *
     * @param lowerBound explicit lower bound
     * @param upperBound explicit upper bound
     */
    public BetaModelFitter(double lowerBound, double upperBound) {
        this.boundaryExtension = 0.0;
        this.explicitLowerBound = lowerBound;
        this.explicitUpperBound = upperBound;
    }

    /**
     * Creates a Beta model fitter configured for L2-normalized vectors.
     *
     * <p>Normalized vectors have values bounded in [-1, 1], so this
     * creates a fitter with explicit bounds.
     *
     * @return a fitter configured for normalized vector data
     */
    public static BetaModelFitter forNormalizedVectors() {
        return new BetaModelFitter(-1.0, 1.0);
    }

    @Override
    protected ScalarModel estimateParameters(DimensionStatistics stats, float[] values) {
        // Use explicit bounds if provided, otherwise estimate from data
        double lower;
        double upper;
        if (explicitLowerBound != null && explicitUpperBound != null) {
            lower = explicitLowerBound;
            upper = explicitUpperBound;
        } else {
            double range = stats.max() - stats.min();
            double extension = range * boundaryExtension;
            lower = stats.min() - extension;
            upper = stats.max() + extension;
        }
        double adjustedRange = upper - lower;

        // Standardize data to [0, 1]
        double sumStd = 0;
        double sumStdSq = 0;
        int n = values != null ? values.length : (int) stats.count();

        if (values != null) {
            for (float v : values) {
                double std = (v - lower) / adjustedRange;
                std = Math.max(0.001, Math.min(0.999, std));  // Clamp to avoid edge issues
                sumStd += std;
                sumStdSq += std * std;
            }
        } else {
            // Use statistics directly
            double meanStd = (stats.mean() - lower) / adjustedRange;
            double varStd = stats.variance() / (adjustedRange * adjustedRange);
            return estimateFromMoments(meanStd, varStd, lower, upper);
        }

        double meanStd = sumStd / n;
        double varStd = (sumStdSq / n) - (meanStd * meanStd);

        // Ensure variance is positive
        varStd = Math.max(varStd, 1e-10);

        // Method of moments estimation
        double common = (meanStd * (1 - meanStd) / varStd) - 1;

        // Ensure common factor is positive for valid parameters
        if (common <= 0) {
            common = 1.0;  // Fallback to uniform-like distribution
        }

        double alpha = meanStd * common;
        double beta = (1 - meanStd) * common;

        // Ensure parameters are valid
        alpha = Math.max(alpha, 0.1);
        beta = Math.max(beta, 0.1);

        return new BetaScalarModel(alpha, beta, lower, upper);
    }

    private ScalarModel estimateFromMoments(double meanStd, double varStd,
                                             double lower, double upper) {
        varStd = Math.max(varStd, 1e-10);
        double common = (meanStd * (1 - meanStd) / varStd) - 1;
        if (common <= 0) common = 1.0;

        double alpha = Math.max(meanStd * common, 0.1);
        double beta = Math.max((1 - meanStd) * common, 0.1);

        return new BetaScalarModel(alpha, beta, lower, upper);
    }

    @Override
    public String getModelType() {
        return BetaScalarModel.MODEL_TYPE;
    }

    @Override
    public boolean supportsBoundedData() {
        return true;
    }

}
