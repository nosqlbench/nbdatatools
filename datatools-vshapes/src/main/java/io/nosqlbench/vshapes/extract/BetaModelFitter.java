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
            // Two-pass approach: compute mean first without clamping to preserve variance
            for (float v : values) {
                double std = (v - lower) / adjustedRange;
                sumStd += std;
                sumStdSq += std * std;
            }
            // Only apply clamping to the final mean, not to individual values
            // This preserves the variance contribution from boundary values
        } else {
            // Use statistics directly
            double meanStd = (stats.mean() - lower) / adjustedRange;
            double varStd = stats.variance() / (adjustedRange * adjustedRange);
            return estimateFromMoments(meanStd, varStd, lower, upper);
        }

        double meanStdRaw = sumStd / n;
        double varStd = (sumStdSq / n) - (meanStdRaw * meanStdRaw);

        // Clamp mean for parameter estimation (but variance was computed from raw values)
        double meanStd = Math.max(1e-6, Math.min(1.0 - 1e-6, meanStdRaw));

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

        // If both α and β are near 1.0, this is effectively a uniform distribution
        // Snap to exactly 1.0 to make the uniform equivalence explicit
        if (Math.abs(alpha - 1.0) < 0.15 && Math.abs(beta - 1.0) < 0.15) {
            alpha = 1.0;
            beta = 1.0;
        }

        return new BetaScalarModel(alpha, beta, lower, upper);
    }

    private ScalarModel estimateFromMoments(double meanStd, double varStd,
                                             double lower, double upper) {
        varStd = Math.max(varStd, 1e-10);
        double common = (meanStd * (1 - meanStd) / varStd) - 1;
        if (common <= 0) common = 1.0;

        double alpha = Math.max(meanStd * common, 0.1);
        double beta = Math.max((1 - meanStd) * common, 0.1);

        // If both α and β are near 1.0, this is effectively a uniform distribution
        if (Math.abs(alpha - 1.0) < 0.15 && Math.abs(beta - 1.0) < 0.15) {
            alpha = 1.0;
            beta = 1.0;
        }

        return new BetaScalarModel(alpha, beta, lower, upper);
    }

    @Override
    public FitResult fit(DimensionStatistics stats, float[] values) {
        ScalarModel model = estimateParameters(stats, values);
        double ksScore = computeKSStatistic(model, values);

        // Apply skewness-based adjustment to improve discrimination.
        // Beta distributions can be skewed (α ≠ β) while Normal is symmetric.
        // Also, Beta with α,β < 1 shows edge-seeking behavior.
        double skewness = stats.skewness();

        // Get the fitted parameters to inform scoring
        BetaScalarModel betaModel = (BetaScalarModel) model;
        double alpha = betaModel.getAlpha();
        double beta = betaModel.getBeta();

        double adjustment = 0;

        // Check for edge-seeking behavior (U-shaped distributions with α,β < 1)
        if (alpha < 1.0 && beta < 1.0) {
            // U-shaped distributions are distinctive - give a bonus
            adjustment -= 0.15 * ksScore;
        }

        // Check if fitted parameters match observed skewness
        // Beta skewness formula: 2(β-α)√(α+β+1) / ((α+β+2)√(αβ))
        double expectedSkew = 0;
        if (alpha > 0 && beta > 0) {
            double sumAB = alpha + beta;
            double sqrtProduct = Math.sqrt(alpha * beta);
            if (sqrtProduct > 0) {
                expectedSkew = 2 * (beta - alpha) * Math.sqrt(sumAB + 1) / ((sumAB + 2) * sqrtProduct);
            }
        }

        // If observed skewness matches expected Beta skewness, give a bonus
        double skewDiff = Math.abs(skewness - expectedSkew);
        if (skewDiff < 0.3) {
            adjustment -= 0.10 * ksScore * (1.0 - skewDiff / 0.3);
        } else if (skewDiff > 0.5 && Math.abs(skewness) < 0.2) {
            // Data is symmetric but Beta predicts skew - penalty
            adjustment += 0.10 * ksScore;
        }

        // CRITICAL: Penalize Beta when data looks Normal-like.
        // Normal distributions have kurtosis ≈ 3.0 and are unbounded.
        // Beta distributions are bounded and typically have lower kurtosis.
        // If data is symmetric (low skew) with kurtosis near 3.0, prefer Normal.
        double kurtosis = stats.kurtosis();
        if (Math.abs(skewness) < 0.3 && Math.abs(kurtosis - 3.0) < 0.5) {
            // Symmetric data with Normal-like kurtosis - strongly penalize Beta
            adjustment += 0.25 * ksScore;
        }

        double adjustedScore = Math.max(0, ksScore + adjustment);
        return new FitResult(model, adjustedScore, getModelType());
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
