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

import java.util.Objects;

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
 * <p>Uses the Kolmogorov-Smirnov test statistic. Lower values indicate better fit.
 *
 * @see BetaScalarModel
 * @see ComponentModelFitter
 */
public final class BetaModelFitter implements ComponentModelFitter {

    private final double boundaryExtension;

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

        // Estimate bounds with optional extension
        double range = stats.max() - stats.min();
        double extension = range * boundaryExtension;
        double lower = stats.min() - extension;
        double upper = stats.max() + extension;
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
            return fitFromMoments(meanStd, varStd, lower, upper, stats);
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

        BetaScalarModel model = new BetaScalarModel(alpha, beta, lower, upper);

        // Compute goodness-of-fit using Kolmogorov-Smirnov-like statistic
        double goodnessOfFit = computeKolmogorovSmirnov(values, alpha, beta, lower, upper);

        return new FitResult(model, goodnessOfFit, getModelType());
    }

    private FitResult fitFromMoments(double meanStd, double varStd,
                                      double lower, double upper, DimensionStatistics stats) {
        varStd = Math.max(varStd, 1e-10);
        double common = (meanStd * (1 - meanStd) / varStd) - 1;
        if (common <= 0) common = 1.0;

        double alpha = Math.max(meanStd * common, 0.1);
        double beta = Math.max((1 - meanStd) * common, 0.1);

        BetaScalarModel model = new BetaScalarModel(alpha, beta, lower, upper);
        // Without raw values, return a default goodness-of-fit
        return new FitResult(model, 0.5, getModelType());
    }

    @Override
    public String getModelType() {
        return BetaScalarModel.MODEL_TYPE;
    }

    @Override
    public boolean supportsBoundedData() {
        return true;
    }

    /**
     * Computes a Kolmogorov-Smirnov-like statistic for beta distribution.
     */
    private double computeKolmogorovSmirnov(float[] values, double alpha, double beta,
                                             double lower, double upper) {
        int n = values.length;
        float[] sorted = values.clone();
        java.util.Arrays.sort(sorted);

        double range = upper - lower;
        double maxD = 0;

        for (int i = 0; i < n; i++) {
            double x = (sorted[i] - lower) / range;
            x = Math.max(0.001, Math.min(0.999, x));

            // Approximate beta CDF using regularized incomplete beta function
            // For simplicity, use a polynomial approximation for common cases
            double empiricalCdf = (double) (i + 1) / n;
            double theoreticalCdf = approximateBetaCDF(x, alpha, beta);

            double d = Math.abs(empiricalCdf - theoreticalCdf);
            maxD = Math.max(maxD, d);
        }

        // Scale by sqrt(n) for KS statistic
        return maxD * Math.sqrt(n);
    }

    /**
     * Approximates the beta CDF using a simple numerical integration.
     */
    private double approximateBetaCDF(double x, double alpha, double beta) {
        // Simple numerical integration using trapezoidal rule
        int steps = 100;
        double sum = 0;
        double dx = x / steps;

        for (int i = 1; i < steps; i++) {
            double t = i * dx;
            sum += Math.pow(t, alpha - 1) * Math.pow(1 - t, beta - 1);
        }
        sum += 0.5 * (Math.pow(dx, alpha - 1) * Math.pow(1 - dx, beta - 1));
        sum += 0.5 * (Math.pow(x, alpha - 1) * Math.pow(1 - x, beta - 1));
        sum *= dx;

        // Normalize by beta function B(alpha, beta)
        double betaFunc = gammaApprox(alpha) * gammaApprox(beta) / gammaApprox(alpha + beta);
        return sum / betaFunc;
    }

    /**
     * Simple gamma function approximation using Stirling's formula.
     */
    private double gammaApprox(double x) {
        if (x < 0.5) {
            return Math.PI / (Math.sin(Math.PI * x) * gammaApprox(1 - x));
        }
        x -= 1;
        double g = 0.99999999999980993;
        double[] c = {676.5203681218851, -1259.1392167224028, 771.32342877765313,
                      -176.61502916214059, 12.507343278686905, -0.13857109526572012,
                      9.9843695780195716e-6, 1.5056327351493116e-7};
        for (int i = 0; i < 8; i++) {
            g += c[i] / (x + i + 1);
        }
        double t = x + 7.5;
        return Math.sqrt(2 * Math.PI) * Math.pow(t, x + 0.5) * Math.exp(-t) * g;
    }
}
