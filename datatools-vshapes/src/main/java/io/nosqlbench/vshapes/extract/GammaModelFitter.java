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

import io.nosqlbench.vshapes.model.GammaScalarModel;

import java.util.Objects;

/**
 * Fits a Gamma distribution to observed data - Pearson Type III.
 *
 * <h2>Algorithm</h2>
 *
 * <p>Uses the method of moments estimation:
 * <ol>
 *   <li>Detect location shift if data has a minimum above 0</li>
 *   <li>Compute sample mean and variance</li>
 *   <li>Estimate shape k = (mean/stdDev)²</li>
 *   <li>Estimate scale θ = variance/mean</li>
 * </ol>
 *
 * <h2>Applicability</h2>
 *
 * <p>Gamma distribution requires:
 * <ul>
 *   <li>Positive skewness (right-skewed data)</li>
 *   <li>Semi-bounded support [location, +∞)</li>
 *   <li>Positive values (after location adjustment)</li>
 * </ul>
 *
 * <h2>Goodness of Fit</h2>
 *
 * <p>Uses a chi-squared-like statistic based on binned data comparison.
 *
 * @see GammaScalarModel
 * @see ComponentModelFitter
 */
public final class GammaModelFitter implements ComponentModelFitter {

    private final boolean detectLocation;

    /**
     * Creates a Gamma model fitter with default settings.
     */
    public GammaModelFitter() {
        this(true);
    }

    /**
     * Creates a Gamma model fitter.
     *
     * @param detectLocation whether to detect location shift from minimum
     */
    public GammaModelFitter(boolean detectLocation) {
        this.detectLocation = detectLocation;
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

        // Detect location shift
        double location = 0.0;
        if (detectLocation && stats.min() > 0) {
            // Use a fraction of the minimum as the location parameter
            location = stats.min() * 0.9;
        } else if (detectLocation && stats.min() < 0) {
            // Shift so all values are positive
            location = stats.min() - 0.1 * Math.abs(stats.min());
        }

        // Compute adjusted statistics
        double adjustedMean = stats.mean() - location;
        double variance = stats.variance();

        // Ensure positive mean for gamma
        if (adjustedMean <= 0) {
            adjustedMean = Math.abs(stats.mean()) + 0.01;
        }

        // Ensure positive variance
        if (variance <= 0) {
            variance = adjustedMean * adjustedMean;
        }

        // Method of moments estimation
        // shape (k) = mean² / variance
        // scale (θ) = variance / mean
        double shape = (adjustedMean * adjustedMean) / variance;
        double scale = variance / adjustedMean;

        // Ensure valid parameters
        shape = Math.max(shape, 0.1);
        scale = Math.max(scale, 1e-10);

        GammaScalarModel model = new GammaScalarModel(shape, scale, location);

        // Compute goodness-of-fit
        double goodnessOfFit = computeGoodnessOfFit(values, shape, scale, location);

        return new FitResult(model, goodnessOfFit, getModelType());
    }

    @Override
    public String getModelType() {
        return GammaScalarModel.MODEL_TYPE;
    }

    @Override
    public boolean supportsBoundedData() {
        return true;  // Gamma is semi-bounded
    }

    /**
     * Computes goodness-of-fit using a chi-squared-like binned comparison.
     */
    private double computeGoodnessOfFit(float[] values, double shape, double scale, double location) {
        if (values == null || values.length < 10) {
            return 0.5;  // Default for insufficient data
        }

        int n = values.length;

        // Sort values
        float[] sorted = values.clone();
        java.util.Arrays.sort(sorted);

        // Compute Kolmogorov-Smirnov-like statistic
        double maxD = 0;
        for (int i = 0; i < n; i++) {
            double x = sorted[i] - location;
            if (x <= 0) x = 0.001;  // Handle edge case

            double empiricalCdf = (double) (i + 1) / n;
            double theoreticalCdf = approximateGammaCDF(x, shape, scale);

            double d = Math.abs(empiricalCdf - theoreticalCdf);
            maxD = Math.max(maxD, d);
        }

        return maxD * Math.sqrt(n);
    }

    /**
     * Approximates the gamma CDF using numerical integration.
     */
    private double approximateGammaCDF(double x, double shape, double scale) {
        // Use regularized incomplete gamma function approximation
        // P(a, x) = γ(a, x) / Γ(a)
        double z = x / scale;
        return regularizedGammaP(shape, z);
    }

    /**
     * Approximates the regularized incomplete gamma function P(a, x).
     * Uses a series expansion for small x and continued fraction for large x.
     */
    private double regularizedGammaP(double a, double x) {
        if (x < 0) return 0;
        if (x == 0) return 0;

        if (x < a + 1) {
            // Use series expansion
            return gammaSeriesP(a, x);
        } else {
            // Use continued fraction
            return 1.0 - gammaContinuedFractionQ(a, x);
        }
    }

    private double gammaSeriesP(double a, double x) {
        double gln = logGamma(a);
        double ap = a;
        double sum = 1.0 / a;
        double del = sum;

        for (int i = 1; i <= 100; i++) {
            ap += 1.0;
            del *= x / ap;
            sum += del;
            if (Math.abs(del) < Math.abs(sum) * 1e-10) {
                return sum * Math.exp(-x + a * Math.log(x) - gln);
            }
        }
        return sum * Math.exp(-x + a * Math.log(x) - gln);
    }

    private double gammaContinuedFractionQ(double a, double x) {
        double gln = logGamma(a);
        double b = x + 1.0 - a;
        double c = 1.0 / 1e-30;
        double d = 1.0 / b;
        double h = d;

        for (int i = 1; i <= 100; i++) {
            double an = -i * (i - a);
            b += 2.0;
            d = an * d + b;
            if (Math.abs(d) < 1e-30) d = 1e-30;
            c = b + an / c;
            if (Math.abs(c) < 1e-30) c = 1e-30;
            d = 1.0 / d;
            double del = d * c;
            h *= del;
            if (Math.abs(del - 1.0) < 1e-10) {
                break;
            }
        }
        return Math.exp(-x + a * Math.log(x) - gln) * h;
    }

    private double logGamma(double x) {
        double[] cof = {76.18009172947146, -86.50532032941677, 24.01409824083091,
                        -1.231739572450155, 0.1208650973866179e-2, -0.5395239384953e-5};
        double y = x;
        double tmp = x + 5.5;
        tmp -= (x + 0.5) * Math.log(tmp);
        double ser = 1.000000000190015;
        for (int j = 0; j < 6; j++) {
            ser += cof[j] / ++y;
        }
        return -tmp + Math.log(2.5066282746310005 * ser / x);
    }
}
