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

import io.nosqlbench.vshapes.model.InverseGammaScalarModel;

import java.util.Objects;

/**
 * Fits an Inverse Gamma distribution to observed data - Pearson Type V.
 *
 * <h2>Algorithm</h2>
 *
 * <p>Uses the method of moments estimation:
 * <ol>
 *   <li>Compute sample mean and variance</li>
 *   <li>Estimate shape α from: α = 2 + mean²/variance</li>
 *   <li>Estimate scale β from: β = mean(α - 1)</li>
 * </ol>
 *
 * <h2>Applicability</h2>
 *
 * <p>Inverse Gamma distribution requires:
 * <ul>
 *   <li>Positive values only</li>
 *   <li>Right-skewed data</li>
 *   <li>α &gt; 2 for finite variance</li>
 * </ul>
 *
 * @see InverseGammaScalarModel
 * @see ComponentModelFitter
 */
public final class InverseGammaModelFitter implements ComponentModelFitter {

    /**
     * Creates an Inverse Gamma model fitter.
     */
    public InverseGammaModelFitter() {
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
        double variance = stats.variance();

        // Large but finite penalty for data outside valid domain
        final double INVALID_DOMAIN_PENALTY = 100.0;

        // Inverse gamma requires positive values
        if (mean <= 0) {
            // Return a poor fit score for non-positive data
            InverseGammaScalarModel model = new InverseGammaScalarModel(3.0, 2.0);
            return new FitResult(model, INVALID_DOMAIN_PENALTY, getModelType());
        }

        // Ensure positive variance
        if (variance <= 0) {
            variance = mean * mean * 0.1;
        }

        // Method of moments: α = 2 + mean²/variance, β = mean(α - 1)
        double shape = 2 + (mean * mean) / variance;

        // Ensure shape is valid (need α > 2 for finite variance)
        shape = Math.max(shape, 2.1);

        double scale = mean * (shape - 1);

        // Ensure positive scale
        scale = Math.max(scale, 1e-10);

        InverseGammaScalarModel model = new InverseGammaScalarModel(shape, scale);

        // Compute goodness-of-fit
        double goodnessOfFit = computeGoodnessOfFit(values, shape, scale, stats);

        return new FitResult(model, goodnessOfFit, getModelType());
    }

    @Override
    public String getModelType() {
        return InverseGammaScalarModel.MODEL_TYPE;
    }

    @Override
    public boolean supportsBoundedData() {
        return true;  // Semi-bounded (0, +∞)
    }

    /**
     * Computes goodness-of-fit using KS-like statistic.
     */
    private double computeGoodnessOfFit(float[] values, double shape, double scale,
                                         DimensionStatistics stats) {
        if (values == null || values.length < 10) {
            return 0.5;
        }

        // Penalize if data has negative values (inverse gamma is positive only)
        if (stats.min() <= 0) {
            return 100.0;  // Large but finite penalty
        }

        int n = values.length;
        float[] sorted = values.clone();
        java.util.Arrays.sort(sorted);

        double maxD = 0;
        for (int i = 0; i < n; i++) {
            double x = sorted[i];
            if (x <= 0) continue;

            double empiricalCdf = (double) (i + 1) / n;
            double theoreticalCdf = approximateInverseGammaCDF(x, shape, scale);

            double d = Math.abs(empiricalCdf - theoreticalCdf);
            maxD = Math.max(maxD, d);
        }

        return maxD * Math.sqrt(n);
    }

    /**
     * Approximates the inverse gamma CDF.
     * Uses the relationship: InverseGamma(α, β) CDF at x = 1 - Gamma(α, 1/β) CDF at 1/x
     */
    private double approximateInverseGammaCDF(double x, double shape, double scale) {
        if (x <= 0) return 0;
        // F(x) = Q(α, β/x) where Q is the upper incomplete gamma function
        double z = scale / x;
        return 1.0 - regularizedGammaP(shape, z);
    }

    private double regularizedGammaP(double a, double x) {
        if (x < 0) return 0;
        if (x == 0) return 0;

        if (x < a + 1) {
            return gammaSeriesP(a, x);
        } else {
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
