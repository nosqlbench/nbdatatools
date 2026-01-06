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

import io.nosqlbench.vshapes.model.BetaPrimeScalarModel;

import java.util.Objects;

/**
 * Fits a Beta Prime distribution to observed data - Pearson Type VI.
 *
 * <h2>Algorithm</h2>
 *
 * <p>Uses the method of moments estimation:
 * <ol>
 *   <li>Compute sample mean and variance</li>
 *   <li>Estimate β from mean and variance relationship</li>
 *   <li>Estimate α from mean: α = mean(β - 1)</li>
 * </ol>
 *
 * <h2>Applicability</h2>
 *
 * <p>Beta Prime distribution requires:
 * <ul>
 *   <li>Positive values only</li>
 *   <li>Right-skewed data</li>
 *   <li>β &gt; 2 for finite variance</li>
 * </ul>
 *
 * @see BetaPrimeScalarModel
 * @see ComponentModelFitter
 */
public final class BetaPrimeModelFitter implements ComponentModelFitter {

    /**
     * Creates a Beta Prime model fitter.
     */
    public BetaPrimeModelFitter() {
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

        // Beta prime requires positive values
        if (mean <= 0) {
            BetaPrimeScalarModel model = new BetaPrimeScalarModel(2.0, 4.0);
            return new FitResult(model, INVALID_DOMAIN_PENALTY, getModelType());
        }

        // Ensure positive variance
        if (variance <= 0) {
            variance = mean * mean * 0.1;
        }

        // Method of moments for beta prime:
        // Mean = α / (β - 1) for β > 1
        // Variance = α(α + β - 1) / [(β - 1)²(β - 2)] for β > 2
        //
        // Let r = variance / mean² = (α + β - 1) / [(β - 1)(β - 2)]
        // This is a quadratic in β that we solve numerically

        double r = variance / (mean * mean);

        // Initial guess: β ≈ 2 + 1/r (rough approximation)
        double beta = Math.max(2.1, 2 + 1.0 / (r + 0.1));

        // Refine β using Newton-Raphson (a few iterations)
        for (int i = 0; i < 10; i++) {
            double bm1 = beta - 1;
            double bm2 = beta - 2;
            if (bm2 <= 0) {
                beta = 2.1;
                break;
            }

            // α from mean: α = mean * (β - 1)
            double alpha = mean * bm1;

            // Expected variance ratio
            double expectedR = (alpha + beta - 1) / (bm1 * bm2);

            // Derivative of expectedR with respect to beta
            double deriv = -(alpha + beta - 1) * (2 * beta - 3) / (bm1 * bm1 * bm2 * bm2)
                           + 1.0 / (bm1 * bm2);

            if (Math.abs(deriv) < 1e-10) break;

            double newBeta = beta - (expectedR - r) / deriv;
            if (newBeta <= 2) newBeta = 2.1;
            if (Math.abs(newBeta - beta) < 1e-6) break;
            beta = newBeta;
        }

        // Ensure valid β
        beta = Math.max(beta, 2.1);

        // Compute α from mean
        double alpha = mean * (beta - 1);
        alpha = Math.max(alpha, 0.1);

        BetaPrimeScalarModel model = new BetaPrimeScalarModel(alpha, beta);

        // Compute goodness-of-fit
        double goodnessOfFit = computeGoodnessOfFit(values, alpha, beta, stats);

        return new FitResult(model, goodnessOfFit, getModelType());
    }

    @Override
    public String getModelType() {
        return BetaPrimeScalarModel.MODEL_TYPE;
    }

    @Override
    public boolean supportsBoundedData() {
        return true;  // Semi-bounded (0, +∞)
    }

    /**
     * Computes goodness-of-fit using KS-like statistic.
     */
    private double computeGoodnessOfFit(float[] values, double alpha, double beta,
                                         DimensionStatistics stats) {
        if (values == null || values.length < 10) {
            return 0.5;
        }

        // Penalize if data has negative values
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
            double theoreticalCdf = approximateBetaPrimeCDF(x, alpha, beta);

            double d = Math.abs(empiricalCdf - theoreticalCdf);
            maxD = Math.max(maxD, d);
        }

        return maxD * Math.sqrt(n);
    }

    /**
     * Approximates the beta prime CDF.
     * Uses the relationship: BetaPrime(α, β) at x has CDF = Beta(α, β) at x/(1+x)
     */
    private double approximateBetaPrimeCDF(double x, double alpha, double beta) {
        if (x <= 0) return 0;
        double t = x / (1 + x);  // Transform to [0, 1]
        return regularizedIncompleteBeta(t, alpha, beta);
    }

    private double regularizedIncompleteBeta(double x, double a, double b) {
        if (x <= 0) return 0;
        if (x >= 1) return 1;

        double bt = Math.exp(logGamma(a + b) - logGamma(a) - logGamma(b) +
                             a * Math.log(x) + b * Math.log(1 - x));

        if (x < (a + 1) / (a + b + 2)) {
            return bt * betaContinuedFraction(x, a, b) / a;
        } else {
            return 1.0 - bt * betaContinuedFraction(1 - x, b, a) / b;
        }
    }

    private double betaContinuedFraction(double x, double a, double b) {
        double qab = a + b;
        double qap = a + 1;
        double qam = a - 1;
        double c = 1;
        double d = 1 - qab * x / qap;
        if (Math.abs(d) < 1e-30) d = 1e-30;
        d = 1 / d;
        double h = d;

        for (int m = 1; m <= 100; m++) {
            int m2 = 2 * m;
            double aa = m * (b - m) * x / ((qam + m2) * (a + m2));
            d = 1 + aa * d;
            if (Math.abs(d) < 1e-30) d = 1e-30;
            c = 1 + aa / c;
            if (Math.abs(c) < 1e-30) c = 1e-30;
            d = 1 / d;
            h *= d * c;

            aa = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2));
            d = 1 + aa * d;
            if (Math.abs(d) < 1e-30) d = 1e-30;
            c = 1 + aa / c;
            if (Math.abs(c) < 1e-30) c = 1e-30;
            d = 1 / d;
            double del = d * c;
            h *= del;
            if (Math.abs(del - 1) < 1e-10) break;
        }
        return h;
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
