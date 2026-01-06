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

package io.nosqlbench.datatools.virtdata.sampling;

import io.nosqlbench.vshapes.model.BetaScalarModel;

/**
 * Sampler for Beta distributions using inverse CDF transform.
 *
 * <p>Uses Newton-Raphson iteration to compute the inverse of the
 * regularized incomplete beta function.
 */
public final class BetaSampler implements ComponentSampler {

    private final double alpha;
    private final double beta;
    private final double lower;
    private final double range;

    // Pre-computed for efficiency
    private final double logBeta;

    /**
     * Creates a sampler bound to the given Beta model.
     *
     * @param model the Beta scalar model
     */
    public BetaSampler(BetaScalarModel model) {
        this.alpha = model.getAlpha();
        this.beta = model.getBeta();
        this.lower = model.getLower();
        this.range = model.getUpper() - model.getLower();
        this.logBeta = logBetaFunction(alpha, beta);
    }

    @Override
    public double sample(double u) {
        // Clamp u to valid range
        u = Math.max(1e-10, Math.min(1 - 1e-10, u));

        // Compute inverse CDF on [0, 1]
        double x = inverseBetaCDF(u);

        // Scale to [lower, upper]
        return lower + range * x;
    }

    /**
     * Computes the inverse Beta CDF using Newton-Raphson iteration.
     */
    private double inverseBetaCDF(double p) {
        // Initial guess using approximation
        double x = initialGuess(p);

        // Newton-Raphson iteration
        for (int i = 0; i < 50; i++) {
            double cdf = betaCDF(x);
            double pdf = betaPDF(x);

            if (pdf < 1e-100) break;

            double error = cdf - p;
            if (Math.abs(error) < 1e-12) break;

            double delta = error / pdf;
            x = x - delta;

            // Keep x in valid range
            x = Math.max(1e-10, Math.min(1 - 1e-10, x));
        }

        return x;
    }

    /**
     * Initial guess for Newton-Raphson using normal approximation.
     */
    private double initialGuess(double p) {
        if (alpha >= 1 && beta >= 1) {
            // Use normal approximation for well-behaved cases
            double mean = alpha / (alpha + beta);
            double var = (alpha * beta) / ((alpha + beta) * (alpha + beta) * (alpha + beta + 1));
            double z = InverseNormalCDF.standardNormalQuantile(p);
            double guess = mean + z * Math.sqrt(var);
            return Math.max(0.01, Math.min(0.99, guess));
        } else {
            // For extreme shapes, use p directly as initial guess
            return Math.max(0.01, Math.min(0.99, p));
        }
    }

    /**
     * Beta PDF: f(x) = x^(α-1) * (1-x)^(β-1) / B(α,β)
     */
    private double betaPDF(double x) {
        if (x <= 0 || x >= 1) return 0;
        return Math.exp((alpha - 1) * Math.log(x) + (beta - 1) * Math.log(1 - x) - logBeta);
    }

    /**
     * Beta CDF using regularized incomplete beta function.
     */
    private double betaCDF(double x) {
        if (x <= 0) return 0;
        if (x >= 1) return 1;
        return regularizedIncompleteBeta(x, alpha, beta);
    }

    /**
     * Regularized incomplete beta function I_x(a, b).
     */
    private double regularizedIncompleteBeta(double x, double a, double b) {
        double bt = Math.exp(a * Math.log(x) + b * Math.log(1 - x) - logBeta);

        if (x < (a + 1) / (a + b + 2)) {
            return bt * betaContinuedFraction(x, a, b) / a;
        } else {
            return 1.0 - bt * betaContinuedFraction(1 - x, b, a) / b;
        }
    }

    /**
     * Continued fraction for incomplete beta function.
     */
    private double betaContinuedFraction(double x, double a, double b) {
        double qab = a + b;
        double qap = a + 1;
        double qam = a - 1;
        double c = 1;
        double d = 1 - qab * x / qap;
        if (Math.abs(d) < 1e-30) d = 1e-30;
        d = 1 / d;
        double h = d;

        for (int m = 1; m <= 200; m++) {
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
            if (Math.abs(del - 1) < 1e-14) break;
        }
        return h;
    }

    /**
     * Log of beta function: log B(a,b) = log Γ(a) + log Γ(b) - log Γ(a+b)
     */
    private static double logBetaFunction(double a, double b) {
        return logGamma(a) + logGamma(b) - logGamma(a + b);
    }

    /**
     * Log gamma function using Lanczos approximation.
     */
    private static double logGamma(double x) {
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
