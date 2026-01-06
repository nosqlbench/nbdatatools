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

import io.nosqlbench.vshapes.model.BetaPrimeScalarModel;

/**
 * Sampler for Beta Prime distributions using inverse CDF transform.
 *
 * <p>Uses the relationship: if X ~ Beta(α, β), then X/(1-X) ~ BetaPrime(α, β).
 * The quantile function is computed by transforming the Beta quantile.
 */
public final class BetaPrimeSampler implements ComponentSampler {

    private final double alpha;  // α
    private final double beta;   // β
    private final double scale;  // σ

    // Pre-computed for efficiency
    private final double logBeta;

    /**
     * Creates a sampler bound to the given Beta Prime model.
     *
     * @param model the Beta Prime scalar model
     */
    public BetaPrimeSampler(BetaPrimeScalarModel model) {
        this.alpha = model.getAlpha();
        this.beta = model.getBeta();
        this.scale = model.getScale();
        this.logBeta = logBetaFunction(alpha, beta);
    }

    @Override
    public double sample(double u) {
        // Clamp u to valid range
        u = Math.max(1e-10, Math.min(1 - 1e-10, u));

        // If X ~ Beta(α, β), then Y = X/(1-X) ~ BetaPrime(α, β)
        // So F_BP^{-1}(u) = x/(1-x) where x = F_Beta^{-1}(u)
        double x = inverseBetaCDF(u);

        // Transform and scale
        return scale * x / (1 - x);
    }

    /**
     * Inverse Beta CDF using Newton-Raphson iteration with bisection fallback.
     */
    private double inverseBetaCDF(double p) {
        if (p <= 0) return 1e-10;
        if (p >= 1) return 1 - 1e-10;

        // Use bisection for extreme values where Newton-Raphson may diverge
        double lo = 1e-10;
        double hi = 1 - 1e-10;

        // Initial guess
        double x = initialGuess(p);
        x = Math.max(lo, Math.min(hi, x));

        // Newton-Raphson iteration with bisection fallback
        for (int i = 0; i < 100; i++) {
            double cdf = betaCDF(x);
            double pdf = betaPDF(x);

            double error = cdf - p;
            if (Math.abs(error) < 1e-12) break;

            // Update bisection bounds
            if (cdf < p) {
                lo = Math.max(lo, x);
            } else {
                hi = Math.min(hi, x);
            }

            // Try Newton-Raphson step
            double newX;
            if (pdf > 1e-100) {
                double delta = error / pdf;
                // Damped Newton step
                newX = x - 0.5 * delta;
            } else {
                // Fallback to bisection
                newX = (lo + hi) / 2;
            }

            // Ensure new x is within bounds
            newX = Math.max(lo + 1e-15, Math.min(hi - 1e-15, newX));

            // If Newton step would go outside bounds, use bisection
            if (newX <= lo || newX >= hi) {
                newX = (lo + hi) / 2;
            }

            // Check for convergence
            if (Math.abs(newX - x) < 1e-15) break;
            x = newX;
        }

        return x;
    }

    private double initialGuess(double p) {
        if (alpha >= 1 && beta >= 1) {
            double mean = alpha / (alpha + beta);
            double var = (alpha * beta) / ((alpha + beta) * (alpha + beta) * (alpha + beta + 1));
            double z = InverseNormalCDF.standardNormalQuantile(p);
            double guess = mean + z * Math.sqrt(var);
            return Math.max(0.01, Math.min(0.99, guess));
        } else {
            return Math.max(0.01, Math.min(0.99, p));
        }
    }

    private double betaPDF(double x) {
        if (x <= 0 || x >= 1) return 0;
        return Math.exp((alpha - 1) * Math.log(x) + (beta - 1) * Math.log(1 - x) - logBeta);
    }

    private double betaCDF(double x) {
        if (x <= 0) return 0;
        if (x >= 1) return 1;
        return regularizedIncompleteBeta(x, alpha, beta);
    }

    private double regularizedIncompleteBeta(double x, double a, double b) {
        double bt = Math.exp(a * Math.log(x) + b * Math.log(1 - x) - logBeta);

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

    private static double logBetaFunction(double a, double b) {
        return logGamma(a) + logGamma(b) - logGamma(a + b);
    }

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
