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

import io.nosqlbench.vshapes.model.PearsonIVScalarModel;

/**
 * Sampler for Pearson Type IV distributions using inverse CDF transform.
 *
 * <p>The Pearson IV distribution PDF is:
 * f(x) = k * [1 + ((x - λ)/a)²]^(-m) * exp(-ν * arctan((x - λ)/a))
 *
 * <p>The inverse CDF is computed numerically using Newton-Raphson iteration.
 */
public final class PearsonIVSampler implements ComponentSampler {

    private final double m;       // Shape parameter
    private final double nu;      // Skewness parameter
    private final double a;       // Scale parameter
    private final double lambda;  // Location parameter

    // Pre-computed constants
    private final double normConstant;  // log normalization constant

    /**
     * Creates a sampler bound to the given Pearson IV model.
     *
     * @param model the Pearson IV scalar model
     */
    public PearsonIVSampler(PearsonIVScalarModel model) {
        this.m = model.getM();
        this.nu = model.getNu();
        this.a = model.getA();
        this.lambda = model.getLambda();
        this.normConstant = computeLogNormConstant();
    }

    @Override
    public double sample(double u) {
        // Clamp u to valid range
        u = Math.max(1e-10, Math.min(1 - 1e-10, u));

        // Compute inverse CDF using Newton-Raphson
        double x = inverseCDF(u);

        return x;
    }

    /**
     * Computes the inverse CDF using Newton-Raphson iteration.
     */
    private double inverseCDF(double p) {
        // Initial guess based on approximate mode/mean
        double x = initialGuess(p);

        // Newton-Raphson iteration
        for (int i = 0; i < 100; i++) {
            double cdf = computeCDF(x);
            double pdf = computePDF(x);

            if (pdf < 1e-100) {
                // If PDF is too small, use bisection step
                if (cdf < p) {
                    x += a;
                } else {
                    x -= a;
                }
                continue;
            }

            double error = cdf - p;
            if (Math.abs(error) < 1e-10) break;

            double delta = error / pdf;
            // Damped Newton step for stability
            double newX = x - Math.max(-2 * a, Math.min(2 * a, delta));

            if (Math.abs(newX - x) < 1e-12) break;
            x = newX;
        }

        return x;
    }

    /**
     * Initial guess for Newton-Raphson.
     */
    private double initialGuess(double p) {
        // Mean of Pearson IV: λ + aν/(2m-2) if m > 1
        double mean = lambda;
        if (m > 1) {
            mean = lambda + a * nu / (2 * m - 2);
        }

        // Approximate standard deviation
        double sigma = a / Math.sqrt(2 * m - 1);

        // Use normal approximation for initial guess
        double z = InverseNormalCDF.standardNormalQuantile(p);
        return mean + sigma * z;
    }

    /**
     * Computes the PDF at point x.
     */
    private double computePDF(double x) {
        double y = (x - lambda) / a;
        double logPdf = normConstant - m * Math.log(1 + y * y) - nu * Math.atan(y);
        return Math.exp(logPdf) / a;
    }

    /**
     * Computes the CDF at point x using numerical integration.
     *
     * <p>Uses adaptive Simpson's rule for accuracy.
     */
    private double computeCDF(double x) {
        // Integration limits - use many standard deviations
        double sigma = a / Math.sqrt(2 * m - 1);
        double lower = lambda - 20 * sigma;

        if (x <= lower) return 0;

        // Numerical integration using Simpson's rule
        return adaptiveSimpson(lower, x, 1e-10, 20);
    }

    /**
     * Adaptive Simpson's quadrature.
     */
    private double adaptiveSimpson(double a, double b, double tol, int maxDepth) {
        double c = (a + b) / 2;
        double h = b - a;
        double fa = computePDF(a);
        double fb = computePDF(b);
        double fc = computePDF(c);
        double S = (h / 6) * (fa + 4 * fc + fb);
        return adaptiveSimpsonAux(a, b, tol, S, fa, fb, fc, maxDepth);
    }

    private double adaptiveSimpsonAux(double a, double b, double tol, double S,
                                       double fa, double fb, double fc, int depth) {
        double c = (a + b) / 2;
        double h = b - a;
        double d = (a + c) / 2;
        double e = (c + b) / 2;
        double fd = computePDF(d);
        double fe = computePDF(e);
        double Sleft = (h / 12) * (fa + 4 * fd + fc);
        double Sright = (h / 12) * (fc + 4 * fe + fb);
        double S2 = Sleft + Sright;

        if (depth <= 0 || Math.abs(S2 - S) < 15 * tol) {
            return S2 + (S2 - S) / 15;
        }

        return adaptiveSimpsonAux(a, c, tol / 2, Sleft, fa, fc, fd, depth - 1) +
               adaptiveSimpsonAux(c, b, tol / 2, Sright, fc, fb, fe, depth - 1);
    }

    /**
     * Computes the log normalization constant.
     */
    private double computeLogNormConstant() {
        // The normalization constant for Pearson IV is complex, involving
        // the beta function and hypergeometric functions.
        // For numerical sampling, we compute it to ensure the PDF integrates to 1.

        // log k = log|Γ(m + iν/2)|² - log(a) - log(Γ(m)) - log(Γ(m - 1/2)) + log(Γ(1/2))

        // Simplified approximation using real-valued gamma functions:
        // k ≈ Γ(m) / (a * sqrt(π) * Γ(m - 1/2)) * factor for ν

        double logGammaM = logGamma(m);
        double logGammaMHalf = logGamma(m - 0.5);
        double logSqrtPi = 0.5 * Math.log(Math.PI);

        // Basic normalization (for ν = 0 this would be exact for Pearson VII)
        double logK = logGammaM - logSqrtPi - logGammaMHalf;

        // Correction for non-zero ν (approximate)
        if (Math.abs(nu) > 1e-10) {
            // The correction involves the complex gamma function
            // |Γ(m + iν/2)|² = π / (cosh(πν/2) * |Γ(1-m-iν/2)|²) approximately
            // For moderate ν and m, use a simple correction factor
            double nuHalf = nu / 2;
            logK -= 0.5 * Math.log(1 + nuHalf * nuHalf / (m * m));
        }

        return logK;
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
