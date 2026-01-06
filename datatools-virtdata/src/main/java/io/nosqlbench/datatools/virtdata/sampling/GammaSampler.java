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

import io.nosqlbench.vshapes.model.GammaScalarModel;

/**
 * Sampler for Gamma distributions using inverse CDF transform.
 *
 * <p>Uses Newton-Raphson iteration to compute the inverse of the
 * lower regularized incomplete gamma function P(k, x).
 */
public final class GammaSampler implements ComponentSampler {

    private final double shape;     // k
    private final double scale;     // θ
    private final double location;  // shift

    // Pre-computed for efficiency
    private final double logGammaShape;

    /**
     * Creates a sampler bound to the given Gamma model.
     *
     * @param model the Gamma scalar model
     */
    public GammaSampler(GammaScalarModel model) {
        this.shape = model.getShape();
        this.scale = model.getScale();
        this.location = model.getLocation();
        this.logGammaShape = logGamma(shape);
    }

    @Override
    public double sample(double u) {
        // Clamp u to valid range
        u = Math.max(1e-10, Math.min(1 - 1e-10, u));

        // Compute inverse CDF (quantile function) on standard gamma
        double x = inverseGammaCDF(u);

        // Scale and shift
        return location + scale * x;
    }

    /**
     * Computes the inverse Gamma CDF using Newton-Raphson iteration.
     *
     * <p>Finds x such that P(shape, x) = p, where P is the lower
     * regularized incomplete gamma function.
     */
    private double inverseGammaCDF(double p) {
        // Initial guess using Wilson-Hilferty approximation for chi-squared
        double x = initialGuess(p);

        // Newton-Raphson iteration
        for (int i = 0; i < 50; i++) {
            double cdf = lowerIncompleteGammaP(shape, x);
            double pdf = gammaPDF(x);

            if (pdf < 1e-100) break;

            double error = cdf - p;
            if (Math.abs(error) < 1e-12) break;

            double delta = error / pdf;
            x = x - delta;

            // Keep x positive
            x = Math.max(1e-10, x);
        }

        return x;
    }

    /**
     * Initial guess using Wilson-Hilferty normal approximation.
     */
    private double initialGuess(double p) {
        if (shape >= 1) {
            // Wilson-Hilferty approximation
            double z = InverseNormalCDF.standardNormalQuantile(p);
            double cbrt = Math.cbrt(shape);
            double h = 1.0 / (9.0 * shape);
            double guess = shape * Math.pow(1 - h + z * Math.sqrt(h), 3);
            return Math.max(0.01, guess);
        } else {
            // For small shape, use p^(1/k) as initial guess
            return Math.pow(p, 1.0 / shape) * shape;
        }
    }

    /**
     * Gamma PDF: f(x) = x^(k-1) * e^(-x) / Γ(k)
     */
    private double gammaPDF(double x) {
        if (x <= 0) return 0;
        return Math.exp((shape - 1) * Math.log(x) - x - logGammaShape);
    }

    /**
     * Lower regularized incomplete gamma function P(a, x).
     *
     * <p>Computed using series or continued fraction depending on x.
     */
    private double lowerIncompleteGammaP(double a, double x) {
        if (x < 0) return 0;
        if (x == 0) return 0;

        if (x < a + 1) {
            // Use series representation
            return gammaSeries(a, x);
        } else {
            // Use continued fraction (complement)
            return 1.0 - gammaContinuedFraction(a, x);
        }
    }

    /**
     * Series expansion for lower incomplete gamma function.
     */
    private double gammaSeries(double a, double x) {
        if (x <= 0) return 0;

        double ap = a;
        double sum = 1.0 / a;
        double del = sum;

        for (int n = 1; n <= 200; n++) {
            ap++;
            del *= x / ap;
            sum += del;
            if (Math.abs(del) < Math.abs(sum) * 1e-14) {
                break;
            }
        }

        return sum * Math.exp(-x + a * Math.log(x) - logGammaShape);
    }

    /**
     * Continued fraction for upper incomplete gamma function.
     */
    private double gammaContinuedFraction(double a, double x) {
        double b = x + 1 - a;
        double c = 1.0 / 1e-30;
        double d = 1.0 / b;
        double h = d;

        for (int i = 1; i <= 200; i++) {
            double an = -i * (i - a);
            b += 2;
            d = an * d + b;
            if (Math.abs(d) < 1e-30) d = 1e-30;
            c = b + an / c;
            if (Math.abs(c) < 1e-30) c = 1e-30;
            d = 1.0 / d;
            double del = d * c;
            h *= del;
            if (Math.abs(del - 1.0) < 1e-14) {
                break;
            }
        }

        return Math.exp(-x + a * Math.log(x) - logGammaShape) * h;
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
