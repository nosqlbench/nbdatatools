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

import io.nosqlbench.vshapes.model.InverseGammaScalarModel;

/**
 * Sampler for Inverse Gamma distributions using inverse CDF transform.
 *
 * <p>Uses the relationship: if X ~ Gamma(α, 1/β), then 1/X ~ InverseGamma(α, β).
 * We compute the inverse CDF by transforming the gamma inverse CDF.
 */
public final class InverseGammaSampler implements ComponentSampler {

    private final double shape;  // α
    private final double scale;  // β

    // Pre-computed for efficiency
    private final double logGammaShape;

    /**
     * Creates a sampler bound to the given Inverse Gamma model.
     *
     * @param model the Inverse Gamma scalar model
     */
    public InverseGammaSampler(InverseGammaScalarModel model) {
        this.shape = model.getShape();
        this.scale = model.getScale();
        this.logGammaShape = logGamma(shape);
    }

    @Override
    public double sample(double u) {
        // Clamp u to valid range
        u = Math.max(1e-10, Math.min(1 - 1e-10, u));

        // If X ~ InverseGamma(α, β), then 1/X ~ Gamma(α, 1/β)
        // P(X ≤ x) = P(1/X ≥ 1/x) = 1 - P(Gamma ≤ 1/x) = 1 - P_gamma(α, 1/(βx))
        // So F_IG(x) = 1 - F_gamma(1/(βx); α, 1/β) = 1 - P(α, β/x)
        // And F_IG^{-1}(u) = β / F_gamma^{-1}(1-u; α, 1)

        // Compute inverse CDF of Gamma(α, 1) at (1-u)
        double gammaQuantile = inverseGammaCDF(1 - u);

        // Transform to inverse gamma
        return scale / gammaQuantile;
    }

    /**
     * Computes the inverse Gamma CDF using Newton-Raphson iteration.
     */
    private double inverseGammaCDF(double p) {
        if (p <= 0) return 0;
        if (p >= 1) return Double.POSITIVE_INFINITY;

        // Initial guess
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

    private double initialGuess(double p) {
        if (shape >= 1) {
            // Wilson-Hilferty approximation
            double z = InverseNormalCDF.standardNormalQuantile(p);
            double cbrt = Math.cbrt(shape);
            double h = 1.0 / (9.0 * shape);
            double guess = shape * Math.pow(1 - h + z * Math.sqrt(h), 3);
            return Math.max(0.01, guess);
        } else {
            return Math.pow(p, 1.0 / shape) * shape;
        }
    }

    private double gammaPDF(double x) {
        if (x <= 0) return 0;
        return Math.exp((shape - 1) * Math.log(x) - x - logGammaShape);
    }

    private double lowerIncompleteGammaP(double a, double x) {
        if (x < 0) return 0;
        if (x == 0) return 0;

        if (x < a + 1) {
            return gammaSeries(a, x);
        } else {
            return 1.0 - gammaContinuedFraction(a, x);
        }
    }

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
