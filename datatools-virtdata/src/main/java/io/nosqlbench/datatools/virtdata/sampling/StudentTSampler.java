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

import io.nosqlbench.vshapes.model.StudentTScalarModel;

/**
 * Sampler for Student's t-distributions using inverse CDF transform.
 *
 * <p>Uses the relationship between t-distribution and regularized
 * incomplete beta function to compute quantiles.
 */
public final class StudentTSampler implements ComponentSampler {

    private final double df;        // degrees of freedom (ν)
    private final double location;  // μ
    private final double scale;     // σ

    // Pre-computed for efficiency
    private final double halfDf;
    private final double logBeta;

    /**
     * Creates a sampler bound to the given Student's t model.
     *
     * @param model the Student's t scalar model
     */
    public StudentTSampler(StudentTScalarModel model) {
        this.df = model.getDegreesOfFreedom();
        this.location = model.getLocation();
        this.scale = model.getScale();
        this.halfDf = df / 2.0;
        this.logBeta = logBetaFunction(0.5, halfDf);
    }

    @Override
    public double sample(double u) {
        // Clamp u to valid range
        u = Math.max(1e-10, Math.min(1 - 1e-10, u));

        // Compute inverse CDF on standard t
        double t = inverseTCDF(u);

        // Apply location and scale
        return location + scale * t;
    }

    /**
     * Computes the inverse t-distribution CDF.
     *
     * <p>Uses the relationship:
     * F_t(x; df) = 1 - I_w(df/2, 1/2) / 2 for x >= 0, where w = df/(df + x²)
     * So: x = sqrt(df * (1-w) / w) where w = I^{-1}(2(1-p); df/2, 1/2)
     */
    private double inverseTCDF(double p) {
        boolean negative = p < 0.5;
        double q = negative ? 2 * p : 2 * (1 - p);

        // Compute inverse of regularized incomplete beta I_w(df/2, 1/2) = q
        double w = inverseBetaCDF(q, halfDf, 0.5);

        // Convert to t-value: x = sqrt(df * (1-w) / w)
        double t;
        if (w <= 0) {
            t = Double.POSITIVE_INFINITY;
        } else if (w >= 1) {
            t = 0;
        } else {
            t = Math.sqrt(df * (1.0 - w) / w);
        }

        return negative ? -t : t;
    }

    /**
     * Inverse of regularized incomplete beta function using Newton-Raphson.
     */
    private double inverseBetaCDF(double p, double a, double b) {
        if (p <= 0) return 0;
        if (p >= 1) return 1;

        // Initial guess
        double x = initialBetaGuess(p, a, b);

        // Newton-Raphson iteration
        for (int i = 0; i < 50; i++) {
            double cdf = regularizedIncompleteBeta(x, a, b);
            double pdf = betaPDF(x, a, b);

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

    private double initialBetaGuess(double p, double a, double b) {
        if (a >= 1 && b >= 1) {
            double mean = a / (a + b);
            double var = (a * b) / ((a + b) * (a + b) * (a + b + 1));
            double z = InverseNormalCDF.standardNormalQuantile(p);
            double guess = mean + z * Math.sqrt(var);
            return Math.max(0.01, Math.min(0.99, guess));
        } else {
            return Math.max(0.01, Math.min(0.99, p));
        }
    }

    private double betaPDF(double x, double a, double b) {
        if (x <= 0 || x >= 1) return 0;
        double lb = logBetaFunction(a, b);
        return Math.exp((a - 1) * Math.log(x) + (b - 1) * Math.log(1 - x) - lb);
    }

    private double regularizedIncompleteBeta(double x, double a, double b) {
        if (x <= 0) return 0;
        if (x >= 1) return 1;

        double lb = logBetaFunction(a, b);
        double bt = Math.exp(a * Math.log(x) + b * Math.log(1 - x) - lb);

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
