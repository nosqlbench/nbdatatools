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

import io.nosqlbench.vshapes.model.StudentTScalarModel;

import java.util.Objects;

/**
 * Fits a Student's t-distribution to observed data - Pearson Type VII.
 *
 * <h2>Algorithm</h2>
 *
 * <p>Uses the method of moments estimation based on excess kurtosis:
 * <ol>
 *   <li>Compute sample mean and standard deviation</li>
 *   <li>Compute sample excess kurtosis</li>
 *   <li>Estimate degrees of freedom: ν = 4 + 6/excessKurtosis</li>
 *   <li>Estimate location μ = sample mean</li>
 *   <li>Estimate scale σ from sample variance: σ² = var * (ν-2)/ν</li>
 * </ol>
 *
 * <h2>Applicability</h2>
 *
 * <p>Student's t-distribution is appropriate when:
 * <ul>
 *   <li>Data is symmetric (low skewness)</li>
 *   <li>Data has heavier tails than normal (kurtosis &gt; 3)</li>
 *   <li>Unbounded support</li>
 * </ul>
 *
 * <h2>Constraints</h2>
 *
 * <p>Degrees of freedom must be &gt; 4 for finite kurtosis.
 * This fitter clamps ν to [4.01, 100].
 *
 * @see StudentTScalarModel
 * @see ComponentModelFitter
 */
public final class StudentTModelFitter implements ComponentModelFitter {

    private static final double MIN_DF = 4.01;  // Must be > 4 for finite kurtosis
    private static final double MAX_DF = 100.0; // Above this, use normal

    private final double maxSkewness;

    /**
     * Creates a Student's t model fitter with default settings.
     */
    public StudentTModelFitter() {
        this(0.5);  // Allow some asymmetry tolerance
    }

    /**
     * Creates a Student's t model fitter.
     *
     * @param maxSkewness maximum allowed skewness magnitude (t is symmetric)
     */
    public StudentTModelFitter(double maxSkewness) {
        this.maxSkewness = maxSkewness;
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
        double excessKurtosis = stats.kurtosis() - 3.0;  // Convert to excess kurtosis

        // Estimate degrees of freedom from excess kurtosis
        // For t-distribution: excess kurtosis = 6/(ν-4) for ν > 4
        // Solving: ν = 4 + 6/excessKurtosis
        double degreesOfFreedom;
        if (excessKurtosis <= 0.06) {
            // Very close to normal, use high df
            degreesOfFreedom = MAX_DF;
        } else {
            degreesOfFreedom = 4 + 6.0 / excessKurtosis;
        }

        // Clamp to valid range
        degreesOfFreedom = Math.max(MIN_DF, Math.min(MAX_DF, degreesOfFreedom));

        // Estimate location (mean)
        double location = mean;

        // Estimate scale from variance
        // For t: Var = σ² * ν/(ν-2)
        // So: σ² = Var * (ν-2)/ν
        double scale;
        if (degreesOfFreedom > 2) {
            scale = Math.sqrt(variance * (degreesOfFreedom - 2) / degreesOfFreedom);
        } else {
            scale = Math.sqrt(variance);
        }

        // Ensure positive scale
        scale = Math.max(scale, 1e-10);

        StudentTScalarModel model = new StudentTScalarModel(degreesOfFreedom, location, scale);

        // Compute goodness-of-fit
        double goodnessOfFit = computeGoodnessOfFit(values, degreesOfFreedom, location, scale, stats);

        return new FitResult(model, goodnessOfFit, getModelType());
    }

    @Override
    public String getModelType() {
        return StudentTScalarModel.MODEL_TYPE;
    }

    @Override
    public boolean supportsBoundedData() {
        return false;  // t-distribution is unbounded
    }

    /**
     * Computes goodness-of-fit considering both tail behavior and symmetry.
     */
    private double computeGoodnessOfFit(float[] values, double df, double location,
                                         double scale, DimensionStatistics stats) {
        if (values == null || values.length < 10) {
            return 0.5;
        }

        // Penalize asymmetric data (t is symmetric)
        double skewnessPenalty = Math.abs(stats.skewness());
        if (skewnessPenalty > maxSkewness) {
            // High penalty for asymmetric data
            skewnessPenalty = skewnessPenalty * 2;
        }

        // Compute KS-like statistic
        int n = values.length;
        float[] sorted = values.clone();
        java.util.Arrays.sort(sorted);

        double maxD = 0;
        for (int i = 0; i < n; i++) {
            double x = (sorted[i] - location) / scale;
            double empiricalCdf = (double) (i + 1) / n;
            double theoreticalCdf = approximateTCDF(x, df);

            double d = Math.abs(empiricalCdf - theoreticalCdf);
            maxD = Math.max(maxD, d);
        }

        double ksStatistic = maxD * Math.sqrt(n);

        // Combine KS statistic with skewness penalty
        return ksStatistic + skewnessPenalty;
    }

    /**
     * Approximates the t-distribution CDF.
     * Uses the relationship: F(x) = 1/2 + x*Γ((ν+1)/2) / (√(νπ)*Γ(ν/2)) * hypergeom
     */
    private double approximateTCDF(double x, double df) {
        // Use the incomplete beta function relationship
        // F(x) = 1 - I_{ν/(ν+x²)}(ν/2, 1/2) / 2 for x > 0
        // F(x) = I_{ν/(ν+x²)}(ν/2, 1/2) / 2 for x < 0
        // where I is the regularized incomplete beta function

        double t = df / (df + x * x);
        double beta = regularizedIncompleteBeta(t, df / 2, 0.5);

        if (x > 0) {
            return 1.0 - beta / 2;
        } else {
            return beta / 2;
        }
    }

    /**
     * Approximates the regularized incomplete beta function I_x(a, b).
     */
    private double regularizedIncompleteBeta(double x, double a, double b) {
        if (x <= 0) return 0;
        if (x >= 1) return 1;

        // Use continued fraction approximation
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
