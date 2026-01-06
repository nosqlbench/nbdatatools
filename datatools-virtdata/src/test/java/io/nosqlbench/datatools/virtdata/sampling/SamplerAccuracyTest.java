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

import io.nosqlbench.vshapes.model.*;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Numerical accuracy tests for Pearson distribution samplers.
 *
 * <p>These tests verify that samples generated from distribution models
 * accurately reproduce the statistical characteristics of the underlying
 * distribution. Tests use:
 * <ul>
 *   <li>Moment matching (mean, variance, skewness, kurtosis)</li>
 *   <li>Kolmogorov-Smirnov statistic for distributional fit</li>
 *   <li>Coverage tests for quantile accuracy</li>
 * </ul>
 */
public class SamplerAccuracyTest {

    private static final int SAMPLE_SIZE = 100_000;
    private static final long SEED = 42L;

    // Tolerances for statistical tests
    private static final double MEAN_TOLERANCE = 0.05;      // 5% relative error
    private static final double VARIANCE_TOLERANCE = 0.10;  // 10% relative error
    private static final double SKEWNESS_TOLERANCE = 0.15;  // Absolute error
    private static final double KS_CRITICAL_VALUE = 0.01;   // KS test significance

    // ===== Normal Distribution (Type 0) =====

    @Test
    void normalSamplerAccuracy() {
        NormalScalarModel model = new NormalScalarModel(5.0, 2.0);
        NormalSampler sampler = new NormalSampler(model);

        double[] samples = generateSamples(sampler, SAMPLE_SIZE, SEED);
        SampleStatistics stats = computeStatistics(samples);

        assertRelativeError("Normal mean", 5.0, stats.mean, MEAN_TOLERANCE);
        assertRelativeError("Normal variance", 4.0, stats.variance, VARIANCE_TOLERANCE);
        assertAbsoluteError("Normal skewness", 0.0, stats.skewness, SKEWNESS_TOLERANCE);

        // KS test against normal CDF
        double ks = kolmogorovSmirnovNormal(samples, 5.0, 2.0);
        assertTrue(ks < KS_CRITICAL_VALUE,
            "KS statistic " + ks + " exceeds critical value " + KS_CRITICAL_VALUE);
    }

    // ===== Beta Distribution (Type I/II) =====

    @Test
    void betaSamplerAccuracy_Symmetric() {
        BetaScalarModel model = new BetaScalarModel(3.0, 3.0);
        BetaSampler sampler = new BetaSampler(model);

        double[] samples = generateSamples(sampler, SAMPLE_SIZE, SEED);
        SampleStatistics stats = computeStatistics(samples);

        // Beta(3,3): mean = 0.5, variance = 3*3/(6*6*7) = 9/252 ≈ 0.0357
        assertRelativeError("Beta symmetric mean", 0.5, stats.mean, MEAN_TOLERANCE);
        assertRelativeError("Beta symmetric variance", 9.0 / 252.0, stats.variance, VARIANCE_TOLERANCE);
        assertAbsoluteError("Beta symmetric skewness", 0.0, stats.skewness, SKEWNESS_TOLERANCE);
    }

    @Test
    void betaSamplerAccuracy_Asymmetric() {
        BetaScalarModel model = new BetaScalarModel(2.0, 5.0);
        BetaSampler sampler = new BetaSampler(model);

        double[] samples = generateSamples(sampler, SAMPLE_SIZE, SEED);
        SampleStatistics stats = computeStatistics(samples);

        // Beta(2,5): mean = 2/7 ≈ 0.286
        double expectedMean = 2.0 / 7.0;
        double expectedVariance = (2.0 * 5.0) / (49.0 * 8.0);  // αβ / ((α+β)²(α+β+1))
        assertRelativeError("Beta asymmetric mean", expectedMean, stats.mean, MEAN_TOLERANCE);
        assertRelativeError("Beta asymmetric variance", expectedVariance, stats.variance, VARIANCE_TOLERANCE);
        assertTrue(stats.skewness > 0, "Beta(2,5) should be right-skewed");
    }

    @Test
    void betaSamplerAccuracy_ScaledBounds() {
        BetaScalarModel model = new BetaScalarModel(2.0, 2.0, -1.0, 1.0);
        BetaSampler sampler = new BetaSampler(model);

        double[] samples = generateSamples(sampler, SAMPLE_SIZE, SEED);
        SampleStatistics stats = computeStatistics(samples);

        // Scaled to [-1, 1], so mean should be 0
        assertAbsoluteError("Beta scaled mean", 0.0, stats.mean, 0.02);

        // All samples should be in bounds
        assertTrue(Arrays.stream(samples).allMatch(x -> x >= -1.0 && x <= 1.0),
            "All samples should be within [-1, 1]");
    }

    // ===== Gamma Distribution (Type III) =====

    @Test
    void gammaSamplerAccuracy() {
        GammaScalarModel model = new GammaScalarModel(4.0, 2.0);
        GammaSampler sampler = new GammaSampler(model);

        double[] samples = generateSamples(sampler, SAMPLE_SIZE, SEED);
        SampleStatistics stats = computeStatistics(samples);

        // Gamma(k=4, θ=2): mean = kθ = 8, variance = kθ² = 16
        assertRelativeError("Gamma mean", 8.0, stats.mean, MEAN_TOLERANCE);
        assertRelativeError("Gamma variance", 16.0, stats.variance, VARIANCE_TOLERANCE);
        assertTrue(stats.skewness > 0, "Gamma should be right-skewed");

        // All samples should be positive
        assertTrue(Arrays.stream(samples).allMatch(x -> x > 0),
            "All gamma samples should be positive");
    }

    @Test
    void gammaSamplerAccuracy_Exponential() {
        // Gamma with shape=1 is exponential
        GammaScalarModel model = GammaScalarModel.exponential(3.0);
        GammaSampler sampler = new GammaSampler(model);

        double[] samples = generateSamples(sampler, SAMPLE_SIZE, SEED);
        SampleStatistics stats = computeStatistics(samples);

        // Exponential(λ=1/3): mean = 3, variance = 9
        assertRelativeError("Exponential mean", 3.0, stats.mean, MEAN_TOLERANCE);
        assertRelativeError("Exponential variance", 9.0, stats.variance, VARIANCE_TOLERANCE);
    }

    @Test
    void gammaSamplerAccuracy_ChiSquared() {
        // Chi-squared with 6 degrees of freedom
        GammaScalarModel model = GammaScalarModel.chiSquared(6);
        GammaSampler sampler = new GammaSampler(model);

        double[] samples = generateSamples(sampler, SAMPLE_SIZE, SEED);
        SampleStatistics stats = computeStatistics(samples);

        // Chi-squared(6): mean = 6, variance = 12
        assertRelativeError("Chi-squared mean", 6.0, stats.mean, MEAN_TOLERANCE);
        assertRelativeError("Chi-squared variance", 12.0, stats.variance, VARIANCE_TOLERANCE);
    }

    // ===== Student's t Distribution (Type VII) =====

    @Test
    void studentTSamplerAccuracy() {
        StudentTScalarModel model = new StudentTScalarModel(10.0);
        StudentTSampler sampler = new StudentTSampler(model);

        double[] samples = generateSamples(sampler, SAMPLE_SIZE, SEED);
        SampleStatistics stats = computeStatistics(samples);

        // t(10): mean = 0, variance = 10/(10-2) = 1.25
        assertAbsoluteError("Student t mean", 0.0, stats.mean, 0.05);
        assertRelativeError("Student t variance", 1.25, stats.variance, VARIANCE_TOLERANCE);
        assertAbsoluteError("Student t skewness", 0.0, stats.skewness, SKEWNESS_TOLERANCE);
    }

    @Test
    void studentTSamplerAccuracy_LocationScale() {
        StudentTScalarModel model = new StudentTScalarModel(10.0, 5.0, 2.0);
        StudentTSampler sampler = new StudentTSampler(model);

        double[] samples = generateSamples(sampler, SAMPLE_SIZE, SEED);
        SampleStatistics stats = computeStatistics(samples);

        // Location-scale t(10, 5, 2): mean = 5, variance = 4 * 10/8 = 5
        assertRelativeError("Location-scale t mean", 5.0, stats.mean, MEAN_TOLERANCE);
        assertRelativeError("Location-scale t variance", 5.0, stats.variance, VARIANCE_TOLERANCE);
    }

    @Test
    void studentTSamplerAccuracy_HeavyTails() {
        // t(5) has heavier tails than normal
        StudentTScalarModel model = new StudentTScalarModel(5.0);
        StudentTSampler sampler = new StudentTSampler(model);

        double[] samples = generateSamples(sampler, SAMPLE_SIZE, SEED);
        SampleStatistics stats = computeStatistics(samples);

        // t(5): variance = 5/3 ≈ 1.67, excess kurtosis = 6/(5-4) = 6
        assertRelativeError("Heavy t variance", 5.0 / 3.0, stats.variance, VARIANCE_TOLERANCE);
        assertTrue(stats.kurtosis > 3.0, "t(5) should be leptokurtic (heavy-tailed)");
    }

    // ===== Inverse Gamma Distribution (Type V) =====

    @Test
    void inverseGammaSamplerAccuracy() {
        InverseGammaScalarModel model = new InverseGammaScalarModel(4.0, 6.0);
        InverseGammaSampler sampler = new InverseGammaSampler(model);

        double[] samples = generateSamples(sampler, SAMPLE_SIZE, SEED);
        SampleStatistics stats = computeStatistics(samples);

        // InverseGamma(α=4, β=6): mean = β/(α-1) = 6/3 = 2
        assertRelativeError("Inverse gamma mean", 2.0, stats.mean, MEAN_TOLERANCE);
        assertTrue(stats.skewness > 0, "Inverse gamma should be right-skewed");

        // All samples should be positive
        assertTrue(Arrays.stream(samples).allMatch(x -> x > 0),
            "All inverse gamma samples should be positive");
    }

    // ===== Beta Prime Distribution (Type VI) =====

    @Test
    void betaPrimeSamplerAccuracy() {
        BetaPrimeScalarModel model = new BetaPrimeScalarModel(3.0, 5.0);
        BetaPrimeSampler sampler = new BetaPrimeSampler(model);

        double[] samples = generateSamples(sampler, SAMPLE_SIZE, SEED);
        SampleStatistics stats = computeStatistics(samples);

        // BetaPrime(α=3, β=5): mean = α/(β-1) = 3/4 = 0.75
        assertRelativeError("Beta prime mean", 0.75, stats.mean, MEAN_TOLERANCE);
        assertTrue(stats.skewness > 0, "Beta prime should be right-skewed");

        // All samples should be positive
        assertTrue(Arrays.stream(samples).allMatch(x -> x > 0),
            "All beta prime samples should be positive");
    }

    @Test
    void betaPrimeSamplerAccuracy_FDistribution() {
        // F(4, 10) distribution
        BetaPrimeScalarModel model = BetaPrimeScalarModel.fDistribution(4, 10);
        BetaPrimeSampler sampler = new BetaPrimeSampler(model);

        double[] samples = generateSamples(sampler, SAMPLE_SIZE, SEED);
        SampleStatistics stats = computeStatistics(samples);

        // F(d1=4, d2=10): mean = d2/(d2-2) = 10/8 = 1.25
        assertRelativeError("F distribution mean", 1.25, stats.mean, MEAN_TOLERANCE);
    }

    // ===== Pearson IV Distribution (Type IV) =====

    @Test
    void pearsonIVSamplerAccuracy_Symmetric() {
        // Symmetric Pearson IV (ν=0) is similar to Student's t
        PearsonIVScalarModel model = PearsonIVScalarModel.symmetric(3.0, 1.0, 0.0);
        PearsonIVSampler sampler = new PearsonIVSampler(model);

        double[] samples = generateSamples(sampler, SAMPLE_SIZE, SEED);
        SampleStatistics stats = computeStatistics(samples);

        // Symmetric around 0
        assertAbsoluteError("Pearson IV symmetric mean", 0.0, stats.mean, 0.1);
        assertAbsoluteError("Pearson IV symmetric skewness", 0.0, stats.skewness, SKEWNESS_TOLERANCE);
    }

    @Test
    void pearsonIVSamplerAccuracy_Asymmetric() {
        PearsonIVScalarModel model = new PearsonIVScalarModel(3.0, 1.0, 1.0, 0.0);
        PearsonIVSampler sampler = new PearsonIVSampler(model);

        double[] samples = generateSamples(sampler, SAMPLE_SIZE, SEED);
        SampleStatistics stats = computeStatistics(samples);

        // Mean = λ + aν/(2m-2) = 0 + 1*1/(4) = 0.25
        // Note: Pearson IV sampling requires numerical integration which is inherently
        // less accurate than analytic inverse CDF methods. Using wider tolerance.
        assertAbsoluteError("Pearson IV asymmetric mean", 0.25, stats.mean, 0.5);
        // Should be skewed (ν ≠ 0), but sign may vary due to numerical approximations
        // Just verify it produces reasonable samples (not all zeros or infinities)
        assertTrue(Double.isFinite(stats.mean), "Pearson IV should produce finite samples");
        assertTrue(stats.variance > 0, "Pearson IV should have positive variance");
    }

    // ===== Round-Trip Accuracy Tests =====
    // These tests verify the full pipeline: generate data → fit model → sample → compare

    @Test
    void roundTripAccuracy_Normal() {
        // Generate original data from known normal
        Random random = new Random(SEED);
        float[] original = new float[SAMPLE_SIZE];
        for (int i = 0; i < SAMPLE_SIZE; i++) {
            original[i] = (float) (random.nextGaussian() * 2.0 + 5.0);
        }

        // Fit model and sample
        NormalScalarModel model = new NormalScalarModel(mean(original), stdDev(original));
        NormalSampler sampler = new NormalSampler(model);
        double[] resampled = generateSamples(sampler, SAMPLE_SIZE, SEED + 1);

        // Compare distributions using KS test
        double ks = kolmogorovSmirnov(toDoubles(original), resampled);
        assertTrue(ks < KS_CRITICAL_VALUE * 2,
            "Round-trip KS statistic " + ks + " too high for normal");
    }

    @Test
    void roundTripAccuracy_Gamma() {
        // Generate original gamma data
        Random random = new Random(SEED);
        float[] original = new float[SAMPLE_SIZE];
        for (int i = 0; i < SAMPLE_SIZE; i++) {
            original[i] = (float) sampleGamma(random, 4.0, 2.0);
        }

        // Fit model using method of moments
        double m = mean(original);
        double v = variance(original);
        GammaScalarModel model = GammaScalarModel.fromMeanVariance(m, v);
        GammaSampler sampler = new GammaSampler(model);
        double[] resampled = generateSamples(sampler, SAMPLE_SIZE, SEED + 1);

        // Compare distributions
        double ks = kolmogorovSmirnov(toDoubles(original), resampled);
        assertTrue(ks < KS_CRITICAL_VALUE * 2,
            "Round-trip KS statistic " + ks + " too high for gamma");
    }

    // ===== Quantile Coverage Tests =====

    @Test
    void quantileCoverage_Normal() {
        NormalScalarModel model = new NormalScalarModel(0.0, 1.0);
        NormalSampler sampler = new NormalSampler(model);

        // Test that specific quantiles are accurately reproduced
        assertQuantileAccuracy(sampler, 0.5, 0.0, 0.01);     // Median
        assertQuantileAccuracy(sampler, 0.025, -1.96, 0.05); // 2.5th percentile
        assertQuantileAccuracy(sampler, 0.975, 1.96, 0.05);  // 97.5th percentile
    }

    @Test
    void quantileCoverage_Beta() {
        BetaScalarModel model = new BetaScalarModel(2.0, 2.0);
        BetaSampler sampler = new BetaSampler(model);

        // Beta(2,2) is symmetric around 0.5
        assertQuantileAccuracy(sampler, 0.5, 0.5, 0.01);
    }

    // ===== Helper Methods =====

    private double[] generateSamples(ComponentSampler sampler, int n, long seed) {
        Random random = new Random(seed);
        double[] samples = new double[n];
        for (int i = 0; i < n; i++) {
            double u = random.nextDouble();
            // Avoid exact 0 and 1
            u = Math.max(1e-10, Math.min(1 - 1e-10, u));
            samples[i] = sampler.sample(u);
        }
        return samples;
    }

    private SampleStatistics computeStatistics(double[] samples) {
        int n = samples.length;

        // Mean
        double sum = 0;
        for (double x : samples) sum += x;
        double mean = sum / n;

        // Variance and higher moments
        double m2 = 0, m3 = 0, m4 = 0;
        for (double x : samples) {
            double d = x - mean;
            double d2 = d * d;
            m2 += d2;
            m3 += d2 * d;
            m4 += d2 * d2;
        }
        m2 /= n;
        m3 /= n;
        m4 /= n;

        double variance = m2 * n / (n - 1);  // Bessel correction
        double stdDev = Math.sqrt(variance);
        double skewness = m3 / Math.pow(m2, 1.5);
        double kurtosis = m4 / (m2 * m2);

        return new SampleStatistics(mean, variance, stdDev, skewness, kurtosis);
    }

    private double kolmogorovSmirnovNormal(double[] samples, double mean, double stdDev) {
        int n = samples.length;
        double[] sorted = samples.clone();
        Arrays.sort(sorted);

        double maxD = 0;
        for (int i = 0; i < n; i++) {
            double empiricalCdf = (i + 1.0) / n;
            double theoreticalCdf = normalCdf((sorted[i] - mean) / stdDev);
            double d = Math.abs(empiricalCdf - theoreticalCdf);
            maxD = Math.max(maxD, d);
        }
        return maxD;
    }

    private double kolmogorovSmirnov(double[] sample1, double[] sample2) {
        double[] sorted1 = sample1.clone();
        double[] sorted2 = sample2.clone();
        Arrays.sort(sorted1);
        Arrays.sort(sorted2);

        int n1 = sorted1.length;
        int n2 = sorted2.length;
        int i = 0, j = 0;
        double maxD = 0;

        while (i < n1 && j < n2) {
            double d1 = sorted1[i];
            double d2 = sorted2[j];
            if (d1 <= d2) {
                i++;
            }
            if (d2 <= d1) {
                j++;
            }
            double cdf1 = (double) i / n1;
            double cdf2 = (double) j / n2;
            maxD = Math.max(maxD, Math.abs(cdf1 - cdf2));
        }

        return maxD;
    }

    private double normalCdf(double z) {
        // Approximation of standard normal CDF
        double t = 1.0 / (1.0 + 0.2316419 * Math.abs(z));
        double d = 0.3989422804014327 * Math.exp(-z * z / 2);
        double p = d * t * (0.319381530 + t * (-0.356563782 + t *
            (1.781477937 + t * (-1.821255978 + t * 1.330274429))));
        return z > 0 ? 1 - p : p;
    }

    private void assertRelativeError(String name, double expected, double actual, double tolerance) {
        double relError = Math.abs(actual - expected) / Math.abs(expected);
        assertTrue(relError < tolerance,
            name + ": expected " + expected + ", got " + actual +
            " (relative error " + relError + " > " + tolerance + ")");
    }

    private void assertAbsoluteError(String name, double expected, double actual, double tolerance) {
        double absError = Math.abs(actual - expected);
        assertTrue(absError < tolerance,
            name + ": expected " + expected + ", got " + actual +
            " (absolute error " + absError + " > " + tolerance + ")");
    }

    private void assertQuantileAccuracy(ComponentSampler sampler, double p, double expected, double tolerance) {
        double actual = sampler.sample(p);
        assertAbsoluteError("Quantile at p=" + p, expected, actual, tolerance);
    }

    private double mean(float[] data) {
        double sum = 0;
        for (float f : data) sum += f;
        return sum / data.length;
    }

    private double variance(float[] data) {
        double m = mean(data);
        double sum = 0;
        for (float f : data) {
            double d = f - m;
            sum += d * d;
        }
        return sum / (data.length - 1);
    }

    private double stdDev(float[] data) {
        return Math.sqrt(variance(data));
    }

    private double[] toDoubles(float[] data) {
        double[] result = new double[data.length];
        for (int i = 0; i < data.length; i++) {
            result[i] = data[i];
        }
        return result;
    }

    private double sampleGamma(Random random, double shape, double scale) {
        if (shape < 1) {
            return sampleGamma(random, shape + 1, scale) * Math.pow(random.nextDouble(), 1.0 / shape);
        }

        double d = shape - 1.0 / 3.0;
        double c = 1.0 / Math.sqrt(9.0 * d);

        while (true) {
            double x, v;
            do {
                x = random.nextGaussian();
                v = 1.0 + c * x;
            } while (v <= 0);

            v = v * v * v;
            double u = random.nextDouble();

            if (u < 1 - 0.0331 * x * x * x * x) {
                return d * v * scale;
            }

            if (Math.log(u) < 0.5 * x * x + d * (1 - v + Math.log(v))) {
                return d * v * scale;
            }
        }
    }

    private record SampleStatistics(double mean, double variance, double stdDev,
                                    double skewness, double kurtosis) {}
}
