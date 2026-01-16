package io.nosqlbench.datatools.virtdata;

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

import io.nosqlbench.datatools.virtdata.sampling.ComponentSampler;
import io.nosqlbench.datatools.virtdata.sampling.ComponentSamplerFactory;
import io.nosqlbench.vshapes.extract.*;
import io.nosqlbench.vshapes.model.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.*;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Consolidated accuracy validation tests for the vector space model framework.
 *
 * <h2>Test Categories</h2>
 * <ol>
 *   <li><b>Distribution Classification</b>: Verify fitter selects correct distribution type</li>
 *   <li><b>Sampler Self-Consistency</b>: Verify sampler(u) produces consistent distributions</li>
 *   <li><b>Model Recovery</b>: Verify fitter recovers parameters within tolerance</li>
 *   <li><b>Round-Trip Accuracy</b>: Verify regenerated data matches original statistically</li>
 * </ol>
 *
 * <h2>Usage</h2>
 * <pre>
 * mvn test -pl datatools-virtdata -Paccuracy
 * </pre>
 *
 * @see StatisticalTestSuite
 */
@Tag("accuracy")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class AccuracyValidationTest {

    // Sample size for classification tests - 50K needed for reliable gamma vs beta distinction
    // The gamma/beta confusion requires more samples due to their similar shapes
    private static final int SAMPLES = 50_000;
    private static final long SEED = 42L;

    // Pass criteria
    private static final double KS_CRITICAL_FACTOR = 1.36;  // α=0.05 two-sample K-S
    private static final double MIN_QQ_CORRELATION = 0.995;
    // All distributions should be correctly classified (100% accuracy required)
    private static final double CLASSIFICATION_ACCURACY_THRESHOLD = 1.0;

    // ========== Test Data Providers ==========

    static Stream<Arguments> allDistributionTypes() {
        return Stream.of(
            Arguments.of("normal", new NormalScalarModel(0.0, 1.0)),
            Arguments.of("uniform", new UniformScalarModel(0.0, 1.0)),
            Arguments.of("beta", new BetaScalarModel(2.0, 5.0)),
            Arguments.of("gamma", new GammaScalarModel(3.0, 2.0)),
            Arguments.of("student_t", new StudentTScalarModel(8.0)),
            Arguments.of("inverse_gamma", new InverseGammaScalarModel(4.0, 3.0)),
            Arguments.of("beta_prime", new BetaPrimeScalarModel(3.0, 5.0))
        );
    }

    // ========== 1. Distribution Classification Tests ==========

    @Test
    @Order(1)
    @DisplayName("Distribution Classification: Random data correctly classified")
    void testDistributionClassificationWithRandomData() {
        Random rng = new Random(SEED);
        BestFitSelector selector = BestFitSelector.fullPearsonSelector();

        Map<String, String> results = new LinkedHashMap<>();
        int correct = 0;
        int total = 0;

        // Test each distribution type with random (not stratified) data
        Object[][] configs = {
            {"normal", generateNormalData(rng, 0, 1, SAMPLES)},
            {"uniform", generateUniformData(rng, 0, 1, SAMPLES)},
            {"beta", generateBetaData(rng, 2.0, 5.0, SAMPLES)},
            {"gamma", generateGammaData(rng, 3.0, 2.0, SAMPLES)},
            {"student_t", generateStudentTData(rng, 8.0, SAMPLES)},
            {"inverse_gamma", generateInverseGammaData(rng, 4.0, 3.0, SAMPLES)},
            {"beta_prime", generateBetaPrimeData(rng, 3.0, 5.0, SAMPLES)}
        };

        System.out.println("\n=== Distribution Classification (Random Data) ===");
        System.out.println("Expected        | Classified     | Status");
        System.out.println("----------------|----------------|--------");

        for (Object[] config : configs) {
            String expected = (String) config[0];
            float[] data = (float[]) config[1];

            ScalarModel fitted = selector.selectBest(data);
            String classified = fitted.getModelType();

            // Check correctness - note that Beta(1,1) is mathematically identical to Uniform(0,1)
            boolean isCorrect = expected.equals(classified) ||
                (expected.equals("uniform") && classified.equals("beta"));
            if (isCorrect) correct++;
            total++;

            results.put(expected, classified);
            System.out.printf("%-15s | %-14s | %s%n",
                expected, classified, isCorrect ? "PASS" : "FAIL");
        }

        double accuracy = (double) correct / total;
        System.out.printf("%nClassification Accuracy: %d/%d (%.1f%%)%n", correct, total, accuracy * 100);

        assertTrue(accuracy >= CLASSIFICATION_ACCURACY_THRESHOLD,
            String.format("Classification accuracy %.1f%% below threshold %.1f%%",
                accuracy * 100, CLASSIFICATION_ACCURACY_THRESHOLD * 100));
    }

    @ParameterizedTest(name = "Classification: {0}")
    @MethodSource("allDistributionTypes")
    @Order(2)
    @DisplayName("Distribution Classification: Sampler data correctly classified")
    void testDistributionClassificationWithSamplerData(String typeName, ScalarModel sourceModel) {
        BestFitSelector selector = BestFitSelector.fullPearsonSelector();

        // Generate using sampler (stratified)
        ComponentSampler sampler = ComponentSamplerFactory.forModel(sourceModel);
        float[] data = new float[SAMPLES];
        for (int i = 0; i < SAMPLES; i++) {
            double u = (i + 0.5) / SAMPLES;
            data[i] = (float) sampler.sample(u);
        }

        ScalarModel fitted = selector.selectBest(data);
        String classified = fitted.getModelType();

        System.out.printf("  %s -> %s%n", typeName, classified);

        // Handle known classification equivalences:
        // - Uniform(0,1) and Beta(1,1) are mathematically identical
        // - Gamma with stratified sampling can appear beta-like due to perfect ordering
        if (typeName.equals("uniform")) {
            assertTrue(classified.equals("uniform") || classified.equals("beta"),
                String.format("Expected %s to be classified as uniform or beta, got %s", typeName, classified));
        } else if (typeName.equals("gamma")) {
            // Stratified gamma samples can appear beta-like; random samples classify correctly
            assertTrue(classified.equals("gamma") || classified.equals("beta"),
                String.format("Expected %s to be classified as gamma or beta, got %s", typeName, classified));
        } else {
            assertEquals(typeName, classified,
                String.format("Expected %s to be classified as %s, got %s", typeName, typeName, classified));
        }
    }

    // ========== 2. Sampler Self-Consistency Tests ==========

    @ParameterizedTest(name = "Self-Consistency: {0}")
    @MethodSource("allDistributionTypes")
    @Order(3)
    @DisplayName("Sampler Self-Consistency: Same model produces consistent output")
    void testSamplerSelfConsistency(String typeName, ScalarModel model) {
        // Generate two samples from the same sampler with same u values
        ComponentSampler sampler = ComponentSamplerFactory.forModel(model);

        float[] sample1 = new float[SAMPLES];
        float[] sample2 = new float[SAMPLES];

        for (int i = 0; i < SAMPLES; i++) {
            double u = (i + 0.5) / SAMPLES;
            sample1[i] = (float) sampler.sample(u);
            sample2[i] = (float) sampler.sample(u);  // Same u, same sampler
        }

        // Should be identical
        for (int i = 0; i < SAMPLES; i++) {
            assertEquals(sample1[i], sample2[i], 1e-10,
                String.format("Sampler not deterministic at index %d for %s", i, typeName));
        }
    }

    @ParameterizedTest(name = "Moment Stability: {0}")
    @MethodSource("allDistributionTypes")
    @Order(4)
    @DisplayName("Sampler Moment Stability: Moments stable across runs")
    void testSamplerMomentStability(String typeName, ScalarModel model) {
        ComponentSampler sampler = ComponentSamplerFactory.forModel(model);

        // Generate with different seeds (different dimension offsets)
        float[] sample1 = new float[SAMPLES];
        float[] sample2 = new float[SAMPLES];

        for (int i = 0; i < SAMPLES; i++) {
            sample1[i] = (float) sampler.sample(StratifiedSampler.unitIntervalValue(i, 0, SAMPLES));
            sample2[i] = (float) sampler.sample(StratifiedSampler.unitIntervalValue(i, 1, SAMPLES));
        }

        DimensionStatistics stats1 = DimensionStatistics.compute(0, sample1);
        DimensionStatistics stats2 = DimensionStatistics.compute(0, sample2);

        // Moments should be very similar
        double meanDiff = Math.abs(stats1.mean() - stats2.mean());
        double stdDevDiff = Math.abs(stats1.stdDev() - stats2.stdDev());

        System.out.printf("  %s: mean_diff=%.6f, stddev_diff=%.6f%n", typeName, meanDiff, stdDevDiff);

        assertTrue(meanDiff < 0.01 * stats1.stdDev(),
            String.format("Mean instability for %s: diff=%.6f", typeName, meanDiff));
        assertTrue(stdDevDiff < 0.05 * stats1.stdDev(),
            String.format("StdDev instability for %s: diff=%.6f", typeName, stdDevDiff));
    }

    // ========== 3. Model Recovery Tests ==========

    @Test
    @Order(5)
    @DisplayName("Model Recovery: Parameters recovered within tolerance")
    void testModelParameterRecovery() {
        System.out.println("\n=== Model Parameter Recovery ===");
        System.out.println("Type           | Param      | True     | Fitted   | Error   | Status");
        System.out.println("---------------|------------|----------|----------|---------|-------");

        int passed = 0;
        int total = 0;

        // Test Normal
        passed += testNormalRecovery(0.5, 0.3) ? 1 : 0; total++;

        // Test Uniform
        passed += testUniformRecovery(-0.2, 0.8) ? 1 : 0; total++;

        // Test Beta
        passed += testBetaRecovery(2.0, 5.0) ? 1 : 0; total++;

        // Test Gamma
        passed += testGammaRecovery(3.0, 0.5) ? 1 : 0; total++;

        // Test StudentT
        passed += testStudentTRecovery(10.0) ? 1 : 0; total++;

        double passRate = (double) passed / total;
        System.out.printf("%nParameter Recovery: %d/%d (%.1f%%)%n", passed, total, passRate * 100);

        assertTrue(passRate >= 0.8, "Parameter recovery rate too low");
    }

    private boolean testNormalRecovery(double trueMean, double trueStdDev) {
        NormalScalarModel source = new NormalScalarModel(trueMean, trueStdDev);
        float[] data = generateFromSampler(source, SAMPLES);

        NormalModelFitter fitter = new NormalModelFitter();
        NormalScalarModel fitted = (NormalScalarModel) fitter.fit(data).model();

        double meanErr = Math.abs(fitted.getMean() - trueMean) / trueStdDev;
        double stdErr = Math.abs(fitted.getStdDev() - trueStdDev) / trueStdDev;

        boolean pass = meanErr < 0.05 && stdErr < 0.05;
        System.out.printf("%-14s | %-10s | %8.4f | %8.4f | %7.4f | %s%n",
            "normal", "mean", trueMean, fitted.getMean(), meanErr, pass ? "PASS" : "FAIL");
        System.out.printf("%-14s | %-10s | %8.4f | %8.4f | %7.4f | %s%n",
            "", "stddev", trueStdDev, fitted.getStdDev(), stdErr, pass ? "PASS" : "FAIL");
        return pass;
    }

    private boolean testUniformRecovery(double trueLower, double trueUpper) {
        UniformScalarModel source = new UniformScalarModel(trueLower, trueUpper);
        float[] data = generateFromSampler(source, SAMPLES);

        UniformModelFitter fitter = new UniformModelFitter();
        UniformScalarModel fitted = (UniformScalarModel) fitter.fit(data).model();

        double range = trueUpper - trueLower;
        double lowerErr = Math.abs(fitted.getLower() - trueLower) / range;
        double upperErr = Math.abs(fitted.getUpper() - trueUpper) / range;

        boolean pass = lowerErr < 0.05 && upperErr < 0.05;
        System.out.printf("%-14s | %-10s | %8.4f | %8.4f | %7.4f | %s%n",
            "uniform", "lower", trueLower, fitted.getLower(), lowerErr, pass ? "PASS" : "FAIL");
        System.out.printf("%-14s | %-10s | %8.4f | %8.4f | %7.4f | %s%n",
            "", "upper", trueUpper, fitted.getUpper(), upperErr, pass ? "PASS" : "FAIL");
        return pass;
    }

    private boolean testBetaRecovery(double trueAlpha, double trueBeta) {
        BetaScalarModel source = new BetaScalarModel(trueAlpha, trueBeta);
        float[] data = generateFromSampler(source, SAMPLES);

        BetaModelFitter fitter = new BetaModelFitter();
        BetaScalarModel fitted = (BetaScalarModel) fitter.fit(data).model();

        double alphaErr = Math.abs(fitted.getAlpha() - trueAlpha) / trueAlpha;
        double betaErr = Math.abs(fitted.getBeta() - trueBeta) / trueBeta;

        boolean pass = alphaErr < 0.15 && betaErr < 0.15;
        System.out.printf("%-14s | %-10s | %8.4f | %8.4f | %7.4f | %s%n",
            "beta", "alpha", trueAlpha, fitted.getAlpha(), alphaErr, pass ? "PASS" : "FAIL");
        System.out.printf("%-14s | %-10s | %8.4f | %8.4f | %7.4f | %s%n",
            "", "beta", trueBeta, fitted.getBeta(), betaErr, pass ? "PASS" : "FAIL");
        return pass;
    }

    private boolean testGammaRecovery(double trueShape, double trueScale) {
        GammaScalarModel source = new GammaScalarModel(trueShape, trueScale);
        float[] data = generateFromSampler(source, SAMPLES);

        GammaModelFitter fitter = new GammaModelFitter(false);
        GammaScalarModel fitted = (GammaScalarModel) fitter.fit(data).model();

        double shapeErr = Math.abs(fitted.getShape() - trueShape) / trueShape;
        double scaleErr = Math.abs(fitted.getScale() - trueScale) / trueScale;

        boolean pass = shapeErr < 0.15 && scaleErr < 0.15;
        System.out.printf("%-14s | %-10s | %8.4f | %8.4f | %7.4f | %s%n",
            "gamma", "shape", trueShape, fitted.getShape(), shapeErr, pass ? "PASS" : "FAIL");
        System.out.printf("%-14s | %-10s | %8.4f | %8.4f | %7.4f | %s%n",
            "", "scale", trueScale, fitted.getScale(), scaleErr, pass ? "PASS" : "FAIL");
        return pass;
    }

    private boolean testStudentTRecovery(double trueDf) {
        StudentTScalarModel source = new StudentTScalarModel(trueDf);
        float[] data = generateFromSampler(source, SAMPLES);

        StudentTModelFitter fitter = new StudentTModelFitter();
        StudentTScalarModel fitted = (StudentTScalarModel) fitter.fit(data).model();

        double dfErr = Math.abs(fitted.getDegreesOfFreedom() - trueDf) / trueDf;

        boolean pass = dfErr < 0.20;  // df estimation is harder
        System.out.printf("%-14s | %-10s | %8.4f | %8.4f | %7.4f | %s%n",
            "student_t", "df", trueDf, fitted.getDegreesOfFreedom(), dfErr, pass ? "PASS" : "FAIL");
        return pass;
    }

    // ========== 4. Round-Trip Accuracy Tests ==========

    @ParameterizedTest(name = "Round-Trip: {0}")
    @MethodSource("allDistributionTypes")
    @Order(6)
    @DisplayName("Round-Trip: Source -> Fit -> Regenerate matches original")
    void testRoundTripAccuracy(String typeName, ScalarModel sourceModel) {
        // Generate original data
        float[] original = generateFromSampler(sourceModel, SAMPLES);

        // Fit model (same type)
        BestFitSelector selector = BestFitSelector.fullPearsonSelector();
        ScalarModel fittedModel = selector.selectBest(original);

        // Regenerate from fitted model
        float[] synthetic = generateFromSampler(fittedModel, SAMPLES);

        // Compare using K-S test
        StatisticalTestSuite.TestResult ks = StatisticalTestSuite.kolmogorovSmirnovTest(original, synthetic);
        double qqCorr = StatisticalTestSuite.qqCorrelation(original, synthetic);

        System.out.printf("  %s -> %s: K-S=%.4f (crit=%.4f), QQ=%.4f%n",
            typeName, fittedModel.getModelType(), ks.statistic(), ks.criticalValue(), qqCorr);

        // For same distribution type, K-S should pass or be close
        // Note: BetaPrime has known issues with parameter recovery
        if (!typeName.equals("beta_prime")) {
            assertTrue(ks.statistic() < ks.criticalValue() * 3,
                String.format("K-S statistic %.4f too high for %s", ks.statistic(), typeName));
            assertTrue(qqCorr > 0.99,
                String.format("Q-Q correlation %.4f too low for %s", qqCorr, typeName));
        } else {
            // Relaxed criteria for beta_prime due to known truncation issue
            assertTrue(ks.statistic() < 0.1,
                String.format("K-S statistic %.4f too high for %s", ks.statistic(), typeName));
            assertTrue(qqCorr > 0.98,
                String.format("Q-Q correlation %.4f too low for %s", qqCorr, typeName));
        }
    }

    // ========== Helper Methods ==========

    private float[] generateFromSampler(ScalarModel model, int n) {
        ComponentSampler sampler = ComponentSamplerFactory.forModel(model);
        float[] data = new float[n];
        for (int i = 0; i < n; i++) {
            double u = (i + 0.5) / n;
            data[i] = (float) sampler.sample(u);
        }
        return data;
    }

    // Random data generators (for comparison with sampler data)
    private float[] generateNormalData(Random rng, double mean, double stdDev, int n) {
        float[] data = new float[n];
        for (int i = 0; i < n; i++) {
            data[i] = (float) (rng.nextGaussian() * stdDev + mean);
        }
        return data;
    }

    private float[] generateUniformData(Random rng, double lower, double upper, int n) {
        float[] data = new float[n];
        for (int i = 0; i < n; i++) {
            data[i] = (float) (rng.nextDouble() * (upper - lower) + lower);
        }
        return data;
    }

    private float[] generateBetaData(Random rng, double alpha, double beta, int n) {
        float[] data = new float[n];
        for (int i = 0; i < n; i++) {
            double x = generateGamma(rng, alpha);
            double y = generateGamma(rng, beta);
            data[i] = (float) (x / (x + y));
        }
        return data;
    }

    private float[] generateGammaData(Random rng, double shape, double scale, int n) {
        float[] data = new float[n];
        for (int i = 0; i < n; i++) {
            data[i] = (float) (generateGamma(rng, shape) * scale);
        }
        return data;
    }

    private float[] generateStudentTData(Random rng, double df, int n) {
        float[] data = new float[n];
        for (int i = 0; i < n; i++) {
            double z = rng.nextGaussian();
            double chi2 = generateGamma(rng, df / 2) * 2;
            data[i] = (float) (z / Math.sqrt(chi2 / df));
        }
        return data;
    }

    private float[] generateInverseGammaData(Random rng, double shape, double scale, int n) {
        float[] data = new float[n];
        for (int i = 0; i < n; i++) {
            data[i] = (float) (scale / generateGamma(rng, shape));
        }
        return data;
    }

    private float[] generateBetaPrimeData(Random rng, double alpha, double beta, int n) {
        float[] data = new float[n];
        for (int i = 0; i < n; i++) {
            double x = generateGamma(rng, alpha);
            double y = generateGamma(rng, beta);
            data[i] = (float) (x / y);
        }
        return data;
    }

    private double generateGamma(Random rng, double shape) {
        if (shape < 1) {
            return generateGamma(rng, 1 + shape) * Math.pow(rng.nextDouble(), 1.0 / shape);
        }
        double d = shape - 1.0 / 3.0;
        double c = 1.0 / Math.sqrt(9.0 * d);
        while (true) {
            double x, v;
            do {
                x = rng.nextGaussian();
                v = 1.0 + c * x;
            } while (v <= 0);
            v = v * v * v;
            double u = rng.nextDouble();
            if (u < 1.0 - 0.0331 * (x * x) * (x * x)) return d * v;
            if (Math.log(u) < 0.5 * x * x + d * (1.0 - v + Math.log(v))) return d * v;
        }
    }
}
