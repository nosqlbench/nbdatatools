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

import io.nosqlbench.vshapes.model.*;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Numerical accuracy tests for model fitters.
 *
 * <p>These tests verify that:
 * <ol>
 *   <li>Each fitter correctly identifies data from its own distribution</li>
 *   <li>Goodness-of-fit scores are on comparable scales</li>
 *   <li>Parameter estimation is accurate for known distributions</li>
 * </ol>
 */
public class FitterNumericalAccuracyTest {

    private static final int SAMPLE_SIZE = 10000;
    private static final long SEED = 12345L;

    // Expected ranges for goodness-of-fit scores when fitting correct distribution
    private static final double MAX_GOOD_FIT_SCORE = 3.0;

    @Test
    void testNormalFitterAccuracy() {
        Random rng = new Random(SEED);
        double trueMean = 5.0;
        double trueStdDev = 2.0;

        float[] data = new float[SAMPLE_SIZE];
        for (int i = 0; i < SAMPLE_SIZE; i++) {
            data[i] = (float) (rng.nextGaussian() * trueStdDev + trueMean);
        }

        NormalModelFitter fitter = new NormalModelFitter();
        ComponentModelFitter.FitResult result = fitter.fit(data);

        NormalScalarModel model = (NormalScalarModel) result.model();

        // Parameter accuracy
        assertEquals(trueMean, model.getMean(), 0.1, "Mean should be accurate");
        assertEquals(trueStdDev, model.getStdDev(), 0.1, "StdDev should be accurate");

        // Goodness-of-fit should indicate good fit
        assertTrue(result.goodnessOfFit() < MAX_GOOD_FIT_SCORE,
            "Normal data should have good fit score, got: " + result.goodnessOfFit());

        System.out.printf("Normal fitter: mean=%.3f (true=%.1f), stdDev=%.3f (true=%.1f), gof=%.4f%n",
            model.getMean(), trueMean, model.getStdDev(), trueStdDev, result.goodnessOfFit());
    }

    @Test
    void testUniformFitterAccuracy() {
        Random rng = new Random(SEED);
        double trueLower = 2.0;
        double trueUpper = 8.0;

        float[] data = new float[SAMPLE_SIZE];
        for (int i = 0; i < SAMPLE_SIZE; i++) {
            data[i] = (float) (rng.nextDouble() * (trueUpper - trueLower) + trueLower);
        }

        UniformModelFitter fitter = new UniformModelFitter();
        ComponentModelFitter.FitResult result = fitter.fit(data);

        UniformScalarModel model = (UniformScalarModel) result.model();

        // Parameter accuracy
        assertEquals(trueLower, model.getLower(), 0.1, "Lower bound should be accurate");
        assertEquals(trueUpper, model.getUpper(), 0.1, "Upper bound should be accurate");

        // Goodness-of-fit should indicate good fit
        assertTrue(result.goodnessOfFit() < MAX_GOOD_FIT_SCORE,
            "Uniform data should have good fit score, got: " + result.goodnessOfFit());

        System.out.printf("Uniform fitter: lower=%.3f (true=%.1f), upper=%.3f (true=%.1f), gof=%.4f%n",
            model.getLower(), trueLower, model.getUpper(), trueUpper, result.goodnessOfFit());
    }

    @Test
    void testBetaFitterAccuracy() {
        Random rng = new Random(SEED);
        double trueAlpha = 2.0;
        double trueBeta = 5.0;

        // Generate beta-distributed data using rejection sampling
        float[] data = generateBetaData(rng, trueAlpha, trueBeta, SAMPLE_SIZE);

        BetaModelFitter fitter = new BetaModelFitter();
        ComponentModelFitter.FitResult result = fitter.fit(data);

        BetaScalarModel model = (BetaScalarModel) result.model();

        // Parameter accuracy (method of moments may have larger error)
        assertEquals(trueAlpha, model.getAlpha(), 0.5, "Alpha should be reasonably accurate");
        assertEquals(trueBeta, model.getBeta(), 1.0, "Beta should be reasonably accurate");

        // Goodness-of-fit should indicate good fit
        assertTrue(result.goodnessOfFit() < MAX_GOOD_FIT_SCORE,
            "Beta data should have good fit score, got: " + result.goodnessOfFit());

        System.out.printf("Beta fitter: alpha=%.3f (true=%.1f), beta=%.3f (true=%.1f), gof=%.4f%n",
            model.getAlpha(), trueAlpha, model.getBeta(), trueBeta, result.goodnessOfFit());
    }

    @Test
    void testGammaFitterAccuracy() {
        Random rng = new Random(SEED);
        double trueShape = 3.0;
        double trueScale = 2.0;

        // Generate gamma-distributed data
        float[] data = generateGammaData(rng, trueShape, trueScale, SAMPLE_SIZE);

        GammaModelFitter fitter = new GammaModelFitter(false);  // No location detection
        ComponentModelFitter.FitResult result = fitter.fit(data);

        GammaScalarModel model = (GammaScalarModel) result.model();

        // Parameter accuracy
        assertEquals(trueShape, model.getShape(), 0.3, "Shape should be accurate");
        assertEquals(trueScale, model.getScale(), 0.3, "Scale should be accurate");

        // Goodness-of-fit should indicate good fit
        assertTrue(result.goodnessOfFit() < MAX_GOOD_FIT_SCORE,
            "Gamma data should have good fit score, got: " + result.goodnessOfFit());

        System.out.printf("Gamma fitter: shape=%.3f (true=%.1f), scale=%.3f (true=%.1f), gof=%.4f%n",
            model.getShape(), trueShape, model.getScale(), trueScale, result.goodnessOfFit());
    }

    @Test
    void testStudentTFitterAccuracy() {
        Random rng = new Random(SEED);
        double trueDf = 5.0;
        double trueLocation = 3.0;
        double trueScale = 1.5;

        // Generate t-distributed data
        float[] data = generateStudentTData(rng, trueDf, trueLocation, trueScale, SAMPLE_SIZE);

        StudentTModelFitter fitter = new StudentTModelFitter();
        ComponentModelFitter.FitResult result = fitter.fit(data);

        StudentTScalarModel model = (StudentTScalarModel) result.model();

        // Parameter accuracy (df estimation can be noisy)
        assertEquals(trueDf, model.getDegreesOfFreedom(), 2.0, "DF should be reasonably accurate");
        assertEquals(trueLocation, model.getLocation(), 0.2, "Location should be accurate");
        assertEquals(trueScale, model.getScale(), 0.3, "Scale should be accurate");

        // Goodness-of-fit should indicate good fit
        assertTrue(result.goodnessOfFit() < MAX_GOOD_FIT_SCORE,
            "StudentT data should have good fit score, got: " + result.goodnessOfFit());

        System.out.printf("StudentT fitter: df=%.3f (true=%.1f), loc=%.3f (true=%.1f), scale=%.3f (true=%.1f), gof=%.4f%n",
            model.getDegreesOfFreedom(), trueDf, model.getLocation(), trueLocation,
            model.getScale(), trueScale, result.goodnessOfFit());
    }

    @Test
    void testCorrectDistributionIsSelected() {
        BestFitSelector fullSelector = BestFitSelector.fullPearsonSelector();
        Random rng = new Random(SEED);

        // Test normal data selects Normal
        float[] normalData = new float[SAMPLE_SIZE];
        for (int i = 0; i < SAMPLE_SIZE; i++) {
            normalData[i] = (float) rng.nextGaussian();
        }
        ScalarModel normalBest = fullSelector.selectBest(normalData);
        assertEquals("normal", normalBest.getModelType(),
            "Normal data should select Normal model");

        // Test uniform data selects Uniform or Beta
        // Note: Beta(1,1) is mathematically identical to Uniform(0,1), so either is acceptable
        float[] uniformData = new float[SAMPLE_SIZE];
        for (int i = 0; i < SAMPLE_SIZE; i++) {
            uniformData[i] = rng.nextFloat();
        }
        ScalarModel uniformBest = fullSelector.selectBest(uniformData);
        assertTrue(uniformBest.getModelType().equals("uniform") || uniformBest.getModelType().equals("beta"),
            "Uniform data should select Uniform or Beta model, got: " + uniformBest.getModelType());

        // Test gamma data selects Gamma
        float[] gammaData = generateGammaData(rng, 3.0, 2.0, SAMPLE_SIZE);
        ScalarModel gammaBest = fullSelector.selectBest(gammaData);
        assertEquals("gamma", gammaBest.getModelType(),
            "Gamma data should select Gamma model, got: " + gammaBest.getModelType());

        System.out.println("Distribution selection test passed:");
        System.out.println("  Normal data -> " + normalBest.getModelType());
        System.out.println("  Uniform data -> " + uniformBest.getModelType());
        System.out.println("  Gamma data -> " + gammaBest.getModelType());
    }

    @Test
    void testGoodnessOfFitScalesAreComparable() {
        Random rng = new Random(SEED);

        // Generate normal data and fit with all fitters
        float[] normalData = new float[SAMPLE_SIZE];
        for (int i = 0; i < SAMPLE_SIZE; i++) {
            normalData[i] = (float) rng.nextGaussian();
        }

        BestFitSelector selector = BestFitSelector.fullPearsonSelector();
        List<ComponentModelFitter.FitResult> allFits = selector.fitAll(normalData);

        System.out.println("\nGoodness-of-fit scores for Normal data:");
        double normalScore = 0, uniformScore = 0;

        for (ComponentModelFitter.FitResult fit : allFits) {
            System.out.printf("  %-12s: %.4f%n", fit.modelType(), fit.goodnessOfFit());
            if (fit.modelType().equals("normal")) {
                normalScore = fit.goodnessOfFit();
            } else if (fit.modelType().equals("uniform")) {
                uniformScore = fit.goodnessOfFit();
            }
        }

        // Normal fit should be better (lower) than uniform fit for normal data
        assertTrue(normalScore < uniformScore,
            "Normal score (" + normalScore + ") should be less than uniform score (" + uniformScore + ")");

        // Generate uniform data and compare
        float[] uniformData = new float[SAMPLE_SIZE];
        for (int i = 0; i < SAMPLE_SIZE; i++) {
            uniformData[i] = rng.nextFloat();
        }

        allFits = selector.fitAll(uniformData);

        System.out.println("\nGoodness-of-fit scores for Uniform data:");
        for (ComponentModelFitter.FitResult fit : allFits) {
            System.out.printf("  %-12s: %.4f%n", fit.modelType(), fit.goodnessOfFit());
            if (fit.modelType().equals("normal")) {
                normalScore = fit.goodnessOfFit();
            } else if (fit.modelType().equals("uniform")) {
                uniformScore = fit.goodnessOfFit();
            }
        }

        // Uniform fit should be better (lower) than normal fit for uniform data
        assertTrue(uniformScore < normalScore,
            "Uniform score (" + uniformScore + ") should be less than normal score (" + normalScore + ")");
    }

    @Test
    void testKSStatisticRangeForGoodFits() {
        Random rng = new Random(SEED);

        // For a perfect K-S fit, the statistic scaled by sqrt(n) should be < 1.36 (p > 0.05)
        // See: https://en.wikipedia.org/wiki/Kolmogorov-Smirnov_test
        double criticalValue = 1.36;

        // Uniform should have good K-S score
        float[] uniformData = new float[SAMPLE_SIZE];
        for (int i = 0; i < SAMPLE_SIZE; i++) {
            uniformData[i] = rng.nextFloat();
        }

        UniformModelFitter uniformFitter = new UniformModelFitter();
        ComponentModelFitter.FitResult uniformResult = uniformFitter.fit(uniformData);

        System.out.printf("K-S statistic for true uniform: %.4f (critical at p=0.05: %.2f)%n",
            uniformResult.goodnessOfFit(), criticalValue);

        // Most fits should be below 2x critical value
        assertTrue(uniformResult.goodnessOfFit() < 2 * criticalValue,
            "K-S score should be in expected range for good fit");
    }

    @Test
    void testPearsonIVFitterHasComparableScore() {
        Random rng = new Random(SEED);

        // Generate skewed data suitable for Pearson IV
        float[] skewedData = new float[SAMPLE_SIZE];
        for (int i = 0; i < SAMPLE_SIZE; i++) {
            // Create skewed data using log-normal like transformation
            double z = rng.nextGaussian();
            skewedData[i] = (float) (Math.exp(0.5 * z) - 1);
        }

        PearsonIVModelFitter fitter = new PearsonIVModelFitter();
        ComponentModelFitter.FitResult result = fitter.fit(skewedData);

        System.out.printf("PearsonIV score for skewed data: %.4f%n", result.goodnessOfFit());

        // Pearson IV uses moment-based scoring - verify it's finite and reasonable
        // Penalty score of 100.0 is used for data not in Type IV region
        // Good fits should be in the 0.1-50 range
        assertTrue(Double.isFinite(result.goodnessOfFit()),
            "PearsonIV score should be finite");
        assertTrue(result.goodnessOfFit() <= 100.0,
            "PearsonIV score should be <= 100 (the penalty score), got: " + result.goodnessOfFit());
    }

    // Helper methods to generate test data

    private float[] generateBetaData(Random rng, double alpha, double beta, int n) {
        float[] data = new float[n];
        for (int i = 0; i < n; i++) {
            // Use gamma ratio method
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

    private double generateGamma(Random rng, double shape) {
        // Marsaglia and Tsang's method
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

            if (u < 1.0 - 0.0331 * (x * x) * (x * x)) {
                return d * v;
            }

            if (Math.log(u) < 0.5 * x * x + d * (1.0 - v + Math.log(v))) {
                return d * v;
            }
        }
    }

    private float[] generateStudentTData(Random rng, double df, double location, double scale, int n) {
        float[] data = new float[n];
        for (int i = 0; i < n; i++) {
            // t = normal / sqrt(chi-squared/df)
            double z = rng.nextGaussian();
            double chi2 = generateGamma(rng, df / 2) * 2;
            double t = z / Math.sqrt(chi2 / df);
            data[i] = (float) (t * scale + location);
        }
        return data;
    }
}
