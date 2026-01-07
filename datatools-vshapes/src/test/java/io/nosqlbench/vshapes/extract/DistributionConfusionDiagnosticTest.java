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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Diagnostic tests to identify distribution confusion issues.
 *
 * <p>This test investigates cases where the BestFitSelector may
 * incorrectly classify one distribution as another.
 */
@Tag("accuracy")
public class DistributionConfusionDiagnosticTest {

    private static final int SAMPLE_SIZE = 100_000;
    private static final long SEED = 42L;

    @Test
    void diagnoseBetaPrimeVsInverseGammaConfusion() {
        Random rng = new Random(SEED);

        // Generate BetaPrime(3, 5) data
        float[] data = generateBetaPrimeData(rng, 3.0, 5.0, SAMPLE_SIZE);

        // Compute statistics
        DimensionStatistics stats = DimensionStatistics.compute(0, data);
        System.out.println("=== BetaPrime(3,5) Data Statistics ===");
        System.out.printf("Mean: %.4f (expected: %.4f)%n", stats.mean(), 3.0 / (5.0 - 1));
        System.out.printf("StdDev: %.4f%n", stats.stdDev());
        System.out.printf("Skewness: %.4f%n", stats.skewness());
        System.out.printf("Kurtosis: %.4f%n", stats.kurtosis());
        System.out.printf("Min: %.4f, Max: %.4f%n", stats.min(), stats.max());

        // Fit with each relevant fitter
        System.out.println("\n=== Goodness-of-Fit Scores ===");

        BetaPrimeModelFitter bpFitter = new BetaPrimeModelFitter();
        ComponentModelFitter.FitResult bpResult = bpFitter.fit(data);
        BetaPrimeScalarModel bpModel = (BetaPrimeScalarModel) bpResult.model();
        System.out.printf("BetaPrime:    alpha=%.3f (true=3.0), beta=%.3f (true=5.0), gof=%.4f%n",
            bpModel.getAlpha(), bpModel.getBeta(), bpResult.goodnessOfFit());

        InverseGammaModelFitter igFitter = new InverseGammaModelFitter();
        ComponentModelFitter.FitResult igResult = igFitter.fit(data);
        InverseGammaScalarModel igModel = (InverseGammaScalarModel) igResult.model();
        System.out.printf("InverseGamma: shape=%.3f, scale=%.3f, gof=%.4f%n",
            igModel.getShape(), igModel.getScale(), igResult.goodnessOfFit());

        GammaModelFitter gFitter = new GammaModelFitter(true);
        ComponentModelFitter.FitResult gResult = gFitter.fit(data);
        GammaScalarModel gModel = (GammaScalarModel) gResult.model();
        System.out.printf("Gamma:        shape=%.3f, scale=%.3f, gof=%.4f%n",
            gModel.getShape(), gModel.getScale(), gResult.goodnessOfFit());

        // Best fit selection
        System.out.println("\n=== Best Fit Selection ===");
        BestFitSelector selector = BestFitSelector.fullPearsonSelector();
        List<ComponentModelFitter.FitResult> allFits = selector.fitAll(data);

        System.out.println("All fits sorted by goodness-of-fit:");
        allFits.stream()
            .sorted((a, b) -> Double.compare(a.goodnessOfFit(), b.goodnessOfFit()))
            .forEach(fit -> System.out.printf("  %-15s: %.4f%n", fit.modelType(), fit.goodnessOfFit()));

        ScalarModel best = selector.selectBest(data);
        System.out.printf("\nSelected: %s%n", best.getModelType());

        // Analysis
        System.out.println("\n=== Analysis ===");
        if (!best.getModelType().equals("beta_prime")) {
            System.out.println("ISSUE DETECTED: BetaPrime data is being classified as " + best.getModelType());
            System.out.printf("BetaPrime GOF: %.4f, %s GOF: %.4f%n",
                bpResult.goodnessOfFit(), best.getModelType(),
                allFits.stream().filter(f -> f.modelType().equals(best.getModelType())).findFirst().get().goodnessOfFit());

            if (bpResult.goodnessOfFit() > igResult.goodnessOfFit()) {
                System.out.println("Root cause: BetaPrime fitter has higher GOF score than InverseGamma");
                System.out.println("Recommendation: Review BetaPrime GOF calculation");
            }
        } else {
            System.out.println("BetaPrime correctly identified");
        }
    }

    @Test
    void diagnoseInverseGammaVsBetaPrimeConfusion() {
        Random rng = new Random(SEED);

        // Generate InverseGamma(4, 3) data
        float[] data = generateInverseGammaData(rng, 4.0, 3.0, SAMPLE_SIZE);

        DimensionStatistics stats = DimensionStatistics.compute(0, data);
        System.out.println("=== InverseGamma(4,3) Data Statistics ===");
        System.out.printf("Mean: %.4f (expected: %.4f)%n", stats.mean(), 3.0 / (4.0 - 1));
        System.out.printf("Skewness: %.4f%n", stats.skewness());
        System.out.printf("Kurtosis: %.4f%n", stats.kurtosis());

        System.out.println("\n=== Goodness-of-Fit Scores ===");

        InverseGammaModelFitter igFitter = new InverseGammaModelFitter();
        ComponentModelFitter.FitResult igResult = igFitter.fit(data);
        System.out.printf("InverseGamma: gof=%.4f%n", igResult.goodnessOfFit());

        BetaPrimeModelFitter bpFitter = new BetaPrimeModelFitter();
        ComponentModelFitter.FitResult bpResult = bpFitter.fit(data);
        System.out.printf("BetaPrime:    gof=%.4f%n", bpResult.goodnessOfFit());

        BestFitSelector selector = BestFitSelector.fullPearsonSelector();
        ScalarModel best = selector.selectBest(data);
        System.out.printf("\nSelected: %s%n", best.getModelType());

        assertEquals("inverse_gamma", best.getModelType(),
            "InverseGamma data should be classified as inverse_gamma");
    }

    @Test
    void analyzeGOFScoreDistribution() {
        Random rng = new Random(SEED);
        BestFitSelector selector = BestFitSelector.fullPearsonSelector();

        System.out.println("=== GOF Score Analysis Across Distribution Types ===\n");

        // Test each distribution type
        String[] types = {"Normal", "Uniform", "Beta", "Gamma", "StudentT", "InverseGamma", "BetaPrime"};
        float[][] testData = {
            generateNormalData(rng, 0, 1, SAMPLE_SIZE),
            generateUniformData(rng, 0, 1, SAMPLE_SIZE),
            generateBetaData(rng, 2, 5, SAMPLE_SIZE),
            generateGammaData(rng, 3, 2, SAMPLE_SIZE),
            generateStudentTData(rng, 8, SAMPLE_SIZE),
            generateInverseGammaData(rng, 4, 3, SAMPLE_SIZE),
            generateBetaPrimeData(rng, 3, 5, SAMPLE_SIZE)
        };

        System.out.println("Distribution     | Best Fit        | GOF Score | Correct?");
        System.out.println("-----------------+-----------------+-----------+---------");

        int correct = 0;
        for (int i = 0; i < types.length; i++) {
            ScalarModel best = selector.selectBest(testData[i]);
            List<ComponentModelFitter.FitResult> fits = selector.fitAll(testData[i]);
            double bestGof = fits.stream()
                .filter(f -> f.modelType().equals(best.getModelType()))
                .findFirst().get().goodnessOfFit();

            String expected = types[i].toLowerCase().replace("studentt", "student_t")
                .replace("inversegamma", "inverse_gamma")
                .replace("betaprime", "beta_prime");

            boolean isCorrect = best.getModelType().equals(expected) ||
                (expected.equals("uniform") && best.getModelType().equals("beta")); // Beta(1,1) = Uniform

            if (isCorrect) correct++;

            System.out.printf("%-16s | %-15s | %9.4f | %s%n",
                types[i], best.getModelType(), bestGof, isCorrect ? "YES" : "NO **");
        }

        System.out.printf("\nAccuracy: %d/%d (%.1f%%)%n", correct, types.length, 100.0 * correct / types.length);
    }

    // Data generation helpers

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
