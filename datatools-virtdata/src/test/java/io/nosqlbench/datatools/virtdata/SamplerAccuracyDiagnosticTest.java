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

import io.nosqlbench.datatools.virtdata.sampling.BetaPrimeSampler;
import io.nosqlbench.datatools.virtdata.sampling.ComponentSampler;
import io.nosqlbench.datatools.virtdata.sampling.ComponentSamplerFactory;
import io.nosqlbench.vshapes.extract.*;
import io.nosqlbench.vshapes.model.*;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;

/**
 * Diagnostic tests for sampler accuracy issues.
 *
 * <p>Investigates cases where sampler-generated data may be
 * misclassified by the model fitters.
 */
@Tag("accuracy")
public class SamplerAccuracyDiagnosticTest {

    private static final int SAMPLE_SIZE = 100_000;
    private static final long SEED = 42L;

    @Test
    void diagnoseBetaPrimeSamplerVsRandomGeneration() {
        Random rng = new Random(SEED);

        System.out.println("=== BetaPrime: Sampler vs Random Generation ===\n");

        // Generate using random gamma ratio (ground truth)
        float[] randomGenerated = generateBetaPrimeData(rng, 3.0, 5.0, SAMPLE_SIZE);

        // Generate using the BetaPrime sampler with stratified sampling
        BetaPrimeScalarModel model = new BetaPrimeScalarModel(3.0, 5.0);
        BetaPrimeSampler sampler = new BetaPrimeSampler(model);

        float[] samplerGenerated = new float[SAMPLE_SIZE];
        for (int i = 0; i < SAMPLE_SIZE; i++) {
            double u = (i + 0.5) / SAMPLE_SIZE;  // Stratified sampling
            samplerGenerated[i] = (float) sampler.sample(u);
        }

        // Compare statistics
        DimensionStatistics randomStats = DimensionStatistics.compute(0, randomGenerated);
        DimensionStatistics samplerStats = DimensionStatistics.compute(0, samplerGenerated);

        System.out.println("Statistics Comparison:");
        System.out.printf("               Random      Sampler     Expected%n");
        System.out.printf("Mean:          %.4f      %.4f      %.4f%n",
            randomStats.mean(), samplerStats.mean(), 3.0 / (5.0 - 1));
        System.out.printf("StdDev:        %.4f      %.4f%n",
            randomStats.stdDev(), samplerStats.stdDev());
        System.out.printf("Skewness:      %.4f      %.4f%n",
            randomStats.skewness(), samplerStats.skewness());
        System.out.printf("Kurtosis:      %.4f      %.4f%n",
            randomStats.kurtosis(), samplerStats.kurtosis());
        System.out.printf("Min:           %.4f      %.4f%n",
            randomStats.min(), samplerStats.min());
        System.out.printf("Max:           %.4f      %.4f%n",
            randomStats.max(), samplerStats.max());

        // Fit each with all fitters
        System.out.println("\nGoodness-of-Fit Scores:");
        System.out.println("Model          Random      Sampler");

        BestFitSelector selector = BestFitSelector.fullPearsonSelector();
        List<ComponentModelFitter.FitResult> randomFits = selector.fitAll(randomGenerated);
        List<ComponentModelFitter.FitResult> samplerFits = selector.fitAll(samplerGenerated);

        for (ComponentModelFitter.FitResult rf : randomFits) {
            ComponentModelFitter.FitResult sf = samplerFits.stream()
                .filter(f -> f.modelType().equals(rf.modelType()))
                .findFirst().orElse(rf);
            System.out.printf("%-14s %9.4f   %9.4f%n",
                rf.modelType(), rf.goodnessOfFit(), sf.goodnessOfFit());
        }

        ScalarModel randomBest = selector.selectBest(randomGenerated);
        ScalarModel samplerBest = selector.selectBest(samplerGenerated);

        System.out.println("\nBest fit selection:");
        System.out.printf("Random:  %s%n", randomBest.getModelType());
        System.out.printf("Sampler: %s%n", samplerBest.getModelType());

        if (!samplerBest.getModelType().equals("beta_prime")) {
            System.out.println("\nISSUE DETECTED: Sampler-generated BetaPrime data is being misclassified!");
            System.out.println("This suggests the sampler produces a subtly different distribution.");

            // Investigate the difference
            System.out.println("\n=== Root Cause Analysis ===");
            System.out.printf("Skewness difference: %.4f (random) vs %.4f (sampler)%n",
                randomStats.skewness(), samplerStats.skewness());
            System.out.printf("Kurtosis difference: %.4f (random) vs %.4f (sampler)%n",
                randomStats.kurtosis(), samplerStats.kurtosis());

            // K-S test between random and sampler
            StatisticalTestSuite.TestResult ks =
                StatisticalTestSuite.kolmogorovSmirnovTest(randomGenerated, samplerGenerated);
            System.out.printf("K-S between random and sampler: %.4f (critical: %.4f)%n",
                ks.statistic(), ks.criticalValue());
        }
    }

    @Test
    void diagnoseAllSamplersVsRandomGeneration() {
        Random rng = new Random(SEED);
        BestFitSelector selector = BestFitSelector.fullPearsonSelector();

        System.out.println("=== All Samplers: Distribution Classification Accuracy ===\n");
        System.out.println("Distribution    Random Best     Sampler Best    Match?");
        System.out.println("-" .repeat(60));

        Object[][] configs = {
            {"normal", new NormalScalarModel(0.0, 1.0)},
            {"uniform", new UniformScalarModel(0.0, 1.0)},
            {"beta", new BetaScalarModel(2.0, 5.0)},
            {"gamma", new GammaScalarModel(3.0, 2.0)},
            {"student_t", new StudentTScalarModel(8.0)},
            {"inverse_gamma", new InverseGammaScalarModel(4.0, 3.0)},
            {"beta_prime", new BetaPrimeScalarModel(3.0, 5.0)}
        };

        int matches = 0;
        for (Object[] config : configs) {
            String typeName = (String) config[0];
            ScalarModel model = (ScalarModel) config[1];

            // Generate using sampler (stratified)
            ComponentSampler sampler = ComponentSamplerFactory.forModel(model);
            float[] samplerData = new float[SAMPLE_SIZE];
            for (int i = 0; i < SAMPLE_SIZE; i++) {
                double u = (i + 0.5) / SAMPLE_SIZE;
                samplerData[i] = (float) sampler.sample(u);
            }

            // Generate using random sampling
            float[] randomData = generateRandomData(rng, typeName, SAMPLE_SIZE);

            ScalarModel randomBest = selector.selectBest(randomData);
            ScalarModel samplerBest = selector.selectBest(samplerData);

            boolean match = randomBest.getModelType().equals(samplerBest.getModelType());
            if (match) matches++;

            System.out.printf("%-15s %-15s %-15s %s%n",
                typeName, randomBest.getModelType(), samplerBest.getModelType(),
                match ? "YES" : "NO **");
        }

        System.out.printf("\nAccuracy: %d/%d (%.1f%%)%n", matches, configs.length,
            100.0 * matches / configs.length);
    }

    private float[] generateRandomData(Random rng, String type, int n) {
        return switch (type) {
            case "normal" -> {
                float[] data = new float[n];
                for (int i = 0; i < n; i++) data[i] = (float) rng.nextGaussian();
                yield data;
            }
            case "uniform" -> {
                float[] data = new float[n];
                for (int i = 0; i < n; i++) data[i] = rng.nextFloat();
                yield data;
            }
            case "beta" -> generateBetaData(rng, 2.0, 5.0, n);
            case "gamma" -> generateGammaData(rng, 3.0, 2.0, n);
            case "student_t" -> generateStudentTData(rng, 8.0, n);
            case "inverse_gamma" -> generateInverseGammaData(rng, 4.0, 3.0, n);
            case "beta_prime" -> generateBetaPrimeData(rng, 3.0, 5.0, n);
            default -> throw new IllegalArgumentException("Unknown type: " + type);
        };
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
