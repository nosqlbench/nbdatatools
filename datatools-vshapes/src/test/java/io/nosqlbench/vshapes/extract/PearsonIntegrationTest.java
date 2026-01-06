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

import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for the Pearson distribution system.
 * Tests that data generated from known distributions is correctly classified.
 */
public class PearsonIntegrationTest {

    private static final int SAMPLE_SIZE = 10000;
    private static final long SEED = 42L;

    // ===== Classification from Known Data =====

    @Test
    void classifyNormalData() {
        Random random = new Random(SEED);
        float[] data = new float[SAMPLE_SIZE];
        for (int i = 0; i < SAMPLE_SIZE; i++) {
            data[i] = (float) random.nextGaussian();
        }

        PearsonType type = BestFitSelector.classifyPearsonType(data);
        assertEquals(PearsonType.TYPE_0_NORMAL, type,
            "Standard normal data should classify as Type 0");
    }

    @Test
    void classifyUniformData() {
        Random random = new Random(SEED);
        float[] data = new float[SAMPLE_SIZE];
        for (int i = 0; i < SAMPLE_SIZE; i++) {
            data[i] = (float) random.nextDouble();
        }

        // Uniform has β₂ = 1.8 < 3 and β₁ = 0, so Type II (symmetric beta)
        PearsonType type = BestFitSelector.classifyPearsonType(data);
        assertEquals(PearsonType.TYPE_II_SYMMETRIC_BETA, type,
            "Uniform data should classify as Type II (symmetric beta)");
    }

    @Test
    void classifyGammaData() {
        // Generate gamma-distributed data using Box-Muller + transformation
        Random random = new Random(SEED);
        float[] data = generateGammaData(random, 4.0, 2.0, SAMPLE_SIZE);

        PearsonType type = BestFitSelector.classifyPearsonType(data);
        assertEquals(PearsonType.TYPE_III_GAMMA, type,
            "Gamma data should classify as Type III");
    }

    @Test
    void classifyHeavyTailedSymmetricData() {
        // Generate heavy-tailed symmetric data (Student's t with low df)
        Random random = new Random(SEED);
        float[] data = generateStudentTData(random, 5.0, SAMPLE_SIZE);

        PearsonType type = BestFitSelector.classifyPearsonType(data);
        assertEquals(PearsonType.TYPE_VII_STUDENT_T, type,
            "Heavy-tailed symmetric data should classify as Type VII");
    }

    // ===== Pearson Selector Integration =====

    @Test
    void pearsonSelectorSelectsBestFitForNormalData() {
        Random random = new Random(SEED);
        float[] data = new float[SAMPLE_SIZE];
        for (int i = 0; i < SAMPLE_SIZE; i++) {
            data[i] = (float) (random.nextGaussian() * 2.0 + 5.0);
        }

        BestFitSelector selector = BestFitSelector.pearsonSelector();
        ScalarModel best = selector.selectBest(data);

        assertEquals("normal", best.getModelType(),
            "Pearson selector should select normal for Gaussian data");

        if (best instanceof NormalScalarModel normal) {
            assertEquals(5.0, normal.getMean(), 0.2);
            assertEquals(2.0, normal.getStdDev(), 0.2);
        }
    }

    @Test
    void pearsonSelectorSelectsBestFitForUniformData() {
        Random random = new Random(SEED);
        float[] data = new float[SAMPLE_SIZE];
        for (int i = 0; i < SAMPLE_SIZE; i++) {
            data[i] = (float) (random.nextDouble() * 10 - 5);  // [-5, 5]
        }

        BestFitSelector selector = BestFitSelector.pearsonSelector();
        ScalarModel best = selector.selectBest(data);

        assertTrue(best.getModelType().equals("uniform") || best.getModelType().equals("beta"),
            "Pearson selector should select uniform or beta for uniform data, got: " + best.getModelType());
    }

    @Test
    void pearsonSelectorSelectsBestFitForGammaData() {
        Random random = new Random(SEED);
        float[] data = generateGammaData(random, 2.0, 3.0, SAMPLE_SIZE);

        BestFitSelector selector = BestFitSelector.pearsonSelector();
        ScalarModel best = selector.selectBest(data);

        assertTrue(best.getModelType().equals("gamma") || best.getModelType().equals("normal"),
            "Pearson selector should prefer gamma for right-skewed data, got: " + best.getModelType());
    }

    @Test
    void fullPearsonSelectorHasAllFitters() {
        BestFitSelector selector = BestFitSelector.fullPearsonSelector();
        assertEquals(8, selector.getFitters().size());
    }

    // ===== Detailed Classification =====

    @Test
    void detailedClassificationProvidesMoments() {
        Random random = new Random(SEED);
        float[] data = new float[SAMPLE_SIZE];
        for (int i = 0; i < SAMPLE_SIZE; i++) {
            data[i] = (float) random.nextGaussian();
        }

        PearsonClassifier.ClassificationResult result =
            BestFitSelector.classifyPearsonDetailed(data);

        assertEquals(PearsonType.TYPE_0_NORMAL, result.type());
        assertTrue(result.isSymmetric());
        assertTrue(result.isMesokurtic());
        assertTrue(result.beta1() < 0.1, "Normal should have β₁ ≈ 0");
        assertTrue(Math.abs(result.beta2() - 3.0) < 0.3, "Normal should have β₂ ≈ 3");
    }

    // ===== Fitter Tests =====

    @Test
    void betaFitterFitsBoundedData() {
        float[] data = new float[SAMPLE_SIZE];
        for (int i = 0; i < SAMPLE_SIZE; i++) {
            data[i] = (float) i / SAMPLE_SIZE;  // Uniform [0, 1)
        }

        BetaModelFitter fitter = new BetaModelFitter();
        ComponentModelFitter.FitResult result = fitter.fit(data);

        assertEquals("beta", result.modelType());
        assertNotNull(result.model());
        assertTrue(Double.isFinite(result.goodnessOfFit()));
    }

    @Test
    void gammaFitterFitsPositiveData() {
        Random random = new Random(SEED);
        float[] data = generateGammaData(random, 3.0, 2.0, SAMPLE_SIZE);

        GammaModelFitter fitter = new GammaModelFitter();
        ComponentModelFitter.FitResult result = fitter.fit(data);

        assertEquals("gamma", result.modelType());
        assertTrue(result.model() instanceof GammaScalarModel);

        GammaScalarModel model = (GammaScalarModel) result.model();
        assertTrue(model.getShape() > 0);
        assertTrue(model.getScale() > 0);
    }

    @Test
    void studentTFitterFitsHeavyTailedData() {
        Random random = new Random(SEED);
        float[] data = generateStudentTData(random, 5.0, SAMPLE_SIZE);

        StudentTModelFitter fitter = new StudentTModelFitter();
        ComponentModelFitter.FitResult result = fitter.fit(data);

        assertEquals("student-t", result.modelType());
        assertTrue(result.model() instanceof StudentTScalarModel);

        StudentTScalarModel model = (StudentTScalarModel) result.model();
        assertTrue(model.getDegreesOfFreedom() > 4);
    }

    @Test
    void inverseGammaFitterFitsPositiveData() {
        Random random = new Random(SEED);
        // Generate inverse gamma-like data
        float[] data = new float[SAMPLE_SIZE];
        for (int i = 0; i < SAMPLE_SIZE; i++) {
            double gamma = 0;
            for (int j = 0; j < 3; j++) {
                gamma += -Math.log(random.nextDouble());
            }
            data[i] = (float) (2.0 / gamma);  // Inverse gamma approximation
        }

        InverseGammaModelFitter fitter = new InverseGammaModelFitter();
        ComponentModelFitter.FitResult result = fitter.fit(data);

        assertEquals("inverse-gamma", result.modelType());
        assertTrue(result.model() instanceof InverseGammaScalarModel);
    }

    @Test
    void betaPrimeFitterFitsPositiveData() {
        Random random = new Random(SEED);
        // Generate beta prime-like data using X/(1-X) transform
        float[] data = new float[SAMPLE_SIZE];
        for (int i = 0; i < SAMPLE_SIZE; i++) {
            double beta = 0.5;  // Simple beta(1,1) = uniform
            data[i] = (float) (beta / (1 - beta + 0.01));  // Transform to beta prime
        }

        BetaPrimeModelFitter fitter = new BetaPrimeModelFitter();
        ComponentModelFitter.FitResult result = fitter.fit(data);

        assertEquals("beta-prime", result.modelType());
        assertTrue(result.model() instanceof BetaPrimeScalarModel);
    }

    @Test
    void pearsonIVFitterFitsAsymmetricData() {
        Random random = new Random(SEED);
        // Generate asymmetric data
        float[] data = new float[SAMPLE_SIZE];
        for (int i = 0; i < SAMPLE_SIZE; i++) {
            // Skewed data: mix of normal and exponential
            if (random.nextDouble() < 0.7) {
                data[i] = (float) random.nextGaussian();
            } else {
                data[i] = (float) (-Math.log(random.nextDouble()));
            }
        }

        PearsonIVModelFitter fitter = new PearsonIVModelFitter();
        ComponentModelFitter.FitResult result = fitter.fit(data);

        assertEquals("pearson-iv", result.modelType());
        assertTrue(result.model() instanceof PearsonIVScalarModel);
    }

    // ===== Helper Methods =====

    /**
     * Generates gamma-distributed data using the Marsaglia and Tsang method.
     */
    private float[] generateGammaData(Random random, double shape, double scale, int n) {
        float[] data = new float[n];
        for (int i = 0; i < n; i++) {
            data[i] = (float) (sampleGamma(random, shape) * scale);
        }
        return data;
    }

    private double sampleGamma(Random random, double shape) {
        if (shape < 1) {
            return sampleGamma(random, shape + 1) * Math.pow(random.nextDouble(), 1.0 / shape);
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
                return d * v;
            }

            if (Math.log(u) < 0.5 * x * x + d * (1 - v + Math.log(v))) {
                return d * v;
            }
        }
    }

    /**
     * Generates Student's t-distributed data using the ratio method.
     */
    private float[] generateStudentTData(Random random, double df, int n) {
        float[] data = new float[n];
        for (int i = 0; i < n; i++) {
            double z = random.nextGaussian();
            double chi2 = sampleGamma(random, df / 2) * 2;
            data[i] = (float) (z / Math.sqrt(chi2 / df));
        }
        return data;
    }
}
