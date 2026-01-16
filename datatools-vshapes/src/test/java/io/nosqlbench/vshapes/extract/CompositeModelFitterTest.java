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

import io.nosqlbench.vshapes.extract.ComponentModelFitter.FitResult;
import io.nosqlbench.vshapes.extract.CompositeModelFitter.CdfValidationResult;
import io.nosqlbench.vshapes.model.CompositeScalarModel;
import io.nosqlbench.vshapes.model.NormalScalarModel;
import io.nosqlbench.vshapes.model.ScalarModel;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link CompositeModelFitter}.
 *
 * <p>This test class verifies:
 * <ol>
 *   <li>Correct composite model fitting for bimodal and trimodal data</li>
 *   <li>Numerical accuracy of weight estimation and CDF validation</li>
 *   <li>Goodness-of-fit scores are in expected ranges</li>
 *   <li>Integration with BestFitSelector</li>
 * </ol>
 */
@Tag("unit")
public class CompositeModelFitterTest {

    private static final long SEED = 42L;
    private static final int SAMPLE_SIZE = 10000;

    // Numerical accuracy tolerances
    private static final double WEIGHT_SUM_TOLERANCE = 0.001;      // Weights must sum to 1.0 ±0.001
    private static final double WEIGHT_ACCURACY_TOLERANCE = 0.15;  // ±15% for weight estimation
    private static final double CDF_MAX_DEVIATION_THRESHOLD = 0.05; // Default CDF threshold
    private static final double CDF_AVG_DEVIATION_THRESHOLD = 0.02; // Average CDF deviation threshold

    // ==================== Basic Fitting Tests ====================

    @Test
    void fitsBimodalData() {
        float[] data = generateBimodal(SAMPLE_SIZE, -2.0, 2.0, 0.5);

        CompositeModelFitter fitter = new CompositeModelFitter();
        FitResult result = fitter.fit(data);

        assertNotNull(result.model(), "Model should not be null");
        assertEquals("composite", result.modelType(), "Model type should be composite");
        assertTrue(result.model() instanceof CompositeScalarModel,
            "Model should be CompositeScalarModel");

        CompositeScalarModel composite = (CompositeScalarModel) result.model();
        assertEquals(2, composite.getComponentCount(), "Should have 2 components");
    }

    @Test
    void fitsTrimodalWithMaxComponents3() {
        float[] data = generateTrimodal(15000, -3.0, 0.0, 3.0, 0.3);

        CompositeModelFitter fitter = new CompositeModelFitter(
            BestFitSelector.boundedDataSelector(), 3);
        FitResult result = fitter.fit(data);

        CompositeScalarModel composite = (CompositeScalarModel) result.model();
        assertEquals(3, composite.getComponentCount(), "Should have 3 components");
    }

    @Test
    void rejectsUnimodalData() {
        float[] data = generateNormal(SAMPLE_SIZE, 0.0, 1.0);

        CompositeModelFitter fitter = new CompositeModelFitter();

        assertThrows(IllegalStateException.class, () -> fitter.fit(data),
            "Should throw for unimodal data");
    }

    // ==================== Numerical Accuracy Tests ====================

    @Test
    @Tag("accuracy")
    void componentWeightsSumToOne() {
        float[] data = generateBimodal(SAMPLE_SIZE, -2.0, 2.0, 0.5);

        CompositeModelFitter fitter = new CompositeModelFitter();
        FitResult result = fitter.fit(data);

        CompositeScalarModel composite = (CompositeScalarModel) result.model();
        double[] weights = composite.getWeights();

        double sum = 0;
        for (double w : weights) {
            sum += w;
        }
        assertEquals(1.0, sum, WEIGHT_SUM_TOLERANCE,
            "Component weights must sum to 1.0, got: " + sum);
    }

    @Test
    @Tag("accuracy")
    void asymmetricBimodalWeightAccuracy() {
        // 70% near -1, 30% near 2
        Random rng = new Random(SEED);
        float[] data = new float[SAMPLE_SIZE];
        int split = 7000;

        for (int i = 0; i < split; i++) {
            data[i] = (float) (-1.0 + rng.nextGaussian() * 0.4);
        }
        for (int i = split; i < SAMPLE_SIZE; i++) {
            data[i] = (float) (2.0 + rng.nextGaussian() * 0.4);
        }

        CompositeModelFitter fitter = new CompositeModelFitter();
        FitResult result = fitter.fit(data);

        CompositeScalarModel composite = (CompositeScalarModel) result.model();
        double[] weights = composite.getWeights();

        double maxWeight = Math.max(weights[0], weights[1]);
        double minWeight = Math.min(weights[0], weights[1]);

        assertEquals(0.7, maxWeight, WEIGHT_ACCURACY_TOLERANCE,
            "Major mode weight should be ~0.7, got: " + maxWeight);
        assertEquals(0.3, minWeight, WEIGHT_ACCURACY_TOLERANCE,
            "Minor mode weight should be ~0.3, got: " + minWeight);

        System.out.printf("Composite weight accuracy: true=[0.7, 0.3], fitted=[%.3f, %.3f]%n",
            maxWeight, minWeight);
    }

    @Test
    @Tag("accuracy")
    void cdfValidationPasses() {
        float[] data = generateBimodal(SAMPLE_SIZE, -2.0, 2.0, 0.5);

        CompositeModelFitter fitter = new CompositeModelFitter();
        FitResult result = fitter.fit(data);

        CdfValidationResult validation = fitter.getLastValidationResult();
        assertNotNull(validation, "Validation result should not be null");
        assertTrue(validation.isValid(), "CDF validation should pass for clean bimodal data");
        assertTrue(validation.maxDeviation() <= CDF_MAX_DEVIATION_THRESHOLD,
            "Max CDF deviation should be <= " + CDF_MAX_DEVIATION_THRESHOLD +
            ", got: " + validation.maxDeviation());
    }

    @Test
    @Tag("accuracy")
    void cdfValidationMetricsInExpectedRange() {
        float[] data = generateBimodal(SAMPLE_SIZE, -2.0, 2.0, 0.5);

        CompositeModelFitter fitter = new CompositeModelFitter();
        fitter.fit(data);

        CdfValidationResult validation = fitter.getLastValidationResult();

        // Max deviation should be in [0, threshold]
        assertTrue(validation.maxDeviation() >= 0,
            "Max deviation should be non-negative");
        assertTrue(validation.maxDeviation() <= validation.threshold(),
            "Max deviation should be <= threshold for valid fit");

        // Average deviation should be less than max
        assertTrue(validation.avgDeviation() <= validation.maxDeviation(),
            "Avg deviation should be <= max deviation");
        assertTrue(validation.avgDeviation() >= 0,
            "Avg deviation should be non-negative");

        // Sample points should be reasonable
        assertTrue(validation.samplePoints() >= 10,
            "Should have at least 10 CDF sample points");

        System.out.printf("CDF validation metrics: max=%.4f, avg=%.4f, threshold=%.4f, points=%d%n",
            validation.maxDeviation(), validation.avgDeviation(),
            validation.threshold(), validation.samplePoints());
    }

    @Test
    @Tag("accuracy")
    void goodnessOfFitInExpectedRange() {
        float[] data = generateBimodal(SAMPLE_SIZE, -2.0, 2.0, 0.5);

        CompositeModelFitter fitter = new CompositeModelFitter();
        FitResult result = fitter.fit(data);

        // GoF should be finite, positive, and reasonable
        assertTrue(Double.isFinite(result.goodnessOfFit()),
            "Goodness-of-fit should be finite");
        assertTrue(result.goodnessOfFit() >= 0,
            "Goodness-of-fit should be non-negative");
        // BIC-based score normalized by sample size
        assertTrue(result.goodnessOfFit() < 100,
            "Normalized BIC score should be < 100 for good fit, got: " + result.goodnessOfFit());

        System.out.printf("Composite model GoF: %.4f%n", result.goodnessOfFit());
    }

    // ==================== Component Model Tests ====================

    @Test
    @Tag("accuracy")
    void componentModelsHaveReasonableParameters() {
        float[] data = generateBimodal(SAMPLE_SIZE, -2.0, 2.0, 0.5);

        CompositeModelFitter fitter = new CompositeModelFitter();
        FitResult result = fitter.fit(data);

        CompositeScalarModel composite = (CompositeScalarModel) result.model();
        ScalarModel[] components = composite.getScalarModels();

        for (int i = 0; i < components.length; i++) {
            assertNotNull(components[i], "Component " + i + " should not be null");
            assertNotNull(components[i].getModelType(),
                "Component " + i + " should have model type");

            // If component is Normal, check parameters are reasonable
            if (components[i] instanceof NormalScalarModel normal) {
                assertTrue(Double.isFinite(normal.getMean()),
                    "Normal component mean should be finite");
                assertTrue(normal.getStdDev() > 0,
                    "Normal component stdDev should be positive");
                assertTrue(normal.getStdDev() < 5,
                    "Normal component stdDev should be reasonable (< 5), got: " + normal.getStdDev());
            }
        }
    }

    // ==================== Integration Tests ====================

    @Test
    @Tag("integration")
    void integrationWithBestFitSelector() {
        float[] bimodalData = generateBimodal(SAMPLE_SIZE, -2.0, 2.0, 0.5);

        BestFitSelector selector = BestFitSelector.multimodalAwareSelector();
        FitResult result = selector.selectBestResult(bimodalData);

        assertNotNull(result.model(), "Should select a model");
        // For clear bimodal data, composite should often be selected
        // (depends on relative BIC scores)
    }

    @Test
    @Tag("integration")
    void bimodalFitsWithDifferentSelectors() {
        float[] data = generateBimodal(SAMPLE_SIZE, -2.0, 2.0, 0.5);

        BestFitSelector[] selectors = {
            BestFitSelector.boundedDataSelector(),
            BestFitSelector.parametricOnly(),
            BestFitSelector.pearsonSelector()
        };

        for (BestFitSelector selector : selectors) {
            CompositeModelFitter fitter = new CompositeModelFitter(selector);
            FitResult result = fitter.fit(data);

            assertNotNull(result.model(), "Should fit with " + selector);
            assertEquals(2, ((CompositeScalarModel) result.model()).getComponentCount());
        }
    }

    // ==================== Edge Cases and Validation ====================

    @Test
    void cdfValidationResultHasFormatSummary() {
        float[] data = generateBimodal(SAMPLE_SIZE, -2.0, 2.0, 0.5);

        CompositeModelFitter fitter = new CompositeModelFitter();
        fitter.fit(data);

        CdfValidationResult validation = fitter.getLastValidationResult();
        String summary = validation.formatSummary();

        assertNotNull(summary, "Summary should not be null");
        assertTrue(summary.contains("CDF Validation"), "Summary should mention CDF Validation");
        assertTrue(summary.contains("PASSED"), "Summary should indicate PASSED for valid fit");
    }

    @Test
    void highCdfDeviationCausesRejection() {
        float[] data = generateBimodal(1000, -2.0, 2.0, 0.5);

        // Use an extremely strict threshold that's unlikely to pass
        CompositeModelFitter fitter = new CompositeModelFitter(
            BestFitSelector.boundedDataSelector(), 3, 0.001);

        assertThrows(IllegalStateException.class, () -> fitter.fit(data),
            "Should reject fit with strict CDF threshold");
    }

    @Test
    void handlesSmallButSufficientData() {
        // Minimum viable bimodal data
        float[] data = generateBimodal(500, -3.0, 3.0, 0.3);

        CompositeModelFitter fitter = new CompositeModelFitter();

        // May or may not fit depending on mode detection
        try {
            FitResult result = fitter.fit(data);
            assertNotNull(result.model());
        } catch (IllegalStateException e) {
            // Expected if not detected as multimodal
            assertTrue(e.getMessage().contains("not multimodal") ||
                       e.getMessage().contains("insufficient"),
                "Exception should be about multimodality or data");
        }
    }

    // ==================== Helper Methods ====================

    private float[] generateNormal(int n, double mean, double stdDev) {
        Random rng = new Random(SEED);
        float[] data = new float[n];
        for (int i = 0; i < n; i++) {
            data[i] = (float) (mean + rng.nextGaussian() * stdDev);
        }
        return data;
    }

    private float[] generateBimodal(int n, double mean1, double mean2, double stdDev) {
        Random rng = new Random(SEED);
        float[] data = new float[n];
        for (int i = 0; i < n; i++) {
            double mean = rng.nextBoolean() ? mean1 : mean2;
            data[i] = (float) (mean + rng.nextGaussian() * stdDev);
        }
        return data;
    }

    private float[] generateTrimodal(int n, double mean1, double mean2, double mean3, double stdDev) {
        Random rng = new Random(SEED);
        float[] data = new float[n];
        for (int i = 0; i < n; i++) {
            int mode = rng.nextInt(3);
            double mean = mode == 0 ? mean1 : (mode == 1 ? mean2 : mean3);
            data[i] = (float) (mean + rng.nextGaussian() * stdDev);
        }
        return data;
    }
}
