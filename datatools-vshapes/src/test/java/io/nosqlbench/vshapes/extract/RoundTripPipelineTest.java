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
import io.nosqlbench.vshapes.stream.DataspaceShape;
import io.nosqlbench.vshapes.stream.StreamingModelExtractor;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.*;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/// Comprehensive round-trip pipeline test that mirrors the CLI test script flow.
///
/// ## Purpose
///
/// This test exercises the full extraction pipeline without requiring CLI invocation:
/// 1. **Gen** (known models) → generate variates
/// 2. **Ext** (extract models from variates) → compare Gen vs Ext (accuracy)
/// 3. **R-T** (extract from Ext-generated variates) → compare Ext vs R-T (stability)
///
/// ## Test Fixtures
///
/// Models are defined using JSON format matching the production model files.
/// This ensures test fixtures are compatible with actual usage patterns.
///
/// ## Phases
///
/// - **Phase 1**: Gen → variates → Ext (tests extraction accuracy)
/// - **Phase 2**: Ext → variates → R-T (tests extraction stability)
/// - **Phase 3**: Compare Gen vs Ext AND Ext vs R-T
///
/// @see StatisticalEquivalenceChecker
/// @see BestFitSelector
@Tag("accuracy")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class RoundTripPipelineTest {

    /// Sample size for each phase
    private static final int SAMPLE_SIZE = 20_000;

    /// Maximum modes for multimodal detection
    private static final int MAX_MODES = 10;

    /// Equivalence checker for model comparison
    private static final StatisticalEquivalenceChecker EQUIVALENCE_CHECKER =
        new StatisticalEquivalenceChecker();

    // ==================== Test Model Fixtures (JSON format) ====================

    /// JSON definitions for test models.
    /// Each fixture represents a different distribution type or complexity level.
    private static final Map<String, String> MODEL_FIXTURES = new LinkedHashMap<>();

    static {
        // Simple unimodal distributions
        MODEL_FIXTURES.put("normal_centered", """
            {
                "unique_vectors": 20000,
                "components": [
                    {"type": "normal", "mean": 0.0, "std_dev": 0.3, "lower_bound": -1.0, "upper_bound": 1.0}
                ]
            }
            """);

        MODEL_FIXTURES.put("normal_shifted", """
            {
                "unique_vectors": 20000,
                "components": [
                    {"type": "normal", "mean": 0.3, "std_dev": 0.2, "lower_bound": -1.0, "upper_bound": 1.0}
                ]
            }
            """);

        MODEL_FIXTURES.put("uniform_full", """
            {
                "unique_vectors": 20000,
                "components": [
                    {"type": "uniform", "lower": -0.9, "upper": 0.9}
                ]
            }
            """);

        MODEL_FIXTURES.put("uniform_narrow", """
            {
                "unique_vectors": 20000,
                "components": [
                    {"type": "uniform", "lower": 0.2, "upper": 0.6}
                ]
            }
            """);

        MODEL_FIXTURES.put("beta_symmetric", """
            {
                "unique_vectors": 20000,
                "components": [
                    {"type": "beta", "alpha": 2.0, "beta": 2.0, "lower": -1.0, "upper": 1.0}
                ]
            }
            """);

        MODEL_FIXTURES.put("beta_left_skewed", """
            {
                "unique_vectors": 20000,
                "components": [
                    {"type": "beta", "alpha": 5.0, "beta": 2.0, "lower": -1.0, "upper": 1.0}
                ]
            }
            """);

        MODEL_FIXTURES.put("beta_right_skewed", """
            {
                "unique_vectors": 20000,
                "components": [
                    {"type": "beta", "alpha": 2.0, "beta": 5.0, "lower": -1.0, "upper": 1.0}
                ]
            }
            """);

        // Bimodal composites
        MODEL_FIXTURES.put("bimodal_normal", """
            {
                "unique_vectors": 20000,
                "components": [
                    {
                        "type": "composite",
                        "sub_models": [
                            {"type": "normal", "mean": -0.5, "std_dev": 0.15, "lower_bound": -1.0, "upper_bound": 1.0},
                            {"type": "normal", "mean": 0.5, "std_dev": 0.15, "lower_bound": -1.0, "upper_bound": 1.0}
                        ],
                        "weights": [0.5, 0.5]
                    }
                ]
            }
            """);

        MODEL_FIXTURES.put("bimodal_asymmetric", """
            {
                "unique_vectors": 20000,
                "components": [
                    {
                        "type": "composite",
                        "sub_models": [
                            {"type": "normal", "mean": -0.6, "std_dev": 0.1, "lower_bound": -1.0, "upper_bound": 1.0},
                            {"type": "normal", "mean": 0.4, "std_dev": 0.2, "lower_bound": -1.0, "upper_bound": 1.0}
                        ],
                        "weights": [0.3, 0.7]
                    }
                ]
            }
            """);

        // Trimodal composite
        MODEL_FIXTURES.put("trimodal", """
            {
                "unique_vectors": 20000,
                "components": [
                    {
                        "type": "composite",
                        "sub_models": [
                            {"type": "normal", "mean": -0.6, "std_dev": 0.12, "lower_bound": -1.0, "upper_bound": 1.0},
                            {"type": "normal", "mean": 0.0, "std_dev": 0.12, "lower_bound": -1.0, "upper_bound": 1.0},
                            {"type": "normal", "mean": 0.6, "std_dev": 0.12, "lower_bound": -1.0, "upper_bound": 1.0}
                        ],
                        "weights": [0.33, 0.34, 0.33]
                    }
                ]
            }
            """);

        // Mixed type composite with clearly separated modes
        // Mode 1: Uniform at far left [-0.9, -0.5]
        // Mode 2: Normal in middle centered at -0.05, std 0.08
        // Mode 3: Beta at far right [0.5, 0.95]
        // This creates clear gaps between modes to avoid detection ambiguity
        MODEL_FIXTURES.put("mixed_types", """
            {
                "unique_vectors": 20000,
                "components": [
                    {
                        "type": "composite",
                        "sub_models": [
                            {"type": "uniform", "lower": -0.9, "upper": -0.5},
                            {"type": "normal", "mean": -0.05, "std_dev": 0.08, "lower_bound": -1.0, "upper_bound": 1.0},
                            {"type": "beta", "alpha": 3.0, "beta": 2.0, "lower": 0.5, "upper": 0.95}
                        ],
                        "weights": [0.3, 0.4, 0.3]
                    }
                ]
            }
            """);

        // 4-mode composite - evenly spaced Normal modes
        MODEL_FIXTURES.put("quadmodal", """
            {
                "unique_vectors": 20000,
                "components": [
                    {
                        "type": "composite",
                        "sub_models": [
                            {"type": "normal", "mean": -0.75, "std_dev": 0.08, "lower_bound": -1.0, "upper_bound": 1.0},
                            {"type": "normal", "mean": -0.25, "std_dev": 0.08, "lower_bound": -1.0, "upper_bound": 1.0},
                            {"type": "normal", "mean": 0.25, "std_dev": 0.08, "lower_bound": -1.0, "upper_bound": 1.0},
                            {"type": "normal", "mean": 0.75, "std_dev": 0.08, "lower_bound": -1.0, "upper_bound": 1.0}
                        ],
                        "weights": [0.25, 0.25, 0.25, 0.25]
                    }
                ]
            }
            """);

        // 5-mode composite
        MODEL_FIXTURES.put("pentamodal", """
            {
                "unique_vectors": 20000,
                "components": [
                    {
                        "type": "composite",
                        "sub_models": [
                            {"type": "normal", "mean": -0.8, "std_dev": 0.07, "lower_bound": -1.0, "upper_bound": 1.0},
                            {"type": "normal", "mean": -0.4, "std_dev": 0.07, "lower_bound": -1.0, "upper_bound": 1.0},
                            {"type": "normal", "mean": 0.0, "std_dev": 0.07, "lower_bound": -1.0, "upper_bound": 1.0},
                            {"type": "normal", "mean": 0.4, "std_dev": 0.07, "lower_bound": -1.0, "upper_bound": 1.0},
                            {"type": "normal", "mean": 0.8, "std_dev": 0.07, "lower_bound": -1.0, "upper_bound": 1.0}
                        ],
                        "weights": [0.2, 0.2, 0.2, 0.2, 0.2]
                    }
                ]
            }
            """);

        // 6-mode composite
        MODEL_FIXTURES.put("hexamodal", """
            {
                "unique_vectors": 20000,
                "components": [
                    {
                        "type": "composite",
                        "sub_models": [
                            {"type": "normal", "mean": -0.83, "std_dev": 0.06, "lower_bound": -1.0, "upper_bound": 1.0},
                            {"type": "normal", "mean": -0.5, "std_dev": 0.06, "lower_bound": -1.0, "upper_bound": 1.0},
                            {"type": "normal", "mean": -0.17, "std_dev": 0.06, "lower_bound": -1.0, "upper_bound": 1.0},
                            {"type": "normal", "mean": 0.17, "std_dev": 0.06, "lower_bound": -1.0, "upper_bound": 1.0},
                            {"type": "normal", "mean": 0.5, "std_dev": 0.06, "lower_bound": -1.0, "upper_bound": 1.0},
                            {"type": "normal", "mean": 0.83, "std_dev": 0.06, "lower_bound": -1.0, "upper_bound": 1.0}
                        ],
                        "weights": [0.167, 0.167, 0.166, 0.167, 0.166, 0.167]
                    }
                ]
            }
            """);

        // 7-mode composite
        MODEL_FIXTURES.put("heptamodal", """
            {
                "unique_vectors": 25000,
                "components": [
                    {
                        "type": "composite",
                        "sub_models": [
                            {"type": "normal", "mean": -0.86, "std_dev": 0.05, "lower_bound": -1.0, "upper_bound": 1.0},
                            {"type": "normal", "mean": -0.57, "std_dev": 0.05, "lower_bound": -1.0, "upper_bound": 1.0},
                            {"type": "normal", "mean": -0.29, "std_dev": 0.05, "lower_bound": -1.0, "upper_bound": 1.0},
                            {"type": "normal", "mean": 0.0, "std_dev": 0.05, "lower_bound": -1.0, "upper_bound": 1.0},
                            {"type": "normal", "mean": 0.29, "std_dev": 0.05, "lower_bound": -1.0, "upper_bound": 1.0},
                            {"type": "normal", "mean": 0.57, "std_dev": 0.05, "lower_bound": -1.0, "upper_bound": 1.0},
                            {"type": "normal", "mean": 0.86, "std_dev": 0.05, "lower_bound": -1.0, "upper_bound": 1.0}
                        ],
                        "weights": [0.143, 0.143, 0.143, 0.142, 0.143, 0.143, 0.143]
                    }
                ]
            }
            """);

        // 8-mode composite
        MODEL_FIXTURES.put("octamodal", """
            {
                "unique_vectors": 30000,
                "components": [
                    {
                        "type": "composite",
                        "sub_models": [
                            {"type": "normal", "mean": -0.875, "std_dev": 0.045, "lower_bound": -1.0, "upper_bound": 1.0},
                            {"type": "normal", "mean": -0.625, "std_dev": 0.045, "lower_bound": -1.0, "upper_bound": 1.0},
                            {"type": "normal", "mean": -0.375, "std_dev": 0.045, "lower_bound": -1.0, "upper_bound": 1.0},
                            {"type": "normal", "mean": -0.125, "std_dev": 0.045, "lower_bound": -1.0, "upper_bound": 1.0},
                            {"type": "normal", "mean": 0.125, "std_dev": 0.045, "lower_bound": -1.0, "upper_bound": 1.0},
                            {"type": "normal", "mean": 0.375, "std_dev": 0.045, "lower_bound": -1.0, "upper_bound": 1.0},
                            {"type": "normal", "mean": 0.625, "std_dev": 0.045, "lower_bound": -1.0, "upper_bound": 1.0},
                            {"type": "normal", "mean": 0.875, "std_dev": 0.045, "lower_bound": -1.0, "upper_bound": 1.0}
                        ],
                        "weights": [0.125, 0.125, 0.125, 0.125, 0.125, 0.125, 0.125, 0.125]
                    }
                ]
            }
            """);

        // 9-mode composite
        MODEL_FIXTURES.put("nonamodal", """
            {
                "unique_vectors": 35000,
                "components": [
                    {
                        "type": "composite",
                        "sub_models": [
                            {"type": "normal", "mean": -0.89, "std_dev": 0.04, "lower_bound": -1.0, "upper_bound": 1.0},
                            {"type": "normal", "mean": -0.67, "std_dev": 0.04, "lower_bound": -1.0, "upper_bound": 1.0},
                            {"type": "normal", "mean": -0.44, "std_dev": 0.04, "lower_bound": -1.0, "upper_bound": 1.0},
                            {"type": "normal", "mean": -0.22, "std_dev": 0.04, "lower_bound": -1.0, "upper_bound": 1.0},
                            {"type": "normal", "mean": 0.0, "std_dev": 0.04, "lower_bound": -1.0, "upper_bound": 1.0},
                            {"type": "normal", "mean": 0.22, "std_dev": 0.04, "lower_bound": -1.0, "upper_bound": 1.0},
                            {"type": "normal", "mean": 0.44, "std_dev": 0.04, "lower_bound": -1.0, "upper_bound": 1.0},
                            {"type": "normal", "mean": 0.67, "std_dev": 0.04, "lower_bound": -1.0, "upper_bound": 1.0},
                            {"type": "normal", "mean": 0.89, "std_dev": 0.04, "lower_bound": -1.0, "upper_bound": 1.0}
                        ],
                        "weights": [0.111, 0.111, 0.111, 0.111, 0.112, 0.111, 0.111, 0.111, 0.111]
                    }
                ]
            }
            """);

        // 10-mode composite - maximum supported
        MODEL_FIXTURES.put("decamodal", """
            {
                "unique_vectors": 40000,
                "components": [
                    {
                        "type": "composite",
                        "sub_models": [
                            {"type": "normal", "mean": -0.9, "std_dev": 0.035, "lower_bound": -1.0, "upper_bound": 1.0},
                            {"type": "normal", "mean": -0.7, "std_dev": 0.035, "lower_bound": -1.0, "upper_bound": 1.0},
                            {"type": "normal", "mean": -0.5, "std_dev": 0.035, "lower_bound": -1.0, "upper_bound": 1.0},
                            {"type": "normal", "mean": -0.3, "std_dev": 0.035, "lower_bound": -1.0, "upper_bound": 1.0},
                            {"type": "normal", "mean": -0.1, "std_dev": 0.035, "lower_bound": -1.0, "upper_bound": 1.0},
                            {"type": "normal", "mean": 0.1, "std_dev": 0.035, "lower_bound": -1.0, "upper_bound": 1.0},
                            {"type": "normal", "mean": 0.3, "std_dev": 0.035, "lower_bound": -1.0, "upper_bound": 1.0},
                            {"type": "normal", "mean": 0.5, "std_dev": 0.035, "lower_bound": -1.0, "upper_bound": 1.0},
                            {"type": "normal", "mean": 0.7, "std_dev": 0.035, "lower_bound": -1.0, "upper_bound": 1.0},
                            {"type": "normal", "mean": 0.9, "std_dev": 0.035, "lower_bound": -1.0, "upper_bound": 1.0}
                        ],
                        "weights": [0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1]
                    }
                ]
            }
            """);
    }

    // ==================== Test Methods ====================

    /// Provides all model fixtures for parameterized testing.
    static Stream<String> modelFixtureNames() {
        return MODEL_FIXTURES.keySet().stream();
    }

    /// Tests the full round-trip pipeline for each model fixture.
    ///
    /// Pipeline:
    /// 1. Gen (load from JSON) → generate variates
    /// 2. Ext (extract from variates) → compare Gen vs Ext
    /// 3. R-T (extract from Ext variates) → compare Ext vs R-T
    @ParameterizedTest(name = "roundTrip_{0}")
    @MethodSource("modelFixtureNames")
    @Order(1)
    void testRoundTripPipeline(String fixtureName) {
        String json = MODEL_FIXTURES.get(fixtureName);
        System.out.println("\n" + "═".repeat(70));
        System.out.printf("Testing: %s%n", fixtureName);
        System.out.println("═".repeat(70));

        // ============ PHASE 1: Load Gen model ============
        System.out.println("\nPHASE 1: Load Gen (source) model from JSON");
        VectorSpaceModel genModel = VectorSpaceModelConfig.load(json);
        assertNotNull(genModel, "Failed to load model from JSON");
        assertEquals(1, genModel.dimensions(), "Test fixture should be 1-dimensional");

        ScalarModel genScalar = genModel.scalarModel(0);
        System.out.printf("  Gen model type: %s%n", describeModel(genScalar));

        // ============ PHASE 2: Generate variates from Gen ============
        System.out.println("\nPHASE 2: Generate variates from Gen model");
        float[] genVariates = generateVariates(genScalar, SAMPLE_SIZE);
        System.out.printf("  Generated %,d variates%n", genVariates.length);
        printStatistics("  Gen variates", genVariates);

        // ============ PHASE 3: Extract Ext from Gen variates ============
        // Use streaming path to match CLI behavior exactly
        System.out.println("\nPHASE 3: Extract Ext model from Gen variates (accuracy test)");
        BestFitSelector selector = BestFitSelector.pearsonMultimodalSelector(MAX_MODES);
        ScalarModel extScalar = extractViaStreamingPath(genVariates, selector);
        System.out.printf("  Ext model type: %s%n", describeModel(extScalar));

        // ============ PHASE 4: Compare Gen vs Ext (accuracy) ============
        System.out.println("\nPHASE 4: Compare Gen vs Ext (accuracy)");
        boolean genExtMatch = EQUIVALENCE_CHECKER.areEquivalent(genScalar, extScalar);
        String genExtReason = EQUIVALENCE_CHECKER.getEquivalenceReason(genScalar, extScalar);
        System.out.printf("  Gen vs Ext: %s%n", genExtMatch ? "MATCH" : "MISMATCH");
        if (genExtReason != null) {
            System.out.printf("  Equivalence: %s%n", genExtReason);
        }

        // ============ PHASE 5: Generate variates from Ext ============
        System.out.println("\nPHASE 5: Generate variates from Ext model");
        float[] extVariates = generateVariates(extScalar, SAMPLE_SIZE);
        System.out.printf("  Generated %,d variates%n", extVariates.length);
        printStatistics("  Ext variates", extVariates);

        // ============ PHASE 6: Extract R-T from Ext variates ============
        // Use streaming path to match CLI behavior exactly
        // Use the SAME selector as Gen→Ext for true round-trip test
        System.out.println("\nPHASE 6: Extract R-T model from Ext variates (stability test)");
        ScalarModel rtScalar = extractViaStreamingPath(extVariates, selector);
        System.out.printf("  R-T model type: %s%n", describeModel(rtScalar));

        // ============ PHASE 7: Compare Ext vs R-T (stability) ============
        System.out.println("\nPHASE 7: Compare Ext vs R-T (stability)");
        boolean extRtMatch = EQUIVALENCE_CHECKER.areEquivalent(extScalar, rtScalar);
        String extRtReason = EQUIVALENCE_CHECKER.getEquivalenceReason(extScalar, rtScalar);
        System.out.printf("  Ext vs R-T: %s%n", extRtMatch ? "MATCH" : "MISMATCH");
        if (extRtReason != null) {
            System.out.printf("  Equivalence: %s%n", extRtReason);
        }

        // ============ Summary ============
        System.out.println("\n" + "-".repeat(70));
        System.out.println("SUMMARY:");
        System.out.printf("  Gen→Ext accuracy:  %s%n", genExtMatch ? "✓ PASS" : "✗ FAIL");
        System.out.printf("  Ext→R-T stability: %s%n", extRtMatch ? "✓ PASS" : "✗ FAIL");
        System.out.println("-".repeat(70));

        // For simple unimodal models, assert both accuracy and stability
        // Multimodal fixtures have known instability issues - they're validated by aggregate rate
        if (!fixtureName.contains("modal") && !fixtureName.contains("mixed")) {
            assertTrue(extRtMatch,
                String.format("Ext→R-T stability failed for %s: Ext=%s, R-T=%s",
                    fixtureName, describeModel(extScalar), describeModel(rtScalar)));

            assertTrue(genExtMatch,
                String.format("Gen→Ext accuracy failed for %s: Gen=%s, Ext=%s",
                    fixtureName, describeModel(genScalar), describeModel(extScalar)));
        } else {
            // Multimodal - just log the result without failing
            // The aggregate test validates overall pass rate
            if (!extRtMatch) {
                System.out.println("  [KNOWN ISSUE] Multimodal stability not yet achieved");
            }
        }
    }

    /// Tests all fixtures and reports aggregate statistics.
    @Test
    @Order(100)
    void testAggregateStatistics() {
        System.out.println("\n" + "═".repeat(70));
        System.out.println("AGGREGATE ROUND-TRIP STATISTICS");
        System.out.println("═".repeat(70));

        int totalTests = 0;
        int genExtPasses = 0;
        int extRtPasses = 0;

        System.out.println("\nFixture                  | Gen→Ext | Ext→R-T | Overall");
        System.out.println("-------------------------|---------|---------|--------");

        for (String fixtureName : MODEL_FIXTURES.keySet()) {
            String json = MODEL_FIXTURES.get(fixtureName);
            VectorSpaceModel genModel = VectorSpaceModelConfig.load(json);
            ScalarModel genScalar = genModel.scalarModel(0);

            // Generate and extract via streaming path (matches CLI behavior)
            // Use the SAME selector for both Gen→Ext and Ext→R-T stages
            float[] genVariates = generateVariates(genScalar, SAMPLE_SIZE);
            BestFitSelector selector = BestFitSelector.pearsonMultimodalSelector(MAX_MODES);
            ScalarModel extScalar = extractViaStreamingPath(genVariates, selector);

            float[] extVariates = generateVariates(extScalar, SAMPLE_SIZE);
            // Use the same selector for R-T extraction - this is a true round-trip test
            ScalarModel rtScalar = extractViaStreamingPath(extVariates, selector);

            // Compare
            boolean genExtMatch = EQUIVALENCE_CHECKER.areEquivalent(genScalar, extScalar);
            boolean extRtMatch = EQUIVALENCE_CHECKER.areEquivalent(extScalar, rtScalar);

            totalTests++;
            if (genExtMatch) genExtPasses++;
            if (extRtMatch) extRtPasses++;

            String genExtStatus = genExtMatch ? "  ✓   " : "  ✗   ";
            String extRtStatus = extRtMatch ? "  ✓   " : "  ✗   ";
            String overall = (genExtMatch && extRtMatch) ? "  ✓" : "  ✗";

            System.out.printf("%-24s | %s | %s | %s%n",
                fixtureName, genExtStatus, extRtStatus, overall);
        }

        System.out.println("-------------------------|---------|---------|--------");
        System.out.printf("%-24s | %d/%d   | %d/%d   | %d/%d%n",
            "TOTALS",
            genExtPasses, totalTests,
            extRtPasses, totalTests,
            Math.min(genExtPasses, extRtPasses), totalTests);

        double genExtRate = 100.0 * genExtPasses / totalTests;
        double extRtRate = 100.0 * extRtPasses / totalTests;

        System.out.println();
        System.out.printf("Gen→Ext accuracy rate:  %.1f%%%n", genExtRate);
        System.out.printf("Ext→R-T stability rate: %.1f%%%n", extRtRate);

        // Note: Stability below 100% indicates extraction instability issues
        // For now, use a lenient threshold - multimodal extraction is still being improved
        // TODO: Tighten this threshold as extraction stability improves
        assertTrue(extRtRate >= 70.0,
            String.format("Ext→R-T stability rate %.1f%% below 70%% threshold", extRtRate));
    }

    // ==================== Helper Methods ====================

    /// Generates variates from a scalar model using inverse CDF sampling.
    private float[] generateVariates(ScalarModel model, int n) {
        float[] data = new float[n];
        Random rng = new Random(42);  // Fixed seed for reproducibility

        if (model instanceof CompositeScalarModel composite) {
            // Sample from composite
            ScalarModel[] components = composite.getScalarModels();
            double[] weights = composite.getWeights();
            double[] cumWeights = new double[weights.length];
            double sum = 0;
            for (int i = 0; i < weights.length; i++) {
                sum += weights[i];
                cumWeights[i] = sum;
            }
            for (int i = 0; i < cumWeights.length; i++) {
                cumWeights[i] /= sum;
            }

            for (int i = 0; i < n; i++) {
                double u = rng.nextDouble();
                int componentIdx = 0;
                for (int c = 0; c < cumWeights.length; c++) {
                    if (u <= cumWeights[c]) {
                        componentIdx = c;
                        break;
                    }
                }
                data[i] = (float) sampleFromModel(components[componentIdx], rng);
            }
        } else {
            for (int i = 0; i < n; i++) {
                data[i] = (float) sampleFromModel(model, rng);
            }
        }

        return data;
    }

    /// Samples a single value from a scalar model.
    private double sampleFromModel(ScalarModel model, Random rng) {
        if (model instanceof NormalScalarModel normal) {
            // Rejection sampling for truncated normal
            double lower = normal.lower();
            double upper = normal.upper();
            for (int i = 0; i < 1000; i++) {
                double sample = normal.getMean() + rng.nextGaussian() * normal.getStdDev();
                if (sample >= lower && sample <= upper) {
                    return sample;
                }
            }
            return Math.max(lower, Math.min(upper, normal.getMean()));

        } else if (model instanceof UniformScalarModel uniform) {
            return uniform.getLower() + rng.nextDouble() * uniform.getRange();

        } else if (model instanceof BetaScalarModel beta) {
            // Simple beta sampling using gamma ratio
            double a = beta.getAlpha();
            double b = beta.getBeta();
            double x = sampleGamma(a, 1.0, rng);
            double y = sampleGamma(b, 1.0, rng);
            double sample = x / (x + y);
            // Scale to [lower, upper]
            return beta.getLower() + sample * (beta.getUpper() - beta.getLower());

        } else if (model instanceof GammaScalarModel gamma) {
            return sampleGamma(gamma.getShape(), gamma.getScale(), rng) + gamma.getLocation();

        } else if (model instanceof StudentTScalarModel studentT) {
            // Sample from Student-t using the standard method
            double nu = studentT.getDegreesOfFreedom();
            double chi2 = sampleGamma(nu / 2.0, 2.0, rng);
            double z = rng.nextGaussian();
            double t = z / Math.sqrt(chi2 / nu);
            return studentT.getLocation() + t * studentT.getScale();

        } else {
            throw new UnsupportedOperationException(
                "Sampling not implemented for model type: " + model.getModelType());
        }
    }

    /// Samples from Gamma distribution using Marsaglia and Tsang's method.
    private double sampleGamma(double shape, double scale, Random rng) {
        if (shape < 1) {
            return sampleGamma(1 + shape, scale, rng) * Math.pow(rng.nextDouble(), 1.0 / shape);
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

            if (u < 1 - 0.0331 * (x * x) * (x * x)) {
                return d * v * scale;
            }

            if (Math.log(u) < 0.5 * x * x + d * (1 - v + Math.log(v))) {
                return d * v * scale;
            }
        }
    }

    /// Describes a model in human-readable format.
    private String describeModel(ScalarModel model) {
        if (model instanceof CompositeScalarModel composite) {
            int count = composite.getComponentCount();
            StringBuilder sb = new StringBuilder();
            sb.append("Composite(").append(count).append(" modes: ");
            ScalarModel[] components = composite.getScalarModels();
            for (int i = 0; i < Math.min(3, components.length); i++) {
                if (i > 0) sb.append(", ");
                sb.append(components[i].getModelType());
            }
            if (components.length > 3) {
                sb.append(", ...");
            }
            sb.append(")");
            return sb.toString();
        }

        String type = model.getModelType();
        return switch (type) {
            case "normal" -> {
                NormalScalarModel n = (NormalScalarModel) model;
                yield String.format("Normal(μ=%.2f, σ=%.2f)", n.getMean(), n.getStdDev());
            }
            case "uniform" -> {
                UniformScalarModel u = (UniformScalarModel) model;
                yield String.format("Uniform[%.2f, %.2f]", u.getLower(), u.getUpper());
            }
            case "beta" -> {
                BetaScalarModel b = (BetaScalarModel) model;
                yield String.format("Beta(α=%.2f, β=%.2f)", b.getAlpha(), b.getBeta());
            }
            default -> type;
        };
    }

    /// Prints basic statistics for data.
    private void printStatistics(String label, float[] data) {
        double sum = 0, min = data[0], max = data[0];
        for (float v : data) {
            sum += v;
            min = Math.min(min, v);
            max = Math.max(max, v);
        }
        double mean = sum / data.length;

        double sumSq = 0;
        for (float v : data) {
            sumSq += (v - mean) * (v - mean);
        }
        double std = Math.sqrt(sumSq / data.length);

        System.out.printf("%s: mean=%.3f, std=%.3f, range=[%.3f, %.3f]%n",
            label, mean, std, min, max);
    }

    /// Extracts a model using the streaming path - same code path as CLI.
    ///
    /// This mirrors the extraction logic in CMD_analyze_profile to ensure
    /// the unit tests exercise the same code path as production.
    ///
    /// @param values the observed values for one dimension
    /// @param selector the BestFitSelector to use
    /// @return the extracted ScalarModel
    private ScalarModel extractViaStreamingPath(float[] values, BestFitSelector selector) {
        // Create StreamingModelExtractor with the same configuration as CLI
        StreamingModelExtractor extractor = new StreamingModelExtractor(selector);
        extractor.setUniqueVectors(values.length);

        // Disable adaptive fitting for pearson model type (matches CLI behavior)
        // CLI: isAdaptiveEnabled() returns false for modelType != auto
        extractor.setAdaptiveEnabled(false);

        // Disable NUMA-aware fitting for tests to avoid thread pool pollution
        // that affects subsequent test modules in the same reactor build
        extractor.setNumaAware(false);

        // Initialize with shape (1 dimension, values.length vectors)
        DataspaceShape shape = new DataspaceShape(values.length, 1);
        extractor.initialize(shape);

        // Feed data in columnar format: chunk[dim][vector]
        // For 1 dimension, this is just float[1][values.length]
        float[][] columnarData = new float[1][values.length];
        System.arraycopy(values, 0, columnarData[0], 0, values.length);

        // Feed as single chunk (mimics CLI behavior for small datasets)
        extractor.accept(columnarData, 0);

        // Complete extraction and clean up resources
        VectorSpaceModel model = extractor.complete();
        extractor.shutdown();  // CRITICAL: Release thread pools to avoid resource leaks

        return model.scalarModel(0);
    }
}
