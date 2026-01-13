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

/// Extended curve variety test covering challenging distribution shapes.
///
/// ## Purpose
///
/// This test extends RoundTripPipelineTest with more challenging cases:
/// - Edge case unimodal shapes (very narrow, very wide, J-shaped, U-shaped)
/// - Overlapping multimodal (modes closer together)
/// - Unequal weight composites (dominant vs minority modes)
/// - Variable width modes within same composite
/// - Boundary-adjacent modes
/// - Randomized fixtures matching generate sketch behavior
///
/// ## Test Strategy
///
/// Each fixture tests Gen → Ext → R-T pipeline with focus on:
/// 1. Gen→Ext accuracy (extraction recovers original distribution)
/// 2. Ext→R-T stability (round-trip produces consistent results)
///
/// Both assertions are required for a test to pass.
///
/// ## Reproducibility
///
/// All fixtures use seed-based random generation to ensure reproducibility.
/// The seed is derived from the fixture name to avoid correlation between tests.
///
/// @see RoundTripPipelineTest for standard fixtures
@Tag("accuracy")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ExtendedCurveVarietyTest {

    /// Sample size for testing
    private static final int SAMPLE_SIZE = 50_000;

    /// Maximum modes to detect
    private static final int MAX_MODES = 10;

    /// Base seed for reproducible random generation
    private static final long BASE_SEED = 20240115L;

    /// Equivalence checker
    private static final StatisticalEquivalenceChecker EQUIVALENCE_CHECKER =
        new StatisticalEquivalenceChecker();

    // ==================== Test Fixture Definitions ====================

    /// Test fixture with name and model
    record TestFixture(String name, String category, ScalarModel model, String description) {}

    /// All test fixtures organized by category
    private static List<TestFixture> createFixtures() {
        List<TestFixture> fixtures = new ArrayList<>();

        // ========== CATEGORY 1: Unimodal Edge Cases ==========

        // Very narrow Normal (spike-like)
        fixtures.add(new TestFixture("normal_very_narrow", "unimodal_edge",
            new NormalScalarModel(0.0, 0.05, -1.0, 1.0),
            "Narrow spike normal, σ=0.05"));

        // Very wide Normal (almost flat)
        fixtures.add(new TestFixture("normal_very_wide", "unimodal_edge",
            new NormalScalarModel(0.0, 0.6, -1.0, 1.0),
            "Wide normal approaching uniform, σ=0.6"));

        // Normal at left edge
        fixtures.add(new TestFixture("normal_left_edge", "unimodal_edge",
            new NormalScalarModel(-0.7, 0.15, -1.0, 1.0),
            "Normal near left boundary, μ=-0.7"));

        // Normal at right edge
        fixtures.add(new TestFixture("normal_right_edge", "unimodal_edge",
            new NormalScalarModel(0.7, 0.15, -1.0, 1.0),
            "Normal near right boundary, μ=0.7"));

        // U-shaped Beta (bimodal appearance but unimodal model)
        fixtures.add(new TestFixture("beta_u_shaped", "unimodal_edge",
            new BetaScalarModel(0.5, 0.5, -1.0, 1.0),
            "U-shaped Beta, α=β=0.5"));

        // J-shaped Beta (left)
        fixtures.add(new TestFixture("beta_j_left", "unimodal_edge",
            new BetaScalarModel(0.8, 3.0, -1.0, 1.0),
            "J-shaped Beta skewed left, α=0.8, β=3.0"));

        // J-shaped Beta (right)
        fixtures.add(new TestFixture("beta_j_right", "unimodal_edge",
            new BetaScalarModel(3.0, 0.8, -1.0, 1.0),
            "J-shaped Beta skewed right, α=3.0, β=0.8"));

        // Very peaked Beta
        fixtures.add(new TestFixture("beta_peaked", "unimodal_edge",
            new BetaScalarModel(10.0, 10.0, -1.0, 1.0),
            "Peaked symmetric Beta, α=β=10"));

        // ========== CATEGORY 2: Overlapping Bimodal ==========

        // Modes very close together (challenging separation)
        fixtures.add(new TestFixture("bimodal_close", "overlapping",
            createComposite(
                List.of(new NormalScalarModel(-0.15, 0.12, -1.0, 1.0),
                        new NormalScalarModel(0.15, 0.12, -1.0, 1.0)),
                new double[]{0.5, 0.5}),
            "Close bimodal, Δμ=0.3, may appear unimodal"));

        // Modes moderately overlapping
        fixtures.add(new TestFixture("bimodal_moderate_overlap", "overlapping",
            createComposite(
                List.of(new NormalScalarModel(-0.25, 0.15, -1.0, 1.0),
                        new NormalScalarModel(0.25, 0.15, -1.0, 1.0)),
                new double[]{0.5, 0.5}),
            "Moderate overlap bimodal, Δμ=0.5"));

        // Trimodal with close outer modes
        fixtures.add(new TestFixture("trimodal_close_outer", "overlapping",
            createComposite(
                List.of(new NormalScalarModel(-0.4, 0.12, -1.0, 1.0),
                        new NormalScalarModel(0.0, 0.12, -1.0, 1.0),
                        new NormalScalarModel(0.4, 0.12, -1.0, 1.0)),
                new double[]{0.33, 0.34, 0.33}),
            "Trimodal with overlapping modes"));

        // ========== CATEGORY 3: Unequal Weights ==========

        // Dominant mode with small secondary
        fixtures.add(new TestFixture("bimodal_dominant", "unequal_weight",
            createComposite(
                List.of(new NormalScalarModel(-0.3, 0.15, -1.0, 1.0),
                        new NormalScalarModel(0.5, 0.15, -1.0, 1.0)),
                new double[]{0.85, 0.15}),
            "Dominant mode (85%) with minority (15%)"));

        // Small minority mode (may be missed)
        fixtures.add(new TestFixture("bimodal_tiny_minority", "unequal_weight",
            createComposite(
                List.of(new NormalScalarModel(0.0, 0.2, -1.0, 1.0),
                        new NormalScalarModel(0.7, 0.08, -1.0, 1.0)),
                new double[]{0.92, 0.08}),
            "Main mode with tiny minority (8%)"));

        // Three modes with varying weights
        fixtures.add(new TestFixture("trimodal_unequal", "unequal_weight",
            createComposite(
                List.of(new NormalScalarModel(-0.5, 0.1, -1.0, 1.0),
                        new NormalScalarModel(0.0, 0.15, -1.0, 1.0),
                        new NormalScalarModel(0.5, 0.1, -1.0, 1.0)),
                new double[]{0.2, 0.6, 0.2}),
            "Trimodal with central dominance (20-60-20)"));

        // Four modes with graduated weights
        fixtures.add(new TestFixture("quadmodal_graduated", "unequal_weight",
            createComposite(
                List.of(new NormalScalarModel(-0.6, 0.1, -1.0, 1.0),
                        new NormalScalarModel(-0.2, 0.1, -1.0, 1.0),
                        new NormalScalarModel(0.2, 0.1, -1.0, 1.0),
                        new NormalScalarModel(0.6, 0.1, -1.0, 1.0)),
                new double[]{0.1, 0.3, 0.4, 0.2}),
            "Quadmodal with graduated weights"));

        // ========== CATEGORY 4: Variable Width Modes ==========

        // Two modes with different widths
        fixtures.add(new TestFixture("bimodal_variable_width", "variable_width",
            createComposite(
                List.of(new NormalScalarModel(-0.4, 0.08, -1.0, 1.0),
                        new NormalScalarModel(0.4, 0.25, -1.0, 1.0)),
                new double[]{0.5, 0.5}),
            "Bimodal: narrow left (σ=0.08), wide right (σ=0.25)"));

        // Three modes with varying widths
        fixtures.add(new TestFixture("trimodal_variable_width", "variable_width",
            createComposite(
                List.of(new NormalScalarModel(-0.5, 0.05, -1.0, 1.0),
                        new NormalScalarModel(0.0, 0.15, -1.0, 1.0),
                        new NormalScalarModel(0.5, 0.1, -1.0, 1.0)),
                new double[]{0.33, 0.34, 0.33}),
            "Trimodal: narrow-wide-medium widths"));

        // Mixed types with different inherent widths
        fixtures.add(new TestFixture("mixed_width_types", "variable_width",
            createComposite(
                List.of(new NormalScalarModel(-0.5, 0.08, -1.0, 1.0),
                        new UniformScalarModel(-0.1, 0.3),
                        new BetaScalarModel(5.0, 2.0, 0.4, 0.95)),
                new double[]{0.35, 0.35, 0.30}),
            "Mixed: narrow Normal, Uniform block, skewed Beta"));

        // ========== CATEGORY 5: Boundary Modes ==========

        // Mode at left boundary
        fixtures.add(new TestFixture("bimodal_left_boundary", "boundary",
            createComposite(
                List.of(new NormalScalarModel(-0.85, 0.08, -1.0, 1.0),
                        new NormalScalarModel(0.2, 0.15, -1.0, 1.0)),
                new double[]{0.4, 0.6}),
            "Bimodal with mode near left boundary"));

        // Mode at right boundary
        fixtures.add(new TestFixture("bimodal_right_boundary", "boundary",
            createComposite(
                List.of(new NormalScalarModel(-0.2, 0.15, -1.0, 1.0),
                        new NormalScalarModel(0.85, 0.08, -1.0, 1.0)),
                new double[]{0.6, 0.4}),
            "Bimodal with mode near right boundary"));

        // Modes at both boundaries
        fixtures.add(new TestFixture("bimodal_both_boundaries", "boundary",
            createComposite(
                List.of(new NormalScalarModel(-0.8, 0.1, -1.0, 1.0),
                        new NormalScalarModel(0.8, 0.1, -1.0, 1.0)),
                new double[]{0.5, 0.5}),
            "Bimodal with modes at both boundaries"));

        // ========== CATEGORY 6: Higher Mode Counts with Variation ==========

        // 5 modes with one dominant
        fixtures.add(new TestFixture("pentamodal_dominant_center", "higher_mode",
            createComposite(
                List.of(new NormalScalarModel(-0.7, 0.08, -1.0, 1.0),
                        new NormalScalarModel(-0.35, 0.08, -1.0, 1.0),
                        new NormalScalarModel(0.0, 0.12, -1.0, 1.0),
                        new NormalScalarModel(0.35, 0.08, -1.0, 1.0),
                        new NormalScalarModel(0.7, 0.08, -1.0, 1.0)),
                new double[]{0.15, 0.15, 0.40, 0.15, 0.15}),
            "5-mode with dominant center (40%)"));

        // 6 modes in two clusters
        fixtures.add(new TestFixture("hexamodal_clustered", "higher_mode",
            createComposite(
                List.of(new NormalScalarModel(-0.7, 0.06, -1.0, 1.0),
                        new NormalScalarModel(-0.55, 0.06, -1.0, 1.0),
                        new NormalScalarModel(-0.4, 0.06, -1.0, 1.0),
                        new NormalScalarModel(0.4, 0.06, -1.0, 1.0),
                        new NormalScalarModel(0.55, 0.06, -1.0, 1.0),
                        new NormalScalarModel(0.7, 0.06, -1.0, 1.0)),
                new double[]{0.167, 0.167, 0.166, 0.167, 0.166, 0.167}),
            "6-mode in two clusters (left and right)"));

        // 7 modes with varying spacings
        fixtures.add(new TestFixture("heptamodal_variable_spacing", "higher_mode",
            createComposite(
                List.of(new NormalScalarModel(-0.8, 0.05, -1.0, 1.0),
                        new NormalScalarModel(-0.5, 0.05, -1.0, 1.0),
                        new NormalScalarModel(-0.3, 0.05, -1.0, 1.0),
                        new NormalScalarModel(0.0, 0.05, -1.0, 1.0),
                        new NormalScalarModel(0.3, 0.05, -1.0, 1.0),
                        new NormalScalarModel(0.5, 0.05, -1.0, 1.0),
                        new NormalScalarModel(0.8, 0.05, -1.0, 1.0)),
                new double[]{0.14, 0.14, 0.14, 0.16, 0.14, 0.14, 0.14}),
            "7-mode with variable spacing"));

        // 8 modes with variable widths
        fixtures.add(new TestFixture("octamodal_variable_width", "higher_mode",
            createComposite(
                List.of(new NormalScalarModel(-0.85, 0.04, -1.0, 1.0),
                        new NormalScalarModel(-0.6, 0.05, -1.0, 1.0),
                        new NormalScalarModel(-0.35, 0.04, -1.0, 1.0),
                        new NormalScalarModel(-0.1, 0.05, -1.0, 1.0),
                        new NormalScalarModel(0.1, 0.05, -1.0, 1.0),
                        new NormalScalarModel(0.35, 0.04, -1.0, 1.0),
                        new NormalScalarModel(0.6, 0.05, -1.0, 1.0),
                        new NormalScalarModel(0.85, 0.04, -1.0, 1.0)),
                new double[]{0.125, 0.125, 0.125, 0.125, 0.125, 0.125, 0.125, 0.125}),
            "8-mode with alternating widths"));

        // 9 modes with unequal weights
        fixtures.add(new TestFixture("nonamodal_unequal", "higher_mode",
            createComposite(
                List.of(new NormalScalarModel(-0.85, 0.04, -1.0, 1.0),
                        new NormalScalarModel(-0.64, 0.04, -1.0, 1.0),
                        new NormalScalarModel(-0.43, 0.04, -1.0, 1.0),
                        new NormalScalarModel(-0.21, 0.04, -1.0, 1.0),
                        new NormalScalarModel(0.0, 0.05, -1.0, 1.0),
                        new NormalScalarModel(0.21, 0.04, -1.0, 1.0),
                        new NormalScalarModel(0.43, 0.04, -1.0, 1.0),
                        new NormalScalarModel(0.64, 0.04, -1.0, 1.0),
                        new NormalScalarModel(0.85, 0.04, -1.0, 1.0)),
                new double[]{0.08, 0.10, 0.12, 0.14, 0.12, 0.14, 0.12, 0.10, 0.08}),
            "9-mode with bell-shaped weights"));

        // 10 modes with clustered pattern
        fixtures.add(new TestFixture("decamodal_clustered", "higher_mode",
            createComposite(
                List.of(new NormalScalarModel(-0.9, 0.035, -1.0, 1.0),
                        new NormalScalarModel(-0.75, 0.035, -1.0, 1.0),
                        new NormalScalarModel(-0.5, 0.035, -1.0, 1.0),
                        new NormalScalarModel(-0.35, 0.035, -1.0, 1.0),
                        new NormalScalarModel(-0.1, 0.035, -1.0, 1.0),
                        new NormalScalarModel(0.1, 0.035, -1.0, 1.0),
                        new NormalScalarModel(0.35, 0.035, -1.0, 1.0),
                        new NormalScalarModel(0.5, 0.035, -1.0, 1.0),
                        new NormalScalarModel(0.75, 0.035, -1.0, 1.0),
                        new NormalScalarModel(0.9, 0.035, -1.0, 1.0)),
                new double[]{0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1}),
            "10-mode with paired clustering"));

        // ========== CATEGORY 7: Randomized Fixtures (matching generate sketch) ==========
        // These fixtures use randomized parameters similar to CMD_generate_sketch.createFullMixModel()
        // to test the same challenging scenarios the profile command encounters.

        Random rng = new Random(BASE_SEED);

        // Randomized bimodal with jittered positions
        for (int i = 1; i <= 3; i++) {
            fixtures.add(createRandomizedBimodal(rng, i));
        }

        // Randomized trimodal with mixed types
        for (int i = 1; i <= 3; i++) {
            fixtures.add(createRandomizedTrimodal(rng, i));
        }

        // Randomized mixed-type composites
        for (int i = 1; i <= 4; i++) {
            fixtures.add(createRandomizedMixedType(rng, i));
        }

        return fixtures;
    }

    /// Creates a randomized bimodal fixture with jittered mode positions.
    /// Mimics the randomization in CMD_generate_sketch.createFullMixModel().
    private static TestFixture createRandomizedBimodal(Random rng, int variant) {
        double modeSpacing = 0.4 + rng.nextDouble() * 0.4; // 0.4-0.8
        double jitter1 = (rng.nextDouble() - 0.5) * modeSpacing * 0.3;
        double jitter2 = (rng.nextDouble() - 0.5) * modeSpacing * 0.3;

        double mean1 = Math.max(-0.85, Math.min(0.0, -modeSpacing / 2 + jitter1));
        double mean2 = Math.max(0.0, Math.min(0.85, modeSpacing / 2 + jitter2));

        double stdDev1 = 0.08 + rng.nextDouble() * 0.12; // 0.08-0.20
        double stdDev2 = 0.08 + rng.nextDouble() * 0.12;

        double weight1 = 0.3 + rng.nextDouble() * 0.4; // 0.3-0.7
        double weight2 = 1.0 - weight1;

        return new TestFixture(
            "random_bimodal_" + variant,
            "randomized",
            createComposite(
                List.of(new NormalScalarModel(mean1, stdDev1, -1.0, 1.0),
                        new NormalScalarModel(mean2, stdDev2, -1.0, 1.0)),
                new double[]{weight1, weight2}),
            String.format("Random bimodal: μ=(%.2f,%.2f), σ=(%.2f,%.2f), w=(%.0f%%,%.0f%%)",
                mean1, mean2, stdDev1, stdDev2, weight1 * 100, weight2 * 100));
    }

    /// Creates a randomized trimodal fixture.
    private static TestFixture createRandomizedTrimodal(Random rng, int variant) {
        double spacing = 0.3 + rng.nextDouble() * 0.2; // 0.3-0.5

        double mean1 = -spacing + (rng.nextDouble() - 0.5) * 0.1;
        double mean2 = 0.0 + (rng.nextDouble() - 0.5) * 0.1;
        double mean3 = spacing + (rng.nextDouble() - 0.5) * 0.1;

        // Clamp to valid range
        mean1 = Math.max(-0.85, Math.min(-0.1, mean1));
        mean3 = Math.max(0.1, Math.min(0.85, mean3));

        double stdDev = 0.06 + rng.nextDouble() * 0.08; // 0.06-0.14

        double w1 = 0.2 + rng.nextDouble() * 0.2;
        double w2 = 0.3 + rng.nextDouble() * 0.3;
        double w3 = 1.0 - w1 - w2;

        // Normalize
        double total = w1 + w2 + w3;
        w1 /= total;
        w2 /= total;
        w3 /= total;

        return new TestFixture(
            "random_trimodal_" + variant,
            "randomized",
            createComposite(
                List.of(new NormalScalarModel(mean1, stdDev, -1.0, 1.0),
                        new NormalScalarModel(mean2, stdDev, -1.0, 1.0),
                        new NormalScalarModel(mean3, stdDev, -1.0, 1.0)),
                new double[]{w1, w2, w3}),
            String.format("Random trimodal: μ=(%.2f,%.2f,%.2f), σ=%.2f",
                mean1, mean2, mean3, stdDev));
    }

    /// Creates a randomized mixed-type composite fixture.
    /// Similar to how generate sketch creates full mix with various distribution types.
    private static TestFixture createRandomizedMixedType(Random rng, int variant) {
        List<ScalarModel> components = new ArrayList<>();
        List<Double> weights = new ArrayList<>();

        int numComponents = 2 + rng.nextInt(2); // 2-3 components

        for (int i = 0; i < numComponents; i++) {
            int typeChoice = rng.nextInt(4);
            double position = -0.6 + i * (1.2 / Math.max(1, numComponents - 1));
            position += (rng.nextDouble() - 0.5) * 0.2; // jitter
            position = Math.max(-0.85, Math.min(0.85, position));

            // Only use bounded distribution types (Normal, Beta, Uniform) for predictable behavior
            // Avoid Gamma since it's unbounded and can extend outside [-1, 1]
            ScalarModel component = switch (typeChoice % 3) {
                case 0 -> new NormalScalarModel(position, 0.08 + rng.nextDouble() * 0.1, -1.0, 1.0);
                case 1 -> {
                    double alpha = 1.5 + rng.nextDouble() * 3.0;
                    double beta = 1.5 + rng.nextDouble() * 3.0;
                    double lower = position - 0.15;
                    double upper = position + 0.15;
                    yield new BetaScalarModel(alpha, beta, Math.max(-1.0, lower), Math.min(1.0, upper));
                }
                default -> {
                    double halfWidth = 0.08 + rng.nextDouble() * 0.08;
                    yield new UniformScalarModel(
                        Math.max(-1.0, position - halfWidth),
                        Math.min(1.0, position + halfWidth));
                }
            };

            components.add(component);
            weights.add(0.2 + rng.nextDouble() * 0.3);
        }

        // Normalize weights
        double[] weightArray = new double[weights.size()];
        double total = weights.stream().mapToDouble(Double::doubleValue).sum();
        for (int i = 0; i < weights.size(); i++) {
            weightArray[i] = weights.get(i) / total;
        }

        return new TestFixture(
            "random_mixed_" + variant,
            "randomized",
            new CompositeScalarModel(components, weightArray),
            String.format("Random mixed: %d components of varying types", numComponents));
    }

    /// Creates a composite model from components and weights.
    private static CompositeScalarModel createComposite(List<ScalarModel> components, double[] weights) {
        return new CompositeScalarModel(components, weights);
    }

    /// Provides fixture names for parameterized tests.
    static Stream<String> fixtureNames() {
        return createFixtures().stream().map(TestFixture::name);
    }

    /// Finds a fixture by name.
    private static TestFixture findFixture(String name) {
        return createFixtures().stream()
            .filter(f -> f.name().equals(name))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Unknown fixture: " + name));
    }

    // ==================== Test Methods ====================

    @ParameterizedTest(name = "{0}")
    @MethodSource("fixtureNames")
    @Order(1)
    void testRoundTrip(String fixtureName) {
        TestFixture fixture = findFixture(fixtureName);
        ScalarModel genModel = fixture.model();

        // Seed based on fixture name for reproducibility without overfitting
        long seed = BASE_SEED + fixtureName.hashCode();

        System.out.println("\n" + "─".repeat(70));
        System.out.printf("Testing: %s (%s)%n", fixture.name(), fixture.category());
        System.out.printf("  Description: %s%n", fixture.description());
        System.out.printf("  Gen model: %s%n", describeModel(genModel));
        System.out.printf("  Seed: %d%n", seed);

        // Phase 1: Generate variates from Gen model
        float[] genVariates = generateVariates(genModel, SAMPLE_SIZE, seed);

        // Phase 2: Extract model (Ext)
        BestFitSelector selector = BestFitSelector.pearsonMultimodalSelector(MAX_MODES);
        ScalarModel extModel = extractViaStreamingPath(genVariates, selector);
        System.out.printf("  Ext model: %s%n", describeModel(extModel));

        // Phase 3: Compare Gen vs Ext
        boolean genExtMatch = EQUIVALENCE_CHECKER.areEquivalent(genModel, extModel);
        String genExtReason = EQUIVALENCE_CHECKER.getEquivalenceReason(genModel, extModel);
        System.out.printf("  Gen→Ext: %s%n", genExtMatch ? "✓ MATCH" : "✗ MISMATCH");
        if (genExtReason != null) {
            System.out.printf("    Reason: %s%n", genExtReason);
        }

        // Phase 4: Generate from Ext and re-extract (R-T)
        float[] extVariates = generateVariates(extModel, SAMPLE_SIZE, seed + 1);
        BestFitSelector rtSelector = BestFitSelector.strictRoundTripSelector(MAX_MODES);
        ScalarModel rtModel = extractViaStreamingPath(extVariates, rtSelector);
        System.out.printf("  R-T model: %s%n", describeModel(rtModel));

        // Phase 5: Compare Ext vs R-T
        boolean extRtMatch = EQUIVALENCE_CHECKER.areEquivalent(extModel, rtModel);
        System.out.printf("  Ext→R-T: %s%n", extRtMatch ? "✓ STABLE" : "✗ UNSTABLE");

        // Both Gen→Ext accuracy AND Ext→R-T stability are required
        assertTrue(genExtMatch,
            String.format("Gen→Ext accuracy failed for %s: Gen=%s, Ext=%s",
                fixtureName, describeModel(genModel), describeModel(extModel)));

        assertTrue(extRtMatch,
            String.format("Ext→R-T stability failed for %s: Ext=%s, R-T=%s",
                fixtureName, describeModel(extModel), describeModel(rtModel)));
    }

    @Test
    @Order(100)
    void testAggregateStatistics() {
        List<TestFixture> fixtures = createFixtures();

        System.out.println("\n" + "═".repeat(80));
        System.out.println("EXTENDED CURVE VARIETY - AGGREGATE STATISTICS");
        System.out.println("═".repeat(80));

        Map<String, int[]> categoryStats = new LinkedHashMap<>();
        int totalPassed = 0, totalTests = 0;

        System.out.println("\nFixture                      | Category       | Gen→Ext | Ext→R-T");
        System.out.println("-----------------------------|----------------|---------|--------");

        for (TestFixture fixture : fixtures) {
            ScalarModel genModel = fixture.model();
            long seed = BASE_SEED + fixture.name().hashCode();
            float[] genVariates = generateVariates(genModel, SAMPLE_SIZE, seed);

            BestFitSelector selector = BestFitSelector.pearsonMultimodalSelector(MAX_MODES);
            ScalarModel extModel = extractViaStreamingPath(genVariates, selector);

            float[] extVariates = generateVariates(extModel, SAMPLE_SIZE, seed + 1);
            BestFitSelector rtSelector = BestFitSelector.strictRoundTripSelector(MAX_MODES);
            ScalarModel rtModel = extractViaStreamingPath(extVariates, rtSelector);

            boolean genExtMatch = EQUIVALENCE_CHECKER.areEquivalent(genModel, extModel);
            boolean extRtMatch = EQUIVALENCE_CHECKER.areEquivalent(extModel, rtModel);

            totalTests++;
            if (genExtMatch && extRtMatch) totalPassed++;

            // Track by category
            categoryStats.computeIfAbsent(fixture.category(), k -> new int[]{0, 0});
            int[] stats = categoryStats.get(fixture.category());
            stats[1]++; // total
            if (genExtMatch && extRtMatch) stats[0]++; // passed

            String genExtStatus = genExtMatch ? "  ✓  " : "  ✗  ";
            String extRtStatus = extRtMatch ? "  ✓  " : "  ✗  ";

            System.out.printf("%-28s | %-14s | %s | %s%n",
                fixture.name(), fixture.category(), genExtStatus, extRtStatus);
        }

        System.out.println("-----------------------------|----------------|---------|--------");

        // Category breakdown
        System.out.println("\nBy Category:");
        for (Map.Entry<String, int[]> entry : categoryStats.entrySet()) {
            int passed = entry.getValue()[0];
            int total = entry.getValue()[1];
            double rate = 100.0 * passed / total;
            System.out.printf("  %-20s: %d/%d (%.1f%%)%n", entry.getKey(), passed, total, rate);
        }

        double overallRate = 100.0 * totalPassed / totalTests;
        System.out.printf("\nOverall: %d/%d (%.1f%%)%n", totalPassed, totalTests, overallRate);
        System.out.println("═".repeat(80));

        // Require high overall pass rate
        assertTrue(overallRate >= 85.0,
            String.format("Overall pass rate %.1f%% below 85%% threshold", overallRate));
    }

    // ==================== Helper Methods ====================

    private ScalarModel extractViaStreamingPath(float[] values, BestFitSelector selector) {
        StreamingModelExtractor extractor = new StreamingModelExtractor(selector);
        extractor.setUniqueVectors(values.length);
        extractor.setAdaptiveEnabled(false);
        extractor.setMaxCompositeComponents(MAX_MODES);
        extractor.setNumaAware(false);

        DataspaceShape shape = new DataspaceShape(values.length, 1);
        extractor.initialize(shape);

        float[][] columnarData = new float[1][values.length];
        System.arraycopy(values, 0, columnarData[0], 0, values.length);
        extractor.accept(columnarData, 0);

        VectorSpaceModel model = extractor.complete();
        extractor.shutdown();

        return model.scalarModel(0);
    }

    private float[] generateVariates(ScalarModel model, int n, long seed) {
        float[] data = new float[n];
        Random rng = new Random(seed);

        if (model instanceof CompositeScalarModel composite) {
            ScalarModel[] components = composite.getScalarModels();
            double[] weights = composite.getWeights();
            double[] cumWeights = computeCumWeights(weights);

            for (int i = 0; i < n; i++) {
                double u = rng.nextDouble();
                int idx = findComponent(cumWeights, u);
                data[i] = (float) sampleFromModel(components[idx], rng);
            }
        } else {
            for (int i = 0; i < n; i++) {
                data[i] = (float) sampleFromModel(model, rng);
            }
        }
        return data;
    }

    private double[] computeCumWeights(double[] weights) {
        double[] cumWeights = new double[weights.length];
        double sum = 0;
        for (int i = 0; i < weights.length; i++) {
            sum += weights[i];
            cumWeights[i] = sum;
        }
        for (int i = 0; i < cumWeights.length; i++) {
            cumWeights[i] /= sum;
        }
        return cumWeights;
    }

    private int findComponent(double[] cumWeights, double u) {
        for (int c = 0; c < cumWeights.length; c++) {
            if (u <= cumWeights[c]) return c;
        }
        return cumWeights.length - 1;
    }

    private double sampleFromModel(ScalarModel model, Random rng) {
        if (model instanceof NormalScalarModel normal) {
            double lower = normal.lower();
            double upper = normal.upper();
            for (int i = 0; i < 1000; i++) {
                double sample = normal.getMean() + rng.nextGaussian() * normal.getStdDev();
                if (sample >= lower && sample <= upper) return sample;
            }
            return Math.max(lower, Math.min(upper, normal.getMean()));

        } else if (model instanceof UniformScalarModel uniform) {
            return uniform.getLower() + rng.nextDouble() * uniform.getRange();

        } else if (model instanceof BetaScalarModel beta) {
            double a = beta.getAlpha();
            double b = beta.getBeta();
            double x = sampleGamma(a, 1.0, rng);
            double y = sampleGamma(b, 1.0, rng);
            double sample = x / (x + y);
            return beta.getLower() + sample * (beta.getUpper() - beta.getLower());

        } else if (model instanceof GammaScalarModel gamma) {
            return sampleGamma(gamma.getShape(), gamma.getScale(), rng) + gamma.getLocation();

        } else if (model instanceof StudentTScalarModel studentT) {
            double nu = studentT.getDegreesOfFreedom();
            double chi2 = sampleGamma(nu / 2.0, 2.0, rng);
            double z = rng.nextGaussian();
            double t = z / Math.sqrt(chi2 / nu);
            return studentT.getLocation() + t * studentT.getScale();

        } else {
            // Fallback
            return -1.0 + rng.nextDouble() * 2.0;
        }
    }

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
            if (u < 1 - 0.0331 * (x * x) * (x * x)) return d * v * scale;
            if (Math.log(u) < 0.5 * x * x + d * (1 - v + Math.log(v))) return d * v * scale;
        }
    }

    private String describeModel(ScalarModel model) {
        if (model instanceof CompositeScalarModel composite) {
            int count = composite.getComponentCount();
            return String.format("Composite(%d modes)", count);
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
            case "gamma" -> "Gamma";
            case "student_t" -> "StudentT";
            default -> type;
        };
    }
}
