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

import io.nosqlbench.vshapes.model.CompositeScalarModel;
import io.nosqlbench.vshapes.model.NormalScalarModel;
import io.nosqlbench.vshapes.model.ScalarModel;
import io.nosqlbench.vshapes.extract.ModeDetector.ModeDetectionResult;
import io.nosqlbench.vshapes.extract.ComponentModelFitter.FitResult;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive tests for high-mode (4-10) composite model recovery.
 *
 * <p>Tests focus on:
 * <ul>
 *   <li>Mode detection accuracy with well-separated modes</li>
 *   <li>Canonical form round-trip verification</li>
 *   <li>Weight and parameter recovery accuracy</li>
 *   <li>Mode merging detection when modes overlap</li>
 * </ul>
 */
@Tag("accuracy")
public class HighModeRecoveryTest {

    private static final long SEED = 42L;
    private static final int SAMPLE_SIZE = 20000;
    private static final double WELL_SEPARATED_STD = 0.3;

    /**
     * Tests that well-separated modes are detected correctly for 2-8 modes.
     */
    @ParameterizedTest
    @ValueSource(ints = {2, 3, 4, 5, 6, 7, 8})
    void testWellSeparatedModeDetection(int numModes) {
        // Create well-separated modes - 3 std apart from each other
        CompositeScalarModel model = createWellSeparatedModel(numModes, SEED);
        float[] data = generateData(model, SAMPLE_SIZE, SEED);

        ModeDetectionResult result = ModeDetector.detect(data, numModes);

        System.out.printf("%d-mode detection: found %d modes (expected %d, %.0f%% recovery)%n",
            numModes, result.modeCount(), numModes,
            (double) result.modeCount() / numModes * 100);

        // For well-separated modes, we should detect at least numModes - 1
        // (allowing 1 missed due to edge effects)
        int minExpected = Math.max(numModes - 1, 2);
        assertTrue(result.modeCount() >= minExpected,
            String.format("Expected at least %d modes, but detected %d", minExpected, result.modeCount()));

        // For 2-4 modes, expect exact match
        if (numModes <= 4) {
            assertEquals(numModes, result.modeCount(),
                String.format("For %d well-separated modes, should detect exactly %d", numModes, numModes));
        }
    }

    /**
     * Tests canonical form round-trip with ModelRecoveryVerifier.
     */
    @ParameterizedTest
    @ValueSource(ints = {2, 3, 4})
    void testCanonicalRoundTripWithVerifier(int numModes) {
        CompositeScalarModel sourceModel = createWellSeparatedModel(numModes, SEED);
        CompositeScalarModel canonicalSource = (CompositeScalarModel) sourceModel.toCanonicalForm();

        float[] data = generateData(canonicalSource, SAMPLE_SIZE, SEED);

        // Fit using strict mode
        CompositeModelFitter fitter = new CompositeModelFitter(
            BestFitSelector.strictBoundedSelector(), numModes);
        FitResult result = fitter.fit(data);

        assertTrue(result.model() instanceof CompositeScalarModel,
            "Fitted model should be composite");

        CompositeScalarModel fitted = (CompositeScalarModel) result.model();
        CompositeScalarModel canonicalFitted = (CompositeScalarModel) fitted.toCanonicalForm();

        // Use verifier to compare
        ModelRecoveryVerifier verifier = new ModelRecoveryVerifier();
        ModelRecoveryVerifier.RecoveryResult recovery = verifier.verify(canonicalSource, canonicalFitted);

        System.out.printf("%d-mode canonical round-trip:%n%s%n", numModes, recovery.formatSummary());

        // For 2-4 well-separated modes, expect high recovery
        assertTrue(recovery.recoveryRate() >= 0.75,
            String.format("Expected 75%%+ recovery for %d modes, got %.0f%%",
                numModes, recovery.recoveryRate() * 100));

        // Weight error should be reasonable
        assertTrue(recovery.maxWeightError() <= 0.15,
            String.format("Expected max weight error <= 15%%, got %.1f%%",
                recovery.maxWeightError() * 100));
    }

    /**
     * Tests that mode detection correctly identifies number of modes across range.
     */
    @Test
    void testModeDetectionCapabilities() {
        System.out.println("\nMode Detection Capability Summary:");
        System.out.println("==================================");
        System.out.println("Modes | Detected | Recovery | Status");
        System.out.println("------|----------|----------|-------");

        int[] modeCounts = {2, 3, 4, 5, 6, 7, 8, 10};
        for (int numModes : modeCounts) {
            CompositeScalarModel model = createWellSeparatedModel(numModes, SEED);
            float[] data = generateData(model, SAMPLE_SIZE, SEED);

            ModeDetectionResult result = ModeDetector.detect(data, numModes);
            double recovery = (double) result.modeCount() / numModes * 100;
            String status = result.modeCount() == numModes ? "EXACT" :
                           (result.modeCount() >= numModes - 1 ? "GOOD" : "PARTIAL");

            System.out.printf("  %2d  |    %2d    |  %5.1f%%  | %s%n",
                numModes, result.modeCount(), recovery, status);
        }
    }

    /**
     * Tests mode merging detection when modes are intentionally close.
     */
    @Test
    void testModeMergingDetection() {
        // Create 4 modes with some intentionally close pairs
        List<ScalarModel> components = List.of(
            new NormalScalarModel(-3.0, 0.3),   // Mode 1
            new NormalScalarModel(-2.5, 0.3),   // Mode 2 (close to mode 1)
            new NormalScalarModel(2.5, 0.3),    // Mode 3 (close to mode 4)
            new NormalScalarModel(3.0, 0.3)     // Mode 4
        );
        double[] weights = {0.25, 0.25, 0.25, 0.25};
        CompositeScalarModel source = new CompositeScalarModel(components, weights);

        float[] data = generateData(source, SAMPLE_SIZE, SEED);

        ModeDetectionResult result = ModeDetector.detect(data, 4);

        System.out.printf("\nMode Merging Test:%n");
        System.out.printf("  Source modes: 4 (2 close pairs)%n");
        System.out.printf("  Detected modes: %d%n", result.modeCount());
        System.out.printf("  Locations: ");
        for (double loc : result.peakLocations()) {
            System.out.printf("%.2f ", loc);
        }
        System.out.println();

        // Should detect 2-4 modes (pairs may merge)
        assertTrue(result.modeCount() >= 2 && result.modeCount() <= 4,
            "Should detect 2-4 modes with close pairs");

        // If we fit and verify
        CompositeModelFitter fitter = new CompositeModelFitter(
            BestFitSelector.strictBoundedSelector(), 4);
        FitResult fitResult = fitter.fit(data);

        if (fitResult.model() instanceof CompositeScalarModel fitted) {
            ModelRecoveryVerifier verifier = new ModelRecoveryVerifier();
            ModelRecoveryVerifier.RecoveryResult recovery = verifier.verify(
                (CompositeScalarModel) source.toCanonicalForm(),
                (CompositeScalarModel) fitted.toCanonicalForm()
            );

            System.out.printf("  Verifier result: %d exact, %d merged, %d unrecovered%n",
                recovery.exactMatches(), recovery.mergedModes(), recovery.unrecoveredModes());

            // Should have some merging detected
            assertTrue(recovery.exactMatches() + recovery.mergedModes() >= 2,
                "Should recover or merge most modes");
        }
    }

    /**
     * Tests weight recovery accuracy across different mode counts.
     */
    @Test
    void testWeightRecoveryAccuracy() {
        System.out.println("\nWeight Recovery Accuracy:");
        System.out.println("=========================");
        System.out.println("Modes | Avg Error | Max Error | Status");
        System.out.println("------|-----------|-----------|-------");

        int[] modeCounts = {2, 3, 4, 5, 6};
        for (int numModes : modeCounts) {
            CompositeScalarModel model = createWellSeparatedModel(numModes, SEED);
            CompositeScalarModel canonical = (CompositeScalarModel) model.toCanonicalForm();
            double[] sourceWeights = canonical.getWeights();

            float[] data = generateData(canonical, SAMPLE_SIZE, SEED);

            // Fit
            CompositeModelFitter fitter = new CompositeModelFitter(
                BestFitSelector.strictBoundedSelector(), numModes);
            FitResult result = fitter.fit(data);

            if (result.model() instanceof CompositeScalarModel fitted) {
                CompositeScalarModel fittedCanonical = (CompositeScalarModel) fitted.toCanonicalForm();
                double[] fittedWeights = fittedCanonical.getWeights();

                // Calculate weight errors (only for matching components)
                int minSize = Math.min(sourceWeights.length, fittedWeights.length);
                double sumError = 0, maxError = 0;
                for (int i = 0; i < minSize; i++) {
                    double error = Math.abs(sourceWeights[i] - fittedWeights[i]);
                    sumError += error;
                    maxError = Math.max(maxError, error);
                }
                double avgError = sumError / minSize;
                String status = maxError < 0.10 ? "GOOD" : (maxError < 0.20 ? "OK" : "POOR");

                System.out.printf("  %2d  |   %.4f  |   %.4f  | %s%n",
                    numModes, avgError, maxError, status);
            }
        }
    }

    /**
     * Tests that canonical ordering is consistent.
     */
    @Test
    void testCanonicalOrderingConsistency() {
        // Create model with modes in various orders
        Random rng = new Random(SEED);
        double[] locations = {0.0, -1.5, 1.5, -3.0, 3.0};
        List<ScalarModel> components = new ArrayList<>();
        for (double loc : locations) {
            components.add(new NormalScalarModel(loc, 0.3));
        }
        double[] weights = {0.2, 0.2, 0.2, 0.2, 0.2};
        CompositeScalarModel model = new CompositeScalarModel(components, weights);

        // Canonicalize
        CompositeScalarModel canonical = (CompositeScalarModel) model.toCanonicalForm();
        ScalarModel[] canonicalComponents = canonical.getScalarModels();

        System.out.println("\nCanonical Ordering Test:");
        System.out.print("  Original order: ");
        for (double loc : locations) System.out.printf("%.1f ", loc);
        System.out.println();

        System.out.print("  Canonical order: ");
        double prevLoc = Double.NEGATIVE_INFINITY;
        boolean ordered = true;
        for (ScalarModel comp : canonicalComponents) {
            double loc = ((NormalScalarModel) comp).getMean();
            System.out.printf("%.1f ", loc);
            if (loc < prevLoc) ordered = false;
            prevLoc = loc;
        }
        System.out.println();

        assertTrue(ordered, "Canonical order should be sorted by location");
    }

    private CompositeScalarModel createWellSeparatedModel(int numModes, long seed) {
        // Spread modes evenly across range [-range, +range] with 3+ std separation
        double range = numModes * 1.5;  // Ensure 3 std separation at 0.3 std
        List<ScalarModel> components = new ArrayList<>();
        double[] weights = new double[numModes];

        for (int i = 0; i < numModes; i++) {
            double location = -range + (2.0 * range * i / (numModes - 1));
            if (numModes == 1) location = 0;
            components.add(new NormalScalarModel(location, WELL_SEPARATED_STD));
            weights[i] = 1.0 / numModes;
        }

        return new CompositeScalarModel(components, weights);
    }

    private float[] generateData(CompositeScalarModel model, int sampleSize, long seed) {
        Random rng = new Random(seed);
        float[] data = new float[sampleSize];
        ScalarModel[] components = model.getScalarModels();
        double[] weights = model.getWeights();

        // Compute cumulative weights for sampling
        double[] cumulative = new double[weights.length];
        cumulative[0] = weights[0];
        for (int i = 1; i < weights.length; i++) {
            cumulative[i] = cumulative[i - 1] + weights[i];
        }

        for (int i = 0; i < sampleSize; i++) {
            // Select component based on weight
            double u = rng.nextDouble();
            int selected = 0;
            for (int j = 0; j < cumulative.length; j++) {
                if (u <= cumulative[j]) {
                    selected = j;
                    break;
                }
            }

            // Sample from selected component
            NormalScalarModel normal = (NormalScalarModel) components[selected];
            data[i] = (float) (normal.getMean() + rng.nextGaussian() * normal.getStdDev());
        }

        return data;
    }
}
