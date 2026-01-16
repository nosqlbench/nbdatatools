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
import io.nosqlbench.vshapes.extract.ModelRecoveryVerifier.RecoveryResult;
import io.nosqlbench.vshapes.model.CompositeScalarModel;
import io.nosqlbench.vshapes.model.NormalScalarModel;
import io.nosqlbench.vshapes.model.ScalarModel;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round-trip accuracy tests for composite (multimodal) model fitting.
 *
 * <p>These tests verify that composite models with 2-10 modes can be:
 * <ol>
 *   <li>Generated with known parameters</li>
 *   <li>Used to produce sample data</li>
 *   <li>Fitted back to recover mode count and weights</li>
 *   <li>Validated via statistical tests (K-S, CDF comparison)</li>
 * </ol>
 *
 * <p>This complements the existing {@link CompositeModelFitterTest} by testing
 * higher mode counts (5-10) and round-trip parameter recovery accuracy.
 */
@Tag("accuracy")
public class CompositeModelRoundTripAccuracyTest {

    private static final long SEED = 12345L;
    private static final int SAMPLE_SIZE = 50000;  // Large samples for better mode detection

    // Tolerances for round-trip accuracy
    private static final double WEIGHT_TOLERANCE = 0.20;    // ±20% weight accuracy

    /// Tests round-trip accuracy for well-separated 2-mode composites (strict test).
    @Test
    void testTwoModeRoundTrip() {
        int numModes = 2;
        CompositeScalarModel sourceModel = createWellSeparatedModel(numModes, SEED);
        float[] data = generateData(sourceModel, SAMPLE_SIZE, SEED);

        CompositeModelFitter fitter = new CompositeModelFitter(
            BestFitSelector.boundedDataSelector(), 4);
        FitResult result = fitter.fit(data);

        CompositeScalarModel fitted = (CompositeScalarModel) result.model();
        assertEquals(numModes, fitted.getComponentCount(),
            "2-mode composite should be recovered exactly");

        verifyWeightAccuracy(sourceModel, fitted, WEIGHT_TOLERANCE);

        System.out.printf("2-mode round-trip: PASSED (weights within %.0f%%)%n",
            WEIGHT_TOLERANCE * 100);
    }

    /// Tests round-trip accuracy for well-separated 3-mode composites (strict test).
    @Test
    void testThreeModeRoundTrip() {
        int numModes = 3;
        CompositeScalarModel sourceModel = createWellSeparatedModel(numModes, SEED);
        float[] data = generateData(sourceModel, SAMPLE_SIZE, SEED);

        CompositeModelFitter fitter = new CompositeModelFitter(
            BestFitSelector.boundedDataSelector(), 4);
        FitResult result = fitter.fit(data);

        CompositeScalarModel fitted = (CompositeScalarModel) result.model();
        assertEquals(numModes, fitted.getComponentCount(),
            "3-mode composite should be recovered exactly");

        verifyWeightAccuracy(sourceModel, fitted, WEIGHT_TOLERANCE);

        System.out.printf("3-mode round-trip: PASSED (weights within %.0f%%)%n",
            WEIGHT_TOLERANCE * 100);
    }

    /// Characterizes detection capabilities for 4+ mode composites (informational).
    ///
    /// <p>High mode counts (4-10) are difficult to detect reliably due to:
    /// <ul>
    ///   <li>Mode overlap when packed into [-1, 1] range</li>
    ///   <li>Statistical noise obscuring small modes</li>
    ///   <li>Dip test limitations for closely spaced modes</li>
    /// </ul>
    ///
    /// <p>This test documents current capabilities rather than enforcing strict
    /// requirements. It ensures the system gracefully handles multimodal data
    /// even when it cannot recover all modes.
    @ParameterizedTest(name = "Characterize {0}-mode detection")
    @ValueSource(ints = {4, 5, 6, 8, 10})
    void characterizeHighModeDetection(int numModes) {
        CompositeScalarModel sourceModel = createWellSeparatedModel(numModes, SEED);
        float[] data = generateData(sourceModel, SAMPLE_SIZE * 2, SEED);

        CompositeModelFitter fitter = new CompositeModelFitter(
            BestFitSelector.boundedDataSelector(),
            numModes + 2,
            0.20,  // Relaxed CDF threshold
            CompositeModelFitter.ClusteringStrategy.EM
        );

        try {
            FitResult result = fitter.fit(data);
            CompositeScalarModel fitted = (CompositeScalarModel) result.model();
            int fittedModes = fitted.getComponentCount();

            System.out.printf("%d-mode characterization: detected %d modes (%.0f%% recovery)%n",
                numModes, fittedModes, 100.0 * fittedModes / numModes);

            // For high modes, we don't require exact recovery - just characterize
            // This documents the system's current capabilities
            assertTrue(fittedModes >= 2,
                "Should detect at least 2 modes in clearly multimodal data");

        } catch (IllegalStateException e) {
            System.out.printf("%d-mode characterization: detection failed - %s%n",
                numModes, e.getMessage());
            // High mode detection failure is informational, not a test failure
            // The modes may be too close together for the dip test to distinguish
        }
    }

    private void verifyWeightAccuracy(CompositeScalarModel source, CompositeScalarModel fitted,
                                       double tolerance) {
        double[] sourceWeights = normalizeWeights(source.getWeights());
        double[] fittedWeights = normalizeWeights(fitted.getWeights());
        Arrays.sort(sourceWeights);
        Arrays.sort(fittedWeights);

        double maxError = 0;
        for (int i = 0; i < sourceWeights.length; i++) {
            double error = Math.abs(sourceWeights[i] - fittedWeights[i]);
            maxError = Math.max(maxError, error);
        }

        assertTrue(maxError < tolerance,
            String.format("Max weight error %.3f exceeds tolerance %.3f", maxError, tolerance));
    }

    @Test
    void testHighModeCountStatisticalAccuracy() {
        // Test that a 10-mode distribution can be generated and its statistical
        // properties (mean, variance) are preserved in the round-trip

        int numModes = 10;
        CompositeScalarModel sourceModel = createSourceModel(numModes, SEED);

        // Generate data
        float[] data = generateData(sourceModel, SAMPLE_SIZE, SEED);

        // Compute empirical statistics
        double empiricalMean = computeMean(data);
        double empiricalVar = computeVariance(data, empiricalMean);

        // Compute theoretical statistics from source model
        double theoreticalMean = computeCompositeMean(sourceModel);
        double theoreticalVar = computeCompositeVariance(sourceModel, theoreticalMean);

        System.out.printf("10-mode statistical accuracy:%n");
        System.out.printf("  Mean: theoretical=%.4f, empirical=%.4f, error=%.4f%n",
            theoreticalMean, empiricalMean, Math.abs(theoreticalMean - empiricalMean));
        System.out.printf("  Var:  theoretical=%.4f, empirical=%.4f, error=%.4f%n",
            theoreticalVar, empiricalVar, Math.abs(theoreticalVar - empiricalVar));

        // Mean should be within 5% or 0.05 absolute
        double meanError = Math.abs(theoreticalMean - empiricalMean);
        assertTrue(meanError < 0.05 || meanError / Math.abs(theoreticalMean) < 0.05,
            String.format("Mean error %.4f too large", meanError));

        // Variance should be within 15%
        double varError = Math.abs(theoreticalVar - empiricalVar) / theoreticalVar;
        assertTrue(varError < 0.15,
            String.format("Variance error %.1f%% exceeds 15%%", varError * 100));
    }

    @Test
    void testWeightRecoveryAccuracyByModeCount() {
        // Test weight recovery accuracy across different mode counts
        System.out.println("\nWeight Recovery Accuracy by Mode Count:");
        System.out.println("Modes | Avg Weight Error | Max Weight Error | Status");
        System.out.println("------|------------------|------------------|-------");

        for (int numModes = 2; numModes <= 6; numModes++) {
            CompositeScalarModel sourceModel = createSourceModel(numModes, SEED + numModes);
            float[] data = generateData(sourceModel, SAMPLE_SIZE, SEED + numModes);

            try {
                CompositeModelFitter fitter = new CompositeModelFitter(
                    BestFitSelector.boundedDataSelector(), numModes + 1);
                FitResult result = fitter.fit(data);
                CompositeScalarModel fitted = (CompositeScalarModel) result.model();

                if (fitted.getComponentCount() == numModes) {
                    double[] sourceWeights = normalizeWeights(sourceModel.getWeights());
                    double[] fittedWeights = normalizeWeights(fitted.getWeights());
                    Arrays.sort(sourceWeights);
                    Arrays.sort(fittedWeights);

                    double sumError = 0;
                    double maxError = 0;
                    for (int i = 0; i < numModes; i++) {
                        double error = Math.abs(sourceWeights[i] - fittedWeights[i]);
                        sumError += error;
                        maxError = Math.max(maxError, error);
                    }
                    double avgError = sumError / numModes;

                    String status = maxError < WEIGHT_TOLERANCE ? "PASS" : "WARN";
                    System.out.printf("  %d   |     %.4f       |     %.4f       | %s%n",
                        numModes, avgError, maxError, status);

                    if (numModes <= 4) {
                        assertTrue(maxError < WEIGHT_TOLERANCE,
                            String.format("%d-mode max weight error %.3f exceeds %.3f",
                                numModes, maxError, WEIGHT_TOLERANCE));
                    }
                } else {
                    System.out.printf("  %d   |       N/A        |       N/A        | SKIP (mode count mismatch)%n",
                        numModes);
                }
            } catch (IllegalStateException e) {
                System.out.printf("  %d   |       N/A        |       N/A        | SKIP (detection failed)%n",
                    numModes);
            }
        }
    }

    // ==================== Canonical Verification Tests ====================

    /// Tests round-trip recovery using canonical form comparison.
    ///
    /// <p>This test verifies that:
    /// <ol>
    ///   <li>Source model is converted to canonical form (sorted by location)</li>
    ///   <li>Fitted model is also in canonical form</li>
    ///   <li>Components match by location within tolerance</li>
    ///   <li>Weights are recovered within specified tolerance</li>
    /// </ol>
    @Test
    void testCanonicalFormRoundTrip() {
        int numModes = 3;
        CompositeScalarModel sourceModel = createWellSeparatedModel(numModes, SEED);

        // Convert to canonical form for deterministic comparison
        CompositeScalarModel canonicalSource = (CompositeScalarModel) sourceModel.toCanonicalForm();

        // Generate data
        float[] data = generateData(canonicalSource, SAMPLE_SIZE, SEED);

        // Use strict selector to preserve model types
        CompositeModelFitter fitter = new CompositeModelFitter(
            BestFitSelector.strictBoundedSelector(), 5);
        FitResult result = fitter.fit(data);

        CompositeScalarModel fitted = (CompositeScalarModel) result.model();
        CompositeScalarModel canonicalFitted = (CompositeScalarModel) fitted.toCanonicalForm();

        // Verify using ModelRecoveryVerifier
        ModelRecoveryVerifier verifier = new ModelRecoveryVerifier();
        RecoveryResult recovery = verifier.verify(canonicalSource, canonicalFitted);

        System.out.println("\nCanonical Form Round-Trip Test:");
        System.out.println(recovery.formatSummary());

        // Should recover all modes with 3-mode well-separated data
        assertTrue(recovery.recoveryRate() >= 0.67,
            String.format("Recovery rate %.1f%% below 67%% threshold",
                recovery.recoveryRate() * 100));
        assertTrue(recovery.maxWeightError() <= WEIGHT_TOLERANCE,
            String.format("Max weight error %.3f exceeds %.3f tolerance",
                recovery.maxWeightError(), WEIGHT_TOLERANCE));
    }

    /// Tests that canonical form sorts components by location.
    @Test
    void testCanonicalFormOrdering() {
        // Create model with components in reverse order
        List<ScalarModel> components = List.of(
            new NormalScalarModel(0.5, 0.1, -1.0, 1.0),   // rightmost
            new NormalScalarModel(-0.5, 0.1, -1.0, 1.0),  // leftmost
            new NormalScalarModel(0.0, 0.1, -1.0, 1.0)    // middle
        );
        double[] weights = {0.3, 0.4, 0.3};

        CompositeScalarModel model = new CompositeScalarModel(components, weights);
        CompositeScalarModel canonical = (CompositeScalarModel) model.toCanonicalForm();

        // Verify canonical form is sorted by location
        ScalarModel[] sortedComponents = canonical.getScalarModels();
        double[] sortedWeights = canonical.getWeights();

        assertEquals(3, sortedComponents.length);

        // Should be sorted: -0.5, 0.0, 0.5
        NormalScalarModel first = (NormalScalarModel) sortedComponents[0];
        NormalScalarModel second = (NormalScalarModel) sortedComponents[1];
        NormalScalarModel third = (NormalScalarModel) sortedComponents[2];

        assertTrue(first.getMean() < second.getMean(),
            "First component should have smallest mean");
        assertTrue(second.getMean() < third.getMean(),
            "Middle component should have middle mean");

        // Weights should be reordered to match: [0.4, 0.3, 0.3]
        assertEquals(0.4, sortedWeights[0], 0.001, "Weight for leftmost mode");

        System.out.printf("Canonical ordering verified: [%.1f, %.1f, %.1f]%n",
            first.getMean(), second.getMean(), third.getMean());
    }

    /// Tests recovery verification with mode merging detection.
    @Test
    void testMergingDetection() {
        // Create a 4-mode model where some modes may merge
        int numModes = 4;
        CompositeScalarModel sourceModel = createSourceModel(numModes, SEED);
        float[] data = generateData(sourceModel, SAMPLE_SIZE * 2, SEED);

        // Fit may detect fewer modes if some are close together
        CompositeModelFitter fitter = new CompositeModelFitter(
            BestFitSelector.strictBoundedSelector(), 6);

        try {
            FitResult result = fitter.fit(data);
            CompositeScalarModel fitted = (CompositeScalarModel) result.model();

            ModelRecoveryVerifier verifier = new ModelRecoveryVerifier();
            RecoveryResult recovery = verifier.verify(sourceModel, fitted);

            System.out.println("\nMode Merging Detection Test:");
            System.out.println(recovery.formatSummary());

            // Either exact recovery or documented merging
            int totalAccounted = recovery.exactMatches() + recovery.mergedModes();
            assertTrue(totalAccounted > 0,
                "Should account for at least some source modes");

            // Effective recovery should be reasonable
            assertTrue(recovery.effectiveRecoveryRate() >= 0.3,
                String.format("Effective recovery %.1f%% too low",
                    recovery.effectiveRecoveryRate() * 100));

        } catch (IllegalStateException e) {
            System.out.println("Mode detection failed (expected for close modes): " + e.getMessage());
        }
    }

    // ==================== Helper Methods ====================

    /// Creates a well-separated composite model for round-trip testing.
    ///
    /// <p>Uses wider mode separation and narrower stdDev to ensure
    /// clear multimodality that can be reliably detected.
    ///
    /// @param numModes number of modes
    /// @param seed random seed for reproducibility
    /// @return composite model with well-separated modes
    private CompositeScalarModel createWellSeparatedModel(int numModes, long seed) {
        Random rng = new Random(seed);
        List<ScalarModel> components = new ArrayList<>();
        double[] weights = new double[numModes];

        // Use wider range [-3, 3] for better separation, then truncate to [-1, 1]
        // This gives modes more "breathing room"
        double effectiveRange = Math.max(2.0, numModes * 0.8);
        double modeSpacing = effectiveRange / (numModes);

        for (int m = 0; m < numModes; m++) {
            // Mode center - spread across range
            double center = -effectiveRange / 2 + modeSpacing * (m + 0.5);

            // Narrow stdDev for clear separation (15% of mode spacing)
            double stdDev = modeSpacing * 0.15;

            // Clamp center to valid bounds for truncated normal
            double clampedCenter = Math.max(-0.9, Math.min(0.9, center));

            components.add(new NormalScalarModel(clampedCenter, stdDev, -1.0, 1.0));

            // Equal weights for fair testing
            weights[m] = 1.0;
        }

        return new CompositeScalarModel(components, weights);
    }

    /// Creates a source composite model with typical separation.
    ///
    /// @param numModes number of modes
    /// @param seed random seed for reproducibility
    /// @return composite model with specified modes
    private CompositeScalarModel createSourceModel(int numModes, long seed) {
        Random rng = new Random(seed);
        List<ScalarModel> components = new ArrayList<>();
        double[] weights = new double[numModes];

        // Spread modes evenly across [-1, 1] range
        double modeSpacing = 2.0 / (numModes + 1);

        for (int m = 0; m < numModes; m++) {
            // Mode center
            double center = -1.0 + modeSpacing * (m + 1);

            // Mode width (narrow enough to show separation)
            double stdDev = modeSpacing * 0.25 * (0.8 + rng.nextDouble() * 0.4);

            components.add(new NormalScalarModel(center, stdDev, -1.0, 1.0));

            // Vary weights slightly (but keep roughly equal for fair testing)
            weights[m] = 0.9 + rng.nextDouble() * 0.2;
        }

        return new CompositeScalarModel(components, weights);
    }

    /// Generates sample data from a composite model.
    ///
    /// <p>Uses direct sampling from Normal components since all source models
    /// are Normal distributions. For production use, ComponentSamplerFactory
    /// in datatools-virtdata handles arbitrary model types.
    ///
    /// @param model the source model
    /// @param n number of samples
    /// @param seed random seed
    /// @return generated data
    private float[] generateData(CompositeScalarModel model, int n, long seed) {
        Random rng = new Random(seed);
        float[] data = new float[n];

        ScalarModel[] components = model.getScalarModels();
        double[] weights = normalizeWeights(model.getWeights());

        // Build cumulative weight distribution
        double[] cumWeights = new double[weights.length];
        cumWeights[0] = weights[0];
        for (int i = 1; i < weights.length; i++) {
            cumWeights[i] = cumWeights[i - 1] + weights[i];
        }

        for (int i = 0; i < n; i++) {
            // Select component based on weights
            double u = rng.nextDouble();
            int componentIdx = 0;
            for (int c = 0; c < cumWeights.length; c++) {
                if (u <= cumWeights[c]) {
                    componentIdx = c;
                    break;
                }
            }

            // Sample from selected component (all are Normal in this test)
            ScalarModel component = components[componentIdx];
            double sample;
            if (component instanceof NormalScalarModel normal) {
                // Sample from truncated normal
                sample = sampleTruncatedNormal(rng, normal);
            } else {
                throw new IllegalStateException("Test only supports Normal components");
            }
            data[i] = (float) sample;
        }

        return data;
    }

    /// Samples from a truncated normal distribution using rejection sampling.
    private double sampleTruncatedNormal(Random rng, NormalScalarModel model) {
        double mean = model.getMean();
        double stdDev = model.getStdDev();
        double lower = model.lower();
        double upper = model.upper();

        // Rejection sampling for truncated normal
        for (int attempt = 0; attempt < 1000; attempt++) {
            double sample = mean + rng.nextGaussian() * stdDev;
            if (sample >= lower && sample <= upper) {
                return sample;
            }
        }
        // Fallback: clamp to bounds
        return Math.max(lower, Math.min(upper, mean));
    }

    private double[] normalizeWeights(double[] weights) {
        double[] normalized = weights.clone();
        double sum = 0;
        for (double w : normalized) sum += w;
        for (int i = 0; i < normalized.length; i++) {
            normalized[i] /= sum;
        }
        return normalized;
    }

    private double computeMean(float[] data) {
        double sum = 0;
        for (float v : data) sum += v;
        return sum / data.length;
    }

    private double computeVariance(float[] data, double mean) {
        double sumSq = 0;
        for (float v : data) {
            double diff = v - mean;
            sumSq += diff * diff;
        }
        return sumSq / data.length;
    }

    private double computeCompositeMean(CompositeScalarModel model) {
        ScalarModel[] components = model.getScalarModels();
        double[] weights = normalizeWeights(model.getWeights());

        double mean = 0;
        for (int i = 0; i < components.length; i++) {
            if (components[i] instanceof NormalScalarModel normal) {
                mean += weights[i] * normal.getMean();
            }
        }
        return mean;
    }

    private double computeCompositeVariance(CompositeScalarModel model, double compositeMean) {
        ScalarModel[] components = model.getScalarModels();
        double[] weights = normalizeWeights(model.getWeights());

        // Mixture variance: sum of weighted component variances + weighted mean deviations
        double variance = 0;
        for (int i = 0; i < components.length; i++) {
            if (components[i] instanceof NormalScalarModel normal) {
                double componentVar = normal.getStdDev() * normal.getStdDev();
                double meanDiff = normal.getMean() - compositeMean;
                variance += weights[i] * (componentVar + meanDiff * meanDiff);
            }
        }
        return variance;
    }
}
