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

import static org.junit.jupiter.api.Assertions.*;

/// Diagnostic test to verify moment computation and equivalence checking.
public class MomentEquivalenceDiagnosticTest {

    @Test
    void diagnoseMultiModeVsUniform() {
        // Simulate what happens with 4-mode composite vs uniform
        // Gen: 4 normal modes at -0.6, -0.2, 0.2, 0.6 with stddev=0.05
        CompositeScalarModel composite = new CompositeScalarModel(
            List.of(
                new NormalScalarModel(-0.6, 0.05, -1, 1),
                new NormalScalarModel(-0.2, 0.05, -1, 1),
                new NormalScalarModel(0.2, 0.05, -1, 1),
                new NormalScalarModel(0.6, 0.05, -1, 1)
            ),
            new double[]{0.25, 0.25, 0.25, 0.25}
        );

        // Ext: Uniform[-0.78, 0.78]
        UniformScalarModel uniform = new UniformScalarModel(-0.78, 0.78);

        System.out.println("=== 4-Mode Composite vs Uniform ===");
        System.out.println();

        System.out.println("Composite moments:");
        System.out.printf("  Mean:     %.6f%n", composite.getMean());
        System.out.printf("  Variance: %.6f%n", composite.getVariance());
        System.out.printf("  StdDev:   %.6f%n", composite.getStdDev());
        System.out.printf("  Skewness: %.6f%n", composite.getSkewness());
        System.out.printf("  Kurtosis: %.6f%n", composite.getKurtosis());
        System.out.println();

        // Wrap uniform for moment computation
        CompositeScalarModel wrappedUniform = CompositeScalarModel.wrap(uniform);

        System.out.println("Uniform moments:");
        System.out.printf("  Mean:     %.6f%n", wrappedUniform.getMean());
        System.out.printf("  Variance: %.6f%n", wrappedUniform.getVariance());
        System.out.printf("  StdDev:   %.6f%n", wrappedUniform.getStdDev());
        System.out.printf("  Skewness: %.6f%n", wrappedUniform.getSkewness());
        System.out.printf("  Kurtosis: %.6f%n", wrappedUniform.getKurtosis());
        System.out.println();

        // Compare
        double meanDiff = Math.abs(composite.getMean() - wrappedUniform.getMean());
        double varDiff = Math.abs(composite.getVariance() - wrappedUniform.getVariance());
        double skewDiff = Math.abs(composite.getSkewness() - wrappedUniform.getSkewness());
        double kurtDiff = Math.abs(composite.getKurtosis() - wrappedUniform.getKurtosis());

        System.out.println("Differences:");
        System.out.printf("  Mean diff:     %.6f%n", meanDiff);
        System.out.printf("  Variance diff: %.6f%n", varDiff);
        System.out.printf("  Skewness diff: %.6f%n", skewDiff);
        System.out.printf("  Kurtosis diff: %.6f%n", kurtDiff);
        System.out.println();

        // Check equivalence with different tolerances
        StatisticalEquivalenceChecker checker = new StatisticalEquivalenceChecker();

        System.out.println("Equivalence checks:");
        System.out.printf("  areEquivalent (default): %s%n", checker.areEquivalent(composite, uniform));
        System.out.printf("  areMomentEquivalent (0.15): %s%n", checker.areMomentEquivalent(composite, uniform, 0.15));
        System.out.printf("  areMomentEquivalent (0.30): %s%n", checker.areMomentEquivalent(composite, uniform, 0.30));
        System.out.printf("  areMomentEquivalent (0.50): %s%n", checker.areMomentEquivalent(composite, uniform, 0.50));
    }

    @Test
    void diagnoseOverlappingModesVsUniform() {
        // More realistic case: 10 modes that overlap significantly
        // Gen: 10 modes spread over [-0.9, 0.9] with stddev such that modes overlap
        double modeSpacing = 1.8 / 9;  // ~0.2 units between modes
        double stddev = 0.08;  // 2.5 stddev overlap between adjacent modes

        List<ScalarModel> modes = new java.util.ArrayList<>();
        double[] weights = new double[10];
        for (int i = 0; i < 10; i++) {
            double mean = -0.9 + i * modeSpacing;
            modes.add(new NormalScalarModel(mean, stddev, -1, 1));
            weights[i] = 0.1;
        }

        CompositeScalarModel composite = new CompositeScalarModel(modes, weights);
        UniformScalarModel uniform = new UniformScalarModel(-0.9, 0.9);

        System.out.println("=== 10-Mode Overlapping Composite vs Uniform ===");
        System.out.println();

        System.out.println("10-mode composite moments:");
        System.out.printf("  Mean:     %.6f%n", composite.getMean());
        System.out.printf("  Variance: %.6f%n", composite.getVariance());
        System.out.printf("  StdDev:   %.6f%n", composite.getStdDev());
        System.out.printf("  Skewness: %.6f%n", composite.getSkewness());
        System.out.printf("  Kurtosis: %.6f%n", composite.getKurtosis());
        System.out.println();

        CompositeScalarModel wrappedUniform = CompositeScalarModel.wrap(uniform);

        System.out.println("Uniform moments:");
        System.out.printf("  Mean:     %.6f%n", wrappedUniform.getMean());
        System.out.printf("  Variance: %.6f%n", wrappedUniform.getVariance());
        System.out.printf("  StdDev:   %.6f%n", wrappedUniform.getStdDev());
        System.out.printf("  Skewness: %.6f%n", wrappedUniform.getSkewness());
        System.out.printf("  Kurtosis: %.6f%n", wrappedUniform.getKurtosis());
        System.out.println();

        // Relative differences
        double varScale = Math.max(composite.getVariance(), wrappedUniform.getVariance());
        double kurtScale = Math.max(Math.abs(composite.getKurtosis()), Math.abs(wrappedUniform.getKurtosis()));

        System.out.println("Relative differences:");
        System.out.printf("  Mean:     %.2f%%%n", 100 * Math.abs(composite.getMean() - wrappedUniform.getMean()));
        System.out.printf("  Variance: %.2f%%%n", 100 * Math.abs(composite.getVariance() - wrappedUniform.getVariance()) / varScale);
        System.out.printf("  Skewness: %.2f%%%n", 100 * Math.abs(composite.getSkewness() - wrappedUniform.getSkewness()));
        System.out.printf("  Kurtosis: %.2f%%%n", 100 * Math.abs(composite.getKurtosis() - wrappedUniform.getKurtosis()) / kurtScale);
        System.out.println();

        StatisticalEquivalenceChecker checker = new StatisticalEquivalenceChecker();
        System.out.println("Equivalence checks:");
        System.out.printf("  areEquivalent (default): %s%n", checker.areEquivalent(composite, uniform));
        System.out.printf("  areMomentEquivalent (0.15): %s%n", checker.areMomentEquivalent(composite, uniform, 0.15));
        System.out.printf("  areMomentEquivalent (0.30): %s%n", checker.areMomentEquivalent(composite, uniform, 0.30));
    }

    @Test
    void diagnoseMultiModeVsBeta() {
        // Case from test output: trimodal composite extracted as Beta
        // Gen: ⊕3[𝒩(-0.5,0.07), β(4.57,2.47)[-0.15,0.15], ▭[0.43,0.58]]
        CompositeScalarModel composite = new CompositeScalarModel(
            List.of(
                new NormalScalarModel(-0.5, 0.07, -1, 1),
                new BetaScalarModel(4.57, 2.47, -0.15, 0.15),
                new UniformScalarModel(0.43, 0.58)
            ),
            new double[]{0.34, 0.34, 0.32}
        );

        // Ext: β(0.97,0.7)[-0.78,0.57]
        BetaScalarModel beta = new BetaScalarModel(0.97, 0.7, -0.78, 0.57);

        System.out.println("=== Trimodal Composite vs Beta ===");
        System.out.println();

        System.out.println("Composite moments:");
        System.out.printf("  Mean:     %.6f%n", composite.getMean());
        System.out.printf("  Variance: %.6f%n", composite.getVariance());
        System.out.printf("  StdDev:   %.6f%n", composite.getStdDev());
        System.out.printf("  Skewness: %.6f%n", composite.getSkewness());
        System.out.printf("  Kurtosis: %.6f%n", composite.getKurtosis());
        System.out.println();

        CompositeScalarModel wrappedBeta = CompositeScalarModel.wrap(beta);

        System.out.println("Beta moments:");
        System.out.printf("  Mean:     %.6f%n", wrappedBeta.getMean());
        System.out.printf("  Variance: %.6f%n", wrappedBeta.getVariance());
        System.out.printf("  StdDev:   %.6f%n", wrappedBeta.getStdDev());
        System.out.printf("  Skewness: %.6f%n", wrappedBeta.getSkewness());
        System.out.printf("  Kurtosis: %.6f%n", wrappedBeta.getKurtosis());
        System.out.println();

        // Check if moments are close
        double meanDiff = Math.abs(composite.getMean() - wrappedBeta.getMean());
        double varDiff = Math.abs(composite.getVariance() - wrappedBeta.getVariance());
        double skewDiff = Math.abs(composite.getSkewness() - wrappedBeta.getSkewness());
        double kurtDiff = Math.abs(composite.getKurtosis() - wrappedBeta.getKurtosis());

        System.out.println("Absolute differences:");
        System.out.printf("  Mean diff:     %.6f%n", meanDiff);
        System.out.printf("  Variance diff: %.6f%n", varDiff);
        System.out.printf("  Skewness diff: %.6f%n", skewDiff);
        System.out.printf("  Kurtosis diff: %.6f%n", kurtDiff);
        System.out.println();

        StatisticalEquivalenceChecker checker = new StatisticalEquivalenceChecker();
        System.out.println("Equivalence checks:");
        System.out.printf("  areEquivalent (default): %s%n", checker.areEquivalent(composite, beta));
        String reason = checker.getEquivalenceReason(composite, beta);
        System.out.printf("  Equivalence reason: %s%n", reason != null ? reason : "NOT EQUIVALENT");

        // Also check CDF difference
        System.out.println();
        System.out.println("CDF comparison (trimodal vs Beta):");
        double maxCdfDiff = 0;
        double lower = Math.min(composite.cdf(-1.0) < 0.01 ? -1.0 : -0.8,
                                 beta.cdf(-1.0) < 0.01 ? -1.0 : -0.8);
        double upper = Math.max(composite.cdf(1.0) > 0.99 ? 1.0 : 0.8,
                                 beta.cdf(1.0) > 0.99 ? 1.0 : 0.8);
        for (int i = 0; i <= 20; i++) {
            double x = lower + i * (upper - lower) / 20.0;
            double cdfComp = composite.cdf(x);
            double cdfBeta = beta.cdf(x);
            double diff = Math.abs(cdfComp - cdfBeta);
            maxCdfDiff = Math.max(maxCdfDiff, diff);
            System.out.printf("  x=%.2f: comp=%.4f, beta=%.4f, diff=%.4f%n", x, cdfComp, cdfBeta, diff);
        }
        System.out.printf("Max CDF difference: %.4f (tolerance: %.4f)%n",
            maxCdfDiff, StatisticalEquivalenceChecker.COMPOSITE_SIMPLE_CDF_TOLERANCE);
    }

    @Test
    void diagnoseTrimodalModeDetection() {
        // Generate data like dim 81: ⊕3[𝒩(-0.5,0.07), β(4.57,2.47)[-0.15,0.15], ▭[0.43,0.58]]
        java.util.Random rng = new java.util.Random(42);
        int n = 50000;
        float[] data = new float[n];

        int perMode = n / 3;

        // Mode 1: Normal at -0.5, stddev 0.07 (34%)
        for (int i = 0; i < perMode; i++) {
            double x = -0.5 + 0.07 * rng.nextGaussian();
            data[i] = (float) Math.max(-1, Math.min(1, x));
        }

        // Mode 2: Beta-like around 0 (34%)
        // Beta(4.57, 2.47) on [-0.15, 0.15] - right-skewed
        for (int i = perMode; i < 2 * perMode; i++) {
            // Approximate Beta sampling using rejection
            double x = -0.15 + 0.30 * (1 - Math.pow(rng.nextDouble(), 1.0/4.57));
            data[i] = (float) Math.max(-1, Math.min(1, x));
        }

        // Mode 3: Uniform in [0.43, 0.58] (32%)
        for (int i = 2 * perMode; i < n; i++) {
            data[i] = (float) (0.43 + 0.15 * rng.nextDouble());
        }

        System.out.println("=== Trimodal Data Mode Detection ===");
        System.out.println();
        System.out.println("Generated trimodal data:");
        System.out.println("  Mode 1: Normal(-0.5, 0.07) - " + perMode + " samples");
        System.out.println("  Mode 2: Beta-like around 0 - " + perMode + " samples");
        System.out.println("  Mode 3: Uniform[0.43, 0.58] - " + (n - 2*perMode) + " samples");
        System.out.println();

        // Test mode detection
        ModeDetector.ModeDetectionResult result = ModeDetector.detect(data, 10);
        System.out.println("ModeDetector.detect(data, 10):");
        System.out.println("  modeCount = " + result.modeCount());
        System.out.println("  isMultimodal = " + result.isMultimodal());
        System.out.printf("  dipStatistic = %.4f%n", result.dipStatistic());
        System.out.println("  peakLocations = " + java.util.Arrays.toString(result.peakLocations()));
        System.out.println();

        ModeDetector.ModeDetectionResult adaptiveResult = ModeDetector.detectAdaptive(data, 10);
        System.out.println("ModeDetector.detectAdaptive(data, 10):");
        System.out.println("  modeCount = " + adaptiveResult.modeCount());
        System.out.println("  isMultimodal = " + adaptiveResult.isMultimodal());
        System.out.printf("  dipStatistic = %.4f%n", adaptiveResult.dipStatistic());
        System.out.println("  peakLocations = " + java.util.Arrays.toString(adaptiveResult.peakLocations()));
        System.out.println();

        // Also test with different maxModes
        for (int maxModes : new int[]{3, 4, 5}) {
            ModeDetector.ModeDetectionResult r = ModeDetector.detectAdaptive(data, maxModes);
            System.out.printf("  maxModes=%d: modeCount=%d, isMultimodal=%s, dip=%.4f%n",
                maxModes, r.modeCount(), r.isMultimodal(), r.dipStatistic());
        }

        assertTrue(adaptiveResult.isMultimodal(),
            "Trimodal data should be detected as multimodal");
        assertTrue(adaptiveResult.modeCount() >= 2,
            "Should detect at least 2 modes");
    }

    @Test
    void diagnoseCdfEquivalence() {
        // Test if CDF-based equivalence is working
        // 10 overlapping modes should produce a CDF very close to uniform
        List<ScalarModel> modes = new java.util.ArrayList<>();
        double[] weights = new double[10];
        double stddev = 0.08;
        for (int i = 0; i < 10; i++) {
            double mean = -0.9 + i * 0.2;
            modes.add(new NormalScalarModel(mean, stddev, -1, 1));
            weights[i] = 0.1;
        }

        CompositeScalarModel composite = new CompositeScalarModel(modes, weights);
        UniformScalarModel uniform = new UniformScalarModel(-0.9, 0.9);

        System.out.println("=== CDF Comparison: 10-Mode vs Uniform ===");
        System.out.println();

        // Compare CDFs at multiple points
        double maxDiff = 0;
        for (int i = 0; i <= 20; i++) {
            double x = -0.9 + i * 0.09;  // 20 points from -0.9 to 0.9
            double cdfComp = composite.cdf(x);
            double cdfUnif = uniform.cdf(x);
            double diff = Math.abs(cdfComp - cdfUnif);
            maxDiff = Math.max(maxDiff, diff);
            System.out.printf("  x=%.2f: comp=%.4f, unif=%.4f, diff=%.4f%n", x, cdfComp, cdfUnif, diff);
        }
        System.out.println();
        System.out.printf("Max CDF difference: %.4f%n", maxDiff);
        System.out.printf("COMPOSITE_SIMPLE_CDF_TOLERANCE: %.4f%n", StatisticalEquivalenceChecker.COMPOSITE_SIMPLE_CDF_TOLERANCE);
        System.out.println();

        if (maxDiff <= StatisticalEquivalenceChecker.COMPOSITE_SIMPLE_CDF_TOLERANCE) {
            System.out.println("CDF equivalence: PASS (diff <= tolerance)");
        } else {
            System.out.println("CDF equivalence: FAIL (diff > tolerance)");
        }
    }
}
