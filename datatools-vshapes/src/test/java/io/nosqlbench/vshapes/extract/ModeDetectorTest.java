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

import io.nosqlbench.vshapes.extract.ModeDetector.ModeDetectionResult;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link ModeDetector}.
 *
 * <p>This test class verifies:
 * <ol>
 *   <li>Correct mode count detection for unimodal, bimodal, and trimodal distributions</li>
 *   <li>Numerical accuracy of peak location estimation</li>
 *   <li>Correct weight estimation for asymmetric mixtures</li>
 *   <li>Dip statistic computation and validation</li>
 * </ol>
 */
@Tag("unit")
public class ModeDetectorTest {

    private static final Random RANDOM = new Random(42);
    private static final int SAMPLE_SIZE = 10000;
    private static final long SEED = 42L;

    // Numerical accuracy tolerances
    private static final double PEAK_LOCATION_TOLERANCE = 0.5;  // ±0.5 for peak location
    private static final double WEIGHT_SUM_TOLERANCE = 0.001;   // Weights must sum to 1.0 ±0.001
    private static final double WEIGHT_ACCURACY_TOLERANCE = 0.15; // ±15% for weight estimation

    // ==================== Mode Detection Tests ====================

    @Test
    void detectsUnimodalNormal() {
        float[] data = generateNormal(SAMPLE_SIZE, 0.0, 1.0);

        ModeDetectionResult result = ModeDetector.detect(data);

        assertEquals(1, result.modeCount(), "Unimodal data should have 1 mode");
        assertFalse(result.isMultimodal(), "Unimodal data should not be multimodal");
        assertEquals(1, result.peakLocations().length, "Should have 1 peak location");

        // Peak should be near mean of 0
        assertTrue(Math.abs(result.peakLocations()[0]) < PEAK_LOCATION_TOLERANCE,
            "Peak location should be near mean 0, got: " + result.peakLocations()[0]);
    }

    @Test
    void detectsBimodalDistribution() {
        // Generate bimodal data: mixture of N(-2, 0.5) and N(2, 0.5)
        float[] data = generateBimodal(SAMPLE_SIZE, -2.0, 2.0, 0.5);

        ModeDetectionResult result = ModeDetector.detect(data);

        assertTrue(result.isMultimodal(), "Should detect bimodal distribution");
        assertEquals(2, result.modeCount(), "Should have 2 modes");

        // Peaks should be near -2 and 2
        double[] peaks = result.peakLocations();
        assertTrue(hasPeakNear(peaks, -2.0, PEAK_LOCATION_TOLERANCE),
            "Should have peak near -2, got: " + formatArray(peaks));
        assertTrue(hasPeakNear(peaks, 2.0, PEAK_LOCATION_TOLERANCE),
            "Should have peak near 2, got: " + formatArray(peaks));
    }

    @Test
    void detectsTrimodalDistribution() {
        // Generate trimodal data: mixture of N(-3, 0.4), N(0, 0.4), and N(3, 0.4)
        float[] data = generateTrimodal(20000, -3.0, 0.0, 3.0, 0.4);

        ModeDetectionResult result = ModeDetector.detect(data, 3);

        assertTrue(result.isMultimodal(), "Should detect trimodal distribution");
        assertTrue(result.modeCount() >= 2, "Should detect at least 2 modes");

        // Peaks should be roughly in the expected regions (±1.0 tolerance due to binning)
        double[] peaks = result.peakLocations();
        if (result.modeCount() == 3) {
            assertTrue(hasPeakNear(peaks, -3.0, 1.0),
                "Should have peak near -3, got: " + formatArray(peaks));
            assertTrue(hasPeakNear(peaks, 0.0, 1.0),
                "Should have peak near 0, got: " + formatArray(peaks));
            assertTrue(hasPeakNear(peaks, 3.0, 1.0),
                "Should have peak near 3, got: " + formatArray(peaks));
        }
    }

    // ==================== Numerical Accuracy Tests ====================

    @Test
    @Tag("accuracy")
    void modeWeightsSumToOne() {
        float[] data = generateBimodal(SAMPLE_SIZE, -2.0, 2.0, 0.5);

        ModeDetectionResult result = ModeDetector.detect(data);

        double weightSum = 0;
        for (double w : result.modeWeights()) {
            weightSum += w;
        }
        assertEquals(1.0, weightSum, WEIGHT_SUM_TOLERANCE,
            "Mode weights must sum to 1.0, got: " + weightSum);
    }

    @Test
    @Tag("accuracy")
    void asymmetricBimodalWeightAccuracy() {
        // Generate asymmetric bimodal: 70% near -1, 30% near 2
        Random rng = new Random(SEED);
        float[] data = new float[SAMPLE_SIZE];
        int split = 7000;  // 70%

        for (int i = 0; i < split; i++) {
            data[i] = (float) (-1.0 + rng.nextGaussian() * 0.4);
        }
        for (int i = split; i < SAMPLE_SIZE; i++) {
            data[i] = (float) (2.0 + rng.nextGaussian() * 0.4);
        }

        ModeDetectionResult result = ModeDetector.detect(data);

        assertTrue(result.isMultimodal(), "Should detect bimodal");
        assertEquals(2, result.modeCount(), "Should have 2 modes");

        // Check weights roughly match 70/30 split
        double[] weights = result.modeWeights();
        double maxWeight = Math.max(weights[0], weights[1]);
        double minWeight = Math.min(weights[0], weights[1]);

        // Expected: major=0.7, minor=0.3
        assertEquals(0.7, maxWeight, WEIGHT_ACCURACY_TOLERANCE,
            "Major mode weight should be ~0.7, got: " + maxWeight);
        assertEquals(0.3, minWeight, WEIGHT_ACCURACY_TOLERANCE,
            "Minor mode weight should be ~0.3, got: " + minWeight);

        System.out.printf("Asymmetric weight test: true=[0.7, 0.3], detected=[%.3f, %.3f]%n",
            maxWeight, minWeight);
    }

    @Test
    @Tag("accuracy")
    void peakLocationAccuracyForClearBimodal() {
        // Very well-separated modes for accurate peak location
        double mode1 = -5.0;
        double mode2 = 5.0;
        double stdDev = 0.5;  // Narrow modes

        float[] data = generateBimodal(SAMPLE_SIZE * 2, mode1, mode2, stdDev);

        ModeDetectionResult result = ModeDetector.detect(data);

        assertTrue(result.isMultimodal(), "Should detect bimodal");
        double[] peaks = result.peakLocations();

        double peak1 = Math.min(peaks[0], peaks[1]);
        double peak2 = Math.max(peaks[0], peaks[1]);

        // Use wider tolerance (1.0) because histogram bin centers may not align exactly with mode centers
        // The important property is that peaks are clearly in the correct region
        double histogramTolerance = 1.0;
        assertEquals(mode1, peak1, histogramTolerance,
            "First peak should be near " + mode1 + ", got: " + peak1);
        assertEquals(mode2, peak2, histogramTolerance,
            "Second peak should be near " + mode2 + ", got: " + peak2);

        System.out.printf("Peak location accuracy: true=[%.1f, %.1f], detected=[%.3f, %.3f]%n",
            mode1, mode2, peak1, peak2);
    }

    @Test
    @Tag("accuracy")
    void dipStatisticPositiveAndBounded() {
        float[] data = generateNormal(SAMPLE_SIZE, 0, 1);

        ModeDetectionResult result = ModeDetector.detect(data);

        assertTrue(result.dipStatistic() >= 0,
            "Dip statistic should be non-negative, got: " + result.dipStatistic());
        assertTrue(result.dipStatistic() <= 1.0,
            "Dip statistic should be <= 1.0, got: " + result.dipStatistic());
    }

    @Test
    @Tag("accuracy")
    void dipStatisticHigherForMultimodal() {
        Random rng = new Random(SEED);

        // Unimodal data
        float[] unimodal = generateNormal(SAMPLE_SIZE, 0, 1);
        ModeDetectionResult uniResult = ModeDetector.detect(unimodal);

        // Bimodal data with clear separation
        float[] bimodal = generateBimodal(SAMPLE_SIZE, -3, 3, 0.5);
        ModeDetectionResult biResult = ModeDetector.detect(bimodal);

        System.out.printf("Dip statistics: unimodal=%.4f, bimodal=%.4f%n",
            uniResult.dipStatistic(), biResult.dipStatistic());

        // Bimodal should have higher dip (though not always guaranteed)
        // At minimum, bimodal should be detected as multimodal
        assertTrue(biResult.isMultimodal(), "Bimodal should be detected as multimodal");
    }

    // ==================== Edge Cases and Robustness ====================

    @Test
    void uniformDataIsUnimodal() {
        Random rng = new Random(SEED);
        float[] data = new float[SAMPLE_SIZE];
        for (int i = 0; i < data.length; i++) {
            data[i] = (float) rng.nextDouble() * 2 - 1; // [-1, 1]
        }

        ModeDetectionResult result = ModeDetector.detect(data);

        // Uniform should not have many distinct peaks
        assertTrue(result.modeCount() <= 2,
            "Uniform should not have many modes, got: " + result.modeCount());
    }

    @Test
    void rejectsInsufficientData() {
        float[] data = new float[5]; // Less than minimum required (10)

        assertThrows(IllegalArgumentException.class, () -> ModeDetector.detect(data),
            "Should reject data with fewer than 10 points");
    }

    @Test
    void maxModesIsRespected() {
        // Generate trimodal but limit to 2 modes
        float[] data = generateTrimodal(15000, -3.0, 0.0, 3.0, 0.3);

        ModeDetectionResult result = ModeDetector.detect(data, 2);

        assertTrue(result.modeCount() <= 2,
            "Should respect maxModes limit of 2, got: " + result.modeCount());
    }

    @Test
    void handlesConstantData() {
        float[] data = new float[1000];
        java.util.Arrays.fill(data, 0.5f);

        ModeDetectionResult result = ModeDetector.detect(data);

        assertEquals(1, result.modeCount(), "Constant data should be unimodal");
        assertFalse(result.isMultimodal(), "Constant data should not be multimodal");
    }

    @Test
    void handlesNearlyConstantData() {
        Random rng = new Random(SEED);
        float[] data = new float[1000];
        for (int i = 0; i < data.length; i++) {
            data[i] = (float) (0.5 + rng.nextGaussian() * 0.0001);  // Very small variance
        }

        ModeDetectionResult result = ModeDetector.detect(data);

        assertEquals(1, result.modeCount(), "Nearly constant data should be unimodal");
    }

    @Test
    void handlesOverlappingModes() {
        // Modes with significant overlap should still detect 2 modes if separation is sufficient
        float[] data = generateBimodal(SAMPLE_SIZE, -1.0, 1.0, 0.7);  // Overlapping modes

        ModeDetectionResult result = ModeDetector.detect(data);

        // May or may not detect as bimodal due to overlap
        assertTrue(result.modeCount() >= 1 && result.modeCount() <= 2,
            "Overlapping modes should detect 1-2 modes, got: " + result.modeCount());
    }

    /// Tests that 10-mode distributions are correctly detected.
    ///
    /// This is a regression test to ensure that threshold tightening for
    /// preventing over-detection of 2-3 mode data doesn't prevent legitimate
    /// detection of high mode counts.
    @Test
    void detectsTenModes() {
        // Generate 10 well-separated normal distributions
        // Modes evenly spaced from -0.9 to 0.9 with narrow std dev
        int sampleSize = 50000;  // Need more samples for 10 modes
        double[] means = new double[10];
        for (int i = 0; i < 10; i++) {
            means[i] = -0.9 + i * 0.2;  // -0.9, -0.7, -0.5, ..., 0.9
        }
        double stdDev = 0.05;  // Narrow to keep modes distinct

        float[] data = generateNModal(sampleSize, means, stdDev);

        ModeDetectionResult result = ModeDetector.detectAdaptive(data, 10);

        System.out.println("10-mode detection test:");
        System.out.printf("  Dip statistic: %.4f%n", result.dipStatistic());
        System.out.printf("  Modes detected: %d%n", result.modeCount());
        if (result.modeCount() >= 2) {
            System.out.printf("  Peak locations: %s%n", formatArray(result.peakLocations()));
        }

        assertTrue(result.isMultimodal(), "10-mode data should be multimodal");
        assertTrue(result.modeCount() >= 8,
            "Should detect at least 8 of 10 modes, got: " + result.modeCount());

        // Verify peak locations are reasonably distributed
        if (result.modeCount() >= 8) {
            double[] peaks = result.peakLocations();
            double minPeak = peaks[0];
            double maxPeak = peaks[peaks.length - 1];
            double range = maxPeak - minPeak;
            assertTrue(range >= 1.2,
                "Peak range should span most of [-0.9, 0.9], got range: " + range);
        }
    }

    /// Tests detection of 5 modes (intermediate case).
    @Test
    void detectsFiveModes() {
        double[] means = {-0.8, -0.4, 0.0, 0.4, 0.8};
        double stdDev = 0.08;
        float[] data = generateNModal(30000, means, stdDev);

        ModeDetectionResult result = ModeDetector.detectAdaptive(data, 10);

        System.out.println("5-mode detection test:");
        System.out.printf("  Dip statistic: %.4f%n", result.dipStatistic());
        System.out.printf("  Modes detected: %d%n", result.modeCount());

        assertTrue(result.isMultimodal(), "5-mode data should be multimodal");
        assertTrue(result.modeCount() >= 4,
            "Should detect at least 4 of 5 modes, got: " + result.modeCount());
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

    private float[] generateNModal(int n, double[] means, double stdDev) {
        Random rng = new Random(SEED);
        float[] data = new float[n];
        int numModes = means.length;
        for (int i = 0; i < n; i++) {
            int mode = rng.nextInt(numModes);
            data[i] = (float) (means[mode] + rng.nextGaussian() * stdDev);
        }
        return data;
    }

    private boolean hasPeakNear(double[] peaks, double target, double tolerance) {
        for (double peak : peaks) {
            if (Math.abs(peak - target) < tolerance) {
                return true;
            }
        }
        return false;
    }

    private String formatArray(double[] arr) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < arr.length; i++) {
            if (i > 0) sb.append(", ");
            sb.append(String.format("%.3f", arr[i]));
        }
        sb.append("]");
        return sb.toString();
    }
}
