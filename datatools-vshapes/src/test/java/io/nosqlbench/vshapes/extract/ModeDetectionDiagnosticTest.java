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
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Diagnostic test to investigate mode detection failures.
 */
public class ModeDetectionDiagnosticTest {

    @Test
    void diagnoseBimodalDetection() {
        Random rng = new Random(42);
        int n = 50000;
        float[] data = new float[n];

        // Generate bimodal data like dim 6: N(-0.33, 0.09) + uniform(0.13, 0.53)
        int mode1Count = n / 2;

        // Mode 1: Normal at -0.33 with stddev 0.09
        for (int i = 0; i < mode1Count; i++) {
            data[i] = (float) (-0.33 + 0.09 * rng.nextGaussian());
            data[i] = Math.max(-1f, Math.min(1f, data[i]));
        }

        // Mode 2: Uniform in [0.13, 0.53]
        for (int i = mode1Count; i < n; i++) {
            data[i] = (float) (0.13 + 0.40 * rng.nextDouble());
        }

        System.out.println("=== Bimodal Detection Diagnostic ===");
        System.out.println("n = " + n);
        System.out.println("Mode 1: Normal(-0.33, 0.09) - " + mode1Count + " samples");
        System.out.println("Mode 2: Uniform(0.13, 0.53) - " + (n - mode1Count) + " samples");
        System.out.println("Expected gap: ~0.46 units");
        System.out.println();

        // Print data range
        float min = Float.MAX_VALUE, max = Float.MIN_VALUE;
        for (float v : data) {
            min = Math.min(min, v);
            max = Math.max(max, v);
        }
        System.out.printf("Data range: [%.4f, %.4f]%n", min, max);
        System.out.println();

        // Test basic detection
        ModeDetectionResult result = ModeDetector.detect(data, 10);
        System.out.println("ModeDetector.detect(data, 10):");
        System.out.println("  modeCount = " + result.modeCount());
        System.out.println("  isMultimodal = " + result.isMultimodal());
        System.out.printf("  dipStatistic = %.4f%n", result.dipStatistic());
        System.out.println("  peakLocations = " + Arrays.toString(result.peakLocations()));
        System.out.println();

        // Test adaptive detection
        ModeDetectionResult adaptiveResult = ModeDetector.detectAdaptive(data, 10);
        System.out.println("ModeDetector.detectAdaptive(data, 10):");
        System.out.println("  modeCount = " + adaptiveResult.modeCount());
        System.out.println("  isMultimodal = " + adaptiveResult.isMultimodal());
        System.out.printf("  dipStatistic = %.4f%n", adaptiveResult.dipStatistic());
        System.out.println("  peakLocations = " + Arrays.toString(adaptiveResult.peakLocations()));
        System.out.println();

        // This SHOULD be detected as bimodal
        assertTrue(result.isMultimodal() || adaptiveResult.isMultimodal(),
            "Well-separated bimodal data should be detected as multimodal");

        if (result.isMultimodal()) {
            assertEquals(2, result.modeCount(),
                "Should detect 2 modes, got: " + result.modeCount());
        }
    }

    @Test
    void diagnoseTrimodalDetection() {
        Random rng = new Random(42);
        int n = 50000;
        float[] data = new float[n];

        // Generate trimodal data like dim 81 from testscript:
        // ⊕3[𝒩(-0.5, 0.06), β(-0.15, 0.15), ▭(0.43, 0.58)]
        int perMode = n / 3;

        // Mode 1: Normal at -0.5 with stddev 0.06
        for (int i = 0; i < perMode; i++) {
            data[i] = (float) (-0.5 + 0.06 * rng.nextGaussian());
            data[i] = Math.max(-1f, Math.min(1f, data[i]));
        }

        // Mode 2: Uniform-ish around 0 (range -0.15 to 0.15)
        for (int i = perMode; i < 2 * perMode; i++) {
            data[i] = (float) (-0.15 + 0.30 * rng.nextDouble());
        }

        // Mode 3: Uniform in [0.43, 0.58]
        for (int i = 2 * perMode; i < n; i++) {
            data[i] = (float) (0.43 + 0.15 * rng.nextDouble());
        }

        System.out.println("=== Trimodal Detection Diagnostic ===");
        System.out.println("n = " + n);
        System.out.println("Mode 1: Normal(-0.5, 0.06) - " + perMode + " samples");
        System.out.println("Mode 2: Uniform(-0.15, 0.15) - " + perMode + " samples");
        System.out.println("Mode 3: Uniform(0.43, 0.58) - " + (n - 2*perMode) + " samples");
        System.out.println("Gaps: ~0.35 and ~0.28 units");
        System.out.println();

        // Test basic detection
        ModeDetectionResult result = ModeDetector.detect(data, 10);
        System.out.println("ModeDetector.detect(data, 10):");
        System.out.println("  modeCount = " + result.modeCount());
        System.out.println("  isMultimodal = " + result.isMultimodal());
        System.out.printf("  dipStatistic = %.4f%n", result.dipStatistic());
        System.out.println("  peakLocations = " + Arrays.toString(result.peakLocations()));
        System.out.println();

        // Test adaptive detection
        ModeDetectionResult adaptiveResult = ModeDetector.detectAdaptive(data, 10);
        System.out.println("ModeDetector.detectAdaptive(data, 10):");
        System.out.println("  modeCount = " + adaptiveResult.modeCount());
        System.out.println("  isMultimodal = " + adaptiveResult.isMultimodal());
        System.out.printf("  dipStatistic = %.4f%n", adaptiveResult.dipStatistic());
        System.out.println("  peakLocations = " + Arrays.toString(adaptiveResult.peakLocations()));
        System.out.println();

        // This SHOULD be detected as multimodal
        assertTrue(result.isMultimodal() || adaptiveResult.isMultimodal(),
            "Well-separated trimodal data should be detected as multimodal");
    }

    @Test
    void diagnoseOverlappingModes() {
        Random rng = new Random(42);
        int n = 50000;
        float[] data = new float[n];

        // Generate 5 overlapping modes like high-mode composites
        // Space modes 0.3 units apart with stddev 0.1 each
        int perMode = n / 5;
        double[] centers = {-0.6, -0.3, 0.0, 0.3, 0.6};
        double stddev = 0.08;

        int idx = 0;
        for (int m = 0; m < 5; m++) {
            int count = (m == 4) ? (n - idx) : perMode;
            for (int i = 0; i < count; i++) {
                data[idx++] = (float) (centers[m] + stddev * rng.nextGaussian());
            }
        }

        System.out.println("=== 5-Mode Detection Diagnostic ===");
        System.out.println("n = " + n);
        System.out.println("Centers: " + Arrays.toString(centers));
        System.out.println("StdDev per mode: " + stddev);
        System.out.println("Gap between modes: 0.3 units");
        System.out.println();

        ModeDetectionResult result = ModeDetector.detect(data, 10);
        System.out.println("ModeDetector.detect(data, 10):");
        System.out.println("  modeCount = " + result.modeCount());
        System.out.println("  isMultimodal = " + result.isMultimodal());
        System.out.printf("  dipStatistic = %.4f%n", result.dipStatistic());
        System.out.println("  peakLocations = " + Arrays.toString(result.peakLocations()));
        System.out.println();

        ModeDetectionResult adaptiveResult = ModeDetector.detectAdaptive(data, 10);
        System.out.println("ModeDetector.detectAdaptive(data, 10):");
        System.out.println("  modeCount = " + adaptiveResult.modeCount());
        System.out.println("  isMultimodal = " + adaptiveResult.isMultimodal());
        System.out.printf("  dipStatistic = %.4f%n", adaptiveResult.dipStatistic());
        System.out.println("  peakLocations = " + Arrays.toString(adaptiveResult.peakLocations()));
        System.out.println();

        // This SHOULD detect at least 2 modes
        assertTrue(result.isMultimodal() || adaptiveResult.isMultimodal(),
            "5-mode data with 0.3 unit gaps should be detected as multimodal");

        // Ideally should detect 4-5 modes
        int detectedModes = Math.max(result.modeCount(), adaptiveResult.modeCount());
        System.out.println("Detected " + detectedModes + " of 5 modes");
    }
}
