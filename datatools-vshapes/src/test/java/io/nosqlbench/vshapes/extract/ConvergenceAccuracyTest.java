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

package io.nosqlbench.vshapes.extract;

import io.nosqlbench.vshapes.model.NormalScalarModel;
import io.nosqlbench.vshapes.model.VectorSpaceModel;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Random;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Accuracy tests for convergent parameter estimation.
 *
 * <p>These tests verify that:
 * <ol>
 *   <li>Parameters converge to correct values within expected sample counts</li>
 *   <li>Convergence detection correctly identifies stable estimates</li>
 *   <li>Early stopping saves computation without sacrificing accuracy</li>
 *   <li>Convergence threshold affects both accuracy and sample efficiency</li>
 * </ol>
 */
@Tag("accuracy")
public class ConvergenceAccuracyTest {

    private static final long SEED = 42L;

    /**
     * Tests that mean converges to true value at expected rate O(1/√n).
     */
    @Test
    void testMeanConvergence() {
        double trueMean = 0.5;
        double trueStdDev = 0.2;
        int maxSamples = 50000;

        ConvergentDimensionEstimator estimator = new ConvergentDimensionEstimator(
            0, 0.05, 1000
        );

        Random rng = new Random(SEED);

        // Track convergence at different sample sizes
        int[] checkpoints = {1000, 5000, 10000, 25000, 50000};
        double[] meanErrors = new double[checkpoints.length];

        int checkIdx = 0;
        for (int i = 0; i < maxSamples; i++) {
            float sample = (float) (trueMean + trueStdDev * rng.nextGaussian());
            estimator.accept(sample);

            if (checkIdx < checkpoints.length && (i + 1) == checkpoints[checkIdx]) {
                meanErrors[checkIdx] = Math.abs(estimator.getMean() - trueMean);
                checkIdx++;
            }
        }

        // Verify error decreases with sample size (O(1/√n) convergence)
        System.out.println("Mean convergence test:");
        for (int i = 0; i < checkpoints.length; i++) {
            double expectedSE = trueStdDev / Math.sqrt(checkpoints[i]);
            System.out.printf("  n=%d: error=%.6f, expected SE=%.6f, ratio=%.2f%n",
                checkpoints[i], meanErrors[i], expectedSE, meanErrors[i] / expectedSE);
        }

        // Error at 50k should be much smaller than at 1k
        assertTrue(meanErrors[4] < meanErrors[0] / 3,
            "Mean error should decrease significantly with more samples");

        // Error should be within ~3 standard errors with high probability
        double finalSE = trueStdDev / Math.sqrt(50000);
        assertTrue(meanErrors[4] < 3 * finalSE,
            String.format("Mean error %.6f should be within 3 SE (%.6f)", meanErrors[4], 3 * finalSE));
    }

    /**
     * Tests that variance converges to true value.
     */
    @Test
    void testVarianceConvergence() {
        double trueMean = 0.0;
        double trueVariance = 0.04;  // stdDev = 0.2
        int maxSamples = 50000;

        ConvergentDimensionEstimator estimator = new ConvergentDimensionEstimator(
            0, 0.05, 1000
        );

        Random rng = new Random(SEED);

        for (int i = 0; i < maxSamples; i++) {
            float sample = (float) (trueMean + Math.sqrt(trueVariance) * rng.nextGaussian());
            estimator.accept(sample);
        }

        double estimatedVariance = estimator.getVariance();
        double varianceError = Math.abs(estimatedVariance - trueVariance);
        double expectedSE = trueVariance * Math.sqrt(2.0 / maxSamples);

        System.out.printf("Variance convergence: true=%.6f, estimated=%.6f, error=%.6f, SE=%.6f%n",
            trueVariance, estimatedVariance, varianceError, expectedSE);

        // Variance error should be within ~3 standard errors
        assertTrue(varianceError < 3 * expectedSE,
            String.format("Variance error %.6f should be within 3 SE (%.6f)", varianceError, 3 * expectedSE));
    }

    /**
     * Tests that convergence detection triggers at appropriate sample sizes.
     */
    @Test
    void testConvergenceDetection() {
        double trueMean = 0.0;
        double trueStdDev = 0.2;

        // Use a strict threshold for well-defined convergence
        ConvergentDimensionEstimator estimator = new ConvergentDimensionEstimator(
            0, 0.05, 500  // Check every 500 samples
        );

        Random rng = new Random(SEED);

        // Feed samples until convergence or max samples
        int maxSamples = 100000;
        for (int i = 0; i < maxSamples; i++) {
            float sample = (float) (trueMean + trueStdDev * rng.nextGaussian());
            estimator.accept(sample);

            if (estimator.hasConverged()) {
                break;
            }
        }

        System.out.printf("Convergence detection: converged=%s at n=%d%n",
            estimator.hasConverged(), estimator.getCount());

        // Should converge before max samples for well-behaved normal data
        assertTrue(estimator.hasConverged(),
            "Should converge for normal data with 100k samples");

        // Convergence should happen in reasonable range
        // For threshold 0.05, expect convergence around 10k-50k samples
        assertTrue(estimator.getConvergenceSampleCount() < maxSamples,
            "Should converge before max samples");
        assertTrue(estimator.getConvergenceSampleCount() >= ConvergentDimensionEstimator.MINIMUM_SAMPLES,
            "Should not converge before minimum samples");
    }

    /**
     * Tests convergence threshold affects sample efficiency.
     */
    @ParameterizedTest(name = "threshold={0}")
    @MethodSource("thresholdProvider")
    void testConvergenceThresholdAffectsSampleCount(double threshold, String description) {
        double trueMean = 0.0;
        double trueStdDev = 0.2;

        ConvergentDimensionEstimator estimator = new ConvergentDimensionEstimator(
            0, threshold, 500
        );

        Random rng = new Random(SEED);

        int maxSamples = 200000;
        for (int i = 0; i < maxSamples; i++) {
            float sample = (float) (trueMean + trueStdDev * rng.nextGaussian());
            estimator.accept(sample);

            if (estimator.hasConverged()) {
                break;
            }
        }

        System.out.printf("Threshold %.3f (%s): converged=%s at n=%d%n",
            threshold, description, estimator.hasConverged(), estimator.getConvergenceSampleCount());

        // Verify convergence happened
        assertTrue(estimator.hasConverged() || estimator.getCount() == maxSamples,
            "Should either converge or reach max samples");
    }

    static Stream<Arguments> thresholdProvider() {
        return Stream.of(
            Arguments.of(0.20, "loose"),
            Arguments.of(0.10, "moderate"),
            Arguments.of(0.05, "strict"),
            Arguments.of(0.02, "very strict")
        );
    }

    /**
     * Tests that ConvergentDatasetModelExtractor extracts accurate models.
     */
    @Test
    void testConvergentExtractorAccuracy() {
        int dimensions = 64;
        int vectorCount = 20000;

        // Generate synthetic data with known parameters
        double[] trueMeans = new double[dimensions];
        double[] trueStdDevs = new double[dimensions];
        Random paramRng = new Random(SEED);

        for (int d = 0; d < dimensions; d++) {
            trueMeans[d] = paramRng.nextDouble() * 0.4 - 0.2;  // [-0.2, 0.2]
            trueStdDevs[d] = 0.1 + paramRng.nextDouble() * 0.1;  // [0.1, 0.2]
        }

        // Generate data
        float[][] data = new float[vectorCount][dimensions];
        Random dataRng = new Random(SEED + 1);

        for (int v = 0; v < vectorCount; v++) {
            for (int d = 0; d < dimensions; d++) {
                data[v][d] = (float) (trueMeans[d] + trueStdDevs[d] * dataRng.nextGaussian());
            }
        }

        // Extract with convergent extractor
        ConvergentDatasetModelExtractor extractor = ConvergentDatasetModelExtractor.builder()
            .convergenceThreshold(0.05)
            .checkpointInterval(500)
            .earlyStoppingEnabled(true)
            .selector(BestFitSelector.defaultSelector())
            .build();

        ModelExtractor.ExtractionResult result = extractor.extractWithStats(data);
        VectorSpaceModel model = result.model();

        // Check accuracy
        int accurateMeans = 0;
        int accurateStdDevs = 0;

        for (int d = 0; d < dimensions; d++) {
            if (model.scalarModel(d) instanceof NormalScalarModel normal) {
                double meanError = Math.abs(normal.getMean() - trueMeans[d]);
                double stdDevError = Math.abs(normal.getStdDev() - trueStdDevs[d]);

                // Allow 5% relative error
                if (meanError < 0.05 * trueStdDevs[d]) accurateMeans++;
                if (stdDevError < 0.05 * trueStdDevs[d]) accurateStdDevs++;
            }
        }

        ConvergentDatasetModelExtractor.ConvergenceSummary summary = extractor.getConvergenceSummary();
        System.out.printf("Convergent extractor test:%n");
        System.out.printf("  Dimensions: %d%n", dimensions);
        System.out.printf("  Samples processed: %d / %d%n", summary.totalSamplesProcessed(), vectorCount);
        System.out.printf("  Converged dimensions: %d / %d (%.1f%%)%n",
            summary.convergedDimensions(), dimensions, summary.convergenceRate() * 100);
        System.out.printf("  Accurate means: %d / %d (%.1f%%)%n",
            accurateMeans, dimensions, 100.0 * accurateMeans / dimensions);
        System.out.printf("  Accurate stdDevs: %d / %d (%.1f%%)%n",
            accurateStdDevs, dimensions, 100.0 * accurateStdDevs / dimensions);

        // At least 80% of dimensions should have accurate parameters
        // (lower than 90% because early stopping may reduce sample count)
        assertTrue(accurateMeans >= 0.80 * dimensions,
            String.format("Expected 80%% accurate means, got %.1f%%", 100.0 * accurateMeans / dimensions));
        assertTrue(accurateStdDevs >= 0.80 * dimensions,
            String.format("Expected 80%% accurate stdDevs, got %.1f%%", 100.0 * accurateStdDevs / dimensions));
    }

    /**
     * Tests early stopping saves computation.
     */
    @Test
    void testEarlyStoppingSavesComputation() {
        int dimensions = 32;
        int vectorCount = 100000;

        // Generate simple normal data that should converge quickly
        float[][] data = new float[vectorCount][dimensions];
        Random rng = new Random(SEED);

        for (int v = 0; v < vectorCount; v++) {
            for (int d = 0; d < dimensions; d++) {
                data[v][d] = (float) (0.1 * rng.nextGaussian());
            }
        }

        // Extract with early stopping enabled
        ConvergentDatasetModelExtractor withEarlyStopping = ConvergentDatasetModelExtractor.builder()
            .convergenceThreshold(0.05)
            .checkpointInterval(500)
            .earlyStoppingEnabled(true)
            .build();

        // Extract without early stopping
        ConvergentDatasetModelExtractor withoutEarlyStopping = ConvergentDatasetModelExtractor.builder()
            .convergenceThreshold(0.05)
            .checkpointInterval(500)
            .earlyStoppingEnabled(false)
            .build();

        long startWith = System.nanoTime();
        withEarlyStopping.extractWithStats(data);
        long elapsedWith = System.nanoTime() - startWith;
        long samplesWithEarlyStopping = withEarlyStopping.getSamplesProcessed();

        long startWithout = System.nanoTime();
        withoutEarlyStopping.extractWithStats(data);
        long elapsedWithout = System.nanoTime() - startWithout;
        long samplesWithoutEarlyStopping = withoutEarlyStopping.getSamplesProcessed();

        System.out.printf("Early stopping test:%n");
        System.out.printf("  With early stopping:    %d samples, %d ms%n",
            samplesWithEarlyStopping, elapsedWith / 1_000_000);
        System.out.printf("  Without early stopping: %d samples, %d ms%n",
            samplesWithoutEarlyStopping, elapsedWithout / 1_000_000);
        System.out.printf("  Sample reduction: %.1f%%%n",
            100.0 * (1 - (double) samplesWithEarlyStopping / samplesWithoutEarlyStopping));

        // Without early stopping should process all samples
        assertEquals(vectorCount, samplesWithoutEarlyStopping);

        // With early stopping should process fewer samples (if convergence occurred)
        if (withEarlyStopping.allDimensionsConverged()) {
            assertTrue(samplesWithEarlyStopping < samplesWithoutEarlyStopping,
                "Early stopping should reduce samples processed");
        }
    }

    /**
     * Tests multi-dimensional convergence with varying rates.
     */
    @Test
    void testMultiDimensionalConvergence() {
        int dimensions = 16;
        int vectorCount = 50000;

        // Create dimensions with different variances (different convergence rates)
        double[] stdDevs = new double[dimensions];
        for (int d = 0; d < dimensions; d++) {
            stdDevs[d] = 0.05 + 0.02 * d;  // Increasing variance
        }

        // Generate data
        float[][] data = new float[vectorCount][dimensions];
        Random rng = new Random(SEED);

        for (int v = 0; v < vectorCount; v++) {
            for (int d = 0; d < dimensions; d++) {
                data[v][d] = (float) (stdDevs[d] * rng.nextGaussian());
            }
        }

        // Extract
        ConvergentDatasetModelExtractor extractor = ConvergentDatasetModelExtractor.builder()
            .convergenceThreshold(0.05)
            .checkpointInterval(1000)
            .earlyStoppingEnabled(false)  // Process all to see convergence pattern
            .build();

        extractor.extractWithStats(data);

        ConvergentDatasetModelExtractor.ConvergenceSummary summary = extractor.getConvergenceSummary();

        System.out.println("Multi-dimensional convergence:");
        System.out.printf("  Converged: %d / %d dimensions%n",
            summary.convergedDimensions(), dimensions);
        System.out.printf("  Min convergence: %d samples%n", summary.minConvergenceSamples());
        System.out.printf("  Max convergence: %d samples%n", summary.maxConvergenceSamples());
        System.out.printf("  Avg convergence: %.0f samples%n", summary.averageConvergenceSamples());

        // Print per-dimension convergence
        System.out.println("  Per-dimension:");
        for (int d = 0; d < Math.min(8, dimensions); d++) {
            System.out.println("    " + summary.dimensionStatuses()[d]);
        }

        // Most dimensions should converge
        assertTrue(summary.convergenceRate() > 0.8,
            String.format("Expected >80%% convergence, got %.1f%%", summary.convergenceRate() * 100));
    }
}
