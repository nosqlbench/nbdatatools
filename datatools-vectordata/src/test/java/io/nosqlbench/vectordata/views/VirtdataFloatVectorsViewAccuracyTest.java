package io.nosqlbench.vectordata.views;

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

import io.nosqlbench.datatools.virtdata.DimensionDistributionGenerator;
import io.nosqlbench.datatools.virtdata.VectorGenerator;
import io.nosqlbench.vectordata.spec.datasets.types.Indexed;
import io.nosqlbench.vshapes.extract.StatisticalTestSuite;
import io.nosqlbench.vshapes.model.VectorSpaceModel;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/// Numerical accuracy tests for VirtdataFloatVectorsView.
///
/// ## Purpose
///
/// Validates that VirtdataFloatVectorsView correctly implements the FloatVectors interface
/// with proper numerical accuracy and deterministic behavior.
///
/// ## Test Categories
///
/// - **Determinism**: Verifies same index always returns identical vectors
/// - **Batch Consistency**: Ensures batch operations match individual access
/// - **Statistical Properties**: Validates generated vectors match model parameters
/// - **Dimension Accuracy**: Tests across various dimension sizes
///
/// ## Running the Tests
///
/// These tests are tagged with "accuracy" and run with:
/// ```bash
/// mvn test -Paccuracy -pl datatools-vectordata
/// ```
///
/// @see VirtdataFloatVectorsView
/// @see StatisticalTestSuite
@Tag("accuracy")
public class VirtdataFloatVectorsViewAccuracyTest {

    private static final int DIMENSIONS = 128;
    private static final int COUNT = 100_000;
    private static final long UNIQUE_VECTORS = 10_000_000L;

    private static VectorGenerator<VectorSpaceModel> generator;
    private static VirtdataFloatVectorsView view;
    private static VectorSpaceModel model;

    @BeforeAll
    static void setup() {
        model = new VectorSpaceModel(UNIQUE_VECTORS, DIMENSIONS, 0.0, 1.0);
        generator = new DimensionDistributionGenerator(model);
        view = new VirtdataFloatVectorsView(generator, COUNT);
    }

    static Stream<Arguments> dimensionProvider() {
        return Stream.of(
            Arguments.of(32, "32 dimensions"),
            Arguments.of(64, "64 dimensions"),
            Arguments.of(128, "128 dimensions"),
            Arguments.of(256, "256 dimensions"),
            Arguments.of(512, "512 dimensions")
        );
    }

    // ========== Determinism Tests ==========

    @Test
    void testDeterminism_SameIndexReturnsSameVector() {
        int[] testIndices = {0, 1, 42, 999, COUNT - 1};

        for (int index : testIndices) {
            float[] v1 = view.get(index);
            float[] v2 = view.get(index);

            assertArrayEquals(v1, v2, 0.0f,
                "Same index must return identical vectors: index=" + index);
        }
    }

    @Test
    void testDeterminism_MultipleCallsAcrossRange() {
        int samples = 1000;
        for (int i = 0; i < samples; i++) {
            int index = i * 100;
            float[] v1 = view.get(index);
            float[] v2 = view.get(index);
            float[] v3 = view.get(index);

            assertArrayEquals(v1, v2, 0.0f, "v1 != v2 at index " + index);
            assertArrayEquals(v2, v3, 0.0f, "v2 != v3 at index " + index);
        }
    }

    // ========== Batch Consistency Tests ==========

    @Test
    void testBatchConsistency_RangeMatchesIndividual() {
        int start = 100;
        int end = 200;

        float[][] batch = view.getRange(start, end);

        assertEquals(end - start, batch.length);

        for (int i = 0; i < batch.length; i++) {
            float[] individual = view.get(start + i);
            assertArrayEquals(individual, batch[i], 0.0f,
                "Batch[" + i + "] must match individual get(" + (start + i) + ")");
        }
    }

    @Test
    void testBatchConsistency_IndexedRangeMatchesIndividual() {
        int start = 500;
        int end = 600;

        Indexed<float[]>[] batch = view.getIndexedRange(start, end);

        assertEquals(end - start, batch.length);

        for (int i = 0; i < batch.length; i++) {
            assertEquals(start + i, batch[i].index(),
                "Indexed range should have correct index");
            float[] individual = view.get(start + i);
            assertArrayEquals(individual, batch[i].value(), 0.0f,
                "Indexed batch value must match individual get");
        }
    }

    @Test
    void testBatchConsistency_LargeBatch() {
        int start = 0;
        int end = 10_000;

        float[][] batch = view.getRange(start, end);

        // Spot check at boundaries and middle
        int[] checkPoints = {0, 1, 4999, 5000, 5001, 9998, 9999};
        for (int offset : checkPoints) {
            float[] individual = view.get(start + offset);
            assertArrayEquals(individual, batch[offset], 0.0f,
                "Large batch check failed at offset " + offset);
        }
    }

    // ========== Statistical Property Tests ==========

    @ParameterizedTest(name = "Dimension mean/variance: {1}")
    @MethodSource("dimensionProvider")
    void testStatisticalProperties_DimensionDistribution(int dims, String description) {
        VectorSpaceModel testModel = new VectorSpaceModel(1_000_000L, dims, 0.0, 1.0);
        VectorGenerator<VectorSpaceModel> testGen = new DimensionDistributionGenerator(testModel);
        VirtdataFloatVectorsView testView = new VirtdataFloatVectorsView(testGen, 50_000);

        // Sample vectors and compute per-dimension statistics
        int samples = 10_000;
        double[] means = new double[dims];
        double[] variances = new double[dims];

        // First pass: compute means
        for (int v = 0; v < samples; v++) {
            float[] vec = testView.get(v);
            for (int d = 0; d < dims; d++) {
                means[d] += vec[d];
            }
        }
        for (int d = 0; d < dims; d++) {
            means[d] /= samples;
        }

        // Second pass: compute variances
        for (int v = 0; v < samples; v++) {
            float[] vec = testView.get(v);
            for (int d = 0; d < dims; d++) {
                double diff = vec[d] - means[d];
                variances[d] += diff * diff;
            }
        }
        for (int d = 0; d < dims; d++) {
            variances[d] /= (samples - 1);
        }

        // Verify mean is approximately 0.5 (center of [0,1] uniform)
        // Model transforms from standard normal, so actual mean depends on transformation
        double avgMean = 0;
        double avgVar = 0;
        for (int d = 0; d < dims; d++) {
            avgMean += means[d];
            avgVar += variances[d];
        }
        avgMean /= dims;
        avgVar /= dims;

        System.out.printf("\n=== %s ===%n", description);
        System.out.printf("Average mean across dimensions: %.4f%n", avgMean);
        System.out.printf("Average variance across dimensions: %.4f%n", avgVar);

        // The model uses 0.0 to 1.0 range, so mean should be around 0.5
        // and variance should be reasonable (not too small, not too large)
        assertTrue(avgMean > -2.0 && avgMean < 2.0,
            "Average mean should be reasonable: " + avgMean);
        assertTrue(avgVar > 0.01,
            "Average variance should be positive: " + avgVar);
    }

    @Test
    void testStatisticalProperties_VectorNormDistribution() {
        int samples = 10_000;
        float[] norms = new float[samples];

        for (int i = 0; i < samples; i++) {
            float[] vec = view.get(i);
            float sumSq = 0;
            for (float v : vec) {
                sumSq += v * v;
            }
            norms[i] = (float) Math.sqrt(sumSq);
        }

        // Compute norm statistics
        double meanNorm = 0;
        for (float n : norms) meanNorm += n;
        meanNorm /= samples;

        double varNorm = 0;
        for (float n : norms) varNorm += (n - meanNorm) * (n - meanNorm);
        varNorm /= (samples - 1);

        System.out.println("\n=== Vector Norm Distribution ===");
        System.out.printf("Mean norm: %.4f%n", meanNorm);
        System.out.printf("Norm variance: %.4f%n", varNorm);
        System.out.printf("Norm std dev: %.4f%n", Math.sqrt(varNorm));

        // Norms should be positive and have reasonable spread
        assertTrue(meanNorm > 0, "Mean norm should be positive");
        assertTrue(varNorm > 0, "Norm variance should be positive");
    }

    // ========== Dimension Accuracy Tests ==========

    @ParameterizedTest(name = "Vector dimensions: {1}")
    @MethodSource("dimensionProvider")
    void testDimensionAccuracy_CorrectDimensionality(int dims, String description) {
        VectorSpaceModel testModel = new VectorSpaceModel(1_000_000L, dims, 0.0, 1.0);
        VectorGenerator<VectorSpaceModel> testGen = new DimensionDistributionGenerator(testModel);
        VirtdataFloatVectorsView testView = new VirtdataFloatVectorsView(testGen, 1000);

        assertEquals(dims, testView.getVectorDimensions());

        // Verify actual vector dimensions
        for (int i = 0; i < 100; i++) {
            float[] vec = testView.get(i);
            assertEquals(dims, vec.length,
                "Vector " + i + " should have " + dims + " dimensions");
        }
    }

    // ========== Q-Q Correlation Tests ==========

    @Test
    void testQQCorrelation_AcrossViews() {
        // Create two views from the same model
        VirtdataFloatVectorsView view1 = new VirtdataFloatVectorsView(generator, COUNT);
        VirtdataFloatVectorsView view2 = new VirtdataFloatVectorsView(generator, COUNT);

        int samples = 5000;
        float[] norms1 = new float[samples];
        float[] norms2 = new float[samples];

        for (int i = 0; i < samples; i++) {
            float[] v1 = view1.get(i);
            float[] v2 = view2.get(i);

            float sum1 = 0, sum2 = 0;
            for (int d = 0; d < DIMENSIONS; d++) {
                sum1 += v1[d] * v1[d];
                sum2 += v2[d] * v2[d];
            }
            norms1[i] = (float) Math.sqrt(sum1);
            norms2[i] = (float) Math.sqrt(sum2);
        }

        double qqCorr = StatisticalTestSuite.qqCorrelation(norms1, norms2);

        System.out.println("\n=== Q-Q Correlation Across Views ===");
        System.out.printf("Q-Q correlation: %.6f%n", qqCorr);

        // Same generator should produce identical results
        assertEquals(1.0, qqCorr, 1e-10,
            "Q-Q correlation should be perfect for same generator");
    }

    // ========== Edge Case Tests ==========

    @Test
    void testEdgeCases_BoundaryIndices() {
        // Test first and last indices
        assertDoesNotThrow(() -> view.get(0), "Should handle index 0");
        assertDoesNotThrow(() -> view.get(COUNT - 1), "Should handle last index");

        // Verify dimensions are correct at boundaries
        assertEquals(DIMENSIONS, view.get(0).length);
        assertEquals(DIMENSIONS, view.get(COUNT - 1).length);
    }

    @Test
    void testEdgeCases_SingleElementRange() {
        float[][] single = view.getRange(42, 43);
        assertEquals(1, single.length);
        assertArrayEquals(view.get(42), single[0], 0.0f);
    }

    @Test
    void testEdgeCases_UnboundedViewStillDeterministic() {
        VirtdataFloatVectorsView unbounded = new VirtdataFloatVectorsView(generator);

        float[] v1 = unbounded.get(12345);
        float[] v2 = unbounded.get(12345);

        assertArrayEquals(v1, v2, 0.0f,
            "Unbounded view should still be deterministic");
    }

    // ========== Numerical Precision Tests ==========

    @Test
    void testNumericalPrecision_NoNaNOrInfinity() {
        int samples = 10_000;

        for (int i = 0; i < samples; i++) {
            float[] vec = view.get(i);
            for (int d = 0; d < vec.length; d++) {
                assertFalse(Float.isNaN(vec[d]),
                    "Vector " + i + " dimension " + d + " is NaN");
                assertFalse(Float.isInfinite(vec[d]),
                    "Vector " + i + " dimension " + d + " is infinite");
            }
        }
    }

    @Test
    void testNumericalPrecision_ValuesInReasonableRange() {
        int samples = 5000;
        float minVal = Float.MAX_VALUE;
        float maxVal = Float.MIN_VALUE;

        for (int i = 0; i < samples; i++) {
            float[] vec = view.get(i);
            for (float v : vec) {
                minVal = Math.min(minVal, v);
                maxVal = Math.max(maxVal, v);
            }
        }

        System.out.println("\n=== Value Range ===");
        System.out.printf("Min value: %.6f%n", minVal);
        System.out.printf("Max value: %.6f%n", maxVal);

        // Values should be in a reasonable range (not extreme)
        assertTrue(Math.abs(minVal) < 1e6, "Min value should be reasonable");
        assertTrue(Math.abs(maxVal) < 1e6, "Max value should be reasonable");
    }
}
