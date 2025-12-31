package io.nosqlbench.datatools.virtdata;

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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link TruncatedGaussianSampler} verifying proper truncated normal inverse transform.
 */
class TruncatedGaussianSamplerTest {

    @Test
    void testOutputBounds() {
        // Standard normal truncated to [-1, 1]
        TruncatedGaussianSampler sampler = new TruncatedGaussianSampler(0.0, 1.0, -1.0, 1.0);

        // Sample at various points across the unit interval
        for (double u = 0.001; u < 1.0; u += 0.01) {
            double value = sampler.sample(u);
            assertTrue(value >= -1.0, "Value " + value + " < -1.0 at u=" + u);
            assertTrue(value <= 1.0, "Value " + value + " > 1.0 at u=" + u);
        }
    }

    @Test
    void testExtremeInputs() {
        TruncatedGaussianSampler sampler = new TruncatedGaussianSampler(0.0, 1.0, -1.0, 1.0);

        // Very close to 0 should give close to lower bound
        double nearLower = sampler.sample(0.0001);
        assertTrue(nearLower >= -1.0, "Near-zero input should still be >= -1.0");
        assertTrue(nearLower < -0.5, "Near-zero input should be in lower part of range");

        // Very close to 1 should give close to upper bound
        double nearUpper = sampler.sample(0.9999);
        assertTrue(nearUpper <= 1.0, "Near-one input should still be <= 1.0");
        assertTrue(nearUpper > 0.5, "Near-one input should be in upper part of range");
    }

    @Test
    void testMedian() {
        // For symmetric bounds around the mean, u=0.5 should give the mean
        TruncatedGaussianSampler sampler = new TruncatedGaussianSampler(0.0, 1.0, -1.0, 1.0);
        double median = sampler.sample(0.5);
        assertEquals(0.0, median, 0.001, "Median should be close to mean for symmetric truncation");
    }

    @Test
    void testAsymmetricBounds() {
        // Mean=0, but truncated to [0, 1] - median should shift right
        TruncatedGaussianSampler sampler = new TruncatedGaussianSampler(0.0, 1.0, 0.0, 1.0);

        double median = sampler.sample(0.5);
        assertTrue(median > 0, "Median should be positive for [0,1] truncation");
        assertTrue(median < 1, "Median should be less than 1");

        // All samples should be in [0, 1]
        for (double u = 0.001; u < 1.0; u += 0.01) {
            double value = sampler.sample(u);
            assertTrue(value >= 0.0, "Value " + value + " < 0.0");
            assertTrue(value <= 1.0, "Value " + value + " > 1.0");
        }
    }

    @Test
    void testWideTruncation() {
        // Wide truncation [-3, 3] should capture ~99.7% of standard normal
        TruncatedGaussianSampler sampler = new TruncatedGaussianSampler(0.0, 1.0, -3.0, 3.0);

        double mass = sampler.probabilityMass();
        assertTrue(mass > 0.99, "Probability mass should be >99% for [-3σ, 3σ]");

        // Median should still be close to 0
        assertEquals(0.0, sampler.sample(0.5), 0.01);
    }

    @Test
    void testDeterminism() {
        TruncatedGaussianSampler sampler = new TruncatedGaussianSampler(0.0, 1.0, -1.0, 1.0);

        double first = sampler.sample(0.42);
        double second = sampler.sample(0.42);
        assertEquals(first, second, "Same input should produce same output");
    }

    @Test
    void testMonotonicity() {
        TruncatedGaussianSampler sampler = new TruncatedGaussianSampler(0.0, 1.0, -1.0, 1.0);

        // Output should increase monotonically with input
        double prev = sampler.sample(0.001);
        for (double u = 0.01; u < 1.0; u += 0.01) {
            double curr = sampler.sample(u);
            assertTrue(curr >= prev, "Output should be monotonically increasing");
            prev = curr;
        }
    }

    @Test
    void testInvalidBounds() {
        assertThrows(IllegalArgumentException.class,
            () -> new TruncatedGaussianSampler(0.0, 1.0, 1.0, -1.0),
            "Should reject lower >= upper");

        assertThrows(IllegalArgumentException.class,
            () -> new TruncatedGaussianSampler(0.0, 1.0, 0.0, 0.0),
            "Should reject lower == upper");

        assertThrows(IllegalArgumentException.class,
            () -> new TruncatedGaussianSampler(0.0, -1.0, -1.0, 1.0),
            "Should reject negative stdDev");
    }

    @Test
    void testVectorGenWithTruncation() {
        // Verify VectorGen correctly uses truncated sampling
        VectorSpaceModel model = VectorSpaceModel.unitBounded(10000, 64);
        VectorGen gen = new VectorGen(model);

        // Generate many vectors and verify all values are in bounds
        for (long ordinal = 0; ordinal < 1000; ordinal++) {
            float[] vector = gen.apply(ordinal);
            for (int d = 0; d < vector.length; d++) {
                assertTrue(vector[d] >= -1.0f,
                    "Vector[" + ordinal + "][" + d + "] = " + vector[d] + " < -1.0");
                assertTrue(vector[d] <= 1.0f,
                    "Vector[" + ordinal + "][" + d + "] = " + vector[d] + " > 1.0");
            }
        }
    }

    @Test
    void testDistributionShape() {
        // Verify the truncated distribution has proper shape
        TruncatedGaussianSampler sampler = new TruncatedGaussianSampler(0.0, 1.0, -1.0, 1.0);

        // Sample at many points and compute moments
        int n = 10000;
        double sum = 0;
        double sumSq = 0;
        for (int i = 1; i <= n; i++) {
            double u = i / (n + 1.0);
            double x = sampler.sample(u);
            sum += x;
            sumSq += x * x;
        }

        double mean = sum / n;
        double variance = sumSq / n - mean * mean;

        // For symmetric truncation, mean should be ~0
        assertEquals(0.0, mean, 0.05, "Mean should be close to 0 for symmetric truncation");

        // Variance should be less than 1 (truncation reduces variance)
        // For N(0,1) truncated to [-1,1], theoretical variance is ~0.29
        assertTrue(variance < 1.0, "Variance should be less than unbounded case");
        assertTrue(variance > 0.2, "Variance should still be substantial, got: " + variance);
    }
}
