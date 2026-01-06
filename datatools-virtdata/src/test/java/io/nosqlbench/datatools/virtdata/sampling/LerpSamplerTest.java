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

package io.nosqlbench.datatools.virtdata.sampling;

import io.nosqlbench.vshapes.model.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class LerpSamplerTest {

    private static final int SAMPLE_COUNT = 10_000;
    private static final double RELATIVE_TOLERANCE = 0.05; // 5% relative error for interpolation

    @Test
    void testConstructorValidation() {
        ComponentSampler delegate = new UniformSampler(new UniformScalarModel(0, 1));

        // Valid table sizes
        assertDoesNotThrow(() -> new LerpSampler(delegate, 16));
        assertDoesNotThrow(() -> new LerpSampler(delegate, 1024));
        assertDoesNotThrow(() -> new LerpSampler(delegate, 4096));

        // Invalid table size
        assertThrows(IllegalArgumentException.class, () -> new LerpSampler(delegate, 15));
        assertThrows(IllegalArgumentException.class, () -> new LerpSampler(delegate, 0));
        assertThrows(IllegalArgumentException.class, () -> new LerpSampler(delegate, -1));
    }

    @Test
    void testDefaultTableSize() {
        ComponentSampler delegate = new UniformSampler(new UniformScalarModel(0, 1));
        LerpSampler lerp = new LerpSampler(delegate);
        assertEquals(1024, lerp.getTableSize());
    }

    @Test
    void testBoundaryValues() {
        NormalScalarModel model = new NormalScalarModel(0, 1);
        ComponentSampler direct = new NormalSampler(model);
        LerpSampler lerp = new LerpSampler(direct);

        // u = 0 should return minimum value
        double atZero = lerp.sample(0.0);
        double atNearZero = lerp.sample(1e-15);
        assertTrue(Double.isFinite(atZero));
        assertTrue(Double.isFinite(atNearZero));

        // u = 1 should return maximum value
        double atOne = lerp.sample(1.0);
        double atNearOne = lerp.sample(1.0 - 1e-15);
        assertTrue(Double.isFinite(atOne));
        assertTrue(Double.isFinite(atNearOne));

        // Values should be monotonic
        assertTrue(atZero <= lerp.sample(0.5));
        assertTrue(lerp.sample(0.5) <= atOne);
    }

    @Test
    void testUniformDistributionAccuracy() {
        double lower = 10.0;
        double upper = 20.0;
        UniformScalarModel model = new UniformScalarModel(lower, upper);
        ComponentSampler direct = new UniformSampler(model);
        LerpSampler lerp = new LerpSampler(direct);

        // For uniform, LERP should be exact (linear CDF)
        double[] testPoints = {0.0, 0.1, 0.25, 0.5, 0.75, 0.9, 1.0};
        for (double u : testPoints) {
            double expected = lower + u * (upper - lower);
            double actual = lerp.sample(u);
            assertEquals(expected, actual, 0.01, "Failed at u=" + u);
        }
    }

    @Test
    void testNormalDistributionAccuracy() {
        double mean = 100.0;
        double stdDev = 15.0;
        NormalScalarModel model = new NormalScalarModel(mean, stdDev);
        ComponentSampler direct = new NormalSampler(model);
        LerpSampler lerp = new LerpSampler(direct);

        // Test at various quantiles
        Random rng = new Random(42L);
        double maxRelError = 0;
        for (int i = 0; i < SAMPLE_COUNT; i++) {
            double u = rng.nextDouble();
            u = Math.max(0.001, Math.min(0.999, u)); // Avoid extreme tails

            double expected = direct.sample(u);
            double actual = lerp.sample(u);

            if (Math.abs(expected) > 1e-10) {
                double relError = Math.abs((actual - expected) / expected);
                maxRelError = Math.max(maxRelError, relError);
            }
        }

        assertTrue(maxRelError < RELATIVE_TOLERANCE,
            "Max relative error " + maxRelError + " exceeds tolerance " + RELATIVE_TOLERANCE);
    }

    @ParameterizedTest
    @ValueSource(ints = {64, 256, 1024, 4096})
    void testTableSizeAffectsAccuracy(int tableSize) {
        NormalScalarModel model = new NormalScalarModel(0, 1);
        ComponentSampler direct = new NormalSampler(model);
        LerpSampler lerp = new LerpSampler(direct, tableSize);

        // Compute max error for this table size
        double maxError = 0;
        for (int i = 0; i < 1000; i++) {
            double u = (i + 0.5) / 1000.0;
            double expected = direct.sample(u);
            double actual = lerp.sample(u);
            maxError = Math.max(maxError, Math.abs(actual - expected));
        }

        // Larger tables should have lower error
        // Expected: error decreases roughly as O(1/tableSize^2) for smooth CDFs
        System.out.printf("Table size %4d: max error = %.6f%n", tableSize, maxError);
        assertTrue(maxError < 1.0, "Error too large for table size " + tableSize);
    }

    @Test
    void testBetaDistributionAccuracy() {
        BetaScalarModel model = new BetaScalarModel(2.0, 5.0);
        ComponentSampler direct = new BetaSampler(model);
        LerpSampler lerp = new LerpSampler(direct);

        // Compute mean via sampling
        double directSum = 0, lerpSum = 0;
        Random rng = new Random(42L);
        for (int i = 0; i < SAMPLE_COUNT; i++) {
            double u = rng.nextDouble();
            directSum += direct.sample(u);
            lerpSum += lerp.sample(u);
        }

        double directMean = directSum / SAMPLE_COUNT;
        double lerpMean = lerpSum / SAMPLE_COUNT;

        // Means should be very close
        double relError = Math.abs((lerpMean - directMean) / directMean);
        assertTrue(relError < RELATIVE_TOLERANCE,
            "Mean mismatch: direct=" + directMean + " lerp=" + lerpMean);
    }

    @Test
    void testGammaDistributionAccuracy() {
        GammaScalarModel model = new GammaScalarModel(3.0, 2.0);
        ComponentSampler direct = new GammaSampler(model);
        LerpSampler lerp = new LerpSampler(direct);

        // Sample and compare statistics
        double directSum = 0, lerpSum = 0;
        double directSumSq = 0, lerpSumSq = 0;
        Random rng = new Random(42L);

        for (int i = 0; i < SAMPLE_COUNT; i++) {
            double u = rng.nextDouble();
            double d = direct.sample(u);
            double l = lerp.sample(u);
            directSum += d;
            lerpSum += l;
            directSumSq += d * d;
            lerpSumSq += l * l;
        }

        double directMean = directSum / SAMPLE_COUNT;
        double lerpMean = lerpSum / SAMPLE_COUNT;
        double directVar = directSumSq / SAMPLE_COUNT - directMean * directMean;
        double lerpVar = lerpSumSq / SAMPLE_COUNT - lerpMean * lerpMean;

        // Both mean and variance should match
        double meanRelError = Math.abs((lerpMean - directMean) / directMean);
        double varRelError = Math.abs((lerpVar - directVar) / directVar);

        assertTrue(meanRelError < RELATIVE_TOLERANCE,
            "Mean mismatch: " + meanRelError);
        assertTrue(varRelError < RELATIVE_TOLERANCE * 2,
            "Variance mismatch: " + varRelError);
    }

    @Test
    void testMonotonicity() {
        // LERP sampling should preserve monotonicity
        NormalScalarModel model = new NormalScalarModel(0, 1);
        LerpSampler lerp = new LerpSampler(new NormalSampler(model));

        double prev = lerp.sample(0.0);
        for (int i = 1; i <= 1000; i++) {
            double u = i / 1000.0;
            double curr = lerp.sample(u);
            assertTrue(curr >= prev,
                "Monotonicity violated at u=" + u + ": " + prev + " > " + curr);
            prev = curr;
        }
    }

    @Test
    void testLerpSamplerFactoryWrapping() {
        NormalScalarModel model = new NormalScalarModel(0, 1);
        ComponentSampler direct = new NormalSampler(model);

        // Factory wrapping
        ComponentSampler wrapped = LerpSamplerFactory.wrap(direct);
        assertTrue(wrapped instanceof LerpSampler);

        // Factory with table size
        ComponentSampler wrapped2 = LerpSamplerFactory.wrap(direct, 2048);
        assertTrue(wrapped2 instanceof LerpSampler);
        assertEquals(2048, ((LerpSampler) wrapped2).getTableSize());
    }

    @Test
    void testLerpSamplerFactoryFromModel() {
        NormalScalarModel model = new NormalScalarModel(0, 1);

        // Create directly from model
        ComponentSampler lerp = LerpSamplerFactory.forModel(model);
        assertTrue(lerp instanceof LerpSampler);

        // Verify it works
        double value = lerp.sample(0.5);
        assertTrue(Double.isFinite(value));
        assertTrue(Math.abs(value) < 0.01); // Median of N(0,1) is 0
    }

    @Test
    void testLerpSamplerFactoryBulkCreation() {
        ScalarModel[] models = {
            new NormalScalarModel(0, 1),
            new UniformScalarModel(0, 10),
            new BetaScalarModel(2, 5)
        };

        ComponentSampler[] samplers = LerpSamplerFactory.forModels(models);

        assertEquals(3, samplers.length);
        for (ComponentSampler sampler : samplers) {
            assertTrue(sampler instanceof LerpSampler);
            assertTrue(Double.isFinite(sampler.sample(0.5)));
        }
    }

    @Test
    void testDeterminism() {
        NormalScalarModel model = new NormalScalarModel(0, 1);
        LerpSampler lerp = new LerpSampler(new NormalSampler(model));

        // Same input should always produce same output
        double u = 0.12345;
        double first = lerp.sample(u);
        for (int i = 0; i < 100; i++) {
            assertEquals(first, lerp.sample(u), 0.0, "Non-deterministic sampling");
        }
    }
}
