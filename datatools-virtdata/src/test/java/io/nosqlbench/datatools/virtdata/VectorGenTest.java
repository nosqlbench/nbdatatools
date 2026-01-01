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

import io.nosqlbench.vshapes.model.VectorSpaceModel;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class VectorGenTest {

    @Test
    void testBasicGeneration() {
        VectorSpaceModel model = new VectorSpaceModel(1000, 128);
        DimensionDistributionGenerator gen = new DimensionDistributionGenerator(model);

        float[] vector = gen.apply(0);
        assertNotNull(vector);
        assertEquals(128, vector.length);
    }

    @Test
    void testDeterminism() {
        VectorSpaceModel model = new VectorSpaceModel(1000, 64);
        DimensionDistributionGenerator gen = new DimensionDistributionGenerator(model);

        float[] first = gen.apply(42);
        float[] second = gen.apply(42);

        assertArrayEquals(first, second, "Same ordinal should produce same vector");
    }

    @Test
    void testDifferentOrdinalsDifferentVectors() {
        VectorSpaceModel model = new VectorSpaceModel(1000, 32);
        DimensionDistributionGenerator gen = new DimensionDistributionGenerator(model);

        float[] v0 = gen.apply(0);
        float[] v1 = gen.apply(1);
        float[] v100 = gen.apply(100);

        assertFalse(Arrays.equals(v0, v1), "Different ordinals should produce different vectors");
        assertFalse(Arrays.equals(v0, v100), "Different ordinals should produce different vectors");
        assertFalse(Arrays.equals(v1, v100), "Different ordinals should produce different vectors");
    }

    @Test
    void testOrdinalWrapping() {
        VectorSpaceModel model = new VectorSpaceModel(100, 16);
        DimensionDistributionGenerator gen = new DimensionDistributionGenerator(model);

        // Ordinal 100 should wrap to 0
        float[] v0 = gen.apply(0);
        float[] v100 = gen.apply(100);
        assertArrayEquals(v0, v100, "Ordinal 100 should wrap to 0 for N=100");

        // Ordinal 150 should wrap to 50
        float[] v50 = gen.apply(50);
        float[] v150 = gen.apply(150);
        assertArrayEquals(v50, v150, "Ordinal 150 should wrap to 50 for N=100");
    }

    @Test
    void testNegativeOrdinalWrapping() {
        VectorSpaceModel model = new VectorSpaceModel(100, 16);
        DimensionDistributionGenerator gen = new DimensionDistributionGenerator(model);

        // Ordinal -1 should wrap to 99
        float[] v99 = gen.apply(99);
        float[] vNeg1 = gen.apply(-1);
        assertArrayEquals(v99, vNeg1, "Ordinal -1 should wrap to 99 for N=100");
    }

    @Test
    void testGenerateInto() {
        VectorSpaceModel model = new VectorSpaceModel(1000, 32);
        DimensionDistributionGenerator gen = new DimensionDistributionGenerator(model);

        float[] target = new float[64];
        gen.generateInto(42, target, 0);
        gen.generateInto(43, target, 32);

        float[] v42 = gen.apply(42);
        float[] v43 = gen.apply(43);

        for (int i = 0; i < 32; i++) {
            assertEquals(v42[i], target[i], "First half should match v42");
            assertEquals(v43[i], target[32 + i], "Second half should match v43");
        }
    }

    @Test
    void testDoubleGeneration() {
        VectorSpaceModel model = new VectorSpaceModel(1000, 16);
        DimensionDistributionGenerator gen = new DimensionDistributionGenerator(model);

        double[] vectorD = gen.applyAsDouble(42);
        float[] vectorF = gen.apply(42);

        assertEquals(16, vectorD.length);

        // Double and float versions should be consistent (within float precision)
        for (int i = 0; i < 16; i++) {
            assertEquals(vectorF[i], (float) vectorD[i], 1e-6, "Double and float versions should match");
        }
    }

    @Test
    void testBatchGeneration() {
        VectorSpaceModel model = new VectorSpaceModel(10000, 64);
        DimensionDistributionGenerator gen = new DimensionDistributionGenerator(model);

        float[][] batch = gen.generateBatch(100, 50);

        assertEquals(50, batch.length);
        for (int i = 0; i < 50; i++) {
            assertEquals(64, batch[i].length);
            float[] individual = gen.apply(100 + i);
            assertArrayEquals(individual, batch[i], "Batch element should match individual generation");
        }
    }

    @Test
    void testFlatBatchGeneration() {
        VectorSpaceModel model = new VectorSpaceModel(10000, 32);
        DimensionDistributionGenerator gen = new DimensionDistributionGenerator(model);

        float[] flat = gen.generateFlatBatch(0, 100);

        assertEquals(100 * 32, flat.length);

        // Verify each vector
        for (int v = 0; v < 100; v++) {
            float[] individual = gen.apply(v);
            for (int d = 0; d < 32; d++) {
                assertEquals(individual[d], flat[v * 32 + d],
                    "Flat batch element [" + v + "][" + d + "] should match");
            }
        }
    }

    @Test
    void testDistributionProperties() {
        // Generate many vectors and check statistical properties
        VectorSpaceModel model = new VectorSpaceModel(100000, 4, 0.0, 1.0);
        DimensionDistributionGenerator gen = new DimensionDistributionGenerator(model);

        int sampleSize = 10000;
        double[] means = new double[4];
        double[] sumSq = new double[4];

        for (long i = 0; i < sampleSize; i++) {
            float[] v = gen.apply(i);
            for (int d = 0; d < 4; d++) {
                means[d] += v[d];
                sumSq[d] += v[d] * v[d];
            }
        }

        // Check mean is approximately 0
        for (int d = 0; d < 4; d++) {
            double mean = means[d] / sampleSize;
            assertTrue(Math.abs(mean) < 0.1, "Mean should be close to 0, got: " + mean + " for dim " + d);
        }

        // Check variance is approximately 1
        for (int d = 0; d < 4; d++) {
            double variance = sumSq[d] / sampleSize - Math.pow(means[d] / sampleSize, 2);
            assertTrue(variance > 0.5 && variance < 1.5,
                "Variance should be close to 1, got: " + variance + " for dim " + d);
        }
    }

    @Test
    void testUniquenessWithinN() {
        // Verify that N distinct ordinals produce N distinct vectors
        int n = 1000;
        VectorSpaceModel model = new VectorSpaceModel(n, 16);
        DimensionDistributionGenerator gen = new DimensionDistributionGenerator(model);

        Set<String> uniqueVectors = new HashSet<>();
        for (int i = 0; i < n; i++) {
            float[] v = gen.apply(i);
            String key = Arrays.toString(v);
            uniqueVectors.add(key);
        }

        assertEquals(n, uniqueVectors.size(), "All N ordinals should produce unique vectors");
    }

    @Test
    void testCustomDistribution() {
        // Test with non-standard Gaussian parameters
        VectorSpaceModel model = new VectorSpaceModel(10000, 8, 5.0, 2.0);
        DimensionDistributionGenerator gen = new DimensionDistributionGenerator(model);

        int sampleSize = 5000;
        double sum = 0;

        for (long i = 0; i < sampleSize; i++) {
            float[] v = gen.apply(i);
            for (int d = 0; d < 8; d++) {
                sum += v[d];
            }
        }

        double overallMean = sum / (sampleSize * 8);

        // Mean should be close to 5.0
        assertTrue(Math.abs(overallMean - 5.0) < 0.2,
            "Mean should be close to 5.0, got: " + overallMean);
    }

    @Test
    void testModelAccessors() {
        VectorSpaceModel model = new VectorSpaceModel(12345, 256);
        DimensionDistributionGenerator gen = new DimensionDistributionGenerator(model);

        assertEquals(12345, gen.uniqueVectors());
        assertEquals(256, gen.dimensions());
        assertSame(model, gen.model());
    }
}
