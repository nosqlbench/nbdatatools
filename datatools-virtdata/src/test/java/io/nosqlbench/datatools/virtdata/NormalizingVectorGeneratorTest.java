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

package io.nosqlbench.datatools.virtdata;

import io.nosqlbench.vshapes.model.VectorSpaceModel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class NormalizingVectorGeneratorTest {

    private static final double EPSILON = 1e-6;
    private static final int DIMENSIONS = 128;
    private static final long UNIQUE_VECTORS = 1_000_000;

    private VectorSpaceModel model;
    private VectorGenerator<VectorSpaceModel> baseGen;
    private VectorGenerator<VectorSpaceModel> normalizedGen;

    @BeforeEach
    void setup() {
        model = new VectorSpaceModel(UNIQUE_VECTORS, DIMENSIONS, 0.0, 1.0);
        baseGen = new DimensionDistributionGenerator(model);
        normalizedGen = new NormalizingVectorGenerator<>(baseGen);
    }

    @Test
    void testOutputVectorsHaveUnitNorm() {
        for (int i = 0; i < 100; i++) {
            float[] v = normalizedGen.apply(i);
            double norm = NormalizingVectorGenerator.l2Norm(v);

            assertEquals(1.0, norm, EPSILON,
                "Vector at ordinal " + i + " has norm " + norm + " (expected 1.0)");
        }
    }

    @Test
    void testDoubleOutputVectorsHaveUnitNorm() {
        for (int i = 0; i < 100; i++) {
            double[] v = normalizedGen.applyAsDouble(i);
            double norm = NormalizingVectorGenerator.l2Norm(v);

            assertEquals(1.0, norm, EPSILON,
                "Vector at ordinal " + i + " has norm " + norm + " (expected 1.0)");
        }
    }

    @Test
    void testGenerateIntoProducesUnitVectors() {
        float[] buffer = new float[DIMENSIONS * 3];

        normalizedGen.generateInto(0, buffer, 0);
        normalizedGen.generateInto(1, buffer, DIMENSIONS);
        normalizedGen.generateInto(2, buffer, DIMENSIONS * 2);

        for (int v = 0; v < 3; v++) {
            double sumSq = 0;
            for (int d = 0; d < DIMENSIONS; d++) {
                float val = buffer[v * DIMENSIONS + d];
                sumSq += val * val;
            }
            double norm = Math.sqrt(sumSq);
            assertEquals(1.0, norm, EPSILON, "Vector " + v + " norm mismatch");
        }
    }

    @Test
    void testBatchGenerationProducesUnitVectors() {
        float[][] batch = normalizedGen.generateBatch(0, 50);

        for (int i = 0; i < batch.length; i++) {
            double norm = NormalizingVectorGenerator.l2Norm(batch[i]);
            assertEquals(1.0, norm, EPSILON, "Batch vector " + i + " norm mismatch");
        }
    }

    @Test
    void testFlatBatchGenerationProducesUnitVectors() {
        int batchSize = 50;
        float[] flat = normalizedGen.generateFlatBatch(0, batchSize);

        for (int v = 0; v < batchSize; v++) {
            double sumSq = 0;
            for (int d = 0; d < DIMENSIONS; d++) {
                float val = flat[v * DIMENSIONS + d];
                sumSq += val * val;
            }
            double norm = Math.sqrt(sumSq);
            assertEquals(1.0, norm, EPSILON, "Flat batch vector " + v + " norm mismatch");
        }
    }

    @Test
    void testDeterminism() {
        // Same ordinal should always produce same normalized vector
        float[] v1 = normalizedGen.apply(42L);
        float[] v2 = normalizedGen.apply(42L);

        assertArrayEquals(v1, v2, "Normalized vectors should be deterministic");
    }

    @Test
    void testDirectionPreservation() {
        // Normalization should preserve direction
        float[] raw = baseGen.apply(42L);
        float[] normalized = normalizedGen.apply(42L);

        // Compute raw norm
        double rawNorm = NormalizingVectorGenerator.l2Norm(raw);

        // Verify normalized = raw / rawNorm
        for (int d = 0; d < DIMENSIONS; d++) {
            double expected = raw[d] / rawNorm;
            assertEquals(expected, normalized[d], EPSILON,
                "Direction not preserved at dimension " + d);
        }
    }

    @Test
    void testGeneratorType() {
        String type = normalizedGen.getGeneratorType();
        assertTrue(type.startsWith("normalizing-"),
            "Type should start with 'normalizing-': " + type);
    }

    @Test
    void testDelegateMetadata() {
        assertEquals(DIMENSIONS, normalizedGen.dimensions());
        assertEquals(UNIQUE_VECTORS, normalizedGen.uniqueVectors());
        assertNotNull(normalizedGen.model());
    }

    @Test
    void testRequiresInitializedDelegate() {
        VectorGenerator<VectorSpaceModel> uninitializedGen = new DimensionDistributionGenerator();

        assertThrows(IllegalStateException.class, () ->
            new NormalizingVectorGenerator<>(uninitializedGen));
    }

    @Test
    void testCannotReinitialize() {
        assertThrows(IllegalStateException.class, () ->
            normalizedGen.initialize(model));
    }

    @Test
    void testFactoryIntegration() {
        // Create via factory with options
        GeneratorOptions options = GeneratorOptions.builder()
            .normalizeL2(true)
            .build();

        VectorGenerator<VectorSpaceModel> gen = VectorGenFactory.create(model, options);

        // Verify output is normalized
        float[] v = gen.apply(42L);
        double norm = NormalizingVectorGenerator.l2Norm(v);
        assertEquals(1.0, norm, EPSILON);
    }

    @Test
    void testLerpWithNormalization() {
        // Create via factory with both LERP and normalization
        GeneratorOptions options = GeneratorOptions.builder()
            .useLerp(true)
            .normalizeL2(true)
            .build();

        VectorGenerator<VectorSpaceModel> gen = VectorGenFactory.create(model, options);

        // Verify output is normalized
        for (int i = 0; i < 100; i++) {
            float[] v = gen.apply(i);
            double norm = NormalizingVectorGenerator.l2Norm(v);
            assertEquals(1.0, norm, EPSILON,
                "LERP+normalized vector at ordinal " + i + " has wrong norm");
        }
    }
}
