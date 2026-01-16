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

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for VectorGeneratorIO SPI discovery and loading.
 */
public class VectorGeneratorIOTest {

    @Test
    void testGetByName() {
        Optional<VectorGenerator<?>> gen = VectorGeneratorIO.get("dimension-distribution");

        assertTrue(gen.isPresent());
        assertEquals("dimension-distribution", gen.get().getGeneratorType());
        assertTrue(gen.get() instanceof DimensionDistributionGenerator);
    }

    @Test
    void testGetScalarByName() {
        Optional<VectorGenerator<?>> gen = VectorGeneratorIO.get("scalar-dimension-distribution");

        assertTrue(gen.isPresent());
        assertEquals("scalar-dimension-distribution", gen.get().getGeneratorType());
        assertTrue(gen.get() instanceof ScalarDimensionDistributionGenerator);
    }

    @Test
    void testGetByNameWithType() {
        Optional<VectorGenerator<VectorSpaceModel>> gen =
            VectorGeneratorIO.get("dimension-distribution", VectorSpaceModel.class);

        assertTrue(gen.isPresent());
        assertEquals("dimension-distribution", gen.get().getGeneratorType());
    }

    @Test
    void testGetUnknown() {
        Optional<VectorGenerator<?>> gen = VectorGeneratorIO.get("unknown-generator");

        assertTrue(gen.isEmpty());
    }

    @Test
    void testGetForModel() {
        Optional<VectorGenerator<?>> gen = VectorGeneratorIO.getForModel(VectorSpaceModel.class);

        assertTrue(gen.isPresent());
        // Should find one of the generators that support VectorSpaceModel
        assertNotNull(gen.get().getGeneratorType());
    }

    @Test
    void testGetForUnknownModel() {
        Optional<VectorGenerator<?>> gen = VectorGeneratorIO.getForModel(String.class);

        assertTrue(gen.isEmpty());
    }

    @Test
    void testGetAll() {
        List<VectorGenerator<?>> all = VectorGeneratorIO.getAll();

        assertFalse(all.isEmpty());
        assertTrue(all.size() >= 2);  // DimensionDistributionGenerator and ScalarDimensionDistributionGenerator
        assertTrue(all.stream().anyMatch(g -> "dimension-distribution".equals(g.getGeneratorType())));
        assertTrue(all.stream().anyMatch(g -> "scalar-dimension-distribution".equals(g.getGeneratorType())));
    }

    @Test
    void testGetAvailableNames() {
        List<String> names = VectorGeneratorIO.getAvailableNames();

        assertFalse(names.isEmpty());
        assertTrue(names.contains("dimension-distribution"));
        assertTrue(names.contains("scalar-dimension-distribution"));
    }

    @Test
    void testGetSupportedModelTypes() {
        List<String> types = VectorGeneratorIO.getSupportedModelTypes();

        assertFalse(types.isEmpty());
        assertTrue(types.contains("VectorSpaceModel"));
    }

    @Test
    void testIsAvailable() {
        assertTrue(VectorGeneratorIO.isAvailable("dimension-distribution"));
        assertTrue(VectorGeneratorIO.isAvailable("scalar-dimension-distribution"));
        assertFalse(VectorGeneratorIO.isAvailable("unknown-generator"));
    }

    @Test
    void testSupportsModel() {
        assertTrue(VectorGeneratorIO.supportsModel(VectorSpaceModel.class));
        assertFalse(VectorGeneratorIO.supportsModel(String.class));
    }

    @Test
    void testCreate() {
        VectorSpaceModel model = new VectorSpaceModel(1000, 128);

        VectorGenerator<VectorSpaceModel> gen = VectorGeneratorIO.create("dimension-distribution", model);

        assertNotNull(gen);
        assertTrue(gen.isInitialized());
        assertEquals(128, gen.dimensions());
        assertEquals(1000, gen.uniqueVectors());
    }

    @Test
    void testCreateThrowsForUnknown() {
        VectorSpaceModel model = new VectorSpaceModel(1000, 128);

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
            () -> VectorGeneratorIO.create("unknown-generator", model));

        assertTrue(ex.getMessage().contains("unknown-generator"));
        assertTrue(ex.getMessage().contains("Available"));
    }

    @Test
    void testCreateForModel() {
        VectorSpaceModel model = new VectorSpaceModel(1000, 64);

        VectorGenerator<VectorSpaceModel> gen = VectorGeneratorIO.createForModel(model);

        assertNotNull(gen);
        assertTrue(gen.isInitialized());
        assertEquals(64, gen.dimensions());
    }

    @Test
    void testGeneratorInitialization() {
        VectorSpaceModel model = new VectorSpaceModel(1000, 32);

        // Get uninitialized generator
        VectorGenerator<?> gen = VectorGeneratorIO.get("dimension-distribution").orElseThrow();
        assertFalse(gen.isInitialized());

        // Initialize
        @SuppressWarnings("unchecked")
        VectorGenerator<VectorSpaceModel> typedGen = (VectorGenerator<VectorSpaceModel>) gen;
        typedGen.initialize(model);

        assertTrue(gen.isInitialized());
        assertEquals(32, gen.dimensions());
    }

    @Test
    void testGeneratorNotInitializedThrows() {
        VectorGenerator<?> gen = VectorGeneratorIO.get("dimension-distribution").orElseThrow();

        assertThrows(IllegalStateException.class, () -> gen.apply(0L));
        assertThrows(IllegalStateException.class, gen::dimensions);
        assertThrows(IllegalStateException.class, gen::uniqueVectors);
    }

    @Test
    void testGeneratorDoubleInitializeThrows() {
        VectorSpaceModel model = new VectorSpaceModel(1000, 32);
        VectorGenerator<VectorSpaceModel> gen = VectorGeneratorIO.create("dimension-distribution", model);

        assertThrows(IllegalStateException.class, () -> gen.initialize(model));
    }

    @Test
    void testGenerateAfterSPILoad() {
        VectorSpaceModel model = new VectorSpaceModel(1000, 64);
        VectorGenerator<VectorSpaceModel> gen = VectorGeneratorIO.create("dimension-distribution", model);

        float[] vector = gen.apply(42L);

        assertNotNull(vector);
        assertEquals(64, vector.length);

        // Check determinism
        float[] vector2 = gen.apply(42L);
        assertArrayEquals(vector, vector2);
    }

    @Test
    void testScalarGeneratorViaSPI() {
        VectorSpaceModel model = new VectorSpaceModel(1000, 32);
        VectorGenerator<VectorSpaceModel> gen = VectorGeneratorIO.create("scalar-dimension-distribution", model);

        float[] vector = gen.apply(0L);
        float[][] batch = gen.generateBatch(0, 10);

        assertEquals(32, vector.length);
        assertEquals(10, batch.length);
        assertEquals(32, batch[0].length);
    }
}
