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

import io.nosqlbench.vshapes.model.GaussianComponentModel;
import io.nosqlbench.vshapes.model.VectorSpaceModel;
import io.nosqlbench.vshapes.model.VectorSpaceModelConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link VectorSpaceModelConfig} JSON serialization.
 */
class VectorSpaceModelConfigTest {

    @Test
    void testUniformConfigFromJson() {
        String json = """
            {
              "unique_vectors": 1000000,
              "dimensions": 128,
              "mean": 0.0,
              "std_dev": 1.0
            }
            """;

        VectorSpaceModel model = VectorSpaceModelConfig.load(json);

        assertEquals(1_000_000, model.uniqueVectors());
        assertEquals(128, model.dimensions());
        GaussianComponentModel c0 = (GaussianComponentModel) model.componentModel(0);
        assertEquals(0.0, c0.getMean());
        assertEquals(1.0, c0.getStdDev());
        assertFalse(c0.isTruncated());
    }

    @Test
    void testUniformTruncatedConfigFromJson() {
        String json = """
            {
              "unique_vectors": 500000,
              "dimensions": 64,
              "mean": 0.0,
              "std_dev": 1.0,
              "lower_bound": -1.0,
              "upper_bound": 1.0
            }
            """;

        VectorSpaceModel model = VectorSpaceModelConfig.load(json);

        assertEquals(500_000, model.uniqueVectors());
        assertEquals(64, model.dimensions());
        GaussianComponentModel component = (GaussianComponentModel) model.componentModel(0);
        assertEquals(0.0, component.getMean());
        assertEquals(1.0, component.getStdDev());
        assertTrue(component.isTruncated());
        assertEquals(-1.0, component.lower());
        assertEquals(1.0, component.upper());
    }

    @Test
    void testPerDimensionConfigFromJson() {
        String json = """
            {
              "unique_vectors": 100000,
              "components": [
                {"mean": 0.0, "std_dev": 1.0},
                {"mean": 0.5, "std_dev": 0.5, "lower_bound": 0.0, "upper_bound": 1.0},
                {"mean": -1.0, "std_dev": 2.0}
              ]
            }
            """;

        VectorSpaceModel model = VectorSpaceModelConfig.load(json);

        assertEquals(100_000, model.uniqueVectors());
        assertEquals(3, model.dimensions());

        // First component: unbounded N(0, 1)
        GaussianComponentModel c0 = (GaussianComponentModel) model.componentModel(0);
        assertEquals(0.0, c0.getMean());
        assertEquals(1.0, c0.getStdDev());
        assertFalse(c0.isTruncated());

        // Second component: truncated N(0.5, 0.5) to [0, 1]
        GaussianComponentModel c1 = (GaussianComponentModel) model.componentModel(1);
        assertEquals(0.5, c1.getMean());
        assertEquals(0.5, c1.getStdDev());
        assertTrue(c1.isTruncated());
        assertEquals(0.0, c1.lower());
        assertEquals(1.0, c1.upper());

        // Third component: unbounded N(-1, 2)
        GaussianComponentModel c2 = (GaussianComponentModel) model.componentModel(2);
        assertEquals(-1.0, c2.getMean());
        assertEquals(2.0, c2.getStdDev());
        assertFalse(c2.isTruncated());
    }

    @Test
    void testRoundTripUniformModel() {
        VectorSpaceModel original = new VectorSpaceModel(1_000_000, 128, 0.0, 1.0);

        VectorSpaceModelConfig config = VectorSpaceModelConfig.fromVectorSpaceModel(original);
        String json = config.toJson();

        VectorSpaceModel restored = VectorSpaceModelConfig.load(json);

        assertEquals(original.uniqueVectors(), restored.uniqueVectors());
        assertEquals(original.dimensions(), restored.dimensions());
        GaussianComponentModel origComp = (GaussianComponentModel) original.componentModel(0);
        GaussianComponentModel restComp = (GaussianComponentModel) restored.componentModel(0);
        assertEquals(origComp.getMean(), restComp.getMean());
        assertEquals(origComp.getStdDev(), restComp.getStdDev());
    }

    @Test
    void testRoundTripTruncatedModel() {
        VectorSpaceModel original = VectorSpaceModel.unitBounded(500_000, 64);

        VectorSpaceModelConfig config = VectorSpaceModelConfig.fromVectorSpaceModel(original);
        String json = config.toJson();

        VectorSpaceModel restored = VectorSpaceModelConfig.load(json);

        assertEquals(original.uniqueVectors(), restored.uniqueVectors());
        assertEquals(original.dimensions(), restored.dimensions());

        GaussianComponentModel origComp = (GaussianComponentModel) original.componentModel(0);
        GaussianComponentModel restComp = (GaussianComponentModel) restored.componentModel(0);
        assertEquals(origComp.getMean(), restComp.getMean());
        assertEquals(origComp.getStdDev(), restComp.getStdDev());
        assertEquals(origComp.isTruncated(), restComp.isTruncated());
        assertEquals(origComp.lower(), restComp.lower());
        assertEquals(origComp.upper(), restComp.upper());
    }

    @Test
    void testRoundTripPerDimensionModel() {
        GaussianComponentModel[] components = {
            new GaussianComponentModel(0.0, 1.0),
            new GaussianComponentModel(0.5, 0.5, 0.0, 1.0),
            new GaussianComponentModel(-1.0, 2.0)
        };
        VectorSpaceModel original = new VectorSpaceModel(100_000, components);

        VectorSpaceModelConfig config = VectorSpaceModelConfig.fromVectorSpaceModel(original);
        String json = config.toJson();

        VectorSpaceModel restored = VectorSpaceModelConfig.load(json);

        assertEquals(original.uniqueVectors(), restored.uniqueVectors());
        assertEquals(original.dimensions(), restored.dimensions());

        for (int i = 0; i < original.dimensions(); i++) {
            GaussianComponentModel orig = (GaussianComponentModel) original.componentModel(i);
            GaussianComponentModel rest = (GaussianComponentModel) restored.componentModel(i);
            assertEquals(orig.getMean(), rest.getMean(), "mean mismatch at dim " + i);
            assertEquals(orig.getStdDev(), rest.getStdDev(), "stdDev mismatch at dim " + i);
            assertEquals(orig.isTruncated(), rest.isTruncated(), "truncated mismatch at dim " + i);
            if (orig.isTruncated()) {
                assertEquals(orig.lower(), rest.lower(), "lower mismatch at dim " + i);
                assertEquals(orig.upper(), rest.upper(), "upper mismatch at dim " + i);
            }
        }
    }

    @Test
    void testLoadFromReader() {
        String json = """
            {
              "unique_vectors": 100,
              "dimensions": 10,
              "mean": 0.0,
              "std_dev": 1.0
            }
            """;

        try (StringReader reader = new StringReader(json)) {
            VectorSpaceModel model = VectorSpaceModelConfig.load(reader);
            assertEquals(100, model.uniqueVectors());
            assertEquals(10, model.dimensions());
        }
    }

    @Test
    void testWriteToWriter() {
        VectorSpaceModel model = new VectorSpaceModel(100, 10, 0.0, 1.0);
        VectorSpaceModelConfig config = VectorSpaceModelConfig.fromVectorSpaceModel(model);

        StringWriter writer = new StringWriter();
        config.toJson(writer);

        String json = writer.toString();
        assertTrue(json.contains("\"unique_vectors\": 100"));
        assertTrue(json.contains("\"dimensions\": 10"));
    }

    @Test
    void testFileRoundTrip(@TempDir Path tempDir) throws IOException {
        VectorSpaceModel original = VectorSpaceModel.unitBounded(1_000_000, 128);
        Path file = tempDir.resolve("test_config.json");

        VectorSpaceModelConfig.saveToFile(original, file);

        assertTrue(file.toFile().exists());

        VectorSpaceModel restored = VectorSpaceModelConfig.loadFromFile(file);

        assertEquals(original.uniqueVectors(), restored.uniqueVectors());
        assertEquals(original.dimensions(), restored.dimensions());
        GaussianComponentModel origComp = (GaussianComponentModel) original.componentModel(0);
        GaussianComponentModel restComp = (GaussianComponentModel) restored.componentModel(0);
        assertEquals(origComp.isTruncated(), restComp.isTruncated());
    }

    @Test
    void testDefaultValues() {
        // Test that missing mean/stdDev use defaults (0.0, 1.0)
        String json = """
            {
              "unique_vectors": 1000,
              "dimensions": 10
            }
            """;

        VectorSpaceModel model = VectorSpaceModelConfig.load(json);

        assertEquals(1000, model.uniqueVectors());
        assertEquals(10, model.dimensions());
        GaussianComponentModel c0 = (GaussianComponentModel) model.componentModel(0);
        assertEquals(0.0, c0.getMean());
        assertEquals(1.0, c0.getStdDev());
    }

    @Test
    void testProgrammaticConstruction() {
        VectorSpaceModelConfig config = new VectorSpaceModelConfig();
        config.setUniqueVectors(1_000_000L);
        config.setDimensions(128);
        config.setMean(0.0);
        config.setStdDev(1.0);
        config.setLowerBound(-1.0);
        config.setUpperBound(1.0);

        VectorSpaceModel model = config.toVectorSpaceModel();

        assertEquals(1_000_000, model.uniqueVectors());
        assertEquals(128, model.dimensions());
        GaussianComponentModel comp = (GaussianComponentModel) model.componentModel(0);
        assertTrue(comp.isTruncated());
        assertEquals(-1.0, comp.lower());
        assertEquals(1.0, comp.upper());
    }

    @Test
    void testConfigToString() {
        VectorSpaceModelConfig config = new VectorSpaceModelConfig();
        config.setUniqueVectors(100L);
        config.setDimensions(10);

        String str = config.toString();
        assertTrue(str.contains("\"unique_vectors\": 100"));
    }

    @Test
    void testMissingRequiredFields() {
        // Missing unique_vectors
        String json1 = """
            {
              "dimensions": 10,
              "mean": 0.0,
              "std_dev": 1.0
            }
            """;

        VectorSpaceModelConfig config1 = VectorSpaceModelConfig.fromJson(json1);
        assertThrows(NullPointerException.class, config1::toVectorSpaceModel);

        // Missing dimensions for uniform config (no components)
        String json2 = """
            {
              "unique_vectors": 1000,
              "mean": 0.0,
              "std_dev": 1.0
            }
            """;

        VectorSpaceModelConfig config2 = VectorSpaceModelConfig.fromJson(json2);
        assertThrows(NullPointerException.class, config2::toVectorSpaceModel);
    }

    @Test
    void testConfigDetectsUniformDistribution() {
        // When all components are identical, config should use uniform representation
        GaussianComponentModel same = new GaussianComponentModel(0.0, 1.0);
        GaussianComponentModel[] components = new GaussianComponentModel[10];
        for (int i = 0; i < 10; i++) {
            components[i] = same;
        }
        VectorSpaceModel model = new VectorSpaceModel(1000, components);

        VectorSpaceModelConfig config = VectorSpaceModelConfig.fromVectorSpaceModel(model);

        assertFalse(config.hasPerDimensionComponents());
        assertNotNull(config.getDimensions());
        assertEquals(10, config.getDimensions());
    }

    @Test
    void testConfigDetectsPerDimensionDistribution() {
        GaussianComponentModel[] components = {
            new GaussianComponentModel(0.0, 1.0),
            new GaussianComponentModel(1.0, 2.0)  // Different from first
        };
        VectorSpaceModel model = new VectorSpaceModel(1000, components);

        VectorSpaceModelConfig config = VectorSpaceModelConfig.fromVectorSpaceModel(model);

        assertTrue(config.hasPerDimensionComponents());
        assertNotNull(config.getComponents());
        assertEquals(2, config.getComponents().length);
    }
}
