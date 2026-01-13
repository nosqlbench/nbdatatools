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

import io.nosqlbench.vshapes.model.CompositeScalarModel;
import io.nosqlbench.vshapes.model.NormalScalarModel;
import io.nosqlbench.vshapes.model.ScalarModel;
import io.nosqlbench.vshapes.model.UniformScalarModel;
import io.nosqlbench.vshapes.model.VectorSpaceModel;
import io.nosqlbench.vshapes.model.VectorSpaceModelConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.file.Path;
import java.util.List;

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
        NormalScalarModel c0 = (NormalScalarModel) model.scalarModel(0);
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
        NormalScalarModel component = (NormalScalarModel) model.scalarModel(0);
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
        NormalScalarModel c0 = (NormalScalarModel) model.scalarModel(0);
        assertEquals(0.0, c0.getMean());
        assertEquals(1.0, c0.getStdDev());
        assertFalse(c0.isTruncated());

        // Second component: truncated N(0.5, 0.5) to [0, 1]
        NormalScalarModel c1 = (NormalScalarModel) model.scalarModel(1);
        assertEquals(0.5, c1.getMean());
        assertEquals(0.5, c1.getStdDev());
        assertTrue(c1.isTruncated());
        assertEquals(0.0, c1.lower());
        assertEquals(1.0, c1.upper());

        // Third component: unbounded N(-1, 2)
        NormalScalarModel c2 = (NormalScalarModel) model.scalarModel(2);
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
        NormalScalarModel origComp = (NormalScalarModel) original.scalarModel(0);
        NormalScalarModel restComp = (NormalScalarModel) restored.scalarModel(0);
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

        NormalScalarModel origComp = (NormalScalarModel) original.scalarModel(0);
        NormalScalarModel restComp = (NormalScalarModel) restored.scalarModel(0);
        assertEquals(origComp.getMean(), restComp.getMean());
        assertEquals(origComp.getStdDev(), restComp.getStdDev());
        assertEquals(origComp.isTruncated(), restComp.isTruncated());
        assertEquals(origComp.lower(), restComp.lower());
        assertEquals(origComp.upper(), restComp.upper());
    }

    @Test
    void testRoundTripPerDimensionModel() {
        NormalScalarModel[] components = {
            new NormalScalarModel(0.0, 1.0),
            new NormalScalarModel(0.5, 0.5, 0.0, 1.0),
            new NormalScalarModel(-1.0, 2.0)
        };
        VectorSpaceModel original = new VectorSpaceModel(100_000, components);

        VectorSpaceModelConfig config = VectorSpaceModelConfig.fromVectorSpaceModel(original);
        String json = config.toJson();

        VectorSpaceModel restored = VectorSpaceModelConfig.load(json);

        assertEquals(original.uniqueVectors(), restored.uniqueVectors());
        assertEquals(original.dimensions(), restored.dimensions());

        for (int i = 0; i < original.dimensions(); i++) {
            NormalScalarModel orig = (NormalScalarModel) original.scalarModel(i);
            NormalScalarModel rest = (NormalScalarModel) restored.scalarModel(i);
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
        NormalScalarModel origComp = (NormalScalarModel) original.scalarModel(0);
        NormalScalarModel restComp = (NormalScalarModel) restored.scalarModel(0);
        assertEquals(origComp.isTruncated(), restComp.isTruncated());
    }

    /// Tests that composite (mixture) models survive file round-trip serialization.
    ///
    /// This test ensures that VectorSpaceModelConfig.ComponentConfig properly
    /// serializes and deserializes CompositeScalarModel instances, including
    /// their sub-models and weights. This is critical for the --reference-model
    /// feature where generated models with composite distributions are saved
    /// and later loaded for comparison.
    @Test
    void testCompositeModelFileRoundTrip(@TempDir Path tempDir) throws IOException {
        // Create a model with composite distributions (mixture models)
        ScalarModel[] dimensions = new ScalarModel[3];

        // Dimension 0: simple normal
        dimensions[0] = new NormalScalarModel(0.0, 1.0, -1.0, 1.0);

        // Dimension 1: 2-component composite (bimodal)
        dimensions[1] = new CompositeScalarModel(
            List.of(
                new NormalScalarModel(-0.5, 0.2, -1.0, 1.0),
                new NormalScalarModel(0.5, 0.2, -1.0, 1.0)
            ),
            new double[]{0.6, 0.4}
        );

        // Dimension 2: 3-component composite with mixed types
        dimensions[2] = new CompositeScalarModel(
            List.of(
                new UniformScalarModel(-1.0, -0.3),
                new NormalScalarModel(0.0, 0.2, -1.0, 1.0),
                new UniformScalarModel(0.3, 1.0)
            ),
            new double[]{0.3, 0.4, 0.3}
        );

        VectorSpaceModel original = new VectorSpaceModel(1_000_000, dimensions);
        Path file = tempDir.resolve("composite_model.json");

        // Save to file
        VectorSpaceModelConfig.saveToFile(original, file);
        assertTrue(file.toFile().exists());

        // Load from file
        VectorSpaceModel restored = VectorSpaceModelConfig.loadFromFile(file);

        // Verify basic structure
        assertEquals(original.uniqueVectors(), restored.uniqueVectors());
        assertEquals(original.dimensions(), restored.dimensions());

        // Verify dimension 0: simple normal
        assertInstanceOf(NormalScalarModel.class, restored.scalarModel(0));

        // Verify dimension 1: 2-component composite
        assertInstanceOf(CompositeScalarModel.class, restored.scalarModel(1));
        CompositeScalarModel restored1 = (CompositeScalarModel) restored.scalarModel(1);
        assertEquals(2, restored1.getComponentCount());
        double[] weights1 = restored1.getWeights();
        assertEquals(0.6, weights1[0], 0.001);
        assertEquals(0.4, weights1[1], 0.001);

        // Verify dimension 2: 3-component composite with mixed types
        assertInstanceOf(CompositeScalarModel.class, restored.scalarModel(2));
        CompositeScalarModel restored2 = (CompositeScalarModel) restored.scalarModel(2);
        assertEquals(3, restored2.getComponentCount());
        ScalarModel[] subModels = restored2.getScalarModels();
        assertInstanceOf(UniformScalarModel.class, subModels[0]);
        assertInstanceOf(NormalScalarModel.class, subModels[1]);
        assertInstanceOf(UniformScalarModel.class, subModels[2]);
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
        NormalScalarModel c0 = (NormalScalarModel) model.scalarModel(0);
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
        NormalScalarModel comp = (NormalScalarModel) model.scalarModel(0);
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
        NormalScalarModel same = new NormalScalarModel(0.0, 1.0);
        NormalScalarModel[] components = new NormalScalarModel[10];
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
        NormalScalarModel[] components = {
            new NormalScalarModel(0.0, 1.0),
            new NormalScalarModel(1.0, 2.0)  // Different from first
        };
        VectorSpaceModel model = new VectorSpaceModel(1000, components);

        VectorSpaceModelConfig config = VectorSpaceModelConfig.fromVectorSpaceModel(model);

        assertTrue(config.hasPerDimensionComponents());
        assertNotNull(config.getComponents());
        assertEquals(2, config.getComponents().length);
    }

    @Test
    void testJsonOutputHasNoNullValues() {
        // Create a model with per-dimension components
        NormalScalarModel[] components = {
            new NormalScalarModel(0.0, 1.0),
            new NormalScalarModel(0.5, 0.5, 0.0, 1.0)  // truncated
        };
        VectorSpaceModel model = new VectorSpaceModel(1000, components);

        VectorSpaceModelConfig config = VectorSpaceModelConfig.fromVectorSpaceModel(model);
        String json = config.toJson();

        // Verify no null values appear in the output
        assertFalse(json.contains("null"), "JSON should not contain null values: " + json);

        // Verify the type field is included
        assertTrue(json.contains("\"type\":"), "JSON should include type field: " + json);

        // Verify model-specific fields are present
        assertTrue(json.contains("\"mean\":"), "Normal model should have mean: " + json);
        assertTrue(json.contains("\"std_dev\":"), "Normal model should have std_dev: " + json);

        // Verify truncation bounds only appear for truncated model
        // The second component has truncation, so we should see lower_bound/upper_bound
        assertTrue(json.contains("\"lower_bound\":"), "Truncated model should have lower_bound: " + json);
        assertTrue(json.contains("\"upper_bound\":"), "Truncated model should have upper_bound: " + json);

        System.out.println("JSON output:\n" + json);
    }

    @Test
    void testUniformJsonOutputIsCompact() {
        // Uniform model should have compact representation (no components array)
        VectorSpaceModel model = new VectorSpaceModel(1_000_000, 128, 0.0, 1.0);

        VectorSpaceModelConfig config = VectorSpaceModelConfig.fromVectorSpaceModel(model);
        String json = config.toJson();

        // Verify no null values
        assertFalse(json.contains("null"), "JSON should not contain null values: " + json);

        // Verify compact format (dimensions, not components)
        assertTrue(json.contains("\"dimensions\":"), "Should use compact dimensions format: " + json);
        assertFalse(json.contains("\"components\":"), "Should not have components array for uniform model: " + json);

        System.out.println("Uniform JSON output:\n" + json);
    }
}
