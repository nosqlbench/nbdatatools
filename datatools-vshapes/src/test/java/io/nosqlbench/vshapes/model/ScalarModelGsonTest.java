package io.nosqlbench.vshapes.model;

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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.nosqlbench.vshapes.checkpoint.VshapesGsonConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for ScalarModel Gson serialization and deserialization.
 *
 * <p>Verifies that:
 * <ul>
 *   <li>All ScalarModel types can be serialized to JSON</li>
 *   <li>All ScalarModel types can be deserialized from JSON</li>
 *   <li>Round-trip serialization preserves model parameters</li>
 *   <li>Polymorphic type resolution works correctly</li>
 * </ul>
 */
@Tag("unit")
public class ScalarModelGsonTest {

    private static Gson gson;

    @BeforeAll
    static void setup() {
        // Create a fresh Gson with all required features
        gson = new GsonBuilder()
            .setPrettyPrinting()
            .disableHtmlEscaping()
            .serializeSpecialFloatingPointValues()
            .registerTypeAdapterFactory(ScalarModelTypeAdapterFactory.create())
            .create();
    }
    private static final double TOLERANCE = 1e-10;

    @Test
    void normalScalarModelRoundTrip() {
        // Use truncated normal to avoid Infinity bounds
        NormalScalarModel original = new NormalScalarModel(5.0, 2.0, 0.0, 10.0);

        String json = gson.toJson(original, ScalarModel.class);
        System.out.println("Normal JSON: " + json);
        assertTrue(json.contains("\"type\": \"normal\"") || json.contains("\"type\":\"normal\""),
            "Expected type field in JSON: " + json);
        assertTrue(json.contains("\"mean\"") || json.contains("mean"),
            "Expected mean field in JSON: " + json);

        ScalarModel restored = gson.fromJson(json, ScalarModel.class);

        assertInstanceOf(NormalScalarModel.class, restored);
        NormalScalarModel restoredNormal = (NormalScalarModel) restored;
        assertEquals(original.getMean(), restoredNormal.getMean(), TOLERANCE);
        assertEquals(original.getStdDev(), restoredNormal.getStdDev(), TOLERANCE);
    }

    @Test
    void uniformScalarModelRoundTrip() {
        UniformScalarModel original = new UniformScalarModel(-1.0, 1.0);

        String json = gson.toJson(original, ScalarModel.class);
        assertTrue(json.contains("\"type\"") && json.contains("\"uniform\""),
            "Expected type field in JSON: " + json);

        ScalarModel restored = gson.fromJson(json, ScalarModel.class);

        assertInstanceOf(UniformScalarModel.class, restored);
        UniformScalarModel restoredUniform = (UniformScalarModel) restored;
        assertEquals(original.getLower(), restoredUniform.getLower(), TOLERANCE);
        assertEquals(original.getUpper(), restoredUniform.getUpper(), TOLERANCE);
    }

    @Test
    void betaScalarModelRoundTrip() {
        BetaScalarModel original = new BetaScalarModel(2.0, 5.0, 0.0, 1.0);

        String json = gson.toJson(original, ScalarModel.class);
        assertTrue(json.contains("\"type\"") && json.contains("\"beta\""),
            "Expected type field in JSON: " + json);

        ScalarModel restored = gson.fromJson(json, ScalarModel.class);

        assertInstanceOf(BetaScalarModel.class, restored);
        BetaScalarModel restoredBeta = (BetaScalarModel) restored;
        assertEquals(original.getAlpha(), restoredBeta.getAlpha(), TOLERANCE);
        assertEquals(original.getBeta(), restoredBeta.getBeta(), TOLERANCE);
        assertEquals(original.getLower(), restoredBeta.getLower(), TOLERANCE);
        assertEquals(original.getUpper(), restoredBeta.getUpper(), TOLERANCE);
    }

    @Test
    void gammaScalarModelRoundTrip() {
        GammaScalarModel original = new GammaScalarModel(2.0, 3.0, 0.5);

        String json = gson.toJson(original, ScalarModel.class);
        assertTrue(json.contains("\"type\"") && json.contains("\"gamma\""),
            "Expected type field in JSON: " + json);

        ScalarModel restored = gson.fromJson(json, ScalarModel.class);

        assertInstanceOf(GammaScalarModel.class, restored);
        GammaScalarModel restoredGamma = (GammaScalarModel) restored;
        assertEquals(original.getShape(), restoredGamma.getShape(), TOLERANCE);
        assertEquals(original.getScale(), restoredGamma.getScale(), TOLERANCE);
        assertEquals(original.getLocation(), restoredGamma.getLocation(), TOLERANCE);
    }

    @Test
    void studentTScalarModelRoundTrip() {
        StudentTScalarModel original = new StudentTScalarModel(5.0, 1.0, 2.0);

        String json = gson.toJson(original, ScalarModel.class);
        assertTrue(json.contains("\"type\"") && json.contains("\"student_t\""),
            "Expected type field in JSON: " + json);

        ScalarModel restored = gson.fromJson(json, ScalarModel.class);

        assertInstanceOf(StudentTScalarModel.class, restored);
        StudentTScalarModel restoredT = (StudentTScalarModel) restored;
        assertEquals(original.getDegreesOfFreedom(), restoredT.getDegreesOfFreedom(), TOLERANCE);
        assertEquals(original.getLocation(), restoredT.getLocation(), TOLERANCE);
        assertEquals(original.getScale(), restoredT.getScale(), TOLERANCE);
    }

    @Test
    void empiricalScalarModelRoundTrip() {
        // Create sample data
        float[] samples = new float[100];
        for (int i = 0; i < samples.length; i++) {
            samples[i] = i / 100.0f;
        }
        EmpiricalScalarModel original = EmpiricalScalarModel.fromData(samples, 10);

        String json = gson.toJson(original, ScalarModel.class);
        assertTrue(json.contains("\"type\"") && json.contains("\"empirical\""),
            "Expected type field in JSON: " + json);

        ScalarModel restored = gson.fromJson(json, ScalarModel.class);

        assertInstanceOf(EmpiricalScalarModel.class, restored);
        EmpiricalScalarModel restoredEmpirical = (EmpiricalScalarModel) restored;
        assertEquals(original.getMin(), restoredEmpirical.getMin(), TOLERANCE);
        assertEquals(original.getMax(), restoredEmpirical.getMax(), TOLERANCE);
        assertEquals(original.getMean(), restoredEmpirical.getMean(), TOLERANCE);
        assertEquals(original.getBinCount(), restoredEmpirical.getBinCount());
    }

    @Test
    void compositeScalarModelRoundTrip() {
        ScalarModel component1 = new NormalScalarModel(0.0, 1.0);
        ScalarModel component2 = new NormalScalarModel(5.0, 0.5);
        CompositeScalarModel original = new CompositeScalarModel(
            List.of(component1, component2),
            new double[]{0.7, 0.3}
        );

        String json = gson.toJson(original, ScalarModel.class);
        assertTrue(json.contains("\"type\"") && json.contains("\"composite\""),
            "Expected type field in JSON: " + json);

        ScalarModel restored = gson.fromJson(json, ScalarModel.class);

        assertInstanceOf(CompositeScalarModel.class, restored);
        CompositeScalarModel restoredComposite = (CompositeScalarModel) restored;
        assertEquals(2, restoredComposite.getComponentCount());
        double[] originalWeights = original.getWeights();
        double[] restoredWeights = restoredComposite.getWeights();
        assertEquals(originalWeights.length, restoredWeights.length);
        for (int i = 0; i < originalWeights.length; i++) {
            assertEquals(originalWeights[i], restoredWeights[i], TOLERANCE);
        }

        // Verify components
        ScalarModel[] restoredModels = restoredComposite.getScalarModels();
        assertInstanceOf(NormalScalarModel.class, restoredModels[0]);
        assertInstanceOf(NormalScalarModel.class, restoredModels[1]);
    }

    @Test
    void allRegisteredTypesRoundTrip() {
        // Test all registered types in ScalarModelTypeAdapterFactory
        List<ScalarModel> models = List.of(
            new NormalScalarModel(0.0, 1.0),
            new UniformScalarModel(0.0, 1.0),
            new BetaScalarModel(2.0, 2.0),
            new GammaScalarModel(2.0, 1.0),
            new InverseGammaScalarModel(3.0, 2.0),
            new StudentTScalarModel(5.0, 0.0, 1.0),
            new BetaPrimeScalarModel(2.0, 3.0)
        );

        for (ScalarModel original : models) {
            String json = gson.toJson(original, ScalarModel.class);
            ScalarModel restored = gson.fromJson(json, ScalarModel.class);

            assertEquals(original.getClass(), restored.getClass(),
                "Type mismatch for " + original.getModelType());
            assertEquals(original.getModelType(), restored.getModelType());
        }
    }

    @Test
    void deserializationWithTypeField() {
        String json = """
            {
              "type": "normal",
              "mean": 3.14,
              "std_dev": 1.59
            }
            """;

        ScalarModel model = gson.fromJson(json, ScalarModel.class);

        assertInstanceOf(NormalScalarModel.class, model);
        NormalScalarModel normal = (NormalScalarModel) model;
        assertEquals(3.14, normal.getMean(), TOLERANCE);
        assertEquals(1.59, normal.getStdDev(), TOLERANCE);
    }

    @Test
    void deserializationWithMissingTypeThrows() {
        String json = """
            {
              "mean": 3.14,
              "stdDev": 1.59
            }
            """;

        assertThrows(IllegalArgumentException.class, () ->
            gson.fromJson(json, ScalarModel.class));
    }

    @Test
    void deserializationWithUnknownTypeThrows() {
        String json = """
            {
              "type": "unknown_distribution",
              "param": 1.0
            }
            """;

        assertThrows(IllegalArgumentException.class, () ->
            gson.fromJson(json, ScalarModel.class));
    }

    @Test
    void nullModelSerializesAsNull() {
        ScalarModel nullModel = null;
        String json = gson.toJson(nullModel, ScalarModel.class);
        assertEquals("null", json);
    }

    @Test
    void nullModelDeserializesAsNull() {
        ScalarModel model = gson.fromJson("null", ScalarModel.class);
        assertNull(model);
    }

    @Test
    void unboundedNormalModelWithInfinity() {
        // Test that unbounded NormalScalarModel (with -Infinity/+Infinity bounds) serializes correctly
        NormalScalarModel original = new NormalScalarModel(0.0, 1.0);  // Unbounded

        String json = gson.toJson(original, ScalarModel.class);
        assertTrue(json.contains("-Infinity") || json.contains("\"-Infinity\""),
            "Expected -Infinity in JSON: " + json);
        assertTrue(json.contains("Infinity"),  // either +Infinity or Infinity
            "Expected Infinity in JSON: " + json);

        ScalarModel restored = gson.fromJson(json, ScalarModel.class);

        assertInstanceOf(NormalScalarModel.class, restored);
        NormalScalarModel restoredNormal = (NormalScalarModel) restored;
        assertEquals(original.getMean(), restoredNormal.getMean(), TOLERANCE);
        assertEquals(original.getStdDev(), restoredNormal.getStdDev(), TOLERANCE);
        assertEquals(Double.NEGATIVE_INFINITY, restoredNormal.lower());
        assertEquals(Double.POSITIVE_INFINITY, restoredNormal.upper());
    }

    @Test
    void vectorSpaceModelWithScalarModelsRoundTrip() {
        // Create a VectorSpaceModel with various ScalarModel types
        ScalarModel[] models = {
            new NormalScalarModel(0.0, 1.0),
            new UniformScalarModel(0.0, 1.0),
            new BetaScalarModel(2.0, 3.0)
        };

        VectorSpaceModel original = new VectorSpaceModel(1000L, models);

        String json = gson.toJson(original);
        VectorSpaceModel restored = gson.fromJson(json, VectorSpaceModel.class);

        assertEquals(original.uniqueVectors(), restored.uniqueVectors());
        assertEquals(original.dimensions(), restored.dimensions());

        for (int i = 0; i < original.dimensions(); i++) {
            assertEquals(original.scalarModel(i).getClass(),
                         restored.scalarModel(i).getClass());
            assertEquals(original.scalarModel(i).getModelType(),
                         restored.scalarModel(i).getModelType());
        }
    }
}
