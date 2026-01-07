package io.nosqlbench.vshapes.extract;

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

import io.nosqlbench.vshapes.model.NormalScalarModel;
import io.nosqlbench.vshapes.model.UniformScalarModel;
import io.nosqlbench.vshapes.model.VectorSpaceModel;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

@Tag("unit")
public class DatasetModelExtractorTest {

    @Test
    void testExtractFromGaussianData() {
        // Generate 1000 vectors of dimension 4 with Gaussian distribution
        Random random = new Random(12345);
        int numVectors = 1000;
        int dimensions = 4;
        float[][] data = new float[numVectors][dimensions];

        double[] trueMeans = {0.0, 5.0, -2.0, 10.0};
        double[] trueStdDevs = {1.0, 2.0, 0.5, 3.0};

        for (int v = 0; v < numVectors; v++) {
            for (int d = 0; d < dimensions; d++) {
                data[v][d] = (float) (random.nextGaussian() * trueStdDevs[d] + trueMeans[d]);
            }
        }

        DatasetModelExtractor extractor = DatasetModelExtractor.normalOnly();
        VectorSpaceModel model = extractor.extractVectorModel(data);

        assertNotNull(model);
        assertEquals(dimensions, model.dimensions());
        assertEquals(DatasetModelExtractor.DEFAULT_UNIQUE_VECTORS, model.uniqueVectors());
        assertTrue(model.isAllNormal());

        // Verify extracted parameters are close to true values
        for (int d = 0; d < dimensions; d++) {
            NormalScalarModel component = (NormalScalarModel) model.scalarModel(d);
            assertEquals(trueMeans[d], component.getMean(), 0.2);
            assertEquals(trueStdDevs[d], component.getStdDev(), 0.2);
        }
    }

    @Test
    void testExtractWithStats() {
        Random random = new Random(12345);
        int numVectors = 500;
        int dimensions = 3;
        float[][] data = new float[numVectors][dimensions];

        for (int v = 0; v < numVectors; v++) {
            for (int d = 0; d < dimensions; d++) {
                data[v][d] = (float) random.nextGaussian();
            }
        }

        DatasetModelExtractor extractor = new DatasetModelExtractor();
        ModelExtractor.ExtractionResult result = extractor.extractWithStats(data);

        assertNotNull(result);
        assertNotNull(result.model());
        assertNotNull(result.dimensionStats());
        assertNotNull(result.fitResults());
        assertTrue(result.extractionTimeMs() >= 0);

        assertEquals(dimensions, result.numDimensions());
        assertEquals(numVectors, result.numVectors());

        // Each dimension should have statistics
        assertEquals(dimensions, result.dimensionStats().length);
        for (int d = 0; d < dimensions; d++) {
            assertEquals(numVectors, result.dimensionStats()[d].count());
        }

        // Summary should be descriptive
        String summary = result.summary();
        assertNotNull(summary);
        assertTrue(summary.contains("3-dimensional"));
    }

    @Test
    void testExtractFromTransposed() {
        int numVectors = 100;
        int dimensions = 5;

        // Create transposed data (dimension-first)
        float[][] transposed = new float[dimensions][numVectors];
        Random random = new Random(12345);

        for (int d = 0; d < dimensions; d++) {
            for (int v = 0; v < numVectors; v++) {
                transposed[d][v] = (float) random.nextGaussian();
            }
        }

        DatasetModelExtractor extractor = new DatasetModelExtractor();
        VectorSpaceModel model = extractor.extractFromTransposed(transposed);

        assertNotNull(model);
        assertEquals(dimensions, model.dimensions());
    }

    @Test
    void testNormalOnlyExtractor() {
        DatasetModelExtractor extractor = DatasetModelExtractor.normalOnly();
        assertNull(extractor.getSelector());
        assertNotNull(extractor.getForcedFitter());
        assertEquals("normal", extractor.getForcedFitter().getModelType());
    }

    @Test
    void testUniformOnlyExtractor() {
        DatasetModelExtractor extractor = DatasetModelExtractor.uniformOnly();
        float[][] data = generateUniformData(100, 3, new Random(12345));

        VectorSpaceModel model = extractor.extractVectorModel(data);

        assertNotNull(model);
        for (int d = 0; d < model.dimensions(); d++) {
            assertTrue(model.scalarModel(d) instanceof UniformScalarModel,
                "Uniform-only extractor should produce uniform models");
        }
    }

    @Test
    void testEmpiricalOnlyExtractor() {
        DatasetModelExtractor extractor = DatasetModelExtractor.empiricalOnly();
        assertNull(extractor.getSelector());
        assertNotNull(extractor.getForcedFitter());
        assertEquals("empirical", extractor.getForcedFitter().getModelType());
    }

    @Test
    void testParametricOnlyExtractor() {
        DatasetModelExtractor extractor = DatasetModelExtractor.parametricOnly();
        assertNotNull(extractor.getSelector());
        assertNull(extractor.getForcedFitter());

        // Selector should have 2 fitters (Gaussian and Uniform)
        assertEquals(2, extractor.getSelector().getFitters().size());
    }

    @Test
    void testExtractWithProgress() {
        Random random = new Random(12345);
        int numVectors = 100;
        int dimensions = 5;
        float[][] data = new float[numVectors][dimensions];

        for (int v = 0; v < numVectors; v++) {
            for (int d = 0; d < dimensions; d++) {
                data[v][d] = (float) random.nextGaussian();
            }
        }

        int[] progressCount = {0};
        DatasetModelExtractor extractor = new DatasetModelExtractor();
        ModelExtractor.ExtractionResult result = extractor.extractWithProgress(data, (progress, message) -> {
            progressCount[0]++;
            assertTrue(progress >= 0 && progress <= 1, "Progress should be in [0, 1]");
            assertNotNull(message);
        });

        assertNotNull(result);
        assertTrue(progressCount[0] > 0, "Progress callback should have been called");
    }

    @Test
    void testExtractNullDataThrows() {
        DatasetModelExtractor extractor = new DatasetModelExtractor();
        assertThrows(IllegalArgumentException.class, () -> extractor.extractVectorModel(null));
    }

    @Test
    void testExtractEmptyDataThrows() {
        DatasetModelExtractor extractor = new DatasetModelExtractor();
        assertThrows(IllegalArgumentException.class, () -> extractor.extractVectorModel(new float[0][]));
    }

    @Test
    void testExtractJaggedDataThrows() {
        DatasetModelExtractor extractor = new DatasetModelExtractor();
        float[][] jagged = {
            {1f, 2f, 3f},
            {4f, 5f}  // Different length
        };
        assertThrows(IllegalArgumentException.class, () -> extractor.extractVectorModel(jagged));
    }

    @Test
    void testCustomUniqueVectors() {
        long customUniqueVectors = 500_000;
        DatasetModelExtractor extractor = new DatasetModelExtractor(
            BestFitSelector.defaultSelector(), customUniqueVectors);

        assertEquals(customUniqueVectors, extractor.getUniqueVectors());

        float[][] data = generateGaussianData(100, 3, new Random(12345));
        VectorSpaceModel model = extractor.extractVectorModel(data);

        assertEquals(customUniqueVectors, model.uniqueVectors());
    }

    @Test
    void testInvalidUniqueVectorsThrows() {
        assertThrows(IllegalArgumentException.class, () ->
            new DatasetModelExtractor(BestFitSelector.defaultSelector(), 0));
        assertThrows(IllegalArgumentException.class, () ->
            new DatasetModelExtractor(BestFitSelector.defaultSelector(), -1));
    }

    private float[][] generateGaussianData(int numVectors, int dimensions, Random random) {
        float[][] data = new float[numVectors][dimensions];
        for (int v = 0; v < numVectors; v++) {
            for (int d = 0; d < dimensions; d++) {
                data[v][d] = (float) random.nextGaussian();
            }
        }
        return data;
    }

    private float[][] generateUniformData(int numVectors, int dimensions, Random random) {
        float[][] data = new float[numVectors][dimensions];
        for (int v = 0; v < numVectors; v++) {
            for (int d = 0; d < dimensions; d++) {
                data[v][d] = (float) random.nextDouble();
            }
        }
        return data;
    }
}
