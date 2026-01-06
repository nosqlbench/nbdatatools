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

import io.nosqlbench.vshapes.model.ScalarModel;
import io.nosqlbench.vshapes.model.VectorSpaceModel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for NumaAwareDatasetModelExtractor.
 */
@Tag("unit")
public class NumaAwareDatasetModelExtractorTest {

    private NumaAwareDatasetModelExtractor extractor;

    @BeforeEach
    void setUp() {
        extractor = NumaAwareDatasetModelExtractor.builder()
            .threadsPerNode(2)
            .batchSize(16)
            .build();
    }

    @AfterEach
    void tearDown() {
        if (extractor != null) {
            extractor.shutdown();
        }
    }

    @Test
    void extractVectorModel_basicExtraction() {
        float[][] data = generateTestData(500, 32, 42);

        VectorSpaceModel model = extractor.extractVectorModel(data);

        assertNotNull(model);
        assertEquals(32, model.dimensions());
    }

    @Test
    void extractVectorModel_matchesSequentialExtractor() {
        float[][] data = generateTestData(300, 24, 123);

        DatasetModelExtractor sequential = new DatasetModelExtractor();
        VectorSpaceModel seqModel = sequential.extractVectorModel(data);
        VectorSpaceModel numaModel = extractor.extractVectorModel(data);

        assertEquals(seqModel.dimensions(), numaModel.dimensions());

        // Verify component types match
        for (int d = 0; d < seqModel.dimensions(); d++) {
            ScalarModel seqComp = seqModel.scalarModel(d);
            ScalarModel numaComp = numaModel.scalarModel(d);
            assertEquals(seqComp.getClass(), numaComp.getClass(),
                "Component type mismatch at dimension " + d);
        }
    }

    @Test
    void extractVectorModel_matchesParallelExtractor() {
        float[][] data = generateTestData(300, 24, 456);

        ParallelDatasetModelExtractor parallel = ParallelDatasetModelExtractor.builder()
            .parallelism(4)
            .build();

        try {
            VectorSpaceModel parModel = parallel.extractVectorModel(data);
            VectorSpaceModel numaModel = extractor.extractVectorModel(data);

            assertEquals(parModel.dimensions(), numaModel.dimensions());

            // Verify component types match
            for (int d = 0; d < parModel.dimensions(); d++) {
                ScalarModel parComp = parModel.scalarModel(d);
                ScalarModel numaComp = numaModel.scalarModel(d);
                assertEquals(parComp.getClass(), numaComp.getClass(),
                    "Component type mismatch at dimension " + d);
            }
        } finally {
            parallel.shutdown();
        }
    }

    @Test
    void extractVectorModel_handlesOddDimensionCounts() {
        // 67 dimensions: not divisible by NUMA nodes or SIMD width
        float[][] data = generateTestData(200, 67, 789);

        VectorSpaceModel model = extractor.extractVectorModel(data);

        assertNotNull(model);
        assertEquals(67, model.dimensions());
    }

    @Test
    void extractVectorModel_handlesSingleDimension() {
        float[][] data = generateTestData(100, 1, 321);

        VectorSpaceModel model = extractor.extractVectorModel(data);

        assertNotNull(model);
        assertEquals(1, model.dimensions());
    }

    @Test
    void extractVectorModel_handlesLargeDimensionCount() {
        float[][] data = generateTestData(100, 512, 654);

        VectorSpaceModel model = extractor.extractVectorModel(data);

        assertNotNull(model);
        assertEquals(512, model.dimensions());
    }

    @Test
    void extractWithStats_returnsExtractionResult() {
        float[][] data = generateTestData(200, 32, 987);

        ModelExtractor.ExtractionResult result = extractor.extractWithStats(data);

        assertNotNull(result);
        assertNotNull(result.model());
        assertEquals(32, result.model().dimensions());
        assertTrue(result.extractionTimeMs() >= 0);
    }

    @Test
    void extractFromTransposed_worksWithPreTransposedData() {
        int numVectors = 200;
        int numDims = 32;
        float[][] transposed = new float[numDims][numVectors];

        Random random = new Random(111);
        for (int d = 0; d < numDims; d++) {
            for (int v = 0; v < numVectors; v++) {
                transposed[d][v] = (float) (random.nextGaussian() * 0.1 + 0.5);
            }
        }

        VectorSpaceModel model = extractor.extractFromTransposed(transposed);

        assertNotNull(model);
        assertEquals(numDims, model.dimensions());
    }

    @Test
    void builder_customConfiguration() {
        NumaAwareDatasetModelExtractor custom = NumaAwareDatasetModelExtractor.builder()
            .threadsPerNode(1)
            .batchSize(32)
            .selector(BestFitSelector.parametricOnly())
            .uniqueVectors(500_000)
            .build();

        try {
            assertEquals(1, custom.getThreadsPerNode());

            float[][] data = generateTestData(100, 16, 222);
            VectorSpaceModel model = custom.extractVectorModel(data);

            assertEquals(500_000, model.uniqueVectors());
        } finally {
            custom.shutdown();
        }
    }

    @Test
    void builder_forcedFitter() {
        NumaAwareDatasetModelExtractor normalOnly = NumaAwareDatasetModelExtractor.builder()
            .threadsPerNode(2)
            .forcedFitter(new NormalModelFitter())
            .build();

        try {
            float[][] data = generateTestData(100, 16, 333);
            VectorSpaceModel model = normalOnly.extractVectorModel(data);

            // All components should be Normal models
            for (int d = 0; d < model.dimensions(); d++) {
                assertEquals("normal", model.scalarModel(d).getModelType(),
                    "Expected normal model at dimension " + d);
            }
        } finally {
            normalOnly.shutdown();
        }
    }

    @Test
    void getProgress_tracksProgress() {
        float[][] data = generateTestData(500, 128, 444);

        // Start extraction in background
        Thread extractionThread = new Thread(() -> extractor.extractVectorModel(data));
        extractionThread.start();

        // Wait and check progress
        try {
            Thread.sleep(50);
            double progress = extractor.getProgress();
            assertTrue(progress >= 0.0 && progress <= 1.0,
                "Progress should be in [0, 1]: " + progress);

            extractionThread.join(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Test
    void getTopology_returnsDetectedTopology() {
        NumaTopology topology = extractor.getTopology();

        assertNotNull(topology);
        assertTrue(topology.nodeCount() >= 1);
        assertTrue(topology.totalCpus() >= 1);
    }

    @Test
    void defaultConstructor_usesAutoDetection() {
        NumaAwareDatasetModelExtractor autoDetect = new NumaAwareDatasetModelExtractor();

        try {
            NumaTopology topology = autoDetect.getTopology();
            assertNotNull(topology);

            float[][] data = generateTestData(100, 16, 555);
            VectorSpaceModel model = autoDetect.extractVectorModel(data);
            assertNotNull(model);
        } finally {
            autoDetect.shutdown();
        }
    }

    @Test
    void threadsPerNodeConstructor_setsCustomThreadCount() {
        NumaAwareDatasetModelExtractor custom = new NumaAwareDatasetModelExtractor(4);

        try {
            assertEquals(4, custom.getThreadsPerNode());

            float[][] data = generateTestData(100, 16, 666);
            VectorSpaceModel model = custom.extractVectorModel(data);
            assertNotNull(model);
        } finally {
            custom.shutdown();
        }
    }

    @Test
    void extractVectorModel_throwsOnNullData() {
        assertThrows(IllegalArgumentException.class, () ->
            extractor.extractVectorModel(null));
    }

    @Test
    void extractVectorModel_throwsOnEmptyData() {
        assertThrows(IllegalArgumentException.class, () ->
            extractor.extractVectorModel(new float[0][]));
    }

    @Test
    void getTotalThreads_calculatesCorrectly() {
        int perNode = extractor.getThreadsPerNode();
        int nodes = extractor.getTopology().nodeCount();
        assertEquals(perNode * nodes, extractor.getTotalThreads());
    }

    private float[][] generateTestData(int numVectors, int numDimensions, long seed) {
        Random random = new Random(seed);
        float[][] data = new float[numVectors][numDimensions];

        for (int v = 0; v < numVectors; v++) {
            for (int d = 0; d < numDimensions; d++) {
                // Mix of distributions per dimension
                if (d % 3 == 0) {
                    data[v][d] = (float) (random.nextGaussian() * 0.1 + 0.5);
                } else if (d % 3 == 1) {
                    data[v][d] = (float) random.nextDouble();
                } else {
                    data[v][d] = (float) (-Math.log(1 - random.nextDouble()) * 0.1 + 0.5);
                }
            }
        }

        return data;
    }
}
