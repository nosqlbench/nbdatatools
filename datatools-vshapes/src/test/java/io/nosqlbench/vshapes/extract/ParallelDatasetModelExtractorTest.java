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
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for ParallelDatasetModelExtractor.
 */
@Tag("unit")
public class ParallelDatasetModelExtractorTest {

    private ParallelDatasetModelExtractor extractor;

    @BeforeEach
    void setUp() {
        extractor = ParallelDatasetModelExtractor.builder()
            .parallelism(4)
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
        float[][] data = generateTestData(1000, 64, 42);

        VectorSpaceModel model = extractor.extractVectorModel(data);

        assertNotNull(model);
        assertEquals(64, model.dimensions());
        assertEquals(ParallelDatasetModelExtractor.DEFAULT_UNIQUE_VECTORS, model.uniqueVectors());
    }

    @Test
    void extractVectorModel_matchesSequentialExtractor() {
        float[][] data = generateTestData(500, 32, 123);

        DatasetModelExtractor sequential = new DatasetModelExtractor();
        VectorSpaceModel seqModel = sequential.extractVectorModel(data);
        VectorSpaceModel parModel = extractor.extractVectorModel(data);

        assertEquals(seqModel.dimensions(), parModel.dimensions());

        // Verify component types match (values may differ slightly due to floating-point)
        for (int d = 0; d < seqModel.dimensions(); d++) {
            ScalarModel seqComp = seqModel.scalarModel(d);
            ScalarModel parComp = parModel.scalarModel(d);
            assertEquals(seqComp.getClass(), parComp.getClass(),
                "Component type mismatch at dimension " + d);
        }
    }

    @Test
    void extractVectorModel_handlesNonMultipleOf8Dimensions() {
        // 67 dimensions = 8 full SIMD batches + 3 remainder
        float[][] data = generateTestData(200, 67, 456);

        VectorSpaceModel model = extractor.extractVectorModel(data);

        assertNotNull(model);
        assertEquals(67, model.dimensions());
    }

    @Test
    void extractVectorModel_handlesSingleDimension() {
        float[][] data = generateTestData(100, 1, 789);

        VectorSpaceModel model = extractor.extractVectorModel(data);

        assertNotNull(model);
        assertEquals(1, model.dimensions());
    }

    @Test
    void extractVectorModel_handlesLargeDimensionCount() {
        float[][] data = generateTestData(100, 1024, 321);

        VectorSpaceModel model = extractor.extractVectorModel(data);

        assertNotNull(model);
        assertEquals(1024, model.dimensions());
    }

    @Test
    void extractWithStats_returnsExtractionResult() {
        float[][] data = generateTestData(200, 32, 654);

        ModelExtractor.ExtractionResult result = extractor.extractWithStats(data);

        assertNotNull(result);
        assertNotNull(result.model());
        assertEquals(32, result.model().dimensions());
        assertTrue(result.extractionTimeMs() >= 0);
    }

    @Test
    void extractWithProgress_callsProgressCallback() {
        float[][] data = generateTestData(200, 64, 987);
        AtomicInteger callCount = new AtomicInteger(0);

        extractor.extractWithProgress(data, (progress, message) -> {
            callCount.incrementAndGet();
            assertTrue(progress >= 0.0 && progress <= 1.0,
                "Progress should be in [0, 1]: " + progress);
            assertNotNull(message);
        });

        assertTrue(callCount.get() > 0, "Progress callback should be called at least once");
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
        ParallelDatasetModelExtractor custom = ParallelDatasetModelExtractor.builder()
            .parallelism(2)
            .batchSize(32)
            .selector(BestFitSelector.parametricOnly())
            .uniqueVectors(500_000)
            .build();

        try {
            assertEquals(2, custom.getParallelism());
            assertEquals(32, custom.getBatchSize());

            float[][] data = generateTestData(100, 16, 222);
            VectorSpaceModel model = custom.extractVectorModel(data);

            assertEquals(500_000, model.uniqueVectors());
        } finally {
            custom.shutdown();
        }
    }

    @Test
    void builder_forcedFitter() {
        ParallelDatasetModelExtractor normalOnly = ParallelDatasetModelExtractor.builder()
            .parallelism(2)
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

        // Wait a bit and check progress
        try {
            Thread.sleep(50);
            double progress = extractor.getProgress();
            // Progress should be between 0 and 1
            assertTrue(progress >= 0.0 && progress <= 1.0,
                "Progress should be in [0, 1]: " + progress);

            extractionThread.join(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Test
    void defaultConstructor_usesCommonPool() {
        ParallelDatasetModelExtractor defaultExtractor = new ParallelDatasetModelExtractor();

        try {
            float[][] data = generateTestData(100, 16, 555);
            VectorSpaceModel model = defaultExtractor.extractVectorModel(data);
            assertNotNull(model);
        } finally {
            defaultExtractor.shutdown();
        }
    }

    @Test
    void parallelismConstructor_createsCustomPool() {
        ParallelDatasetModelExtractor customParallel = new ParallelDatasetModelExtractor(2);

        try {
            assertEquals(2, customParallel.getParallelism());

            float[][] data = generateTestData(100, 16, 666);
            VectorSpaceModel model = customParallel.extractVectorModel(data);
            assertNotNull(model);
        } finally {
            customParallel.shutdown();
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
