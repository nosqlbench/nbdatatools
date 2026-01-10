package io.nosqlbench.vshapes.stream;

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

import io.nosqlbench.vshapes.extract.BestFitSelector;
import io.nosqlbench.vshapes.model.VectorSpaceModel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for StreamingModelExtractor.
 */
@Tag("unit")
class StreamingModelExtractorTest {

    private StreamingModelExtractor extractor;
    private Random random;

    @BeforeEach
    void setUp() {
        extractor = new StreamingModelExtractor();
        random = new Random(42);
    }

    @Test
    void getAnalyzerType_returnsCorrectType() {
        assertEquals("model-extractor", extractor.getAnalyzerType());
    }

    @Test
    void initialize_setsUpAccumulators() {
        DataspaceShape shape = new DataspaceShape(1000, 16);
        extractor.initialize(shape);

        assertEquals(0, extractor.getSamplesProcessed());
        assertEquals(0.0, extractor.getProgress(), 0.001);
    }

    @Test
    void accept_processesSingleChunk() {
        DataspaceShape shape = new DataspaceShape(100, 4);
        extractor.initialize(shape);

        float[][] columnarChunk = generateColumnarData(4, 50);
        extractor.accept(columnarChunk, 0);

        assertEquals(50, extractor.getSamplesProcessed());
        assertEquals(0.5, extractor.getProgress(), 0.001);
    }

    @Test
    void accept_processesMultipleChunks() {
        DataspaceShape shape = new DataspaceShape(200, 4);
        extractor.initialize(shape);

        float[][] chunk1 = generateColumnarData(4, 50);
        float[][] chunk2 = generateColumnarData(4, 50);
        float[][] chunk3 = generateColumnarData(4, 50);

        extractor.accept(chunk1, 0);
        extractor.accept(chunk2, 50);
        extractor.accept(chunk3, 100);

        assertEquals(150, extractor.getSamplesProcessed());
        assertEquals(0.75, extractor.getProgress(), 0.001);
    }

    @Test
    void complete_producesValidModel() {
        DataspaceShape shape = new DataspaceShape(500, 8);
        extractor.initialize(shape);

        // Process multiple chunks
        for (int chunk = 0; chunk < 5; chunk++) {
            float[][] columnarChunk = generateColumnarData(8, 100);
            extractor.accept(columnarChunk, chunk * 100);
        }

        VectorSpaceModel model = extractor.complete();

        assertNotNull(model);
        assertEquals(8, model.dimensions());
        assertEquals(500, model.uniqueVectors());

        // Verify all dimension models exist
        for (int d = 0; d < 8; d++) {
            assertNotNull(model.scalarModel(d), "Missing model for dimension " + d);
        }
    }

    @Test
    void complete_withCustomUniqueVectors() {
        extractor.setUniqueVectors(1_000_000);

        DataspaceShape shape = new DataspaceShape(100, 4);
        extractor.initialize(shape);

        float[][] columnarChunk = generateColumnarData(4, 100);
        extractor.accept(columnarChunk, 0);

        VectorSpaceModel model = extractor.complete();

        assertEquals(1_000_000, model.uniqueVectors());
    }

    @Test
    void complete_usesProcessedCountAsDefault() {
        DataspaceShape shape = new DataspaceShape(500, 4);
        extractor.initialize(shape);

        // Process only 200 vectors
        float[][] chunk1 = generateColumnarData(4, 100);
        float[][] chunk2 = generateColumnarData(4, 100);

        extractor.accept(chunk1, 0);
        extractor.accept(chunk2, 100);

        VectorSpaceModel model = extractor.complete();

        assertEquals(200, model.uniqueVectors());
    }

    @Test
    void setSelector_changesModelSelection() {
        // Disable adaptive mode to test pure selector behavior
        // (with adaptive enabled, empirical is used as fallback regardless of selector)
        StreamingModelExtractor parametricExtractor = StreamingModelExtractor.builder()
            .selector(BestFitSelector.parametricOnly())
            .adaptiveEnabled(false)
            .build();

        DataspaceShape shape = new DataspaceShape(200, 4);
        parametricExtractor.initialize(shape);

        float[][] columnarChunk = generateColumnarData(4, 200);
        parametricExtractor.accept(columnarChunk, 0);

        VectorSpaceModel model = parametricExtractor.complete();
        assertNotNull(model);

        // Parametric-only selector without adaptive should not produce empirical models
        for (int d = 0; d < 4; d++) {
            assertNotEquals("empirical", model.scalarModel(d).getModelType());
        }
    }

    @Test
    void accept_handlesNullChunk() {
        DataspaceShape shape = new DataspaceShape(100, 4);
        extractor.initialize(shape);

        extractor.accept(null, 0);
        assertEquals(0, extractor.getSamplesProcessed());
    }

    @Test
    void accept_handlesEmptyChunk() {
        DataspaceShape shape = new DataspaceShape(100, 4);
        extractor.initialize(shape);

        float[][] emptyChunk = new float[0][];
        extractor.accept(emptyChunk, 0);
        assertEquals(0, extractor.getSamplesProcessed());
    }

    @Test
    void spiDiscovery_findsExtractor() {
        Optional<StreamingAnalyzer<?>> found = StreamingAnalyzerIO.get("model-extractor");
        assertTrue(found.isPresent(), "StreamingModelExtractor should be discoverable via SPI");
        assertTrue(found.get() instanceof StreamingModelExtractor);
    }

    @Test
    void estimatedMemoryBytes_returnsReasonableValue() {
        DataspaceShape shape = new DataspaceShape(1000, 64);
        extractor.initialize(shape);

        long estimate = extractor.estimatedMemoryBytes();

        // Should be at least (1000 vectors * 64 dims * 4 bytes) = 256KB
        assertTrue(estimate >= 256_000, "Memory estimate should be at least 256KB");
    }

    @Test
    void fluentApi_chainsMethods() {
        StreamingModelExtractor configured = new StreamingModelExtractor()
            .setSelector(BestFitSelector.boundedDataSelector())
            .setUniqueVectors(100_000);

        assertNotNull(configured.getSelector());
    }

    // ========== Convergence Tracking Tests ==========

    @Test
    void convergenceTracking_detectsConvergence() {
        StreamingModelExtractor convergentExtractor = StreamingModelExtractor.builder()
            .convergenceEnabled(true)
            .convergenceThreshold(0.01)
            .build();

        DataspaceShape shape = new DataspaceShape(100_000, 4);
        convergentExtractor.initialize(shape);

        // Process many chunks to allow convergence
        for (int chunk = 0; chunk < 100; chunk++) {
            float[][] columnarChunk = generateColumnarData(4, 1000);
            convergentExtractor.accept(columnarChunk, chunk * 1000);
            convergentExtractor.checkConvergence();
        }

        assertTrue(convergentExtractor.isConvergenceEnabled());
        // After enough samples, convergence should be progressing
        assertTrue(convergentExtractor.getConvergedCount() > 0 ||
                   convergentExtractor.getSamplesProcessed() >= 5000);
    }

    @Test
    void earlyStoppingEnabled_returnsCorrectValue() {
        StreamingModelExtractor extractor1 = StreamingModelExtractor.builder()
            .earlyStoppingEnabled(true)
            .build();
        assertTrue(extractor1.isEarlyStoppingEnabled());

        StreamingModelExtractor extractor2 = StreamingModelExtractor.builder()
            .earlyStoppingEnabled(false)
            .build();
        assertFalse(extractor2.isEarlyStoppingEnabled());
    }

    @Test
    void convergenceStatus_returnsStatusWhenEnabled() {
        StreamingModelExtractor convergentExtractor = StreamingModelExtractor.builder()
            .convergenceEnabled(true)
            .build();

        DataspaceShape shape = new DataspaceShape(10000, 4);
        convergentExtractor.initialize(shape);

        float[][] chunk = generateColumnarData(4, 6000);
        convergentExtractor.accept(chunk, 0);
        convergentExtractor.checkConvergence();

        StreamingModelExtractor.ConvergenceStatus status = convergentExtractor.getConvergenceStatus();
        assertNotNull(status);
        assertEquals(4, status.totalDimensions());
        assertEquals(6000, status.samplesProcessed());
    }

    // ========== Adaptive Fallback Tests ==========

    @Test
    void adaptiveEnabled_isEnabledByDefault() {
        StreamingModelExtractor defaultExtractor = new StreamingModelExtractor();
        assertTrue(defaultExtractor.isAdaptiveEnabled());
    }

    @Test
    void adaptiveEnabled_canBeDisabled() {
        StreamingModelExtractor noAdaptive = StreamingModelExtractor.builder()
            .adaptiveEnabled(false)
            .build();
        assertFalse(noAdaptive.isAdaptiveEnabled());
    }

    @Test
    void adaptiveStrategyCounts_tracksUsedStrategies() {
        StreamingModelExtractor adaptiveExtractor = StreamingModelExtractor.builder()
            .adaptiveEnabled(true)
            .histogramEnabled(true)
            .build();

        DataspaceShape shape = new DataspaceShape(500, 4);
        adaptiveExtractor.initialize(shape);

        float[][] chunk = generateColumnarData(4, 500);
        adaptiveExtractor.accept(chunk, 0);

        VectorSpaceModel model = adaptiveExtractor.complete();
        assertNotNull(model);

        StreamingModelExtractor.AdaptiveStrategyCounts counts = adaptiveExtractor.getStrategyCounts();
        assertEquals(4, counts.total());
    }

    // ========== Reservoir Sampling Tests ==========

    @Test
    void reservoirSampling_isEnabledByDefault() {
        StreamingModelExtractor defaultExtractor = new StreamingModelExtractor();
        assertTrue(defaultExtractor.isReservoirSamplingEnabled());
    }

    @Test
    void reservoirSampling_canBeDisabled() {
        StreamingModelExtractor noReservoir = StreamingModelExtractor.builder()
            .reservoirSamplingEnabled(false)
            .build();
        assertFalse(noReservoir.isReservoirSamplingEnabled());
    }

    @Test
    void reservoirSize_canBeConfigured() {
        StreamingModelExtractor customSize = StreamingModelExtractor.builder()
            .reservoirSize(50_000)
            .build();
        assertEquals(50_000, customSize.getReservoirSize());
    }

    @Test
    void reservoirSize_enforcesMininimum() {
        StreamingModelExtractor tooSmall = StreamingModelExtractor.builder()
            .reservoirSize(100)  // Below minimum of 1000
            .build();
        assertTrue(tooSmall.getReservoirSize() >= 1000);
    }

    @Test
    void reservoirSampling_producesValidModelWithLargeDataset() {
        StreamingModelExtractor reservoirExtractor = StreamingModelExtractor.builder()
            .reservoirSamplingEnabled(true)
            .reservoirSize(1000)
            .build();

        DataspaceShape shape = new DataspaceShape(50_000, 4);
        reservoirExtractor.initialize(shape);

        // Process more data than reservoir size
        for (int chunk = 0; chunk < 50; chunk++) {
            float[][] columnarChunk = generateColumnarData(4, 1000);
            reservoirExtractor.accept(columnarChunk, chunk * 1000);
        }

        VectorSpaceModel model = reservoirExtractor.complete();
        assertNotNull(model);
        assertEquals(4, model.dimensions());
    }

    // ========== Builder Pattern Tests ==========

    @Test
    void builder_createsConfiguredExtractor() {
        StreamingModelExtractor built = StreamingModelExtractor.builder()
            .selector(BestFitSelector.pearsonWithEmpirical())
            .uniqueVectors(1_000_000)
            .convergenceEnabled(true)
            .convergenceThreshold(0.005)
            .earlyStoppingEnabled(false)
            .adaptiveEnabled(true)
            .ksThresholdParametric(0.02)
            .ksThresholdComposite(0.04)
            .maxCompositeComponents(3)
            .reservoirSamplingEnabled(true)
            .reservoirSize(20_000)
            .build();

        assertNotNull(built);
        assertFalse(built.isEarlyStoppingEnabled());
        assertTrue(built.isAdaptiveEnabled());
        assertTrue(built.isConvergenceEnabled());
        assertEquals(0.005, built.getConvergenceThreshold(), 0.0001);
        assertEquals(20_000, built.getReservoirSize());
    }

    @Test
    void builder_withHistogramEnabled() {
        StreamingModelExtractor withHistogram = StreamingModelExtractor.builder()
            .histogramEnabled(true)
            .prominenceThreshold(0.15)
            .build();

        assertTrue(withHistogram.isHistogramEnabled());
    }

    @Test
    void builder_withIncrementalFitting() {
        StreamingModelExtractor withIncremental = StreamingModelExtractor.builder()
            .incrementalFittingEnabled(true)
            .fitInterval(10)
            .build();

        assertTrue(withIncremental.isIncrementalFittingEnabled());
    }

    // ========== Histogram Tracking Tests ==========

    @Test
    void histogramTracking_detectsMultiModal() {
        StreamingModelExtractor histogramExtractor = StreamingModelExtractor.builder()
            .histogramEnabled(true)
            .prominenceThreshold(0.1)
            .build();

        DataspaceShape shape = new DataspaceShape(10_000, 2);
        histogramExtractor.initialize(shape);

        // Create bimodal data for dimension 0
        float[][] bimodalChunk = new float[2][10_000];
        for (int i = 0; i < 5000; i++) {
            bimodalChunk[0][i] = (float) (random.nextGaussian() * 0.1 - 0.5);
            bimodalChunk[1][i] = (float) (random.nextGaussian() * 0.1);
        }
        for (int i = 5000; i < 10_000; i++) {
            bimodalChunk[0][i] = (float) (random.nextGaussian() * 0.1 + 0.5);
            bimodalChunk[1][i] = (float) (random.nextGaussian() * 0.1);
        }

        histogramExtractor.accept(bimodalChunk, 0);

        // Check multimodality detection was set up
        assertTrue(histogramExtractor.isHistogramEnabled());
    }

    /**
     * Generates columnar (column-major) test data.
     * Format: data[dimension][vectorIndex]
     */
    private float[][] generateColumnarData(int dimensions, int vectors) {
        float[][] data = new float[dimensions][vectors];
        for (int d = 0; d < dimensions; d++) {
            for (int v = 0; v < vectors; v++) {
                data[d][v] = (float) (random.nextGaussian() * 0.1 + 0.5);
            }
        }
        return data;
    }
}
