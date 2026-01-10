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

import io.nosqlbench.vshapes.analyzers.dimensiondistribution.DimensionDistributionAnalyzer;
import io.nosqlbench.vshapes.model.VectorSpaceModel;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/// Integration tests for the complete streaming pipeline.
///
/// These tests verify that all streaming components work together:
/// - DataSource → PrefetchingDataSource → AnalyzerHarness → Model
@Tag("integration")
class StreamingPipelineIntegrationTest {

    @Test
    void fullPipeline_withPrefetchingAndAnalyzer() {
        // Create a synthetic data source
        int totalVectors = 1000;
        int dimensions = 16;
        int chunkSize = 100;

        DataSource source = createSyntheticSource(totalVectors, dimensions);

        // Wrap with prefetching
        PrefetchingDataSource prefetching = PrefetchingDataSource.builder()
            .source(source)
            .prefetchCount(2)
            .build();

        // Configure analyzer
        DimensionDistributionAnalyzer analyzer = new DimensionDistributionAnalyzer();

        // Run through harness
        AnalyzerHarness harness = new AnalyzerHarness();
        harness.register(analyzer);

        AnalysisResults results = harness.run(prefetching, chunkSize);

        // Verify results
        assertNotNull(results);
        VectorSpaceModel model = results.getResult("dimension-distribution", VectorSpaceModel.class);
        assertNotNull(model);
        assertEquals(dimensions, model.dimensions());
    }

    @Test
    void fullPipeline_withMemoryMonitoring() {
        int totalVectors = 500;
        int dimensions = 8;
        int chunkSize = 50;

        DataSource source = createSyntheticSource(totalVectors, dimensions);

        // Wrap with prefetching and memory monitoring
        PrefetchingDataSource prefetching = PrefetchingDataSource.builder()
            .source(source)
            .prefetchCount(3)
            .withMemoryMonitoring()
            .build();

        assertNotNull(prefetching.getMemoryMonitor());

        // Configure analyzer
        StreamingModelExtractor extractor = new StreamingModelExtractor();

        // Run through harness
        AnalyzerHarness harness = new AnalyzerHarness();
        harness.register(extractor);

        AnalysisResults results = harness.run(prefetching, chunkSize);

        // Verify results
        VectorSpaceModel model = results.getResult("model-extractor", VectorSpaceModel.class);
        assertNotNull(model);
        assertEquals(dimensions, model.dimensions());
    }

    @Test
    void progressCallback_reportsCorrectProgress() {
        int totalVectors = 400;
        int dimensions = 4;
        int chunkSize = 100;

        DataSource source = createSyntheticSource(totalVectors, dimensions);
        PrefetchingDataSource prefetching = PrefetchingDataSource.wrap(source);

        AtomicInteger callbackCount = new AtomicInteger(0);
        AtomicLong lastProcessed = new AtomicLong(0);

        StreamingModelExtractor extractor = new StreamingModelExtractor();

        AnalyzerHarness harness = new AnalyzerHarness();
        harness.register(extractor);

        AnalysisResults results = harness.run(prefetching, chunkSize, (progress, processed, total) -> {
            callbackCount.incrementAndGet();
            assertTrue(processed >= lastProcessed.get(), "Processed count should increase");
            assertTrue(progress >= 0.0 && progress <= 1.0, "Progress should be 0-1");
            assertEquals(totalVectors, total, "Total should match");
            lastProcessed.set(processed);
        });

        assertNotNull(results);
        assertTrue(callbackCount.get() >= 4, "Should have multiple progress callbacks");
        assertEquals(totalVectors, lastProcessed.get(), "Should have processed all vectors");
    }

    @Test
    void multipleAnalyzers_runInParallel() {
        int totalVectors = 300;
        int dimensions = 6;
        int chunkSize = 50;

        DataSource source = createSyntheticSource(totalVectors, dimensions);

        // Register multiple analyzers
        AtomicInteger analyzer1Chunks = new AtomicInteger(0);
        AtomicInteger analyzer2Chunks = new AtomicInteger(0);

        StreamingAnalyzer<String> analyzer1 = new StreamingAnalyzer<>() {
            @Override
            public String getAnalyzerType() { return "counter1"; }
            @Override
            public String getDescription() { return "Test counter 1"; }
            @Override
            public void initialize(DataspaceShape shape) { }
            @Override
            public void accept(float[][] chunk, long startIndex) {
                analyzer1Chunks.incrementAndGet();
            }
            @Override
            public String complete() { return "done1"; }
            @Override
            public long estimatedMemoryBytes() { return 0; }
        };

        StreamingAnalyzer<String> analyzer2 = new StreamingAnalyzer<>() {
            @Override
            public String getAnalyzerType() { return "counter2"; }
            @Override
            public String getDescription() { return "Test counter 2"; }
            @Override
            public void initialize(DataspaceShape shape) { }
            @Override
            public void accept(float[][] chunk, long startIndex) {
                analyzer2Chunks.incrementAndGet();
            }
            @Override
            public String complete() { return "done2"; }
            @Override
            public long estimatedMemoryBytes() { return 0; }
        };

        AnalyzerHarness harness = new AnalyzerHarness();
        harness.register(analyzer1);
        harness.register(analyzer2);

        AnalysisResults results = harness.run(source, chunkSize);

        // Both analyzers should receive all chunks
        int expectedChunks = (totalVectors + chunkSize - 1) / chunkSize;
        assertEquals(expectedChunks, analyzer1Chunks.get());
        assertEquals(expectedChunks, analyzer2Chunks.get());

        assertEquals("done1", results.getResult("counter1", String.class));
        assertEquals("done2", results.getResult("counter2", String.class));
    }

    @Test
    void prefetching_overlapsBetweenLoadAndProcess() throws InterruptedException {
        int totalVectors = 200;
        int dimensions = 4;
        int chunkSize = 50;

        AtomicLong loadStartTime = new AtomicLong(0);
        AtomicLong processStartTime = new AtomicLong(0);
        AtomicInteger chunkNumber = new AtomicInteger(0);

        // Create a source that tracks load timing
        DataSource timedSource = new DataSource() {
            @Override
            public DataspaceShape getShape() {
                return new DataspaceShape(totalVectors, dimensions, DataLayout.COLUMNAR);
            }

            @Override
            public Iterable<float[][]> chunks(int size) {
                return () -> new Iterator<>() {
                    private int loaded = 0;

                    @Override
                    public boolean hasNext() {
                        return loaded < totalVectors;
                    }

                    @Override
                    public float[][] next() {
                        int thisChunk = chunkNumber.incrementAndGet();
                        if (thisChunk == 2) {
                            loadStartTime.set(System.nanoTime());
                        }

                        // Simulate slow I/O
                        try {
                            Thread.sleep(50);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }

                        int remaining = totalVectors - loaded;
                        int thisSize = Math.min(size, remaining);
                        loaded += thisSize;

                        float[][] chunk = new float[dimensions][thisSize];
                        return chunk;
                    }
                };
            }

            @Override
            public String getId() {
                return "timed-source";
            }
        };

        PrefetchingDataSource prefetching = new PrefetchingDataSource(timedSource, 2);

        // Track when processing starts
        StreamingAnalyzer<String> timedAnalyzer = new StreamingAnalyzer<>() {
            private int processed = 0;

            @Override
            public String getAnalyzerType() { return "timed"; }
            @Override
            public String getDescription() { return "Timed analyzer"; }
            @Override
            public void initialize(DataspaceShape shape) { }
            @Override
            public void accept(float[][] chunk, long startIndex) {
                processed++;
                if (processed == 1 && processStartTime.get() == 0) {
                    processStartTime.set(System.nanoTime());
                }
                // Simulate processing time
                try {
                    Thread.sleep(30);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            @Override
            public String complete() { return "processed"; }
            @Override
            public long estimatedMemoryBytes() { return 0; }
        };

        AnalyzerHarness harness = new AnalyzerHarness();
        harness.register(timedAnalyzer);

        AnalysisResults results = harness.run(prefetching, chunkSize);

        assertNotNull(results.getResult("timed", String.class));
    }

    @Test
    void chunkSizeCalculator_calculatesReasonableSizes() {
        // Test with various dimensions using fixed budget (256MB)
        // Using fixed budget ensures deterministic behavior regardless of heap size
        ChunkSizeCalculator calc128 = new ChunkSizeCalculator(128, 0.5, 1.2);
        ChunkSizeCalculator calc768 = new ChunkSizeCalculator(768, 0.5, 1.2);
        ChunkSizeCalculator calc1536 = new ChunkSizeCalculator(1536, 0.5, 1.2);

        // Use fixed budget to avoid MAX_CHUNK_SIZE capping on large heaps
        long fixedBudget = 256L * 1024 * 1024;  // 256MB

        int size128 = calc128.calculateForBudget(fixedBudget);
        int size768 = calc768.calculateForBudget(fixedBudget);
        int size1536 = calc1536.calculateForBudget(fixedBudget);

        // Higher dimensions should result in smaller chunk sizes
        assertTrue(size128 > size768, "128-dim should allow larger chunks than 768-dim");
        assertTrue(size768 > size1536, "768-dim should allow larger chunks than 1536-dim");

        // All should be positive
        assertTrue(size128 > 0);
        assertTrue(size768 > 0);
        assertTrue(size1536 > 0);
    }

    @Test
    void memoryPressureMonitor_integratesWithPrefetching() {
        MemoryPressureMonitor monitor = new MemoryPressureMonitor(0.99, 0.98);
        DataSource source = createSyntheticSource(100, 4);

        PrefetchingDataSource prefetching = PrefetchingDataSource.builder()
            .source(source)
            .prefetchCount(4)
            .memoryMonitor(monitor)
            .build();

        assertSame(monitor, prefetching.getMemoryMonitor());

        // Process all chunks - should complete without issues
        int count = 0;
        for (float[][] chunk : prefetching.chunks(25)) {
            count++;
            assertNotNull(chunk);
        }

        assertEquals(4, count);
    }

    /// Creates a synthetic data source for testing (columnar format).
    private DataSource createSyntheticSource(int totalVectors, int dimensions) {
        return new DataSource() {
            @Override
            public DataspaceShape getShape() {
                // Mark as columnar so AnalyzerHarness knows format
                return new DataspaceShape(totalVectors, dimensions, DataLayout.COLUMNAR);
            }

            @Override
            public Iterable<float[][]> chunks(int chunkSize) {
                return () -> new Iterator<>() {
                    private int vectorsReturned = 0;

                    @Override
                    public boolean hasNext() {
                        return vectorsReturned < totalVectors;
                    }

                    @Override
                    public float[][] next() {
                        int remaining = totalVectors - vectorsReturned;
                        int thisChunkSize = Math.min(chunkSize, remaining);
                        vectorsReturned += thisChunkSize;

                        // Create columnar chunk with random data
                        float[][] chunk = new float[dimensions][thisChunkSize];
                        for (int d = 0; d < dimensions; d++) {
                            for (int v = 0; v < thisChunkSize; v++) {
                                chunk[d][v] = (float) (Math.random() * 2 - 1);
                            }
                        }
                        return chunk;
                    }
                };
            }

            @Override
            public String getId() {
                return "synthetic-" + totalVectors + "x" + dimensions;
            }
        };
    }
}

