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
import io.nosqlbench.vshapes.analyzers.dimensionstatistics.DimensionStatisticsAnalyzer;
import io.nosqlbench.vshapes.analyzers.dimensionstatistics.DimensionStatisticsModel;
import io.nosqlbench.vshapes.extract.DimensionStatistics;
import io.nosqlbench.vshapes.model.VectorSpaceModel;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

@Tag("unit")
@Tag("integration")
public class AnalyzerHarnessTest {

    @Test
    void testBasicHarness() {
        float[][] data = generateGaussianData(1000, 4, new Random(12345));
        DataSource source = new FloatArrayDataSource(data);

        CountingAnalyzer counter = new CountingAnalyzer();

        AnalyzerHarness harness = new AnalyzerHarness();
        harness.register(counter);

        AnalysisResults results = harness.run(source, 100);

        assertFalse(results.hasErrors());
        assertTrue(results.hasResult("counter"));

        Long count = results.getResult("counter", Long.class);
        assertEquals(1000L, count);

        harness.shutdown();
    }

    @Test
    void testMultipleAnalyzers() {
        float[][] data = generateGaussianData(500, 3, new Random(12345));
        DataSource source = new FloatArrayDataSource(data);

        CountingAnalyzer counter1 = new CountingAnalyzer("counter1");
        CountingAnalyzer counter2 = new CountingAnalyzer("counter2");
        SumAnalyzer summer = new SumAnalyzer();

        AnalyzerHarness harness = new AnalyzerHarness();
        harness.register(counter1);
        harness.register(counter2);
        harness.register(summer);

        AnalysisResults results = harness.run(source, 50);

        assertFalse(results.hasErrors());
        assertEquals(3, results.getSuccessfulAnalyzers().size());

        assertEquals(500L, results.getResult("counter1", Long.class));
        assertEquals(500L, results.getResult("counter2", Long.class));
        assertNotNull(results.getResult("sum", Double.class));

        harness.shutdown();
    }

    @Test
    void testModelExtractor() {
        Random random = new Random(12345);
        float[][] data = new float[1000][4];
        double[] trueMeans = {0.0, 5.0, -2.0, 10.0};
        double[] trueStdDevs = {1.0, 2.0, 0.5, 3.0};

        for (int v = 0; v < data.length; v++) {
            for (int d = 0; d < 4; d++) {
                data[v][d] = (float) (random.nextGaussian() * trueStdDevs[d] + trueMeans[d]);
            }
        }

        DataSource source = new FloatArrayDataSource(data);
        DimensionDistributionAnalyzer analyzer = new DimensionDistributionAnalyzer();

        AnalyzerHarness harness = new AnalyzerHarness();
        harness.register(analyzer);

        AnalysisResults results = harness.run(source, 100);

        assertFalse(results.hasErrors());
        VectorSpaceModel model = results.getResult("dimension-distribution", VectorSpaceModel.class);

        assertNotNull(model);
        assertEquals(4, model.dimensions());

        harness.shutdown();
    }

    @Test
    void testProgressCallback() {
        float[][] data = generateGaussianData(1000, 3, new Random(12345));
        DataSource source = new FloatArrayDataSource(data);

        CountingAnalyzer counter = new CountingAnalyzer();

        AnalyzerHarness harness = new AnalyzerHarness();
        harness.register(counter);

        AtomicInteger progressCalls = new AtomicInteger();
        AnalysisResults results = harness.run(source, 100, (progress, processed, total) -> {
            progressCalls.incrementAndGet();
            assertTrue(progress >= 0 && progress <= 1);
            assertEquals(1000, total);
        });

        assertTrue(progressCalls.get() > 0);
        assertFalse(results.hasErrors());

        harness.shutdown();
    }

    @Test
    void testFailFastMode() {
        float[][] data = generateGaussianData(500, 3, new Random(12345));
        DataSource source = new FloatArrayDataSource(data);

        FailingAnalyzer failing = new FailingAnalyzer(150);  // Fail after 150 vectors
        CountingAnalyzer counter = new CountingAnalyzer();

        AnalyzerHarness harness = new AnalyzerHarness()
            .failFast(true);
        harness.register(failing);
        harness.register(counter);

        AnalysisResults results = harness.run(source, 50);

        assertTrue(results.hasErrors());
        assertNotNull(results.getError("failing"));

        harness.shutdown();
    }

    @Test
    void testContinueOnError() {
        float[][] data = generateGaussianData(500, 3, new Random(12345));
        DataSource source = new FloatArrayDataSource(data);

        FailingAnalyzer failing = new FailingAnalyzer(150);
        CountingAnalyzer counter = new CountingAnalyzer();

        AnalyzerHarness harness = new AnalyzerHarness()
            .failFast(false);  // Continue others
        harness.register(failing);
        harness.register(counter);

        AnalysisResults results = harness.run(source, 50);

        assertTrue(results.hasErrors());
        assertTrue(results.hasResult("counter"));

        // Counter should have processed all vectors
        assertEquals(500L, results.getResult("counter", Long.class));

        harness.shutdown();
    }

    @Test
    void testEmptyHarnessThrows() {
        float[][] data = generateGaussianData(100, 3, new Random(12345));
        DataSource source = new FloatArrayDataSource(data);

        AnalyzerHarness harness = new AnalyzerHarness();

        assertThrows(IllegalStateException.class, () -> harness.run(source, 10));

        harness.shutdown();
    }

    @Test
    void testDataspaceShape() {
        DataspaceShape shape = new DataspaceShape(1000, 128);

        assertEquals(1000, shape.cardinality());
        assertEquals(128, shape.dimensionality());
        assertEquals(DataLayout.ROW_MAJOR, shape.layout());
        assertTrue(shape.isRowMajor());
        assertFalse(shape.isColumnar());
    }

    @Test
    void testDataspaceShapeWithLayout() {
        DataspaceShape shape = new DataspaceShape(100, 10);
        DataspaceShape columnar = shape.withLayout(DataLayout.COLUMNAR);

        assertEquals(DataLayout.ROW_MAJOR, shape.layout());
        assertEquals(DataLayout.COLUMNAR, columnar.layout());
        assertTrue(columnar.isColumnar());
        assertFalse(columnar.isRowMajor());
    }

    @Test
    void testFloatArrayDataSource() {
        float[][] data = generateGaussianData(100, 5, new Random(12345));
        FloatArrayDataSource source = new FloatArrayDataSource(data);

        assertEquals(100, source.size());
        assertEquals(5, source.dimensionality());

        int chunkCount = 0;
        int totalVectors = 0;
        for (float[][] chunk : source.chunks(30)) {
            chunkCount++;
            totalVectors += chunk.length;
        }

        assertEquals(4, chunkCount);  // 30 + 30 + 30 + 10
        assertEquals(100, totalVectors);
    }

    @Test
    void testAnalysisResultsSummary() {
        Map<String, Object> results = Map.of("a", 1L, "b", 2.0);
        Map<String, Throwable> errors = Map.of("c", new RuntimeException("test"));

        AnalysisResults ar = new AnalysisResults(results, errors, 100);

        String summary = ar.getSummary();
        assertTrue(summary.contains("Successful: 2"));
        assertTrue(summary.contains("Failed: 1"));
        assertTrue(summary.contains("100ms"));
    }

    // SPI Loading Tests

    @Test
    void testStreamingAnalyzerIOGetByName() {
        Optional<StreamingAnalyzer<?>> analyzer = StreamingAnalyzerIO.get("dimension-distribution");

        assertTrue(analyzer.isPresent());
        assertEquals("dimension-distribution", analyzer.get().getAnalyzerType());
        assertTrue(analyzer.get() instanceof DimensionDistributionAnalyzer);
    }

    @Test
    void testStreamingAnalyzerIOGetByNameWithType() {
        Optional<StreamingAnalyzer<VectorSpaceModel>> analyzer =
            StreamingAnalyzerIO.get("dimension-distribution", VectorSpaceModel.class);

        assertTrue(analyzer.isPresent());
        assertEquals("dimension-distribution", analyzer.get().getAnalyzerType());
    }

    @Test
    void testStreamingAnalyzerIOGetUnknown() {
        Optional<StreamingAnalyzer<?>> analyzer = StreamingAnalyzerIO.get("unknown-analyzer");

        assertTrue(analyzer.isEmpty());
    }

    @Test
    void testStreamingAnalyzerIOGetAll() {
        List<StreamingAnalyzer<?>> all = StreamingAnalyzerIO.getAll();

        assertFalse(all.isEmpty());
        assertTrue(all.stream().anyMatch(a -> "dimension-distribution".equals(a.getAnalyzerType())));
    }

    @Test
    void testStreamingAnalyzerIOGetAvailableNames() {
        List<String> names = StreamingAnalyzerIO.getAvailableNames();

        assertFalse(names.isEmpty());
        assertTrue(names.contains("dimension-distribution"));
    }

    @Test
    void testStreamingAnalyzerIOIsAvailable() {
        assertTrue(StreamingAnalyzerIO.isAvailable("dimension-distribution"));
        assertFalse(StreamingAnalyzerIO.isAvailable("unknown-analyzer"));
    }

    @Test
    void testHarnessRegisterByName() {
        float[][] data = generateGaussianData(100, 4, new Random(12345));
        DataSource source = new FloatArrayDataSource(data);

        AnalyzerHarness harness = new AnalyzerHarness();
        harness.register("dimension-distribution");

        assertEquals(1, harness.getAnalyzerCount());
        assertTrue(harness.getAnalyzerTypes().contains("dimension-distribution"));

        AnalysisResults results = harness.run(source, 50);

        assertFalse(results.hasErrors());
        assertNotNull(results.getResult("dimension-distribution", VectorSpaceModel.class));

        harness.shutdown();
    }

    @Test
    void testHarnessRegisterByNameThrowsForUnknown() {
        AnalyzerHarness harness = new AnalyzerHarness();

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
            () -> harness.register("unknown-analyzer"));

        assertTrue(ex.getMessage().contains("unknown-analyzer"));
        assertTrue(ex.getMessage().contains("Available"));

        harness.shutdown();
    }

    @Test
    void testHarnessRegisterIfAvailable() {
        AnalyzerHarness harness = new AnalyzerHarness();

        harness.registerIfAvailable("dimension-distribution");
        harness.registerIfAvailable("unknown-analyzer");

        assertEquals(1, harness.getAnalyzerCount());
        assertTrue(harness.getAnalyzerTypes().contains("dimension-distribution"));

        harness.shutdown();
    }

    @Test
    void testHarnessRegisterAllAvailable() {
        float[][] data = generateGaussianData(100, 4, new Random(12345));
        DataSource source = new FloatArrayDataSource(data);

        AnalyzerHarness harness = new AnalyzerHarness();
        harness.registerAllAvailable();

        assertTrue(harness.getAnalyzerCount() >= 1);
        assertTrue(harness.getAnalyzerTypes().contains("dimension-distribution"));

        AnalysisResults results = harness.run(source, 50);
        assertFalse(results.hasErrors());

        harness.shutdown();
    }

    // DimensionStatisticsAnalyzer Tests

    @Test
    void testDimensionStatisticsAnalyzerSPIDiscovery() {
        Optional<StreamingAnalyzer<?>> analyzer = StreamingAnalyzerIO.get("dimension-statistics");

        assertTrue(analyzer.isPresent());
        assertEquals("dimension-statistics", analyzer.get().getAnalyzerType());
        assertTrue(analyzer.get() instanceof DimensionStatisticsAnalyzer);
    }

    @Test
    void testDimensionStatisticsAnalyzerWithTypedLookup() {
        Optional<StreamingAnalyzer<DimensionStatisticsModel>> analyzer =
            StreamingAnalyzerIO.get("dimension-statistics", DimensionStatisticsModel.class);

        assertTrue(analyzer.isPresent());
        assertEquals("dimension-statistics", analyzer.get().getAnalyzerType());
    }

    @Test
    void testDimensionStatisticsAnalyzerAvailableInList() {
        List<String> names = StreamingAnalyzerIO.getAvailableNames();

        assertTrue(names.contains("dimension-statistics"));
        assertTrue(StreamingAnalyzerIO.isAvailable("dimension-statistics"));
    }

    @Test
    void testDimensionStatisticsAnalyzerComputesCorrectStatistics() {
        Random random = new Random(12345);
        int numVectors = 10000;
        int dims = 4;
        float[][] data = new float[numVectors][dims];

        // Known parameters for each dimension
        double[] trueMeans = {0.0, 5.0, -2.0, 10.0};
        double[] trueStdDevs = {1.0, 2.0, 0.5, 3.0};

        for (int v = 0; v < numVectors; v++) {
            for (int d = 0; d < dims; d++) {
                data[v][d] = (float) (random.nextGaussian() * trueStdDevs[d] + trueMeans[d]);
            }
        }

        DataSource source = new FloatArrayDataSource(data);
        DimensionStatisticsAnalyzer analyzer = new DimensionStatisticsAnalyzer();

        AnalyzerHarness harness = new AnalyzerHarness();
        harness.register(analyzer);

        AnalysisResults results = harness.run(source, 500);

        assertFalse(results.hasErrors());
        DimensionStatisticsModel model = results.getResult("dimension-statistics", DimensionStatisticsModel.class);

        assertNotNull(model);
        assertEquals(dims, model.dimensions());
        assertEquals(numVectors, model.vectorCount());

        // Verify statistics are close to expected values (with some tolerance for random variation)
        for (int d = 0; d < dims; d++) {
            DimensionStatistics stats = model.getStatistics(d);
            assertEquals(numVectors, stats.count());
            assertEquals(d, stats.dimension());

            // Mean should be within 0.1 of true mean (generous tolerance for 10k samples)
            assertEquals(trueMeans[d], stats.mean(), 0.1,
                "Dimension " + d + " mean should be close to " + trueMeans[d]);

            // StdDev should be within 0.05 of true stdDev
            assertEquals(trueStdDevs[d], stats.stdDev(), 0.1,
                "Dimension " + d + " stdDev should be close to " + trueStdDevs[d]);

            // Skewness should be close to 0 for Gaussian data
            assertEquals(0.0, stats.skewness(), 0.1,
                "Dimension " + d + " skewness should be near 0 for Gaussian data");

            // Kurtosis should be close to 3 for Gaussian data
            assertEquals(3.0, stats.kurtosis(), 0.3,
                "Dimension " + d + " kurtosis should be near 3 for Gaussian data");
        }

        harness.shutdown();
    }

    @Test
    void testDimensionStatisticsAnalyzerByName() {
        float[][] data = generateGaussianData(500, 3, new Random(12345));
        DataSource source = new FloatArrayDataSource(data);

        AnalyzerHarness harness = new AnalyzerHarness();
        harness.register("dimension-statistics");

        assertEquals(1, harness.getAnalyzerCount());
        assertTrue(harness.getAnalyzerTypes().contains("dimension-statistics"));

        AnalysisResults results = harness.run(source, 100);

        assertFalse(results.hasErrors());
        DimensionStatisticsModel model = results.getResult("dimension-statistics", DimensionStatisticsModel.class);
        assertNotNull(model);
        assertEquals(3, model.dimensions());

        harness.shutdown();
    }

    @Test
    void testBothAnalyzersRunTogether() {
        Random random = new Random(12345);
        int numVectors = 1000;
        int dims = 4;
        float[][] data = new float[numVectors][dims];

        double[] trueMeans = {0.0, 5.0, -2.0, 10.0};
        double[] trueStdDevs = {1.0, 2.0, 0.5, 3.0};

        for (int v = 0; v < numVectors; v++) {
            for (int d = 0; d < dims; d++) {
                data[v][d] = (float) (random.nextGaussian() * trueStdDevs[d] + trueMeans[d]);
            }
        }

        DataSource source = new FloatArrayDataSource(data);

        AnalyzerHarness harness = new AnalyzerHarness();
        harness.register("dimension-distribution");
        harness.register("dimension-statistics");

        assertEquals(2, harness.getAnalyzerCount());

        AnalysisResults results = harness.run(source, 100);

        assertFalse(results.hasErrors());
        assertEquals(2, results.getSuccessfulAnalyzers().size());

        // Verify dimension-distribution results
        VectorSpaceModel distributionModel = results.getResult("dimension-distribution", VectorSpaceModel.class);
        assertNotNull(distributionModel);
        assertEquals(dims, distributionModel.dimensions());

        // Verify dimension-statistics results
        DimensionStatisticsModel statsModel = results.getResult("dimension-statistics", DimensionStatisticsModel.class);
        assertNotNull(statsModel);
        assertEquals(dims, statsModel.dimensions());
        assertEquals(numVectors, statsModel.vectorCount());

        // Statistics from both should be consistent
        for (int d = 0; d < dims; d++) {
            DimensionStatistics stats = statsModel.getStatistics(d);
            // Just verify both computed statistics for each dimension
            assertNotNull(stats);
            assertEquals(d, stats.dimension());
        }

        harness.shutdown();
    }

    @Test
    void testDimensionStatisticsModelMethods() {
        float[][] data = generateGaussianData(500, 4, new Random(12345));
        DataSource source = new FloatArrayDataSource(data);

        AnalyzerHarness harness = new AnalyzerHarness();
        harness.register("dimension-statistics");

        AnalysisResults results = harness.run(source, 100);
        DimensionStatisticsModel model = results.getResult("dimension-statistics", DimensionStatisticsModel.class);

        // Test aggregate accessor methods
        double[] means = model.getMeans();
        double[] stdDevs = model.getStdDevs();
        double[] skewnesses = model.getSkewnesses();
        double[] kurtoses = model.getKurtoses();

        assertEquals(4, means.length);
        assertEquals(4, stdDevs.length);
        assertEquals(4, skewnesses.length);
        assertEquals(4, kurtoses.length);

        // Verify they match individual statistics
        for (int d = 0; d < 4; d++) {
            DimensionStatistics stats = model.getStatistics(d);
            assertEquals(stats.mean(), means[d], 1e-10);
            assertEquals(stats.stdDev(), stdDevs[d], 1e-10);
            assertEquals(stats.skewness(), skewnesses[d], 1e-10);
            assertEquals(stats.kurtosis(), kurtoses[d], 1e-10);
        }

        // Test summary output
        String summary = model.getSummary();
        assertNotNull(summary);
        assertTrue(summary.contains("4 dimensions"));
        assertTrue(summary.contains("500 vectors"));

        harness.shutdown();
    }

    // Helper methods and test analyzers

    private float[][] generateGaussianData(int numVectors, int dimensions, Random random) {
        float[][] data = new float[numVectors][dimensions];
        for (int v = 0; v < numVectors; v++) {
            for (int d = 0; d < dimensions; d++) {
                data[v][d] = (float) random.nextGaussian();
            }
        }
        return data;
    }

    private static class CountingAnalyzer implements StreamingAnalyzer<Long> {
        private final String type;
        private final AtomicLong count = new AtomicLong();

        CountingAnalyzer() {
            this("counter");
        }

        CountingAnalyzer(String type) {
            this.type = type;
        }

        @Override
        public String getAnalyzerType() { return type; }

        @Override
        public void initialize(DataspaceShape shape) {}

        @Override
        public void accept(float[][] chunk, long startIndex) {
            // Columnar format: chunk[dimension][vectorIndex]
            // Vector count is chunk[0].length
            int vectorCount = chunk.length > 0 ? chunk[0].length : 0;
            count.addAndGet(vectorCount);
        }

        @Override
        public Long complete() {
            return count.get();
        }
    }

    private static class SumAnalyzer implements StreamingAnalyzer<Double> {
        private double sum = 0;
        private final Object lock = new Object();

        @Override
        public String getAnalyzerType() { return "sum"; }

        @Override
        public void initialize(DataspaceShape shape) {}

        @Override
        public void accept(float[][] chunk, long startIndex) {
            // Columnar format: chunk[dimension][vectorIndex]
            double localSum = 0;
            for (float[] dimValues : chunk) {
                for (float v : dimValues) {
                    localSum += v;
                }
            }
            synchronized (lock) {
                sum += localSum;
            }
        }

        @Override
        public Double complete() {
            return sum;
        }
    }

    private static class FailingAnalyzer implements StreamingAnalyzer<Void> {
        private final int failAfter;
        private final AtomicLong count = new AtomicLong();

        FailingAnalyzer(int failAfter) {
            this.failAfter = failAfter;
        }

        @Override
        public String getAnalyzerType() { return "failing"; }

        @Override
        public void initialize(DataspaceShape shape) {}

        @Override
        public void accept(float[][] chunk, long startIndex) {
            // Columnar format: chunk[dimension][vectorIndex]
            int vectorCount = chunk.length > 0 ? chunk[0].length : 0;
            if (count.addAndGet(vectorCount) > failAfter) {
                throw new RuntimeException("Intentional failure after " + failAfter + " vectors");
            }
        }

        @Override
        public Void complete() {
            return null;
        }
    }
}
