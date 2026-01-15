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

import io.nosqlbench.vshapes.extract.ModeDetector.ModeDetectionResult;
import io.nosqlbench.vshapes.model.ScalarModel;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * JMH benchmark for multimodal detection and composite model fitting.
 *
 * <h2>Purpose</h2>
 *
 * <p>Establishes performance baselines for:
 * <ul>
 *   <li>{@link ModeDetector} - histogram-based mode detection</li>
 *   <li>{@link CompositeModelFitter} - composite model fitting</li>
 *   <li>{@link BestFitSelector#multimodalAwareSelector()} - integrated selection</li>
 * </ul>
 *
 * <h2>Running</h2>
 *
 * <pre>{@code
 * mvn test -pl datatools-vshapes -Pperformance -Dtest=MultimodalFittingJmhBenchmark
 * }</pre>
 *
 * <h2>Expected Performance Baselines</h2>
 *
 * <p>Target throughput on typical hardware (2024+ CPU):
 * <ul>
 *   <li>Mode detection: < 5ms for 10,000 samples</li>
 *   <li>Composite fitting: < 50ms for 10,000 samples</li>
 *   <li>Full multimodal selection: < 100ms for 10,000 samples</li>
 * </ul>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 2, time = 2)
@Measurement(iterations = 3, time = 2)
@Fork(value = 1, jvmArgs = {"--add-modules", "jdk.incubator.vector", "--enable-native-access=ALL-UNNAMED"})
@Tag("performance")
public class MultimodalFittingJmhBenchmark {

    // ==================== Benchmark Parameters ====================

    @Param({"1000", "5000", "10000", "25000"})
    private int sampleSize;

    @Param({"2", "3"})
    private int numModes;

    // ==================== Test Data ====================

    private float[] unimodalData;
    private float[] bimodalData;
    private float[] trimodalData;

    private CompositeModelFitter compositeFitter;
    private BestFitSelector multimodalSelector;
    private BestFitSelector standardSelector;

    // ==================== Setup ====================

    @Setup(Level.Trial)
    public void setup() {
        Random random = new Random(42);

        // Generate unimodal data
        unimodalData = new float[sampleSize];
        for (int i = 0; i < sampleSize; i++) {
            unimodalData[i] = (float) random.nextGaussian();
        }

        // Generate bimodal data: N(-2, 0.5) + N(2, 0.5)
        bimodalData = new float[sampleSize];
        for (int i = 0; i < sampleSize; i++) {
            double mean = random.nextBoolean() ? -2.0 : 2.0;
            bimodalData[i] = (float) (mean + random.nextGaussian() * 0.5);
        }

        // Generate trimodal data: N(-3, 0.4) + N(0, 0.4) + N(3, 0.4)
        trimodalData = new float[sampleSize];
        for (int i = 0; i < sampleSize; i++) {
            int mode = random.nextInt(3);
            double mean = mode == 0 ? -3.0 : (mode == 1 ? 0.0 : 3.0);
            trimodalData[i] = (float) (mean + random.nextGaussian() * 0.4);
        }

        // Create fitters and selectors
        compositeFitter = new CompositeModelFitter(
            BestFitSelector.boundedDataSelector(), numModes);
        multimodalSelector = BestFitSelector.multimodalAwareSelector();
        standardSelector = BestFitSelector.boundedDataWithEmpirical();
    }

    // ==================== Mode Detection Benchmarks ====================

    @Benchmark
    public void modeDetection_unimodal(Blackhole bh) {
        ModeDetectionResult result = ModeDetector.detect(unimodalData);
        bh.consume(result);
    }

    @Benchmark
    public void modeDetection_bimodal(Blackhole bh) {
        ModeDetectionResult result = ModeDetector.detect(bimodalData);
        bh.consume(result);
    }

    @Benchmark
    public void modeDetection_trimodal(Blackhole bh) {
        ModeDetectionResult result = ModeDetector.detect(trimodalData, 3);
        bh.consume(result);
    }

    // ==================== Composite Fitting Benchmarks ====================

    @Benchmark
    public void compositeFitting_bimodal(Blackhole bh) {
        try {
            ComponentModelFitter.FitResult result = compositeFitter.fit(bimodalData);
            bh.consume(result);
        } catch (IllegalStateException e) {
            // Data not detected as multimodal - this is normal for some configurations
            bh.consume(e);
        }
    }

    @Benchmark
    public void compositeFitting_trimodal(Blackhole bh) {
        try {
            ComponentModelFitter.FitResult result = compositeFitter.fit(trimodalData);
            bh.consume(result);
        } catch (IllegalStateException e) {
            bh.consume(e);
        }
    }

    // ==================== Full Selection Benchmarks ====================

    @Benchmark
    public void multimodalSelector_unimodal(Blackhole bh) {
        ScalarModel model = multimodalSelector.selectBest(unimodalData);
        bh.consume(model);
    }

    @Benchmark
    public void multimodalSelector_bimodal(Blackhole bh) {
        ScalarModel model = multimodalSelector.selectBest(bimodalData);
        bh.consume(model);
    }

    @Benchmark
    public void standardSelector_bimodal(Blackhole bh) {
        ScalarModel model = standardSelector.selectBest(bimodalData);
        bh.consume(model);
    }

    // ==================== Comparison: Multimodal vs Standard ====================

    @Benchmark
    public void comparison_multimodalOverhead_unimodal(Blackhole bh) {
        // Measure overhead of multimodal detection on unimodal data
        ScalarModel multiResult = multimodalSelector.selectBest(unimodalData);
        ScalarModel stdResult = standardSelector.selectBest(unimodalData);
        bh.consume(multiResult);
        bh.consume(stdResult);
    }

    // ==================== JUnit Test Runner ====================

    /**
     * Runs the benchmark as a JUnit test.
     *
     * <p>Use this for quick validation. For accurate benchmarks, run with:
     * <pre>mvn test -pl datatools-vshapes -Pperformance -Dtest=MultimodalFittingJmhBenchmark</pre>
     */
    @Test
    void runBenchmark() throws RunnerException {
        Options opt = new OptionsBuilder()
            .include(MultimodalFittingJmhBenchmark.class.getSimpleName())
            .warmupIterations(2)
            .measurementIterations(3)
            .forks(1)
            .param("sampleSize", "10000")  // Use representative size for quick test
            .param("numModes", "2")
            .jvmArgs("--add-modules", "jdk.incubator.vector", "--enable-native-access=ALL-UNNAMED")
            .resultFormat(ResultFormatType.TEXT)
            .build();

        new Runner(opt).run();
    }

    /**
     * Quick smoke test to verify benchmark setup works.
     *
     * <p>This test runs the actual benchmark methods once without JMH
     * overhead to verify they don't throw exceptions.
     */
    @Test
    void smokeTest() {
        // Initialize with moderate sample size
        sampleSize = 5000;
        numModes = 2;
        setup();

        // Verify mode detection works
        ModeDetectionResult uniResult = ModeDetector.detect(unimodalData);
        assertModeDetectionValid(uniResult);

        ModeDetectionResult biResult = ModeDetector.detect(bimodalData);
        assertModeDetectionValid(biResult);

        ModeDetectionResult triResult = ModeDetector.detect(trimodalData, 3);
        assertModeDetectionValid(triResult);

        // Verify selectors work
        ScalarModel model = multimodalSelector.selectBest(unimodalData);
        assert model != null : "Selector should return model";

        System.out.println("Smoke test passed:");
        System.out.printf("  Unimodal: %d mode(s)%n", uniResult.modeCount());
        System.out.printf("  Bimodal: %d mode(s)%n", biResult.modeCount());
        System.out.printf("  Trimodal: %d mode(s)%n", triResult.modeCount());
    }

    private void assertModeDetectionValid(ModeDetectionResult result) {
        assert result != null : "Result should not be null";
        assert result.modeCount() >= 1 : "Should detect at least 1 mode";
        assert result.peakLocations() != null : "Peak locations should not be null";
        assert result.modeWeights() != null : "Mode weights should not be null";
        assert result.dipStatistic() >= 0 : "Dip statistic should be non-negative";
    }
}
