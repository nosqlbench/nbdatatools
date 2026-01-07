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
 * JMH benchmark for BestFitSelector performance comparison.
 *
 * <h2>Purpose</h2>
 *
 * <p>Measures the throughput of different selector configurations to understand
 * the performance impact of adding more fitters to the selection process.
 *
 * <h2>Configurations Benchmarked</h2>
 *
 * <ul>
 *   <li>{@link BestFitSelector#boundedDataSelector()} - Normal, Beta, Uniform</li>
 *   <li>{@link BestFitSelector#boundedDataWithEmpirical()} - Above + Empirical</li>
 *   <li>{@link BestFitSelector#fullPearsonSelector()} - All Pearson types + Empirical</li>
 *   <li>{@link BestFitSelector#multimodalAwareSelector()} - Multimodal support</li>
 * </ul>
 *
 * <h2>Running</h2>
 *
 * <pre>{@code
 * mvn test -pl datatools-vshapes -Pperformance -Dtest=BestFitSelectorJmhBenchmark
 * }</pre>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1, jvmArgs = {"--add-modules", "jdk.incubator.vector"})
@Tag("performance")
public class BestFitSelectorJmhBenchmark {

    @Param({"1024", "10000"})
    private int dataSize;

    // Different data distributions for testing
    private float[] uniformData;
    private float[] normalData;
    private float[] bimodalData;

    private DimensionStatistics uniformStats;
    private DimensionStatistics normalStats;
    private DimensionStatistics bimodalStats;

    // Different selector configurations
    private BestFitSelector boundedSelector;
    private BestFitSelector boundedWithEmpirical;
    private BestFitSelector fullPearsonSelector;
    private BestFitSelector multimodalSelector;

    @Setup(Level.Trial)
    public void setup() {
        Random random = new Random(42);

        // Generate uniform data [0, 1]
        uniformData = new float[dataSize];
        for (int i = 0; i < dataSize; i++) {
            uniformData[i] = random.nextFloat();
        }
        uniformStats = DimensionStatistics.compute(0, uniformData);

        // Generate normal data (mean=0.5, stddev=0.15, bounded to [0,1])
        normalData = new float[dataSize];
        for (int i = 0; i < dataSize; i++) {
            double v = random.nextGaussian() * 0.15 + 0.5;
            normalData[i] = (float) Math.max(0, Math.min(1, v));
        }
        normalStats = DimensionStatistics.compute(0, normalData);

        // Generate bimodal data (mixture of two normals)
        bimodalData = new float[dataSize];
        for (int i = 0; i < dataSize; i++) {
            if (random.nextBoolean()) {
                double v = random.nextGaussian() * 0.1 + 0.3;
                bimodalData[i] = (float) Math.max(0, Math.min(1, v));
            } else {
                double v = random.nextGaussian() * 0.1 + 0.7;
                bimodalData[i] = (float) Math.max(0, Math.min(1, v));
            }
        }
        bimodalStats = DimensionStatistics.compute(0, bimodalData);

        // Initialize selectors
        boundedSelector = BestFitSelector.boundedDataSelector();
        boundedWithEmpirical = BestFitSelector.boundedDataWithEmpirical();
        fullPearsonSelector = BestFitSelector.fullPearsonSelector();
        multimodalSelector = BestFitSelector.multimodalAwareSelector();
    }

    // Bounded selector benchmarks
    @Benchmark
    public void boundedSelector_uniformData(Blackhole bh) {
        ComponentModelFitter.FitResult result = boundedSelector.selectBestResult(uniformStats, uniformData);
        bh.consume(result);
    }

    @Benchmark
    public void boundedSelector_normalData(Blackhole bh) {
        ComponentModelFitter.FitResult result = boundedSelector.selectBestResult(normalStats, normalData);
        bh.consume(result);
    }

    // Bounded + Empirical selector benchmarks
    @Benchmark
    public void boundedWithEmpirical_uniformData(Blackhole bh) {
        ComponentModelFitter.FitResult result = boundedWithEmpirical.selectBestResult(uniformStats, uniformData);
        bh.consume(result);
    }

    @Benchmark
    public void boundedWithEmpirical_normalData(Blackhole bh) {
        ComponentModelFitter.FitResult result = boundedWithEmpirical.selectBestResult(normalStats, normalData);
        bh.consume(result);
    }

    // Full Pearson selector benchmarks
    @Benchmark
    public void fullPearsonSelector_uniformData(Blackhole bh) {
        ComponentModelFitter.FitResult result = fullPearsonSelector.selectBestResult(uniformStats, uniformData);
        bh.consume(result);
    }

    @Benchmark
    public void fullPearsonSelector_normalData(Blackhole bh) {
        ComponentModelFitter.FitResult result = fullPearsonSelector.selectBestResult(normalStats, normalData);
        bh.consume(result);
    }

    // Multimodal selector benchmarks
    @Benchmark
    public void multimodalSelector_unimodalData(Blackhole bh) {
        ComponentModelFitter.FitResult result = multimodalSelector.selectBestResult(normalStats, normalData);
        bh.consume(result);
    }

    @Benchmark
    public void multimodalSelector_bimodalData(Blackhole bh) {
        ComponentModelFitter.FitResult result = multimodalSelector.selectBestResult(bimodalStats, bimodalData);
        bh.consume(result);
    }

    /**
     * Runs the JMH benchmark from JUnit.
     * Enable with: mvn test -pl datatools-vshapes -Pperformance
     */
    @Test
    @Tag("performance")
    public void runBenchmark() throws RunnerException {
        Options opt = new OptionsBuilder()
            .include(BestFitSelectorJmhBenchmark.class.getSimpleName())
            .warmupIterations(2)
            .measurementIterations(3)
            .forks(1)
            .jvmArgs("--add-modules", "jdk.incubator.vector")
            .resultFormat(ResultFormatType.TEXT)
            .build();

        new Runner(opt).run();
    }
}
