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

import io.nosqlbench.vshapes.model.VectorSpaceModel;
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
 * JMH benchmark comparing ModelExtractor implementations.
 *
 * <h2>Purpose</h2>
 *
 * <p>Compares the performance of different model extraction strategies:
 * <ul>
 *   <li>{@link DatasetModelExtractor} - Single-threaded SIMD</li>
 *   <li>{@link ParallelDatasetModelExtractor} - ForkJoinPool parallel</li>
 *   <li>{@link VirtualThreadModelExtractor} - Virtual threads + microbatching</li>
 * </ul>
 *
 * <h2>Running</h2>
 *
 * <pre>{@code
 * mvn test -pl datatools-vshapes -Pperformance -Dtest=ModelExtractorJmhBenchmark
 * }</pre>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 2, time = 2)
@Measurement(iterations = 3, time = 2)
@Fork(value = 1, jvmArgs = {"--add-modules", "jdk.incubator.vector"})
@Tag("performance")
public class ModelExtractorJmhBenchmark {

    @Param({"100", "500", "1000"})
    private int dimensions;

    @Param({"10000", "25000"})
    private int vectors;

    private float[][] data;
    private DatasetModelExtractor simdExtractor;
    private ParallelDatasetModelExtractor parallelExtractor;
    private VirtualThreadModelExtractor optimizedExtractor;

    @Setup(Level.Trial)
    public void setup() {
        Random random = new Random(42);

        // Generate random vector data
        data = new float[vectors][dimensions];
        for (int v = 0; v < vectors; v++) {
            for (int d = 0; d < dimensions; d++) {
                // Mixed distribution: some normal, some uniform
                if (d % 3 == 0) {
                    data[v][d] = (float) (random.nextGaussian() * 0.3 + 0.5);
                } else {
                    data[v][d] = random.nextFloat();
                }
            }
        }

        // Create extractors
        BestFitSelector selector = BestFitSelector.defaultSelector();

        simdExtractor = new DatasetModelExtractor(selector, vectors);

        parallelExtractor = ParallelDatasetModelExtractor.builder()
            .selector(selector)
            .uniqueVectors(vectors)
            .build();

        optimizedExtractor = VirtualThreadModelExtractor.builder()
            .selector(selector)
            .uniqueVectors(vectors)
            .build();
    }

    @TearDown(Level.Trial)
    public void teardown() {
        if (parallelExtractor != null) {
            parallelExtractor.shutdown();
        }
    }

    @Benchmark
    public void extractSIMD(Blackhole bh) {
        VectorSpaceModel model = simdExtractor.extractVectorModel(data);
        bh.consume(model);
    }

    @Benchmark
    public void extractParallel(Blackhole bh) {
        VectorSpaceModel model = parallelExtractor.extractVectorModel(data);
        bh.consume(model);
    }

    @Benchmark
    public void extractOptimized(Blackhole bh) {
        VectorSpaceModel model = optimizedExtractor.extractVectorModel(data);
        bh.consume(model);
    }

    /**
     * Runs the benchmark as a JUnit test.
     */
    @Test
    void runBenchmark() throws RunnerException {
        Options opt = new OptionsBuilder()
            .include(ModelExtractorJmhBenchmark.class.getSimpleName())
            .warmupIterations(2)
            .measurementIterations(3)
            .forks(1)
            .jvmArgs("--add-modules", "jdk.incubator.vector")
            .resultFormat(ResultFormatType.TEXT)
            .build();

        new Runner(opt).run();
    }
}
