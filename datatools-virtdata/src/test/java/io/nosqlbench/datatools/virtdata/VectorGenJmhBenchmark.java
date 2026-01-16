package io.nosqlbench.datatools.virtdata;

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
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;
import java.util.function.LongFunction;

/**
 * JMH benchmarks comparing Panama SIMD vs Scalar implementations of VectorGen.
 *
 * <p>Run with:
 * <pre>
 * mvn test-compile exec:java -Dexec.mainClass="io.nosqlbench.datatools.virtdata.VectorGenJmhBenchmark" \
 *     -Dexec.classpathScope=test
 * </pre>
 *
 * <p>Or run the main method directly from an IDE.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1, jvmArgs = {"--add-modules", "jdk.incubator.vector", "--enable-native-access=ALL-UNNAMED"})
public class VectorGenJmhBenchmark {

    @Param({"64", "128", "256", "512", "1024"})
    private int dimensions;

    @Param({"1", "100", "1000"})
    private int batchSize;

    private LongFunction<float[]> panamaGen;
    private LongFunction<float[]> scalarGen;
    private ScalarDimensionDistributionGenerator scalarGenTyped;
    private DimensionDistributionGenerator panamaGenTyped;

    // LERP and normalized generators
    private VectorGenerator<VectorSpaceModel> lerpGen;
    private VectorGenerator<VectorSpaceModel> normalizedGen;
    private VectorGenerator<VectorSpaceModel> lerpNormalizedGen;

    private long ordinal;

    @Setup(Level.Trial)
    public void setup() {
        VectorSpaceModel model = new VectorSpaceModel(10_000_000L, dimensions, 0.0, 1.0);

        // Create both implementations
        scalarGen = VectorGenFactory.create(model, VectorGenFactory.Mode.SCALAR);
        scalarGenTyped = (ScalarDimensionDistributionGenerator) scalarGen;

        if (VectorGenFactory.isPanamaAvailable()) {
            panamaGen = VectorGenFactory.create(model, VectorGenFactory.Mode.PANAMA);
            panamaGenTyped = (DimensionDistributionGenerator) panamaGen;
        } else {
            // Fallback to scalar if Panama not available
            panamaGen = scalarGen;
            panamaGenTyped = null;
        }

        // Create LERP-optimized generator
        GeneratorOptions lerpOptions = GeneratorOptions.builder()
            .useLerp(true)
            .build();
        lerpGen = VectorGenFactory.create(model, lerpOptions);

        // Create normalized generator
        GeneratorOptions normOptions = GeneratorOptions.builder()
            .normalizeL2(true)
            .build();
        normalizedGen = VectorGenFactory.create(model, normOptions);

        // Create LERP + normalized generator
        GeneratorOptions bothOptions = GeneratorOptions.builder()
            .useLerp(true)
            .normalizeL2(true)
            .build();
        lerpNormalizedGen = VectorGenFactory.create(model, bothOptions);

        ordinal = 0;
    }

    // ==================== Single Vector Benchmarks ====================

    @Benchmark
    public void singleVector_Panama(Blackhole bh) {
        bh.consume(panamaGen.apply(ordinal++));
    }

    @Benchmark
    public void singleVector_Scalar(Blackhole bh) {
        bh.consume(scalarGen.apply(ordinal++));
    }

    // ==================== Batch Generation Benchmarks ====================

    @Benchmark
    public void batchFlat_Panama(Blackhole bh) {
        if (panamaGenTyped != null) {
            bh.consume(panamaGenTyped.generateFlatBatch(ordinal, batchSize));
        } else {
            bh.consume(scalarGenTyped.generateFlatBatch(ordinal, batchSize));
        }
        ordinal += batchSize;
    }

    @Benchmark
    public void batchFlat_Scalar(Blackhole bh) {
        bh.consume(scalarGenTyped.generateFlatBatch(ordinal, batchSize));
        ordinal += batchSize;
    }

    @Benchmark
    public void batch2D_Panama(Blackhole bh) {
        if (panamaGenTyped != null) {
            bh.consume(panamaGenTyped.generateBatch(ordinal, batchSize));
        } else {
            bh.consume(scalarGenTyped.generateBatch(ordinal, batchSize));
        }
        ordinal += batchSize;
    }

    @Benchmark
    public void batch2D_Scalar(Blackhole bh) {
        bh.consume(scalarGenTyped.generateBatch(ordinal, batchSize));
        ordinal += batchSize;
    }

    // ==================== Double Precision Benchmarks ====================

    @Benchmark
    public void singleVectorDouble_Panama(Blackhole bh) {
        if (panamaGenTyped != null) {
            bh.consume(panamaGenTyped.applyAsDouble(ordinal++));
        } else {
            bh.consume(scalarGenTyped.applyAsDouble(ordinal++));
        }
    }

    @Benchmark
    public void singleVectorDouble_Scalar(Blackhole bh) {
        bh.consume(scalarGenTyped.applyAsDouble(ordinal++));
    }

    // ==================== LERP Optimized Benchmarks ====================

    @Benchmark
    public void singleVector_Lerp(Blackhole bh) {
        bh.consume(lerpGen.apply(ordinal++));
    }

    @Benchmark
    public void batchFlat_Lerp(Blackhole bh) {
        bh.consume(lerpGen.generateFlatBatch(ordinal, batchSize));
        ordinal += batchSize;
    }

    // ==================== Normalized Benchmarks ====================

    @Benchmark
    public void singleVector_Normalized(Blackhole bh) {
        bh.consume(normalizedGen.apply(ordinal++));
    }

    @Benchmark
    public void batchFlat_Normalized(Blackhole bh) {
        bh.consume(normalizedGen.generateFlatBatch(ordinal, batchSize));
        ordinal += batchSize;
    }

    // ==================== LERP + Normalized Benchmarks ====================

    @Benchmark
    public void singleVector_LerpNormalized(Blackhole bh) {
        bh.consume(lerpNormalizedGen.apply(ordinal++));
    }

    @Benchmark
    public void batchFlat_LerpNormalized(Blackhole bh) {
        bh.consume(lerpNormalizedGen.generateFlatBatch(ordinal, batchSize));
        ordinal += batchSize;
    }

    /**
     * Run the benchmarks.
     */
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
            .include(VectorGenJmhBenchmark.class.getSimpleName())
            .forks(1)
            .jvmArgs("--add-modules", "jdk.incubator.vector", "--enable-native-access=ALL-UNNAMED")
            .build();

        new Runner(opt).run();
    }
}
