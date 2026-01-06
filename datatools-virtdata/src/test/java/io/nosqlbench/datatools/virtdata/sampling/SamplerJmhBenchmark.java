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

package io.nosqlbench.datatools.virtdata.sampling;

import io.nosqlbench.vshapes.model.*;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadLocalRandom;

/**
 * JMH benchmarks for individual sampler implementations.
 *
 * <p>Compares direct vs LERP-optimized sampling for each distribution type.
 *
 * <p>Run with:
 * <pre>
 * mvn test-compile exec:java -Dexec.mainClass="io.nosqlbench.datatools.virtdata.sampling.SamplerJmhBenchmark" \
 *     -Dexec.classpathScope=test
 * </pre>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1)
public class SamplerJmhBenchmark {

    @Param({"NORMAL", "UNIFORM", "BETA", "GAMMA", "STUDENT_T"})
    private String samplerType;

    @Param({"DIRECT", "LERP"})
    private String samplerMode;

    private ComponentSampler sampler;
    private double[] precomputedU;

    @Setup(Level.Trial)
    public void setup() {
        // Create base sampler
        ComponentSampler baseSampler = createBaseSampler(samplerType);

        // Wrap with LERP if requested
        if ("LERP".equals(samplerMode)) {
            sampler = new LerpSampler(baseSampler);
        } else {
            sampler = baseSampler;
        }

        // Pre-compute random u values for consistent benchmarking
        precomputedU = new double[10_000];
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        for (int i = 0; i < precomputedU.length; i++) {
            precomputedU[i] = rng.nextDouble(1e-10, 1 - 1e-10);
        }
    }

    private static ComponentSampler createBaseSampler(String type) {
        switch (type) {
            case "NORMAL":
                return new NormalSampler(new NormalScalarModel(0.0, 1.0));
            case "UNIFORM":
                return new UniformSampler(new UniformScalarModel(0.0, 1.0));
            case "BETA":
                return new BetaSampler(new BetaScalarModel(2.0, 5.0));
            case "GAMMA":
                return new GammaSampler(new GammaScalarModel(3.0, 2.0));
            case "STUDENT_T":
                return new StudentTSampler(new StudentTScalarModel(10.0));
            default:
                throw new IllegalArgumentException("Unknown sampler type: " + type);
        }
    }

    // ==================== Single Sample Benchmarks ====================

    @Benchmark
    public double singleSample() {
        int idx = ThreadLocalRandom.current().nextInt(precomputedU.length);
        return sampler.sample(precomputedU[idx]);
    }

    @Benchmark
    @OperationsPerInvocation(1000)
    public void batchSample_1000(Blackhole bh) {
        for (int i = 0; i < 1000; i++) {
            bh.consume(sampler.sample(precomputedU[i]));
        }
    }

    /**
     * Run the benchmarks.
     */
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
            .include(SamplerJmhBenchmark.class.getSimpleName())
            .forks(1)
            .build();

        new Runner(opt).run();
    }
}
