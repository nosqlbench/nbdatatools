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
 * JMH benchmark for individual model fitter performance comparison.
 *
 * <h2>Purpose</h2>
 *
 * <p>Measures the throughput of each model fitter to identify performance
 * characteristics and optimization opportunities. This helps guide decisions
 * about fitter ordering in {@link BestFitSelector}.
 *
 * <h2>Benchmark Configurations</h2>
 *
 * <ul>
 *   <li>Data sizes: 1K, 10K elements</li>
 *   <li>All distribution fitters compared</li>
 *   <li>Throughput measured in ops/second</li>
 * </ul>
 *
 * <h2>Running</h2>
 *
 * <pre>{@code
 * mvn test -pl datatools-vshapes -Pperformance -Dtest=ModelFitterJmhBenchmark
 * }</pre>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1, jvmArgs = {"--add-modules", "jdk.incubator.vector", "--enable-native-access=ALL-UNNAMED"})
@Tag("performance")
public class ModelFitterJmhBenchmark {

    @Param({"1024", "10000"})
    private int dataSize;

    // Data arrays for different distribution types
    private float[] uniformData;
    private float[] normalData;
    private float[] betaData;
    private float[] gammaData;

    // Statistics computed from data
    private DimensionStatistics uniformStats;
    private DimensionStatistics normalStats;
    private DimensionStatistics betaStats;
    private DimensionStatistics gammaStats;

    // Fitters
    private UniformModelFitter uniformFitter;
    private NormalModelFitter normalFitter;
    private BetaModelFitter betaFitter;
    private GammaModelFitter gammaFitter;
    private EmpiricalModelFitter empiricalFitter;

    @Setup(Level.Trial)
    public void setup() {
        Random random = new Random(42);

        // Generate uniform data [0, 1]
        uniformData = new float[dataSize];
        for (int i = 0; i < dataSize; i++) {
            uniformData[i] = random.nextFloat();
        }
        uniformStats = DimensionStatistics.compute(0, uniformData);

        // Generate normal data (mean=0.5, stddev=0.1)
        normalData = new float[dataSize];
        for (int i = 0; i < dataSize; i++) {
            normalData[i] = (float) (random.nextGaussian() * 0.1 + 0.5);
        }
        normalStats = DimensionStatistics.compute(0, normalData);

        // Generate beta-like data [0, 1]
        betaData = new float[dataSize];
        for (int i = 0; i < dataSize; i++) {
            // Approximate beta(2, 5) by rejection sampling from uniform
            double x = random.nextDouble();
            double y = random.nextDouble() * 2;  // Max density for beta(2,5)
            double density = 30 * x * Math.pow(1 - x, 4);
            while (y > density) {
                x = random.nextDouble();
                y = random.nextDouble() * 2;
                density = 30 * x * Math.pow(1 - x, 4);
            }
            betaData[i] = (float) x;
        }
        betaStats = DimensionStatistics.compute(0, betaData);

        // Generate gamma-like data
        gammaData = new float[dataSize];
        for (int i = 0; i < dataSize; i++) {
            // Simple gamma approximation using sum of exponentials
            double sum = 0;
            for (int j = 0; j < 3; j++) {
                sum -= Math.log(random.nextDouble());
            }
            gammaData[i] = (float) (sum / 2);
        }
        gammaStats = DimensionStatistics.compute(0, gammaData);

        // Initialize fitters
        uniformFitter = new UniformModelFitter();
        normalFitter = new NormalModelFitter();
        betaFitter = new BetaModelFitter();
        gammaFitter = new GammaModelFitter();
        empiricalFitter = new EmpiricalModelFitter();
    }

    @Benchmark
    public void uniformFitter_uniformData(Blackhole bh) {
        ComponentModelFitter.FitResult result = uniformFitter.fit(uniformStats, uniformData);
        bh.consume(result);
    }

    @Benchmark
    public void normalFitter_normalData(Blackhole bh) {
        ComponentModelFitter.FitResult result = normalFitter.fit(normalStats, normalData);
        bh.consume(result);
    }

    @Benchmark
    public void betaFitter_betaData(Blackhole bh) {
        ComponentModelFitter.FitResult result = betaFitter.fit(betaStats, betaData);
        bh.consume(result);
    }

    @Benchmark
    public void gammaFitter_gammaData(Blackhole bh) {
        ComponentModelFitter.FitResult result = gammaFitter.fit(gammaStats, gammaData);
        bh.consume(result);
    }

    @Benchmark
    public void empiricalFitter_uniformData(Blackhole bh) {
        ComponentModelFitter.FitResult result = empiricalFitter.fit(uniformStats, uniformData);
        bh.consume(result);
    }

    @Benchmark
    public void empiricalFitter_normalData(Blackhole bh) {
        ComponentModelFitter.FitResult result = empiricalFitter.fit(normalStats, normalData);
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
            .include(ModelFitterJmhBenchmark.class.getSimpleName())
            .warmupIterations(2)
            .measurementIterations(3)
            .forks(1)
            .jvmArgs("--add-modules", "jdk.incubator.vector", "--enable-native-access=ALL-UNNAMED")
            .resultFormat(ResultFormatType.TEXT)
            .build();

        new Runner(opt).run();
    }
}
