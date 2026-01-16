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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Quick sanity tests for sampler throughput.
 *
 * <p>These tests verify that samplers meet minimum performance requirements.
 * For detailed benchmarking, use the JMH benchmarks.
 */
class SamplerThroughputTest {

    private static final int WARMUP_ITERATIONS = 10_000;
    private static final int BENCHMARK_ITERATIONS = 100_000;

    // Minimum throughput thresholds per sampler type
    // PearsonIV uses numerical integration and is inherently slow
    static Stream<Arguments> samplerProvider() {
        return Stream.of(
            Arguments.of("Normal", 50.0, new NormalSampler(new NormalScalarModel(0, 1))),
            Arguments.of("Uniform", 100.0, new UniformSampler(new UniformScalarModel(0, 1))),
            Arguments.of("Beta", 50.0, new BetaSampler(new BetaScalarModel(2, 5))),
            Arguments.of("Gamma", 50.0, new GammaSampler(new GammaScalarModel(3, 2))),
            Arguments.of("StudentT", 50.0, new StudentTSampler(new StudentTScalarModel(10))),
            Arguments.of("InverseGamma", 50.0, new InverseGammaSampler(new InverseGammaScalarModel(3, 2))),
            Arguments.of("BetaPrime", 50.0, new BetaPrimeSampler(new BetaPrimeScalarModel(3, 5))),
            // PearsonIV uses numerical integration - much slower but still usable
            Arguments.of("PearsonIV", 1.0, new PearsonIVSampler(new PearsonIVScalarModel(2.0, 0.5, 1.0, 0.0)))
        );
    }

    @ParameterizedTest(name = "{0} sampler throughput")
    @MethodSource("samplerProvider")
    void testSamplerThroughput(String name, double minOpsPerMs, ComponentSampler sampler) {
        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            sampler.sample(0.5);
        }

        // Benchmark
        long start = System.nanoTime();
        double sum = 0;
        for (int i = 0; i < BENCHMARK_ITERATIONS; i++) {
            sum += sampler.sample((i + 0.5) / BENCHMARK_ITERATIONS);
        }
        long elapsed = System.nanoTime() - start;

        // Prevent dead code elimination
        assertTrue(Double.isFinite(sum), "Sum should be finite");

        double opsPerMs = (double) BENCHMARK_ITERATIONS / (elapsed / 1_000_000.0);
        System.out.printf("%s sampler: %.1f Kops/sec%n", name, opsPerMs);

        assertTrue(opsPerMs > minOpsPerMs,
            name + " sampler too slow: " + opsPerMs + " Kops/sec (min: " + minOpsPerMs + ")");
    }

    @Test
    void testLerpSamplerThroughputImprovement() {
        // LERP should be significantly faster than direct sampling for complex distributions
        GammaScalarModel model = new GammaScalarModel(3, 2);
        ComponentSampler direct = new GammaSampler(model);
        ComponentSampler lerp = new LerpSampler(direct);

        // Warmup both
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            direct.sample(0.5);
            lerp.sample(0.5);
        }

        // Benchmark direct
        long start = System.nanoTime();
        double directSum = 0;
        for (int i = 0; i < BENCHMARK_ITERATIONS; i++) {
            directSum += direct.sample((i + 0.5) / BENCHMARK_ITERATIONS);
        }
        long directElapsed = System.nanoTime() - start;

        // Benchmark LERP
        start = System.nanoTime();
        double lerpSum = 0;
        for (int i = 0; i < BENCHMARK_ITERATIONS; i++) {
            lerpSum += lerp.sample((i + 0.5) / BENCHMARK_ITERATIONS);
        }
        long lerpElapsed = System.nanoTime() - start;

        double directOpsPerMs = (double) BENCHMARK_ITERATIONS / (directElapsed / 1_000_000.0);
        double lerpOpsPerMs = (double) BENCHMARK_ITERATIONS / (lerpElapsed / 1_000_000.0);
        double speedup = lerpOpsPerMs / directOpsPerMs;

        System.out.printf("Gamma direct: %.1f Kops/sec%n", directOpsPerMs);
        System.out.printf("Gamma LERP:   %.1f Kops/sec%n", lerpOpsPerMs);
        System.out.printf("LERP speedup: %.1fx%n", speedup);

        // LERP should be at least 2x faster for Gamma
        assertTrue(speedup > 2.0, "LERP speedup only " + speedup + "x (expected > 2x)");
    }

    @Test
    void testUniformSamplerIsAlreadyFast() {
        // Uniform sampler is already O(1), LERP should have minimal impact
        UniformScalarModel model = new UniformScalarModel(0, 1);
        ComponentSampler direct = new UniformSampler(model);
        ComponentSampler lerp = new LerpSampler(direct);

        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            direct.sample(0.5);
            lerp.sample(0.5);
        }

        // Benchmark
        long start = System.nanoTime();
        for (int i = 0; i < BENCHMARK_ITERATIONS; i++) {
            direct.sample((i + 0.5) / BENCHMARK_ITERATIONS);
        }
        long directElapsed = System.nanoTime() - start;

        start = System.nanoTime();
        for (int i = 0; i < BENCHMARK_ITERATIONS; i++) {
            lerp.sample((i + 0.5) / BENCHMARK_ITERATIONS);
        }
        long lerpElapsed = System.nanoTime() - start;

        double directOpsPerMs = (double) BENCHMARK_ITERATIONS / (directElapsed / 1_000_000.0);
        double lerpOpsPerMs = (double) BENCHMARK_ITERATIONS / (lerpElapsed / 1_000_000.0);

        System.out.printf("Uniform direct: %.1f Kops/sec%n", directOpsPerMs);
        System.out.printf("Uniform LERP:   %.1f Kops/sec%n", lerpOpsPerMs);

        // Uniform should be very fast in both cases
        assertTrue(directOpsPerMs > 100, "Uniform direct should be > 100 Kops/sec");
        assertTrue(lerpOpsPerMs > 100, "Uniform LERP should be > 100 Kops/sec");
    }

    @Test
    void testDirectVsLerpSamplerConsistency() {
        // LERP and direct should produce similar distributions
        BetaScalarModel model = new BetaScalarModel(2, 5);
        ComponentSampler direct = new BetaSampler(model);
        ComponentSampler lerp = new LerpSampler(direct);

        double directSum = 0, lerpSum = 0;
        int count = 10_000;
        for (int i = 0; i < count; i++) {
            double u = (i + 0.5) / count;
            directSum += direct.sample(u);
            lerpSum += lerp.sample(u);
        }

        double directMean = directSum / count;
        double lerpMean = lerpSum / count;

        // Means should be very close
        double relError = Math.abs((lerpMean - directMean) / directMean);
        assertTrue(relError < 0.01, "Mean mismatch: " + relError);
    }
}
