package io.nosqlbench.vectordata.spec.predicates;

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

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

/// JMH benchmark for PNode (hand-rolled binary) encode/decode throughput
/// across tree sizes.
///
/// ## Running
///
/// Via maven exec:
/// ```bash
/// mvn test-compile -pl datatools-vectordata -am && \
/// mvn exec:java -pl datatools-vectordata \
///   -Dexec.mainClass="org.openjdk.jmh.Main" \
///   -Dexec.classpathScope=test \
///   -Dexec.args="PredicateFormatBenchmark -f 1 -wi 2 -i 3 -t 1"
/// ```
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
@Tag("performance")
public class PredicateFormatBenchmark {

    /// Tree size parameter: small (single leaf), medium (3-child AND), large (nested ~6 nodes)
    @Param({"small", "medium", "large"})
    private String treeSize;

    // --- PNode artifacts ---
    private PNode<?> pnode;
    private byte[] pnodeEncoded;

    // Reusable encode buffer
    private ByteBuffer encodeBuf;

    @Setup(Level.Trial)
    public void setup() {
        switch (treeSize) {
            case "small":
                pnode = new PredicateNode(0, OpType.EQ, 42L);
                break;
            case "medium":
                pnode = new ConjugateNode(ConjugateType.AND,
                    new PredicateNode(0, OpType.GE, 10L),
                    new PredicateNode(1, OpType.LT, 50L),
                    new PredicateNode(2, OpType.EQ, 99L)
                );
                break;
            case "large":
                pnode = new ConjugateNode(ConjugateType.AND,
                    new PredicateNode(0, OpType.GE, 10L),
                    new ConjugateNode(ConjugateType.OR,
                        new PredicateNode(1, OpType.EQ, 5L),
                        new PredicateNode(2, OpType.LT, 20L),
                        new PredicateNode(3, OpType.IN, 7L, 9L, 11L)
                    ),
                    new PredicateNode(4, OpType.NE, 0L)
                );
                break;
            default:
                throw new IllegalArgumentException("Unknown treeSize: " + treeSize);
        }

        // Pre-encode for decode benchmarks
        encodeBuf = ByteBuffer.allocate(4096);
        pnode.encode(encodeBuf);
        encodeBuf.flip();
        pnodeEncoded = new byte[encodeBuf.remaining()];
        encodeBuf.get(pnodeEncoded);
    }

    // ==================== PNode Benchmarks ====================

    /// Benchmark PNode binary encoding throughput
    @Benchmark
    public void pnodeEncode(Blackhole bh) {
        encodeBuf.clear();
        pnode.encode(encodeBuf);
        bh.consume(encodeBuf);
    }

    /// Benchmark PNode binary decoding throughput
    @Benchmark
    public void pnodeDecode(Blackhole bh) {
        ByteBuffer buf = ByteBuffer.wrap(pnodeEncoded);
        bh.consume(PNode.fromBuffer(buf));
    }

    /// Runs the benchmark from JUnit (tagged as performance, excluded from default suite)
    @Test
    @Tag("performance")
    public void runBenchmark() throws RunnerException {
        Options opt = new OptionsBuilder()
            .include(PredicateFormatBenchmark.class.getSimpleName())
            .warmupIterations(2)
            .measurementIterations(3)
            .forks(1)
            .resultFormat(ResultFormatType.TEXT)
            .build();

        new Runner(opt).run();
    }

    /// Entry point for running benchmarks from command line
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
            .include(PredicateFormatBenchmark.class.getSimpleName())
            .forks(1)
            .build();

        new Runner(opt).run();
    }
}
