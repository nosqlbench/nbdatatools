package io.nosqlbench.vectordata.spec.metadata;

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

import java.util.concurrent.TimeUnit;

/// JMH benchmark for MNode binary encode/decode throughput
/// across metadata record sizes.
///
/// ## Running
///
/// ```bash
/// mvn clean test -pl datatools-vectordata -am \
///   -Pbench-metadata-format -DskipTests=true
/// ```
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
@Tag("performance")
public class MetadataFormatBenchmark {

    /// Record size parameter
    @Param({"small", "medium", "large"})
    private String recordSize;

    // --- MNode artifacts ---
    private MNode mnode;
    private byte[] mnodeEncoded;

    @Setup(Level.Trial)
    public void setup() {
        switch (recordSize) {
            case "small":
                // 2 fields: one string, one int
                mnode = MNode.of("name", "dataset-alpha", "dims", 128L);
                break;
            case "medium":
                // 5 fields: mixed types
                mnode = MNode.of(
                    "name", "glove-100-angular",
                    "dims", 100L,
                    "metric", "angular",
                    "train_size", 1183514L,
                    "normalized", true
                );
                break;
            case "large":
                // 10 fields including blob and float
                mnode = MNode.of(
                    "name", "sift-1M-euclidean",
                    "dims", 128L,
                    "metric", "euclidean",
                    "train_size", 1000000L,
                    "test_size", 10000L,
                    "normalized", false,
                    "build_time", 42.5,
                    "version", "2.1.0",
                    "checksum", new byte[]{0x1a, 0x2b, 0x3c, 0x4d, 0x5e, 0x6f, 0x70, (byte)0x81},
                    "description", "SIFT1M dataset with 128-dimensional vectors for nearest neighbor benchmarks"
                );
                break;
            default:
                throw new IllegalArgumentException("Unknown recordSize: " + recordSize);
        }

        // Pre-encode for decode benchmarks
        mnodeEncoded = mnode.toBytes();
    }

    // ==================== MNode Benchmarks ====================

    /// Benchmark MNode binary encoding throughput
    @Benchmark
    public void mnodeEncode(Blackhole bh) {
        bh.consume(mnode.toBytes());
    }

    /// Benchmark MNode binary decoding throughput
    @Benchmark
    public void mnodeDecode(Blackhole bh) {
        bh.consume(MNode.fromBytes(mnodeEncoded));
    }

    /// Benchmark MNode decode + accessor (simulates real read path)
    @Benchmark
    public void mnodeDecodeAccess(Blackhole bh) {
        MNode decoded = MNode.fromBytes(mnodeEncoded);
        bh.consume(decoded.getString("name"));
        bh.consume(decoded.getLong("dims"));
    }

    /// Runs the benchmark from JUnit (tagged as performance, excluded from default suite)
    @Test
    @Tag("performance")
    public void runBenchmark() throws RunnerException {
        Options opt = new OptionsBuilder()
            .include(MetadataFormatBenchmark.class.getSimpleName())
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
            .include(MetadataFormatBenchmark.class.getSimpleName())
            .forks(1)
            .build();

        new Runner(opt).run();
    }
}
