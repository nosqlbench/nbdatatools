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

package io.nosqlbench.slabtastic;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/// JMH benchmarks for slabtastic read and write performance.
///
/// ## Benchmark Categories
///
/// - **Sequential Write**: Measures write throughput for appending records
/// - **Random Read**: Measures latency of random-access reads by ordinal
/// - **Sequential Read**: Measures throughput of reading records in order
/// - **Page Assembly**: Measures the cost of building and serializing pages
///
/// ## Running the Benchmarks
///
/// ```bash
/// mvn test-compile exec:java \
///     -Dexec.mainClass="io.nosqlbench.slabtastic.SlabJmhBenchmark" \
///     -Dexec.classpathScope=test -pl datatools-slabtastic
/// ```
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
@Tag("performance")
public class SlabJmhBenchmark implements SlabConstants {

    @Param({"512", "4096", "65536"})
    private int pageSize;

    @Param({"64", "256", "1024"})
    private int recordSize;

    private Path benchDir;
    private Path readFile;
    private SlabReader reader;
    private int totalRecords;
    private byte[][] testPayloads;
    private Random rng;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        benchDir = Files.createTempDirectory("slab-bench");
        totalRecords = 10_000;
        rng = new Random(42);

        // Pre-generate payloads
        testPayloads = new byte[totalRecords][];
        for (int i = 0; i < totalRecords; i++) {
            testPayloads[i] = new byte[recordSize];
            rng.nextBytes(testPayloads[i]);
        }

        // Write the read-benchmark file
        readFile = benchDir.resolve("read-bench.slab");
        try (SlabWriter writer = new SlabWriter(readFile, pageSize)) {
            for (int i = 0; i < totalRecords; i++) {
                writer.write(i, testPayloads[i]);
            }
        }
        reader = new SlabReader(readFile);
        rng = new Random(42); // reset for deterministic access patterns
    }

    @TearDown(Level.Trial)
    public void teardown() throws IOException {
        if (reader != null) reader.close();
        // Clean up temp files
        try (var stream = Files.walk(benchDir)) {
            stream.sorted(java.util.Comparator.reverseOrder())
                .forEach(p -> {
                    try { Files.deleteIfExists(p); } catch (IOException ignored) {}
                });
        }
    }

    /// Measures random-access read throughput by ordinal.
    @Benchmark
    public void randomRead(Blackhole bh) {
        long ordinal = rng.nextInt(totalRecords);
        Optional<ByteBuffer> result = reader.get(ordinal);
        bh.consume(result);
    }

    /// Measures sequential read throughput over all records.
    @Benchmark
    public void sequentialRead(Blackhole bh) {
        for (int i = 0; i < 100; i++) {
            Optional<ByteBuffer> result = reader.get(i);
            bh.consume(result);
        }
    }

    /// Measures page assembly and serialization throughput.
    @Benchmark
    public void pageAssembly(Blackhole bh) {
        java.util.List<byte[]> records = new java.util.ArrayList<>();
        int accumulated = 0;
        for (int i = 0; i < 10 && i < testPayloads.length; i++) {
            records.add(testPayloads[i]);
            accumulated += testPayloads[i].length;
            int needed = HEADER_SIZE + accumulated + (records.size() + 1) * OFFSET_ENTRY_SIZE + FOOTER_V1_SIZE;
            if (((needed + PAGE_ALIGNMENT - 1) / PAGE_ALIGNMENT) * PAGE_ALIGNMENT > pageSize) {
                break;
            }
        }
        if (!records.isEmpty()) {
            SlabPage page = new SlabPage(0L, PAGE_TYPE_DATA, records);
            ByteBuffer buf = page.toByteBuffer();
            bh.consume(buf);
        }
    }

    /// Measures sequential write throughput (writes 1000 records per invocation).
    @Benchmark
    public void sequentialWrite(Blackhole bh) throws IOException {
        Path file = benchDir.resolve("write-bench-" + Thread.currentThread().getId() + ".slab");
        try (SlabWriter writer = new SlabWriter(file, pageSize)) {
            for (int i = 0; i < 1000; i++) {
                writer.write(i, testPayloads[i % testPayloads.length]);
            }
        }
        bh.consume(file);
        Files.deleteIfExists(file);
    }

    /// JUnit entry point for running benchmarks from mvn test.
    /// Tagged as "performance" so it is excluded from normal test runs.
    @Test
    @Tag("performance")
    void runBenchmarks() throws RunnerException {
        Options opts = new OptionsBuilder()
            .include(SlabJmhBenchmark.class.getSimpleName())
            .resultFormat(ResultFormatType.JSON)
            .result("target/slab-jmh-results.json")
            .build();
        new Runner(opts).run();
    }

    /// Standalone entry point.
    ///
    /// @param args command-line arguments (passed to JMH)
    /// @throws RunnerException if the benchmark fails
    public static void main(String[] args) throws RunnerException {
        Options opts = new OptionsBuilder()
            .include(SlabJmhBenchmark.class.getSimpleName())
            .resultFormat(ResultFormatType.JSON)
            .result("target/slab-jmh-results.json")
            .build();
        new Runner(opts).run();
    }
}
