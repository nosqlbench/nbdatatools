package io.nosqlbench.vectordata.spec.datasets.impl.xvec;

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

import io.nosqlbench.jetty.testserver.JettyFileServerExtension;
import io.nosqlbench.vectordata.downloader.DatasetEntry;
import io.nosqlbench.vectordata.downloader.VirtualVectorTestDataView;
import io.nosqlbench.vectordata.layoutv2.DSProfile;
import io.nosqlbench.vectordata.layoutv2.DSProfileGroup;
import io.nosqlbench.vectordata.layoutv2.DSSource;
import io.nosqlbench.vectordata.layoutv2.DSView;
import io.nosqlbench.vectordata.merklev2.MerkleDataImpl;
import io.nosqlbench.vectordata.merklev2.MerkleRef;
import io.nosqlbench.vectordata.spec.datasets.types.BaseVectors;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/// JMH benchmark measuring concurrent read throughput across three read paths
/// at varying thread counts (10, 50, 200 virtual-thread-equivalent JMH threads).
///
/// All threads share the same view instances (`Scope.Benchmark`) to measure
/// contention and scalability of:
/// 1. **directPath** — pure mmap via `CoreXVecVectorDatasetViewMethods(Path, ...)`
/// 2. **maChannelCached** — `MAFileChannel` synchronous fast-path after `prebuffer()`
/// 3. **promotedVirtual** — view-level promoted mmap after `prebuffer()`
///
/// Uses `@Fork(0)` so the benchmark runs in the same JVM as the Jetty test server.
/// Thread counts are swept via the JMH runner API in {@link #runBenchmark()}.
@ExtendWith(JettyFileServerExtension.class)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 2)
@Fork(0)
public class ConcurrentReadPathBenchmark {

    private static final int DIMENSIONS = 64;
    private static final int VECTORS = 10_000;

    /// Shared state across all benchmark threads.
    @State(Scope.Benchmark)
    public static class SharedViews {
        CoreXVecVectorDatasetViewMethods<float[]> directPathView;
        BaseVectors maChannelView;
        BaseVectors promotedVirtualView;

        @Setup(Level.Trial)
        public void setup() throws Exception {
            Path tempDir = Files.createTempDirectory("concurrent_bench");
            Path sourceFile = createFvecFile(tempDir, "concurrent.fvecs", DIMENSIONS, VECTORS);

            // Direct path — pure mmap
            directPathView = new CoreXVecVectorDatasetViewMethods<>(sourceFile, null, "fvecs");

            // MAFileChannel cached — prebuffered but NOT promoted
            VirtualVectorTestDataView maView = createVirtualView(tempDir, sourceFile, "ma");
            maChannelView = maView.getBaseVectors()
                .orElseThrow(() -> new RuntimeException("Expected base vectors for MAFileChannel path"));
            maChannelView.prebuffer().get();

            // Promoted virtual — prebuffered AND promoted
            VirtualVectorTestDataView promotedView = createVirtualView(tempDir, sourceFile, "prom");
            promotedView.prebuffer().get();
            promotedVirtualView = promotedView.getBaseVectors()
                .orElseThrow(() -> new RuntimeException("Expected base vectors for promoted path"));
        }
    }

    /// Per-thread ordinal counter so threads access different vector indices.
    @State(Scope.Thread)
    public static class ThreadState {
        long ordinal;

        @Setup(Level.Iteration)
        public void reset() {
            ordinal = Thread.currentThread().threadId() * 7919L;
        }
    }

    // ==================== Single-read benchmarks ====================

    @Benchmark
    public void directPath_singleRead(SharedViews views, ThreadState ts, Blackhole bh) {
        bh.consume(views.directPathView.get(ts.ordinal % VECTORS));
        ts.ordinal++;
    }

    @Benchmark
    public void maChannelCached_singleRead(SharedViews views, ThreadState ts, Blackhole bh) {
        bh.consume(views.maChannelView.get(ts.ordinal % VECTORS));
        ts.ordinal++;
    }

    @Benchmark
    public void promotedVirtual_singleRead(SharedViews views, ThreadState ts, Blackhole bh) {
        bh.consume(views.promotedVirtualView.get(ts.ordinal % VECTORS));
        ts.ordinal++;
    }

    // ==================== Range-read benchmarks ====================

    @Benchmark
    public void directPath_rangeRead100(SharedViews views, ThreadState ts, Blackhole bh) {
        long start = ts.ordinal % (VECTORS - 100);
        bh.consume(views.directPathView.getRange(start, start + 100));
        ts.ordinal += 100;
    }

    @Benchmark
    public void maChannelCached_rangeRead100(SharedViews views, ThreadState ts, Blackhole bh) {
        long start = ts.ordinal % (VECTORS - 100);
        bh.consume(views.maChannelView.getRange(start, start + 100));
        ts.ordinal += 100;
    }

    @Benchmark
    public void promotedVirtual_rangeRead100(SharedViews views, ThreadState ts, Blackhole bh) {
        long start = ts.ordinal % (VECTORS - 100);
        bh.consume(views.promotedVirtualView.getRange(start, start + 100));
        ts.ordinal += 100;
    }

    // ==================== Runner ====================

    /// Runs the concurrent benchmark at 10, 50, and 200 threads.
    ///
    /// Each thread count is a separate JMH run so results are clearly labeled.
    /// Enable with: `mvn test -pl datatools-vectordata -Dtest=ConcurrentReadPathBenchmark#runBenchmark`
    @Test
    @Tag("performance")
    void runBenchmark() throws RunnerException {
        for (int threads : new int[]{10, 50, 200}) {
            System.out.printf("%n========== %d threads ==========%n%n", threads);

            Options opt = new OptionsBuilder()
                .include(ConcurrentReadPathBenchmark.class.getSimpleName())
                .threads(threads)
                .warmupIterations(2)
                .measurementIterations(3)
                .forks(0)
                .resultFormat(ResultFormatType.TEXT)
                .result("jmh-concurrent-" + threads + "t.text")
                .build();

            new Runner(opt).run();
        }
    }

    // ==================== Helpers ====================

    /// Creates a catalog-derived {@link VirtualVectorTestDataView} backed by
    /// the given source file served via the Jetty test server.
    ///
    /// @param tempDir    working directory for cache files
    /// @param sourceFile the local fvec source file
    /// @param tag        unique tag for server path isolation
    /// @return a VirtualVectorTestDataView ready for prebuffering
    private static VirtualVectorTestDataView createVirtualView(Path tempDir, Path sourceFile, String tag)
        throws Exception
    {
        String uniqueId = tag + "_" + System.nanoTime();
        Path serverDir = JettyFileServerExtension.TEMP_RESOURCES_ROOT.resolve(uniqueId);
        Files.createDirectories(serverDir);
        Path serverFile = serverDir.resolve(sourceFile.getFileName().toString());
        Files.copy(sourceFile, serverFile);

        MerkleDataImpl merkleRef = (MerkleDataImpl) MerkleRef.fromDataSimple(serverFile).get();
        Path mrefPath = serverFile.resolveSibling(serverFile.getFileName() + ".mref");
        merkleRef.save(mrefPath);

        URL baseUrl = JettyFileServerExtension.getBaseUrl();

        DSSource source = new DSSource(sourceFile.getFileName().toString());
        DSView dsView = new DSView("base_vectors", source, null);

        DSProfile profile = new DSProfile();
        profile.setName("test");
        profile.put("base_vectors", dsView);

        DSProfileGroup profileGroup = new DSProfileGroup();
        profileGroup.put("test", profile);

        DatasetEntry entry = new DatasetEntry(
            "bench-" + uniqueId,
            new URL(baseUrl + "temp/" + uniqueId + "/"),
            Map.of(),
            profileGroup,
            null
        );

        Path cacheRoot = tempDir.resolve("cache_" + uniqueId);
        Files.createDirectories(cacheRoot);

        return new VirtualVectorTestDataView(cacheRoot, entry, profile);
    }

    /// Creates an fvec file with the given dimensions and vector count.
    ///
    /// Each record: 4-byte LE dimension header + dimensions * 4-byte LE floats.
    /// Uses a fixed random seed for reproducibility.
    ///
    /// @param dir        directory to create the file in
    /// @param name       filename
    /// @param dimensions number of float components per vector
    /// @param count      number of vectors
    /// @return path to the created file
    private static Path createFvecFile(Path dir, String name, int dimensions, int count)
        throws IOException
    {
        Path file = dir.resolve(name);
        int recordBytes = 4 + dimensions * Float.BYTES;
        ByteBuffer buf = ByteBuffer.allocate(recordBytes * count);
        buf.order(ByteOrder.LITTLE_ENDIAN);

        Random rng = new Random(42);
        for (int v = 0; v < count; v++) {
            buf.putInt(dimensions);
            for (int d = 0; d < dimensions; d++) {
                buf.putFloat(rng.nextFloat() * 2.0f - 1.0f);
            }
        }
        buf.flip();
        Files.write(file, buf.array());
        return file;
    }
}
