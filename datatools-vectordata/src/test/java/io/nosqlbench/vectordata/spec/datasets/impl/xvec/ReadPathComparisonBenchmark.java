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
import io.nosqlbench.vectordata.merklev2.MAFileChannel;
import io.nosqlbench.vectordata.merklev2.MerkleDataImpl;
import io.nosqlbench.vectordata.merklev2.MerkleRef;
import io.nosqlbench.vectordata.spec.datasets.types.BaseVectors;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
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

import static org.junit.jupiter.api.Assertions.*;

/// JMH+JFR benchmark comparing three read paths on the same data.
///
/// 1. **directPath** — `CoreXVecVectorDatasetViewMethods(Path, ...)` — pure mmap,
///    what {@link io.nosqlbench.vectordata.discovery.vector.FilesystemVectorTestDataView} uses
/// 2. **maChannelCached** — `CoreXVecVectorDatasetViewMethods(MAFileChannel, ...)` where
///    all chunks are pre-cached via `prebuffer()` — MAFileChannel channel-read fast-path
/// 3. **promotedVirtual** — `VirtualVectorTestDataView` after `prebuffer()` +
///    view-level promotion — should match directPath performance
///
/// Uses `@Fork(0)` so the benchmark runs in the same JVM as the Jetty test server,
/// allowing the MAFileChannel path to use the catalog-derived `prebuffer()` flow.
@ExtendWith(JettyFileServerExtension.class)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(0)
public class ReadPathComparisonBenchmark {

  private static final int DIMENSIONS = 64;
  private static final int VECTORS = 10_000;

  // JMH state
  private CoreXVecVectorDatasetViewMethods<float[]> directPathView;
  private BaseVectors maChannelView;
  private BaseVectors promotedVirtualView;
  private long ordinal;

  // ==================== JUnit Integration Test ====================

  @Test
  void testPromotedVirtualMatchesDirectPath(@TempDir Path tempDir) throws Exception {
    Path sourceFile = createFvecFile(tempDir, "compare_test.fvecs", DIMENSIONS, VECTORS);

    // 1. Direct path view (pure mmap)
    CoreXVecVectorDatasetViewMethods<float[]> directView =
        new CoreXVecVectorDatasetViewMethods<>(sourceFile, null, "fvecs");

    // 2. Create catalog-derived VirtualVectorTestDataView and prebuffer
    VirtualVectorTestDataView virtualView = createVirtualView(tempDir, sourceFile, "cmp");

    // Prebuffer and promote
    virtualView.prebuffer().get();

    // After promotion, getBaseVectors() should return the promoted view
    BaseVectors promotedBase = virtualView.getBaseVectors()
        .orElseThrow(() -> new AssertionError("Expected promoted base vectors"));

    // Verify the promoted view uses mmap (channel should be null)
    if (promotedBase instanceof BaseVectorsXvecImpl impl) {
      assertNull(impl.getChannel(),
          "Promoted view should use Path-based mmap (channel == null)");
    }

    // Verify data matches between direct path and promoted virtual
    assertEquals(directView.getCount(), promotedBase.getCount(),
        "Vector count should match");
    assertEquals(directView.getVectorDimensions(), promotedBase.getVectorDimensions(),
        "Dimensions should match");

    // Spot-check vectors
    for (int i : new int[]{0, VECTORS / 2, VECTORS - 1}) {
      float[] directVec = directView.get(i);
      float[] promotedVec = promotedBase.get(i);
      assertArrayEquals(directVec, promotedVec, 1e-6f,
          "Vector " + i + " should match between direct and promoted paths");
    }

    // Spot-check range read
    float[][] directRange = directView.getRange(0, 100);
    float[][] promotedRange = promotedBase.getRange(0, 100);
    assertEquals(directRange.length, promotedRange.length);
    for (int i = 0; i < directRange.length; i++) {
      assertArrayEquals(directRange[i], promotedRange[i], 1e-6f,
          "Range vector " + i + " should match");
    }

    System.out.println("=== Read Path Comparison Test ===");
    System.out.printf("Direct path and promoted virtual views produce identical results%n");
    System.out.printf("Vectors: %d, Dimensions: %d%n", VECTORS, DIMENSIONS);
  }

  // ==================== JMH Benchmark ====================

  /// Sets up all three read paths for benchmarking.
  ///
  /// Creates an fvec file, serves it via the Jetty test server, and sets up:
  /// - Direct path: pure mmap via `CoreXVecVectorDatasetViewMethods(Path, ...)`
  /// - MAFileChannel cached: catalog-derived `VirtualVectorTestDataView` after `prebuffer()`,
  ///   but before view-level promotion (channel reads only)
  /// - Promoted virtual: catalog-derived `VirtualVectorTestDataView` after `prebuffer()` +
  ///   view-level promotion (Path-based mmap)
  @Setup(Level.Trial)
  public void setupBenchmark() throws Exception {
    Path tempDir = Files.createTempDirectory("read_path_bench");
    Path sourceFile = createFvecFile(tempDir, "bench.fvecs", DIMENSIONS, VECTORS);

    // Direct path — pure mmap
    directPathView = new CoreXVecVectorDatasetViewMethods<>(sourceFile, null, "fvecs");

    // MAFileChannel cached — catalog-derived, prebuffered but NOT promoted.
    // Get the base vectors before prebuffering to capture the MAFileChannel-backed view.
    VirtualVectorTestDataView maView = createVirtualView(tempDir, sourceFile, "ma");
    maChannelView = maView.getBaseVectors()
        .orElseThrow(() -> new RuntimeException("Expected base vectors for MAFileChannel path"));
    // Prebuffer to populate the cache — but since we already hold the pre-promotion
    // view reference, reads will still go through MAFileChannel
    maChannelView.prebuffer().get();

    // Promoted virtual — catalog-derived, prebuffered AND promoted.
    VirtualVectorTestDataView promotedView = createVirtualView(tempDir, sourceFile, "prom");
    promotedView.prebuffer().get();
    promotedVirtualView = promotedView.getBaseVectors()
        .orElseThrow(() -> new RuntimeException("Expected base vectors for promoted path"));

    ordinal = 0;
  }

  @Benchmark
  public void directPath_singleRead(Blackhole bh) {
    bh.consume(directPathView.get(ordinal % VECTORS));
    ordinal++;
  }

  @Benchmark
  public void maChannelCached_singleRead(Blackhole bh) {
    bh.consume(maChannelView.get(ordinal % VECTORS));
    ordinal++;
  }

  @Benchmark
  public void promotedVirtual_singleRead(Blackhole bh) {
    bh.consume(promotedVirtualView.get(ordinal % VECTORS));
    ordinal++;
  }

  @Benchmark
  public void directPath_rangeRead100(Blackhole bh) {
    long start = ordinal % (VECTORS - 100);
    bh.consume(directPathView.getRange(start, start + 100));
    ordinal += 100;
  }

  @Benchmark
  public void maChannelCached_rangeRead100(Blackhole bh) {
    long start = ordinal % (VECTORS - 100);
    bh.consume(maChannelView.getRange(start, start + 100));
    ordinal += 100;
  }

  @Benchmark
  public void promotedVirtual_rangeRead100(Blackhole bh) {
    long start = ordinal % (VECTORS - 100);
    bh.consume(promotedVirtualView.getRange(start, start + 100));
    ordinal += 100;
  }

  @Benchmark
  public void directPath_rangeRead1000(Blackhole bh) {
    long start = ordinal % (VECTORS - 1000);
    bh.consume(directPathView.getRange(start, start + 1000));
    ordinal += 1000;
  }

  @Benchmark
  public void maChannelCached_rangeRead1000(Blackhole bh) {
    long start = ordinal % (VECTORS - 1000);
    bh.consume(maChannelView.getRange(start, start + 1000));
    ordinal += 1000;
  }

  @Benchmark
  public void promotedVirtual_rangeRead1000(Blackhole bh) {
    long start = ordinal % (VECTORS - 1000);
    bh.consume(promotedVirtualView.getRange(start, start + 1000));
    ordinal += 1000;
  }

  /// Runs the JMH benchmark from JUnit with JFR-compatible flags.
  ///
  /// The Jetty server extension is active in this JVM, so `@Fork(0)` keeps
  /// the benchmark in-process where the MAFileChannel can reach the server.
  ///
  /// Enable with: `mvn test -pl datatools-vectordata -Dtest=ReadPathComparisonBenchmark#runBenchmark`
  @Test
  @Tag("performance")
  void runBenchmark() throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(ReadPathComparisonBenchmark.class.getSimpleName())
        .warmupIterations(2)
        .measurementIterations(3)
        .forks(0)
        .resultFormat(ResultFormatType.TEXT)
        .build();

    new Runner(opt).run();
  }

  // ==================== Helpers ====================

  /// Creates a catalog-derived {@link VirtualVectorTestDataView} backed by
  /// the given source file served via the Jetty test server.
  ///
  /// @param tempDir  working directory for cache files
  /// @param sourceFile  the local fvec source file
  /// @param tag  unique tag for server path isolation
  /// @return a VirtualVectorTestDataView ready for prebuffering
  private VirtualVectorTestDataView createVirtualView(Path tempDir, Path sourceFile, String tag)
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
