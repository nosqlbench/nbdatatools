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

/// Integration test verifying the full remote-fetch to view-level promoted mmap read path.
///
/// This test confirms that:
/// 1. A dataset can be fetched from a remote HTTP server via {@link MAFileChannel}
/// 2. After prebuffering, the cache is fully valid
/// 3. {@link VirtualVectorTestDataView} promotes from MAFileChannel-backed views
///    to Path-based memory-mapped views via {@code promoteToLocalViews()}
/// 4. Post-promotion reads return correct data with {@code channel == null}
/// 5. The promoted read path achieves high throughput (verified via JMH)
///
/// The JMH benchmark targets the final-stage reading after promotion and uses
/// JFR-compatible JVM flags for detailed profiling.
@ExtendWith(JettyFileServerExtension.class)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(value = 1, jvmArgs = {
    "-XX:+UnlockDiagnosticVMOptions",
    "-XX:+DebugNonSafepoints"
})
public class MmapAutoPromotionIntegrationTest {

    private static final int DIMENSIONS = 64;
    private static final int VECTORS = 10_000;
    private static final int RECORD_SIZE = 4 + DIMENSIONS * Float.BYTES;

    /// JMH state: a view that has been promoted to mmap after prebuffering.
    private CoreXVecVectorDatasetViewMethods<float[]> promotedView;
    private long ordinal;

    // ==================== JUnit Integration Tests ====================

    @Test
    void testViewLevelPromotionThroughVirtualView(@TempDir Path tempDir) throws Exception {
        // -- Step 1: Generate test fvec data and serve it via HTTP --
        Path sourceFile = createFvecFile(tempDir, "promote_test.fvecs", DIMENSIONS, VECTORS);
        String uniqueId = String.valueOf(System.nanoTime());
        Path serverDir = JettyFileServerExtension.TEMP_RESOURCES_ROOT.resolve("promote_" + uniqueId);
        Files.createDirectories(serverDir);
        Path serverFile = serverDir.resolve("promote_test.fvecs");
        Files.copy(sourceFile, serverFile);

        // Create merkle reference for the server file
        MerkleDataImpl merkleRef = (MerkleDataImpl) MerkleRef.fromDataSimple(serverFile).get();
        Path mrefPath = serverFile.resolveSibling(serverFile.getFileName() + ".mref");
        merkleRef.save(mrefPath);

        URL baseUrl = JettyFileServerExtension.getBaseUrl();

        // -- Step 2: Set up VirtualVectorTestDataView with a minimal profile --
        DSSource source = new DSSource("promote_test.fvecs");
        DSView dsView = new DSView("base_vectors", source, null);

        DSProfile profile = new DSProfile();
        profile.setName("test");
        profile.put("base_vectors", dsView);

        DSProfileGroup profileGroup = new DSProfileGroup();
        profileGroup.put("test", profile);

        DatasetEntry entry = new DatasetEntry(
            "test-dataset",
            new URL(baseUrl + "temp/promote_" + uniqueId + "/"),
            Map.of(),
            profileGroup,
            null
        );

        Path cacheRoot = tempDir.resolve("vcache");
        Files.createDirectories(cacheRoot);

        VirtualVectorTestDataView virtualView =
            new VirtualVectorTestDataView(cacheRoot, entry, profile);

        // -- Step 3: Access base vectors (creates MAFileChannel-backed view) --
        BaseVectors baseVectors = virtualView.getBaseVectors()
            .orElseThrow(() -> new AssertionError("Expected base vectors"));

        assertEquals(DIMENSIONS, baseVectors.getVectorDimensions());
        assertEquals(VECTORS, baseVectors.getCount());

        // Before prebuffer, verify the view is channel-backed
        if (baseVectors instanceof BaseVectorsXvecImpl impl) {
            assertNotNull(impl.getChannel(),
                "Before prebuffer, view should be channel-backed");
        }

        // -- Step 4: Prebuffer all data (triggers promoteToLocalViews) --
        virtualView.prebuffer().get();

        // -- Step 5: After promotion, getBaseVectors() returns the promoted view --
        BaseVectors promoted = virtualView.getBaseVectors()
            .orElseThrow(() -> new AssertionError("Expected promoted base vectors"));

        if (promoted instanceof BaseVectorsXvecImpl impl) {
            assertNull(impl.getChannel(),
                "After promotion, view should use Path-based mmap (channel == null)");
        }

        // -- Step 6: Verify reads return correct data after view-level promotion --
        float[] vector0 = promoted.get(0);
        assertNotNull(vector0);
        assertEquals(DIMENSIONS, vector0.length);

        // Verify specific values match what we generated
        float[] expected0 = readExpectedVector(sourceFile, 0);
        assertArrayEquals(expected0, vector0, 1e-6f,
            "Vector 0 should match source data after view-level promotion");

        // Spot check a vector in the middle
        int midIndex = VECTORS / 2;
        float[] vectorMid = promoted.get(midIndex);
        float[] expectedMid = readExpectedVector(sourceFile, midIndex);
        assertArrayEquals(expectedMid, vectorMid, 1e-6f,
            "Vector at mid-point should match source data");

        // Spot check last vector
        float[] vectorLast = promoted.get(VECTORS - 1);
        float[] expectedLast = readExpectedVector(sourceFile, VECTORS - 1);
        assertArrayEquals(expectedLast, vectorLast, 1e-6f,
            "Last vector should match source data");

        // -- Step 7: Verify range reads --
        float[][] range = promoted.getRange(0, 100);
        assertEquals(100, range.length);
        for (int i = 0; i < 100; i++) {
            float[] expected = readExpectedVector(sourceFile, i);
            assertArrayEquals(expected, range[i], 1e-6f,
                "Range vector " + i + " should match source");
        }

        System.out.println("=== View-Level Promotion Integration Test ===");
        System.out.printf("Vectors: %d, Dimensions: %d%n", VECTORS, DIMENSIONS);
        System.out.println("Promoted view channel is null (mmap): " +
            (promoted instanceof BaseVectorsXvecImpl impl2 && impl2.getChannel() == null));
        System.out.println("All reads verified correct after view-level promotion");
    }

    // ==================== JMH Benchmark ====================

    /// Sets up the promoted view for JMH benchmarking.
    ///
    /// Creates an fvec file and opens it with the Path-based constructor
    /// to simulate the post-promotion state.
    @Setup(Level.Trial)
    public void setupBenchmark() throws Exception {
        Path tempDir = Files.createTempDirectory("mmap_bench");
        Path sourceFile = createFvecFile(tempDir, "bench.fvecs", DIMENSIONS, VECTORS);

        // For benchmarking, use the local file path directly since the JMH fork
        // won't have the Jetty server. This exercises the SegmentedMappedBuffer
        // read path which is the post-promotion path we want to measure.
        promotedView = new CoreXVecVectorDatasetViewMethods<>(
            sourceFile, null, "fvecs"
        );
        ordinal = 0;
    }

    @Benchmark
    public void singleVectorRead(Blackhole bh) {
        bh.consume(promotedView.get(ordinal % VECTORS));
        ordinal++;
    }

    @Benchmark
    public void batchRangeRead_100(Blackhole bh) {
        long start = ordinal % (VECTORS - 100);
        bh.consume(promotedView.getRange(start, start + 100));
        ordinal += 100;
    }

    @Benchmark
    public void batchRangeRead_1000(Blackhole bh) {
        long start = ordinal % (VECTORS - 1000);
        bh.consume(promotedView.getRange(start, start + 1000));
        ordinal += 1000;
    }

    /// Runs the JMH benchmark from JUnit with JFR-compatible flags.
    ///
    /// Enable with: `mvn test -pl datatools-vectordata -Dtest=MmapAutoPromotionIntegrationTest#runBenchmark`
    @Test
    @Tag("performance")
    void runBenchmark() throws RunnerException {
        Options opt = new OptionsBuilder()
            .include(MmapAutoPromotionIntegrationTest.class.getSimpleName())
            .warmupIterations(2)
            .measurementIterations(3)
            .forks(1)
            .jvmArgs(
                "-XX:+UnlockDiagnosticVMOptions",
                "-XX:+DebugNonSafepoints"
            )
            .resultFormat(ResultFormatType.TEXT)
            .build();

        new Runner(opt).run();
    }

    // ==================== Helpers ====================

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

    /// Reads a specific vector from the source fvec file for verification.
    ///
    /// @param fvecPath path to the fvec file
    /// @param index    vector index
    /// @return the float array at that index
    private static float[] readExpectedVector(Path fvecPath, int index) throws IOException {
        int recordBytes = 4 + DIMENSIONS * Float.BYTES;
        byte[] allBytes = Files.readAllBytes(fvecPath);
        ByteBuffer buf = ByteBuffer.wrap(allBytes).order(ByteOrder.LITTLE_ENDIAN);
        buf.position(index * recordBytes + 4); // skip dim header
        float[] result = new float[DIMENSIONS];
        buf.asFloatBuffer().get(result);
        return result;
    }
}
