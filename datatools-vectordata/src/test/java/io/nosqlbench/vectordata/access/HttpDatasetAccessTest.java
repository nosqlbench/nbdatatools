package io.nosqlbench.vectordata.access;

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
import io.nosqlbench.vectordata.layoutv2.DSInterval;
import io.nosqlbench.vectordata.layoutv2.DSWindow;
import io.nosqlbench.vectordata.merklev2.MAFileChannel;
import io.nosqlbench.vectordata.merklev2.MerkleDataImpl;
import io.nosqlbench.vectordata.merklev2.MerkleRef;
import io.nosqlbench.vectordata.merklev2.MerkleShape;
import io.nosqlbench.vectordata.merklev2.MerkleState;
import io.nosqlbench.vectordata.spec.datasets.impl.xvec.BaseVectorsXvecImpl;
import io.nosqlbench.vectordata.spec.datasets.impl.xvec.NeighborIndicesXvecImpl;
import io.nosqlbench.vectordata.spec.datasets.impl.xvec.QueryVectorsXvecImpl;
import io.nosqlbench.vectordata.util.TestFixturePaths;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.BitSet;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/// Integration tests for loading and accessing vector datasets over HTTP
/// via merkle-verified cached channels.
///
/// Mirrors test scenarios from vectordata-rs `http_access.rs` and `data_access.rs`.
/// Tests HTTP-based vector reading, windowed cached access, and parallel prebuffering.
@ExtendWith(JettyFileServerExtension.class)
class HttpDatasetAccessTest {

    @TempDir
    Path tempDir;

    /// Holds all the paths and objects needed for a single test setup.
    private static class SetupResult {
        final URL url;
        final Path cachePath;
        final Path statePath;
        final Path serverFile;
        final MerkleDataImpl merkleRef;
        final long fileSize;

        SetupResult(URL url, Path cachePath, Path statePath, Path serverFile,
                    MerkleDataImpl merkleRef, long fileSize) {
            this.url = url;
            this.cachePath = cachePath;
            this.statePath = statePath;
            this.serverFile = serverFile;
            this.merkleRef = merkleRef;
            this.fileSize = fileSize;
        }
    }

    /// Creates a vector file on the server and prepares all merkle/cache paths.
    private SetupResult setupFvec(TestInfo testInfo, String filename, int dim, int count) throws Exception {
        Path testDir = TestFixturePaths.createTestSpecificTempDir(testInfo);
        Path serverFile = TestVectorFileHelper.createFvec(testDir, filename, dim, count);
        return finishSetup(testInfo, filename, serverFile);
    }

    /// Creates an ivec file on the server and prepares all merkle/cache paths.
    private SetupResult setupIvec(TestInfo testInfo, String filename, int dim, int count) throws Exception {
        Path testDir = TestFixturePaths.createTestSpecificTempDir(testInfo);
        Path serverFile = TestVectorFileHelper.createIvec(testDir, filename, dim, count);
        return finishSetup(testInfo, filename, serverFile);
    }

    /// Completes setup by generating merkle ref/state and building cache paths.
    private SetupResult finishSetup(TestInfo testInfo, String filename, Path serverFile) throws Exception {
        long fileSize = Files.size(serverFile);

        MerkleDataImpl merkleRef = (MerkleDataImpl) MerkleRef.fromDataSimple(serverFile).get();
        Path mrefPath = serverFile.resolveSibling(serverFile.getFileName() + ".mref");
        merkleRef.save(mrefPath);

        URL url = TestFixturePaths.createTestSpecificServerUrl(testInfo, filename);

        String cacheFilename = TestFixturePaths.createTestSpecificFilename(testInfo, filename + ".cache");
        String stateFilename = TestFixturePaths.createTestSpecificFilename(testInfo, filename + ".mrkl");
        Path cachePath = tempDir.resolve(cacheFilename);
        Path statePath = tempDir.resolve(stateFilename);

        return new SetupResult(url, cachePath, statePath, serverFile, merkleRef, fileSize);
    }

    // ---- HTTP Dataset Loading ----

    /// Verifies reading float vectors from an HTTP-served fvec file via MAFileChannel.
    @Test
    void testHttpFvecReaderViaChannel(TestInfo testInfo) throws Exception {
        SetupResult s = setupFvec(testInfo, "base.fvec", 4, 10);

        try (MAFileChannel channel = new MAFileChannel(s.cachePath, s.statePath, s.url.toString())) {
            long sourceSize = channel.size();
            BaseVectorsXvecImpl baseVectors = new BaseVectorsXvecImpl(channel, sourceSize, DSWindow.ALL, "fvec");

            assertThat(baseVectors.getVectorDimensions()).isEqualTo(4);
            assertThat(baseVectors.getCount()).isEqualTo(10);

            float[] v0 = baseVectors.get(0);
            assertThat(v0).containsExactly(0f, 1f, 2f, 3f);

            float[] v9 = baseVectors.get(9);
            assertThat(v9).containsExactly(36f, 37f, 38f, 39f);
        }
    }

    /// Verifies reading integer neighbor indices from an HTTP-served ivec file via MAFileChannel.
    @Test
    void testHttpIvecReaderViaChannel(TestInfo testInfo) throws Exception {
        SetupResult s = setupIvec(testInfo, "neighbors.ivec", 3, 5);

        try (MAFileChannel channel = new MAFileChannel(s.cachePath, s.statePath, s.url.toString())) {
            long sourceSize = channel.size();
            NeighborIndicesXvecImpl neighbors = new NeighborIndicesXvecImpl(channel, sourceSize, DSWindow.ALL, "ivec");

            assertThat(neighbors.getVectorDimensions()).isEqualTo(3);
            assertThat(neighbors.getCount()).isEqualTo(5);

            int[] v0 = neighbors.get(0);
            assertThat(v0).containsExactly(0, 1, 2);

            int[] v4 = neighbors.get(4);
            assertThat(v4).containsExactly(12, 13, 14);
        }
    }

    /// Verifies that base vectors and query vectors can be opened and read independently
    /// from two separate HTTP-served fvec files.
    @Test
    void testHttpBaseAndQueryVectors(TestInfo testInfo) throws Exception {
        SetupResult sBase = setupFvec(testInfo, "base.fvec", 4, 10);
        SetupResult sQuery = setupFvec(testInfo, "query.fvec", 4, 5);

        try (MAFileChannel baseCh = new MAFileChannel(sBase.cachePath, sBase.statePath, sBase.url.toString());
             MAFileChannel queryCh = new MAFileChannel(sQuery.cachePath, sQuery.statePath, sQuery.url.toString())) {

            BaseVectorsXvecImpl baseVectors = new BaseVectorsXvecImpl(baseCh, baseCh.size(), DSWindow.ALL, "fvec");
            QueryVectorsXvecImpl queryVectors = new QueryVectorsXvecImpl(queryCh, queryCh.size(), DSWindow.ALL, "fvec");

            assertThat(baseVectors.getCount()).isEqualTo(10);
            assertThat(queryVectors.getCount()).isEqualTo(5);

            float[] base0 = baseVectors.get(0);
            assertThat(base0).containsExactly(0f, 1f, 2f, 3f);

            float[] query0 = queryVectors.get(0);
            assertThat(query0).containsExactly(0f, 1f, 2f, 3f);

            float[] query4 = queryVectors.get(4);
            assertThat(query4).containsExactly(16f, 17f, 18f, 19f);
        }
    }

    /// Verifies non-sequential (random) access to vectors via HTTP channel.
    @Test
    void testHttpRandomAccessPattern(TestInfo testInfo) throws Exception {
        SetupResult s = setupFvec(testInfo, "base.fvec", 4, 10);

        try (MAFileChannel channel = new MAFileChannel(s.cachePath, s.statePath, s.url.toString())) {
            BaseVectorsXvecImpl baseVectors = new BaseVectorsXvecImpl(channel, channel.size(), DSWindow.ALL, "fvec");

            int[] indices = {7, 2, 9, 0};
            for (int idx : indices) {
                float[] v = baseVectors.get(idx);
                float expectedStart = idx * 4f;
                assertThat(v).containsExactly(expectedStart, expectedStart + 1f, expectedStart + 2f, expectedStart + 3f);
            }
        }
    }

    // ---- Windowed Cached Access ----

    /// Verifies windowed access: a DSWindow of [100, 300) over a 1000-vector file
    /// should expose exactly 200 vectors, with logical index 0 mapping to physical index 100.
    @Test
    void testCachedVectorViewWindowed(TestInfo testInfo) throws Exception {
        SetupResult s = setupFvec(testInfo, "base.fvec", 4, 1000);

        DSWindow window = new DSWindow(Collections.singletonList(new DSInterval(100, 300)));

        try (MAFileChannel channel = new MAFileChannel(s.cachePath, s.statePath, s.url.toString())) {
            BaseVectorsXvecImpl baseVectors = new BaseVectorsXvecImpl(channel, channel.size(), window, "fvec");

            assertThat(baseVectors.getCount()).isEqualTo(200);

            // Logical index 0 -> physical index 100, values = [100*4, 100*4+1, ...]
            float[] v0 = baseVectors.get(0);
            assertThat(v0).containsExactly(400f, 401f, 402f, 403f);

            // Logical index 199 -> physical index 299, values = [299*4, 299*4+1, ...]
            float[] v199 = baseVectors.get(199);
            assertThat(v199).containsExactly(1196f, 1197f, 1198f, 1199f);
        }
    }

    /// Verifies partial fetch: accessing a single vector in a large file should not
    /// require downloading all chunks. The merkle state should show fewer valid chunks
    /// than the total number of chunks.
    @Test
    void testCachedVectorViewPartialFetch(TestInfo testInfo) throws Exception {
        // dim=128, count=10000 -> record size = 4 + 128*4 = 516 bytes, total ~5MB
        SetupResult s = setupFvec(testInfo, "large.fvec", 128, 10000);

        try (MAFileChannel channel = new MAFileChannel(s.cachePath, s.statePath, s.url.toString())) {
            BaseVectorsXvecImpl baseVectors = new BaseVectorsXvecImpl(channel, channel.size(), DSWindow.ALL, "fvec");

            // Access a single vector in the middle of the file
            float[] v5000 = baseVectors.get(5000);
            float expectedStart = 5000f * 128f;
            assertThat(v5000[0]).isEqualTo(expectedStart);
            assertThat(v5000[127]).isEqualTo(expectedStart + 127f);

            // Verify partial download: fewer valid chunks than total
            MerkleState state = MerkleState.load(s.statePath);
            MerkleShape shape = s.merkleRef.getShape();
            int validChunks = state.getValidChunks().cardinality();
            int totalChunks = shape.getLeafCount();
            assertThat(validChunks).isGreaterThan(0);
            assertThat(validChunks).isLessThan(totalChunks);
        }
    }

    // ---- Prebuffer and Promotion ----

    /// Verifies that prebuffering downloads all data, after which vector reads
    /// succeed from the local cache.
    @Test
    void testPrebufferThenReadVectors(TestInfo testInfo) throws Exception {
        SetupResult s = setupFvec(testInfo, "base.fvec", 4, 10);

        try (MAFileChannel channel = new MAFileChannel(s.cachePath, s.statePath, s.url.toString())) {
            // Prebuffer the entire file
            channel.prebuffer(0, channel.size()).get();

            // All chunks should now be valid
            MerkleState state = MerkleState.load(s.statePath);
            MerkleShape shape = s.merkleRef.getShape();
            assertThat(state.getValidChunks().cardinality()).isEqualTo(shape.getLeafCount());

            // Reads should work from cache
            BaseVectorsXvecImpl baseVectors = new BaseVectorsXvecImpl(channel, channel.size(), DSWindow.ALL, "fvec");
            assertThat(baseVectors.getCount()).isEqualTo(10);

            float[] v0 = baseVectors.get(0);
            assertThat(v0).containsExactly(0f, 1f, 2f, 3f);

            float[] v9 = baseVectors.get(9);
            assertThat(v9).containsExactly(36f, 37f, 38f, 39f);
        }
    }

    /// Verifies that calling prebuffer twice is idempotent: the second call should
    /// be a no-op and the merkle state should remain unchanged.
    @Test
    void testPrebufferIdempotent(TestInfo testInfo) throws Exception {
        SetupResult s = setupFvec(testInfo, "base.fvec", 4, 10);

        try (MAFileChannel channel = new MAFileChannel(s.cachePath, s.statePath, s.url.toString())) {
            long size = channel.size();

            // First prebuffer
            channel.prebuffer(0, size).get();

            MerkleState state1 = MerkleState.load(s.statePath);
            MerkleShape shape = s.merkleRef.getShape();
            BitSet valid1 = state1.getValidChunks();
            assertThat(valid1.cardinality()).isEqualTo(shape.getLeafCount());

            // Second prebuffer (should be a no-op)
            channel.prebuffer(0, size).get();

            MerkleState state2 = MerkleState.load(s.statePath);
            BitSet valid2 = state2.getValidChunks();
            assertThat(valid2.cardinality()).isEqualTo(shape.getLeafCount());
            assertThat(valid2).isEqualTo(valid1);
        }
    }
}
