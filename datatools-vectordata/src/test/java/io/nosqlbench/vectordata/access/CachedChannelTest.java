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
import io.nosqlbench.vectordata.merklev2.MAFileChannel;
import io.nosqlbench.vectordata.merklev2.MerkleDataImpl;
import io.nosqlbench.vectordata.merklev2.MerkleRef;
import io.nosqlbench.vectordata.merklev2.MerkleShape;
import io.nosqlbench.vectordata.merklev2.MerkleState;
import io.nosqlbench.vectordata.util.TestFixturePaths;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.BitSet;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

/// Integration tests for merkle-verified cached file channel (MAFileChannel).
///
/// Mirrors test scenarios from vectordata-rs `tests/cached_channel.rs`.
/// Tests on-demand chunk download, crash recovery, prebuffering, and integrity verification.
@ExtendWith(JettyFileServerExtension.class)
class CachedChannelTest {

    @TempDir
    Path tempDir;

    /// Creates a test content file and serves it via HTTP, returning the URL.
    private SetupResult setup(TestInfo testInfo, int size) throws Exception {
        byte[] content = new byte[size];
        for (int i = 0; i < size; i++) {
            content[i] = (byte) (i % 256);
        }

        String filename = TestFixturePaths.createTestSpecificFilename(testInfo, "data.bin");
        Path testDir = TestFixturePaths.createTestSpecificTempDir(testInfo);
        Path serverFile = testDir.resolve(filename);
        Files.write(serverFile, content);

        // Create merkle reference
        MerkleDataImpl merkleRef = (MerkleDataImpl) MerkleRef.fromDataSimple(serverFile).get();
        Path mrefPath = serverFile.resolveSibling(serverFile.getFileName() + ".mref");
        merkleRef.save(mrefPath);

        URL url = TestFixturePaths.createTestSpecificServerUrl(testInfo, filename);

        // Cache paths
        String cacheFilename = TestFixturePaths.createTestSpecificFilename(testInfo, "cache.dat");
        String stateFilename = TestFixturePaths.createTestSpecificFilename(testInfo, "state.mrkl");
        Path cachePath = tempDir.resolve(cacheFilename);
        Path statePath = tempDir.resolve(stateFilename);

        return new SetupResult(url, cachePath, statePath, content, merkleRef);
    }

    private static class SetupResult {
        final URL url;
        final Path cachePath;
        final Path statePath;
        final byte[] content;
        final MerkleDataImpl merkleRef;

        SetupResult(URL url, Path cachePath, Path statePath, byte[] content, MerkleDataImpl merkleRef) {
            this.url = url;
            this.cachePath = cachePath;
            this.statePath = statePath;
            this.content = content;
            this.merkleRef = merkleRef;
        }
    }

    /// Verifies cold-start on-demand read: first read triggers chunk download.
    @Test
    void testColdStartOnDemandRead(TestInfo testInfo) throws Exception {
        SetupResult s = setup(testInfo, 4096);

        try (MAFileChannel channel = new MAFileChannel(s.cachePath, s.statePath, s.url.toString())) {
            // Read first 16 bytes — triggers chunk 0 download
            ByteBuffer buf = ByteBuffer.allocate(16);
            Future<Integer> future = channel.read(buf, 0);
            int bytesRead = future.get();

            assertThat(bytesRead).isEqualTo(16);
            buf.flip();
            for (int i = 0; i < 16; i++) {
                assertThat(buf.get()).isEqualTo((byte) (i % 256));
            }

            // State should show at least one valid chunk
            MerkleState state = MerkleState.load(s.statePath);
            assertThat(state.getValidChunks().cardinality()).isGreaterThan(0);
        }
    }

    /// Verifies reading the entire file through the cached channel.
    @Test
    void testReadEntireFile(TestInfo testInfo) throws Exception {
        int size = 4096;
        SetupResult s = setup(testInfo, size);

        try (MAFileChannel channel = new MAFileChannel(s.cachePath, s.statePath, s.url.toString())) {
            ByteBuffer buf = ByteBuffer.allocate(size);
            // Read in chunks to cover all parts
            int totalRead = 0;
            while (totalRead < size) {
                int toRead = Math.min(1024, size - totalRead);
                ByteBuffer chunk = ByteBuffer.allocate(toRead);
                int bytesRead = channel.read(chunk, totalRead).get();
                assertThat(bytesRead).isEqualTo(toRead);
                chunk.flip();
                for (int i = 0; i < toRead; i++) {
                    assertThat(chunk.get()).isEqualTo((byte) ((totalRead + i) % 256));
                }
                totalRead += toRead;
            }

            // All chunks should be valid now
            MerkleState state = MerkleState.load(s.statePath);
            MerkleShape shape = s.merkleRef.getShape();
            assertThat(state.getValidChunks().cardinality()).isEqualTo(shape.getLeafCount());
        }
    }

    /// Verifies prebuffer downloads all chunks.
    @Test
    void testPrebufferDownloadsAll(TestInfo testInfo) throws Exception {
        int size = 4096;
        SetupResult s = setup(testInfo, size);

        try (MAFileChannel channel = new MAFileChannel(s.cachePath, s.statePath, s.url.toString())) {
            // Prebuffer entire file
            channel.prebuffer(0, size).get();

            // All chunks should be valid
            MerkleState state = MerkleState.load(s.statePath);
            MerkleShape shape = s.merkleRef.getShape();
            assertThat(state.getValidChunks().cardinality()).isEqualTo(shape.getLeafCount());

            // Subsequent reads should work from cache (no network)
            ByteBuffer buf = ByteBuffer.allocate(16);
            int bytesRead = channel.read(buf, 0).get();
            assertThat(bytesRead).isEqualTo(16);
            buf.flip();
            assertThat(buf.get(0)).isEqualTo((byte) 0);
        }
    }

    /// Verifies partial read followed by prebuffer only downloads remaining chunks.
    @Test
    void testPrebufferPartialThenComplete(TestInfo testInfo) throws Exception {
        int size = 4096;
        SetupResult s = setup(testInfo, size);

        try (MAFileChannel channel = new MAFileChannel(s.cachePath, s.statePath, s.url.toString())) {
            // Read just the first 100 bytes (downloads chunk 0)
            ByteBuffer buf = ByteBuffer.allocate(100);
            channel.read(buf, 0).get();

            MerkleState stateAfterRead = MerkleState.load(s.statePath);
            int validAfterRead = stateAfterRead.getValidChunks().cardinality();
            assertThat(validAfterRead).isGreaterThan(0);

            // Prebuffer the rest
            channel.prebuffer(0, size).get();

            MerkleState stateAfterPrebuffer = MerkleState.load(s.statePath);
            MerkleShape shape = s.merkleRef.getShape();
            assertThat(stateAfterPrebuffer.getValidChunks().cardinality()).isEqualTo(shape.getLeafCount());
        }
    }

    /// Verifies crash recovery: .mrkl state persists across channel reopens.
    @Test
    void testResumeAfterCrash(TestInfo testInfo) throws Exception {
        int size = 4096;
        SetupResult s = setup(testInfo, size);

        // Session 1: Read some data, then "crash" (close channel)
        try (MAFileChannel channel = new MAFileChannel(s.cachePath, s.statePath, s.url.toString())) {
            ByteBuffer buf = ByteBuffer.allocate(100);
            channel.read(buf, 0).get();
        }

        // Check state persisted
        MerkleState stateAfterCrash = MerkleState.load(s.statePath);
        int validAfterCrash = stateAfterCrash.getValidChunks().cardinality();
        assertThat(validAfterCrash).isGreaterThan(0);

        // Session 2: Reopen and continue
        try (MAFileChannel channel = new MAFileChannel(s.cachePath, s.statePath, s.url.toString())) {
            // Read from already-cached region should work without network
            ByteBuffer buf = ByteBuffer.allocate(16);
            int bytesRead = channel.read(buf, 0).get();
            assertThat(bytesRead).isEqualTo(16);
            buf.flip();
            assertThat(buf.get(0)).isEqualTo((byte) 0);

            // Prebuffer remaining
            channel.prebuffer(0, size).get();

            MerkleState finalState = MerkleState.load(s.statePath);
            MerkleShape shape = s.merkleRef.getShape();
            assertThat(finalState.getValidChunks().cardinality()).isEqualTo(shape.getLeafCount());
        }
    }

    /// Verifies that a second prebuffer after completion is a no-op.
    @Test
    void testAlreadyCompletePrebufferIsNoop(TestInfo testInfo) throws Exception {
        int size = 4096;
        SetupResult s = setup(testInfo, size);

        try (MAFileChannel channel = new MAFileChannel(s.cachePath, s.statePath, s.url.toString())) {
            channel.prebuffer(0, size).get();

            MerkleState state1 = MerkleState.load(s.statePath);
            MerkleShape shape = s.merkleRef.getShape();
            assertThat(state1.getValidChunks().cardinality()).isEqualTo(shape.getLeafCount());

            // Second prebuffer should be a no-op
            channel.prebuffer(0, size).get();

            MerkleState state2 = MerkleState.load(s.statePath);
            assertThat(state2.getValidChunks().cardinality()).isEqualTo(shape.getLeafCount());
        }
    }
}
