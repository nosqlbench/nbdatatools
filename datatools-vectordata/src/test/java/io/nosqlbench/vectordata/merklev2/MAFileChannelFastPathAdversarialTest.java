package io.nosqlbench.vectordata.merklev2;

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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/// Adversarial tests for the MAFileChannel fast cached-read path.
///
/// Exercises the mmap promotion, volatile {@code fullyCached} flag,
/// concurrent reads, boundary conditions, and the transition from
/// uncached to fully-cached state under contention.
@ExtendWith(JettyFileServerExtension.class)
class MAFileChannelFastPathAdversarialTest {

    // ==================== Concurrent read correctness ====================

    /// Hammers a fully-cached MAFileChannel with 200 concurrent threads,
    /// each reading randomly-chosen byte ranges and verifying data integrity
    /// against the original source file.
    @Test
    void concurrentRandomReadsReturnCorrectData(@TempDir Path tempDir) throws Exception {
        int fileSize = 3 * 1024 * 1024; // 3 MB, multiple chunks
        byte[] sourceData = createTestData(fileSize);
        MAFileChannel channel = createPrebufferedChannel(tempDir, sourceData, "conc_rand");

        assertTrue(channel.isFullyCached(), "Channel should be fully cached after prebuffer");
        assertTrue(channel.isRangeCached(0, fileSize), "Full range should be cached");

        int threadCount = 200;
        CyclicBarrier barrier = new CyclicBarrier(threadCount);
        AtomicInteger failures = new AtomicInteger();
        Random seedRng = new Random(42);
        long[] seeds = new long[threadCount];
        for (int i = 0; i < threadCount; i++) seeds[i] = seedRng.nextLong();

        List<Thread> threads = new ArrayList<>();
        for (int t = 0; t < threadCount; t++) {
            final int threadIdx = t;
            Thread thread = Thread.ofVirtual().start(() -> {
                try {
                    barrier.await();
                    Random rng = new Random(seeds[threadIdx]);
                    for (int iter = 0; iter < 500; iter++) {
                        int readLen = rng.nextInt(1, 8192);
                        int pos = rng.nextInt(fileSize - readLen);
                        ByteBuffer buf = ByteBuffer.allocate(readLen);
                        Future<Integer> future = channel.read(buf, pos);
                        int bytesRead = future.get();
                        assertEquals(readLen, bytesRead,
                            "Thread " + threadIdx + " iter " + iter + ": expected full read");
                        buf.flip();
                        byte[] actual = new byte[bytesRead];
                        buf.get(actual);
                        byte[] expected = Arrays.copyOfRange(sourceData, pos, pos + readLen);
                        assertArrayEquals(expected, actual,
                            "Thread " + threadIdx + " iter " + iter +
                                " at pos=" + pos + " len=" + readLen + ": data mismatch");
                    }
                } catch (Throwable e) {
                    failures.incrementAndGet();
                    System.err.println("Thread " + threadIdx + " failed: " + e.getMessage());
                }
            });
            threads.add(thread);
        }

        for (Thread t : threads) t.join(30_000);
        assertEquals(0, failures.get(), "Some threads reported data mismatches or errors");
        channel.close();
    }

    /// Reads from many threads using the CompletionHandler-based read API.
    /// Ensures the fast path works correctly through that code path too.
    @Test
    void concurrentCompletionHandlerReadsReturnCorrectData(@TempDir Path tempDir) throws Exception {
        int fileSize = 2 * 1024 * 1024;
        byte[] sourceData = createTestData(fileSize);
        MAFileChannel channel = createPrebufferedChannel(tempDir, sourceData, "conc_handler");

        int threadCount = 50;
        CyclicBarrier barrier = new CyclicBarrier(threadCount);
        AtomicInteger failures = new AtomicInteger();

        List<Thread> threads = new ArrayList<>();
        for (int t = 0; t < threadCount; t++) {
            final int threadIdx = t;
            Thread thread = Thread.ofVirtual().start(() -> {
                try {
                    barrier.await();
                    Random rng = new Random(threadIdx * 31L);
                    for (int iter = 0; iter < 200; iter++) {
                        int readLen = rng.nextInt(1, 4096);
                        int pos = rng.nextInt(fileSize - readLen);
                        ByteBuffer buf = ByteBuffer.allocate(readLen);
                        CompletableFuture<Integer> result = new CompletableFuture<>();

                        channel.read(buf, pos, null,
                            new java.nio.channels.CompletionHandler<Integer, Void>() {
                                @Override
                                public void completed(Integer bytesRead, Void att) {
                                    result.complete(bytesRead);
                                }
                                @Override
                                public void failed(Throwable exc, Void att) {
                                    result.completeExceptionally(exc);
                                }
                            });

                        int bytesRead = result.get();
                        assertEquals(readLen, bytesRead);
                        buf.flip();
                        byte[] actual = new byte[bytesRead];
                        buf.get(actual);
                        byte[] expected = Arrays.copyOfRange(sourceData, pos, pos + readLen);
                        assertArrayEquals(expected, actual,
                            "CompletionHandler read mismatch at pos=" + pos);
                    }
                } catch (Throwable e) {
                    failures.incrementAndGet();
                    System.err.println("CompletionHandler thread " + threadIdx + " failed: " + e);
                }
            });
            threads.add(thread);
        }

        for (Thread t : threads) t.join(30_000);
        assertEquals(0, failures.get());
        channel.close();
    }

    // ==================== Boundary conditions ====================

    /// Reads exactly at chunk boundaries: first byte, last byte, and
    /// spans that cross chunk boundaries.
    @Test
    void readsAtChunkBoundaries(@TempDir Path tempDir) throws Exception {
        int fileSize = 3 * 1024 * 1024;
        byte[] sourceData = createTestData(fileSize);

        Path sourceFile = writeSourceFile(tempDir, sourceData, "chunk_bound");
        SetupResult setup = setupServerAndMerkle(tempDir, sourceFile, "chunk_bound");
        int chunkSize = (int) setup.shape.getChunkSize();

        Path localCache = tempDir.resolve("chunk_bound_cache.dat");
        Path merklePath = tempDir.resolve("chunk_bound_state.mrkl");
        MAFileChannel channel = new MAFileChannel(
            localCache, merklePath, setup.remoteUrl);
        channel.prebuffer(0, fileSize).get();

        // Read first byte of file
        assertReadMatches(channel, sourceData, 0, 1);
        // Read last byte of file
        assertReadMatches(channel, sourceData, fileSize - 1, 1);
        // Read last byte of first chunk
        assertReadMatches(channel, sourceData, chunkSize - 1, 1);
        // Read first byte of second chunk
        assertReadMatches(channel, sourceData, chunkSize, 1);
        // Read spanning chunk boundary: last 100 bytes of chunk 0 + first 100 bytes of chunk 1
        assertReadMatches(channel, sourceData, chunkSize - 100, 200);
        // Read spanning two chunk boundaries (chunk 0 end -> chunk 2 start)
        if (fileSize >= 2 * chunkSize + 100) {
            assertReadMatches(channel, sourceData, chunkSize - 50, chunkSize + 100);
        }
        // Read entire file
        assertReadMatches(channel, sourceData, 0, fileSize);

        channel.close();
    }

    /// Reads with a zero-length buffer (should return 0 or -1 gracefully).
    @Test
    void zeroLengthReadDoesNotFail(@TempDir Path tempDir) throws Exception {
        int fileSize = 1024 * 1024;
        byte[] sourceData = createTestData(fileSize);
        MAFileChannel channel = createPrebufferedChannel(tempDir, sourceData, "zero_len");

        ByteBuffer emptyBuf = ByteBuffer.allocate(0);
        Future<Integer> future = channel.read(emptyBuf, 0);
        int bytesRead = future.get();
        // FileChannel.read with 0 remaining returns 0
        assertEquals(0, bytesRead, "Zero-length read should return 0");

        channel.close();
    }

    /// Single-byte reads at every position in a small file to catch
    /// off-by-one errors in mmap slice logic.
    @Test
    void singleByteReadAtEveryPosition(@TempDir Path tempDir) throws Exception {
        // Use a smaller file so this completes quickly
        int fileSize = 64 * 1024; // 64 KB
        byte[] sourceData = createTestData(fileSize);
        MAFileChannel channel = createPrebufferedChannel(tempDir, sourceData, "byte_walk");

        for (int pos = 0; pos < fileSize; pos++) {
            ByteBuffer buf = ByteBuffer.allocate(1);
            Future<Integer> future = channel.read(buf, pos);
            int bytesRead = future.get();
            assertEquals(1, bytesRead, "Should read 1 byte at pos " + pos);
            buf.flip();
            assertEquals(sourceData[pos], buf.get(), "Data mismatch at pos " + pos);
        }

        channel.close();
    }

    // ==================== fullyCached latch semantics ====================

    /// Verifies that isRangeCached transitions from checking the BitSet to
    /// the volatile fast path once fully cached, and that it never falsely
    /// reports cached before prebuffer completes.
    @Test
    void fullyCachedLatchTransition(@TempDir Path tempDir) throws Exception {
        int fileSize = 2 * 1024 * 1024;
        byte[] sourceData = createTestData(fileSize);

        Path sourceFile = writeSourceFile(tempDir, sourceData, "latch_trans");
        SetupResult setup = setupServerAndMerkle(tempDir, sourceFile, "latch_trans");

        Path localCache = tempDir.resolve("latch_cache.dat");
        Path merklePath = tempDir.resolve("latch_state.mrkl");

        MAFileChannel channel = new MAFileChannel(
            localCache, merklePath, setup.remoteUrl);

        // Before prebuffer: should NOT be fully cached
        assertFalse(channel.isFullyCached(), "Should not be fully cached before prebuffer");
        assertFalse(channel.isRangeCached(0, fileSize),
            "Full range should not be cached before prebuffer");

        // Prebuffer the entire file
        channel.prebuffer(0, fileSize).get();

        // After prebuffer: should be fully cached
        assertTrue(channel.isFullyCached(), "Should be fully cached after prebuffer");
        assertTrue(channel.isRangeCached(0, fileSize), "Full range should be cached");

        // Verify multiple calls to isRangeCached are consistent (volatile path)
        for (int i = 0; i < 1000; i++) {
            assertTrue(channel.isRangeCached(0, fileSize),
                "isRangeCached should consistently return true once latched");
        }

        // Verify reads after latch are correct
        assertReadMatches(channel, sourceData, 0, fileSize);

        channel.close();
    }

    // ==================== read after close ====================

    /// Verifies that reads after close fail gracefully rather than
    /// returning stale mmap data.
    @Test
    void readAfterCloseFailsGracefully(@TempDir Path tempDir) throws Exception {
        int fileSize = 1024 * 1024;
        byte[] sourceData = createTestData(fileSize);
        MAFileChannel channel = createPrebufferedChannel(tempDir, sourceData, "after_close");

        // Verify read works before close
        assertReadMatches(channel, sourceData, 0, 100);

        channel.close();

        // Read after close should return a failed future
        ByteBuffer buf = ByteBuffer.allocate(100);
        Future<Integer> future = channel.read(buf, 0);
        assertInstanceOf(CompletableFuture.class, future);
        assertTrue(((CompletableFuture<Integer>) future).isCompletedExceptionally(),
            "Read after close should fail");
    }

    // ==================== Concurrent reads during prebuffer ====================

    /// Starts reads concurrently with an ongoing prebuffer operation.
    /// Some reads should succeed (via download-on-demand or cache hit),
    /// and data must match the source.
    @Test
    void readsOverlapWithPrebuffer(@TempDir Path tempDir) throws Exception {
        int fileSize = 3 * 1024 * 1024;
        byte[] sourceData = createTestData(fileSize);

        Path sourceFile = writeSourceFile(tempDir, sourceData, "overlap_pb");
        SetupResult setup = setupServerAndMerkle(tempDir, sourceFile, "overlap_pb");

        Path localCache = tempDir.resolve("overlap_cache.dat");
        Path merklePath = tempDir.resolve("overlap_state.mrkl");

        MAFileChannel channel = new MAFileChannel(
            localCache, merklePath, setup.remoteUrl);

        // Start prebuffer (async)
        CompletableFuture<Void> prebufferFuture = channel.prebuffer(0, fileSize);

        // Immediately start concurrent reads — some will trigger on-demand download,
        // others may hit the cache if prebuffer has progressed
        int threadCount = 20;
        AtomicInteger failures = new AtomicInteger();
        List<Thread> threads = new ArrayList<>();

        for (int t = 0; t < threadCount; t++) {
            final int threadIdx = t;
            Thread thread = Thread.ofVirtual().start(() -> {
                try {
                    Random rng = new Random(threadIdx * 97L);
                    for (int iter = 0; iter < 50; iter++) {
                        int readLen = rng.nextInt(1, 4096);
                        int pos = rng.nextInt(fileSize - readLen);
                        ByteBuffer buf = ByteBuffer.allocate(readLen);
                        Future<Integer> future = channel.read(buf, pos);
                        int bytesRead = future.get();
                        assertTrue(bytesRead > 0,
                            "Should read at least 1 byte at pos " + pos);
                        buf.flip();
                        byte[] actual = new byte[bytesRead];
                        buf.get(actual);
                        byte[] expected = Arrays.copyOfRange(sourceData, pos, pos + bytesRead);
                        assertArrayEquals(expected, actual,
                            "Data mismatch during overlap at pos=" + pos + " len=" + bytesRead);
                    }
                } catch (Throwable e) {
                    failures.incrementAndGet();
                    System.err.println("Overlap thread " + threadIdx + " failed: " + e);
                }
            });
            threads.add(thread);
        }

        // Wait for both prebuffer and reader threads
        prebufferFuture.get();
        for (Thread t : threads) t.join(30_000);

        assertEquals(0, failures.get(), "Some overlap reader threads failed");
        assertTrue(channel.isFullyCached(), "Should be fully cached after prebuffer completes");

        channel.close();
    }

    // ==================== Direct ByteBuffer reads ====================

    /// Reads into a direct ByteBuffer (off-heap) to verify the mmap
    /// path handles both heap and direct destination buffers.
    @Test
    void directByteBufferReadsReturnCorrectData(@TempDir Path tempDir) throws Exception {
        int fileSize = 2 * 1024 * 1024;
        byte[] sourceData = createTestData(fileSize);
        MAFileChannel channel = createPrebufferedChannel(tempDir, sourceData, "direct_buf");

        Random rng = new Random(123);
        for (int iter = 0; iter < 200; iter++) {
            int readLen = rng.nextInt(1, 8192);
            int pos = rng.nextInt(fileSize - readLen);
            ByteBuffer directBuf = ByteBuffer.allocateDirect(readLen);
            Future<Integer> future = channel.read(directBuf, pos);
            int bytesRead = future.get();
            assertEquals(readLen, bytesRead);
            directBuf.flip();
            byte[] actual = new byte[bytesRead];
            directBuf.get(actual);
            byte[] expected = Arrays.copyOfRange(sourceData, pos, pos + readLen);
            assertArrayEquals(expected, actual,
                "Direct buffer mismatch at pos=" + pos + " len=" + readLen);
        }

        channel.close();
    }

    // ==================== Helpers ====================

    /// Verifies that reading {@code length} bytes at {@code position} from the
    /// channel matches the corresponding slice from {@code sourceData}.
    private void assertReadMatches(MAFileChannel channel, byte[] sourceData,
                                   int position, int length) throws Exception {
        ByteBuffer buf = ByteBuffer.allocate(length);
        Future<Integer> future = channel.read(buf, position);
        int bytesRead = future.get();
        assertEquals(length, bytesRead, "Expected full read at pos=" + position + " len=" + length);
        buf.flip();
        byte[] actual = new byte[bytesRead];
        buf.get(actual);
        byte[] expected = Arrays.copyOfRange(sourceData, position, position + length);
        assertArrayEquals(expected, actual,
            "Data mismatch at pos=" + position + " len=" + length);
    }

    /// Creates a deterministic byte array of the given size.
    private byte[] createTestData(int size) {
        byte[] data = new byte[size];
        Random rng = new Random(0xDEADBEEF);
        rng.nextBytes(data);
        return data;
    }

    /// Writes source data to a file and returns the path.
    private Path writeSourceFile(Path tempDir, byte[] data, String tag) throws IOException {
        Path file = tempDir.resolve(tag + ".bin");
        Files.write(file, data);
        return file;
    }

    /// Creates a fully-prebuffered MAFileChannel backed by the given source data.
    private MAFileChannel createPrebufferedChannel(Path tempDir, byte[] sourceData, String tag)
        throws Exception
    {
        Path sourceFile = writeSourceFile(tempDir, sourceData, tag);
        SetupResult setup = setupServerAndMerkle(tempDir, sourceFile, tag);

        Path localCache = tempDir.resolve(tag + "_cache.dat");
        Path merklePath = tempDir.resolve(tag + "_state.mrkl");

        MAFileChannel channel = new MAFileChannel(
            localCache, merklePath, setup.remoteUrl);
        channel.prebuffer(0, sourceData.length).get();
        return channel;
    }

    /// Publishes a file via the Jetty test server and creates merkle reference data.
    private SetupResult setupServerAndMerkle(Path tempDir, Path sourceFile, String tag)
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
        String remoteUrl = baseUrl + "temp/" + uniqueId + "/" + sourceFile.getFileName();
        return new SetupResult(remoteUrl, merkleRef.getShape());
    }

    private record SetupResult(String remoteUrl, MerkleShape shape) {}
}
