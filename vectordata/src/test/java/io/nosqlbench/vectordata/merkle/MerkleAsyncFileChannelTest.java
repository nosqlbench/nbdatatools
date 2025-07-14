package io.nosqlbench.vectordata.merkle;

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

import io.nosqlbench.vectordata.status.MemoryEventSink;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/// Test class for verifying that MerkleAsyncFileChannel works correctly.
/// This test creates local files and merkle trees to test MerkleAsyncFileChannel functionality.
public class MerkleAsyncFileChannelTest {

    /// Test that MerkleAsyncFileChannel can read a file correctly.
    ///
    /// 1. Create a test file with random data
    /// 2. Create a merkle tree for the file
    /// 3. Create a MerkleAsyncFileChannel instance to read the file
    /// 4. Read data from the file and verify it matches the original data
    @Test
    void testMerkleAsyncFileChannelRead(@TempDir Path tempDir) throws IOException, ExecutionException, InterruptedException, TimeoutException {
        // Create a unique test file name
        String uniqueId = "MerkleAsyncFileChannelTest_" + System.nanoTime() + "_" + Thread.currentThread().getId();
        Path testFile = tempDir.resolve(uniqueId + "_test_data.bin");

        // Create test data (4KB of random data)
        byte[] testData = new byte[4096];
        new Random().nextBytes(testData);

        // Write the test data to the file
        Files.write(testFile, testData);

        // Create a merkle tree for the file
        Path merkleFile = tempDir.resolve(uniqueId + "_test_data.bin.mrkl");
        MerkleRange range = new MerkleRange(0, testData.length); // Range covering the entire file
        MerkleTree tree = MerkleTree.fromFile(testFile, 1024, range); // Use 1KB chunk size
        tree.save(merkleFile);

        // Create a reference merkle tree file
        Path refMerkleFile = tempDir.resolve(uniqueId + "_test_data.bin.mref");
        Files.copy(merkleFile, refMerkleFile);

        // Create a local URL for testing
        String localUrl = "file://" + testFile.toString();

        // Create a custom event sink to track events
        MemoryEventSink eventSink = new MemoryEventSink();

        // Create a MerkleAsyncFileChannel instance with the test file
        try (MerkleAsyncFileChannel channel = new MerkleAsyncFileChannel(testFile, localUrl, eventSink, true)) {
            // Check the file size
            long fileSize = channel.size();
            assertEquals(testData.length, fileSize, "File size should match test data length");

            // Read the first 1024 bytes using the Future-based API
            ByteBuffer buffer = ByteBuffer.allocate(1024);
            int bytesRead = channel.read(buffer, 0).get(5, TimeUnit.SECONDS);
            assertEquals(1024, bytesRead, "Should read 1024 bytes");

            buffer.flip();
            byte[] readData = new byte[bytesRead];
            buffer.get(readData);

            // Verify the data matches the original
            for (int i = 0; i < bytesRead; i++) {
                assertEquals(testData[i], readData[i], "Data at position " + i + " should match");
            }

            // Read the next 1024 bytes using the CompletionHandler-based API
            ByteBuffer buffer2 = ByteBuffer.allocate(1024);
            CompletableFuture<Integer> readFuture = new CompletableFuture<>();

            channel.read(buffer2, 1024, null, new CompletionHandler<Integer, Void>() {
                @Override
                public void completed(Integer result, Void attachment) {
                    readFuture.complete(result);
                }

                @Override
                public void failed(Throwable exc, Void attachment) {
                    readFuture.completeExceptionally(exc);
                }
            });

            int bytesRead2 = readFuture.get(5, TimeUnit.SECONDS);
            assertEquals(1024, bytesRead2, "Should read 1024 bytes");

            buffer2.flip();
            byte[] readData2 = new byte[bytesRead2];
            buffer2.get(readData2);

            // Verify the data matches the original
            for (int i = 0; i < bytesRead2; i++) {
                assertEquals(testData[i + 1024], readData2[i], "Data at position " + (i + 1024) + " should match");
            }

            // Test prebuffering
            CompletableFuture<Void> prebufferFuture = channel.prebuffer(2048, 1024);
            prebufferFuture.get(5, TimeUnit.SECONDS);

            // Read the prebuffered data
            ByteBuffer buffer3 = ByteBuffer.allocate(1024);
            int bytesRead3 = channel.read(buffer3, 2048).get(5, TimeUnit.SECONDS);
            assertEquals(1024, bytesRead3, "Should read 1024 bytes");

            buffer3.flip();
            byte[] readData3 = new byte[bytesRead3];
            buffer3.get(readData3);

            // Verify the data matches the original
            for (int i = 0; i < bytesRead3; i++) {
                assertEquals(testData[i + 2048], readData3[i], "Data at position " + (i + 2048) + " should match");
            }

            // Test unsupported operations
            try {
                channel.write(ByteBuffer.allocate(10), 0).get();
                fail("Write operation should throw an exception");
            } catch (ExecutionException e) {
                assertTrue(e.getCause() instanceof UnsupportedOperationException, 
                    "Cause should be UnsupportedOperationException, but was: " + e.getCause().getClass().getName());
                assertEquals("Write operations are not supported", e.getCause().getMessage());
            }

            assertThrows(UnsupportedOperationException.class, () -> {
                channel.truncate(100);
            }, "Truncate operation should throw UnsupportedOperationException");

            assertThrows(UnsupportedOperationException.class, () -> {
                channel.tryLock(0, 100, false);
            }, "TryLock operation should throw UnsupportedOperationException");

            // Test that the channel is open
            assertTrue(channel.isOpen(), "Channel should be open");
        }

        // Verify that the channel is closed after the try-with-resources block
        // This is implicit in the try-with-resources, but we can't test it directly

        System.out.println("[DEBUG_LOG] Successfully tested MerkleAsyncFileChannel");
    }
}
