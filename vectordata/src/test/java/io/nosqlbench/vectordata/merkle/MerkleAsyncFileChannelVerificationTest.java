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

import io.nosqlbench.vectordata.status.NoOpDownloadEventSink;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/// Test class for verifying that merkle chunk verification succeeds when MerkleAsyncFileChannel is used.
/// This test creates local files and merkle trees to test MerkleAsyncFileChannel verification functionality.
public class MerkleAsyncFileChannelVerificationTest {

    private static final int CHUNK_SIZE = 1024; // 1KB chunks
    private static final int TEST_DATA_SIZE = CHUNK_SIZE * 4; // 4KB test data
    private static final Random random = new Random(42); // Fixed seed for reproducibility

    /**
     * Creates random test data of the specified size.
     *
     * @param size The size of the test data in bytes
     * @return A byte array containing the test data
     */
    private byte[] createTestData(int size) {
        byte[] data = new byte[size];
        random.nextBytes(data);
        return data;
    }

    /**
     * Test that verification of a merkle chunk succeeds when MerkleAsyncFileChannel is used.
     * This test will:
     * 1. Create a test file with known content
     * 2. Create a merkle tree for that file
     * 3. Create a reference merkle tree (identical to the original for successful verification)
     * 4. Create a MerkleAsyncFileChannel instance
     * 5. Read data from the file, which triggers verification
     * 6. Verify that the data read matches the expected data
     */
    @Test
    void testMerkleAsyncFileChannelChunkVerificationSuccess(@TempDir Path tempDir) throws IOException, InterruptedException, java.util.concurrent.ExecutionException {
        // Create test data
        byte[] testData = createTestData(TEST_DATA_SIZE);

        // Create a test file with class name to avoid collisions
        Path testFile = tempDir.resolve("MerkleAsyncFileChannelVerificationTest_test_data.bin");
        Files.write(testFile, testData);

        // Create a merkle tree for the test file
        Path merkleFile = tempDir.resolve("MerkleAsyncFileChannelVerificationTest_test_data.bin.mrkl");
        MerkleTree tree = MerkleTree.fromFile(testFile, CHUNK_SIZE, new MerkleRange(0, TEST_DATA_SIZE));
        tree.save(merkleFile);

        // Create a reference merkle tree (identical to the original for successful verification)
        Path refMerkleFile = tempDir.resolve("MerkleAsyncFileChannelVerificationTest_test_data.bin.mref");
        Files.copy(merkleFile, refMerkleFile);

        // Create a MerkleAsyncFileChannel instance with the test file
        // Use a local file URL and the testing constructor with NoOpDownloadEventSink
        String localUrl = "file://" + testFile.toAbsolutePath();
        try (MerkleAsyncFileChannel merkleChannel = new MerkleAsyncFileChannel(testFile, localUrl, new NoOpDownloadEventSink(), true)) {
            // Read data from the file, which should trigger verification
            byte[] buffer = new byte[CHUNK_SIZE];
            int bytesRead = merkleChannel.read(java.nio.ByteBuffer.wrap(buffer), 0).get();

            // Verify that we read the expected number of bytes
            assertEquals(CHUNK_SIZE, bytesRead, "Should read " + CHUNK_SIZE + " bytes");

            // Verify that the buffer contains the expected data
            byte[] expectedChunkData = Arrays.copyOfRange(testData, 0, CHUNK_SIZE);
            assertArrayEquals(expectedChunkData, buffer, "Buffer should contain the expected data");

            // If we got here without exceptions and the data matches, then verification succeeded
            System.out.println("[DEBUG_LOG] Verification succeeded - data read matches expected data");
        }
    }
}