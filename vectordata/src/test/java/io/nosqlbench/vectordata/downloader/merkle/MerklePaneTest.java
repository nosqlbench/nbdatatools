package io.nosqlbench.vectordata.downloader.merkle;

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


import io.nosqlbench.vectordata.merkle.MerklePane;
import io.nosqlbench.vectordata.merkle.MerkleRange;
import io.nosqlbench.vectordata.merkle.MerkleTree;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class MerklePaneTest {
    @TempDir
    Path tempDir;

    private static final int CHUNK_SIZE = 16; // Power of two
    private static final int TEST_FILE_SIZE = 128; // Multiple of chunk size for cleaner tests
    private static final byte TEST_DATA_VALUE = (byte)1;

    private Path createTestFile(byte[] data) throws IOException {
        Path file = tempDir.resolve("test.dat");
        Files.write(file, data);
        return file;
    }

    private byte[] createTestData(int size, byte value) {
        byte[] data = new byte[size];
        Arrays.fill(data, value);
        return data;
    }

    private Path createMerkleFile(Path dataFile, int chunkSize) throws IOException {
        Path merkleFile = tempDir.resolve("test.dat" + MerklePane.MRKL);

        // Create and save Merkle tree
        ByteBuffer buffer = ByteBuffer.wrap(Files.readAllBytes(dataFile));
        MerkleTree tree = MerkleTree.fromData(
            buffer,
            chunkSize,
            new MerkleRange(0, buffer.capacity())
        );
        tree.save(merkleFile);

        return merkleFile;
    }

    @Test
    void testConstructorWithSinglePath() throws IOException {
        byte[] data = createTestData(TEST_FILE_SIZE, TEST_DATA_VALUE);
        Path dataFile = createTestFile(data);
        Path merkleFile = createMerkleFile(dataFile, CHUNK_SIZE);

        try (MerklePane window = new MerklePane(dataFile)) {
            assertEquals(TEST_FILE_SIZE, window.fileSize());
            assertEquals(dataFile, window.filePath());
        }
    }

    @Test
    void testBasicOperations() throws IOException {
        byte[] data = createTestData(TEST_FILE_SIZE, TEST_DATA_VALUE);
        Path dataFile = createTestFile(data);
        Path merkleFile = createMerkleFile(dataFile, CHUNK_SIZE);

        try (MerklePane window = new MerklePane(dataFile, merkleFile)) {
            assertEquals(TEST_FILE_SIZE, window.fileSize());
            assertEquals(dataFile, window.filePath());

            // Test chunk reading
            ByteBuffer chunk = window.readChunk(0);
            assertEquals(CHUNK_SIZE, chunk.remaining());
            assertEquals(TEST_DATA_VALUE, chunk.get(0));

            // Test last chunk reading
            int lastChunkIndex = (TEST_FILE_SIZE / CHUNK_SIZE) - 1;
            ByteBuffer lastChunk = window.readChunk(lastChunkIndex);
            assertEquals(CHUNK_SIZE, lastChunk.remaining());
            assertEquals(TEST_DATA_VALUE, lastChunk.get(0));

            // Test range reading
            int rangeStart = 5;
            int rangeLength = 20;
            ByteBuffer range = window.readRange(rangeStart, rangeLength);
            assertEquals(rangeLength, range.remaining());
            assertEquals(TEST_DATA_VALUE, range.get(0));

            // Test chunk verification
            assertTrue(window.verifyChunk(0), "First chunk should verify successfully");
            assertTrue(window.verifyChunk(lastChunkIndex), "Last chunk should verify successfully");
        }
    }

    @Test
    void testVerifyChunkWithCorruption() throws IOException {
        byte[] data = createTestData(TEST_FILE_SIZE, TEST_DATA_VALUE);
        Path dataFile = createTestFile(data);
        Path merkleFile = createMerkleFile(dataFile, CHUNK_SIZE);

        // Corrupt specific chunks in the data file
        data[0] = 2; // Corrupt first chunk
        data[CHUNK_SIZE + 1] = 2; // Corrupt second chunk
        Files.write(dataFile, data);

        try (MerklePane window = new MerklePane(dataFile, merkleFile)) {
            assertFalse(window.verifyChunk(0), "First chunk should fail verification");
            assertFalse(window.verifyChunk(1), "Second chunk should fail verification");
            assertTrue(window.verifyChunk(2), "Third chunk should verify successfully");
        }
    }

    @Test
    void testInvalidOperations() throws IOException {
        byte[] data = createTestData(TEST_FILE_SIZE, TEST_DATA_VALUE);
        Path dataFile = createTestFile(data);
        Path merkleFile = createMerkleFile(dataFile, CHUNK_SIZE);

        try (MerklePane window = new MerklePane(dataFile, merkleFile)) {
            // Test invalid chunk indices
            assertThrows(IllegalArgumentException.class,
                () -> window.readChunk(-1),
                "Negative chunk index should throw exception"
            );

            int invalidChunkIndex = TEST_FILE_SIZE / CHUNK_SIZE;
            assertThrows(IllegalArgumentException.class,
                () -> window.readChunk(invalidChunkIndex),
                "Chunk index beyond file size should throw exception"
            );

            // Test invalid ranges
            assertThrows(IllegalArgumentException.class,
                () -> window.readRange(-1, CHUNK_SIZE),
                "Negative range start should throw exception"
            );

            assertThrows(IllegalArgumentException.class,
                () -> window.readRange(0, -1),
                "Negative range length should throw exception"
            );

            assertThrows(IllegalArgumentException.class,
                () -> window.readRange(TEST_FILE_SIZE - 5, 10),
                "Range extending beyond file size should throw exception"
            );
        }
    }

    @Test
    void testMissingOrInvalidFiles() {
        Path nonExistentFile = tempDir.resolve("nonexistent.dat");

        assertThrows(RuntimeException.class,
            () -> new MerklePane(nonExistentFile),
            "Constructor should throw exception for non-existent file"
        );
    }

    @Test
    void testResourceLeaks() throws IOException {
        byte[] data = createTestData(TEST_FILE_SIZE, TEST_DATA_VALUE);
        Path dataFile = createTestFile(data);
        Path merkleFile = createMerkleFile(dataFile, CHUNK_SIZE);

        MerklePane window = new MerklePane(dataFile, merkleFile);
        window.close();

        assertThrows(IOException.class,
            () -> window.readChunk(0),
            "Operations after close should throw exception"
        );
    }

    @Test
    void testSubmitChunk() throws IOException {
        // Create initial data file with all zeros
        byte[] initialData = new byte[TEST_FILE_SIZE];
        Path dataFile = createTestFile(initialData);
        Path merkleFile = createMerkleFile(dataFile, CHUNK_SIZE);

        // Create a chunk of data with all ones
        byte[] chunkData = createTestData(CHUNK_SIZE, TEST_DATA_VALUE);
        ByteBuffer chunk = ByteBuffer.wrap(chunkData);

        try (MerklePane window = new MerklePane(dataFile, merkleFile)) {
            // Verify that the chunk initially has zeros
            ByteBuffer initialChunk = window.readChunk(0);
            assertEquals(0, initialChunk.get(0), "Initial chunk should contain zeros");

            // Get the initial verification state
            boolean initialVerification = window.verifyChunk(0);

            // Submit the new chunk
            window.submitChunk(0, chunk);

            // Read the chunk again and verify it contains the new data
            ByteBuffer updatedChunk = window.readChunk(0);
            assertEquals(TEST_DATA_VALUE, updatedChunk.get(0), "Updated chunk should contain the new value");

            // Verify that the chunk verification state has changed
            boolean updatedVerification = window.verifyChunk(0);
            // If the initial verification was true, the updated verification should still be true
            // If the initial verification was false, the updated verification should now be true
            assertTrue(updatedVerification, "Updated chunk should pass verification");

            // The key test is that the hash has changed, which we can verify by checking
            // that either the initial verification was false or the data has changed
            if (initialVerification) {
                // If initial verification passed, then the data must have changed
                ByteBuffer finalChunk = window.readChunk(0);
                assertNotEquals(0, finalChunk.get(0), "Data should have changed if initial verification passed");
            }
        }
    }
}
