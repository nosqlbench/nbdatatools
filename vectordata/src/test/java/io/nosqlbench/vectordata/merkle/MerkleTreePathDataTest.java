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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Tests for the MerkleTree.fromData(Path) method.
 * This tests the implementation that uses RandomAccessFile and virtual threads.
 */
public class MerkleTreePathDataTest {

    @TempDir
    Path tempDir;

    @Test
    void testFromDataWithPath() throws IOException, InterruptedException, ExecutionException {
        // Create a test file with random data, using a smaller size to avoid memory issues
        int fileSize = 5 * 1024 * 1024; // 5 MB
        Path testFile = createTestFile(fileSize, "MerkleTreePathDataTest_test_data_with_path.bin");

        // Define chunk size
        long chunkSize = 1024 * 1024; // 1 MB

        // Create a MerkleTree using the new method
        MerkleTreeBuildProgress progress = MerkleTree.fromData(testFile);
        MerkleTree treeFromPath = progress.getFuture().get();

        // Verify the tree was created correctly
        assertNotNull(treeFromPath, "MerkleTree should not be null");
        assertEquals(chunkSize, treeFromPath.getChunkSize(), "Chunk size should match");
        assertEquals(fileSize, treeFromPath.totalSize(), "Total size should match");
        assertEquals((int) Math.ceil((double) fileSize / chunkSize), treeFromPath.getNumberOfLeaves(), 
                    "Number of leaves should match");

        // Verify that the root hash is not null
        assertNotNull(treeFromPath.getHash(0), "Root hash should not be null");

        // Verify that the root hash has the expected length
        assertEquals(32, treeFromPath.getHash(0).length, "Root hash should have length 32");
    }

    @Test
    void testFromDataWithPathPartialRange() throws IOException, InterruptedException, ExecutionException {
        // Create a test file with random data
        int fileSize = 10 * 1024 * 1024; // 10 MB
        Path testFile = createTestFile(fileSize, "MerkleTreePathDataTest_test_data_with_path_partial_range.bin");

        // Note: The method now uses the full file size automatically, not partial ranges
        // Define expected chunk size (will be auto-calculated)
        long expectedChunkSize = 1024 * 1024; // 1 MB (expected automatic calculation)

        // Create a MerkleTree using the new method
        MerkleTreeBuildProgress progress = MerkleTree.fromData(testFile);
        MerkleTree treeFromPath = progress.getFuture().get();

        // Verify the tree was created correctly
        assertNotNull(treeFromPath, "MerkleTree should not be null");
        assertEquals(expectedChunkSize, treeFromPath.getChunkSize(), "Chunk size should be auto-calculated");
        assertEquals(fileSize, treeFromPath.totalSize(), "Total size should match full file size");
        assertEquals((int) Math.ceil((double) fileSize / expectedChunkSize), treeFromPath.getNumberOfLeaves(), 
                    "Number of leaves should match");

        // Verify that the root hash is not null
        assertNotNull(treeFromPath.getHash(0), "Root hash should not be null");

        // Verify that the root hash has the expected length
        assertEquals(32, treeFromPath.getHash(0).length, "Root hash should have length 32");
    }

    @Test
    void testFromDataWithLargeFile() throws IOException, InterruptedException, ExecutionException {
        // Create a larger test file, but not too large to avoid memory issues
        int fileSize = 20 * 1024 * 1024; // 20 MB
        Path testFile = createTestFile(fileSize, "MerkleTreePathDataTest_test_data_with_large_file.bin");

        // Define chunk size
        long chunkSize = 1024 * 1024; // 1 MB

        // Create a MerkleTree using the new method
        MerkleTreeBuildProgress progress = MerkleTree.fromData(testFile);
        MerkleTree treeFromPath = progress.getFuture().get();

        // Verify the tree was created correctly
        assertNotNull(treeFromPath, "MerkleTree should not be null");
        assertEquals(chunkSize, treeFromPath.getChunkSize(), "Chunk size should match");
        assertEquals(fileSize, treeFromPath.totalSize(), "Total size should match");

        // Verify that the root hash is not null
        assertNotNull(treeFromPath.getHash(0), "Root hash should not be null");

        // Verify that the root hash has the expected length
        assertEquals(32, treeFromPath.getHash(0).length, "Root hash should have length 32");
    }

    /**
     * Creates a test file with random data.
     * 
     * @param size The size of the file to create
     * @param filename The name of the file to create
     * @return The path to the created file
     * @throws IOException If an I/O error occurs
     */
    private Path createTestFile(int size, String filename) throws IOException {
        Path filePath = tempDir.resolve(filename);
        byte[] data = new byte[size];
        new Random(42).nextBytes(data); // Use fixed seed for reproducibility
        Files.write(filePath, data, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        return filePath;
    }
}
