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

import io.nosqlbench.vectordata.merklev2.MerkleDataImpl;
import io.nosqlbench.vectordata.merklev2.MerkleRefFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for creating a Merkle tree for a file that doesn't exist yet.
 */
public class MerkleTreeEmptyFileTest {

    @TempDir
    Path tempDir;

    @Test
    void testCreateMerkleTreeForNonExistentFile() throws IOException, InterruptedException, ExecutionException {
        // Define a path for a file that doesn't exist yet
        Path nonExistentFile = tempDir.resolve("non_existent_file.bin");

        // Verify the file doesn't exist
        assertFalse(Files.exists(nonExistentFile), "File should not exist yet");

        // Define file size - chunk size will be calculated automatically
        int fileSize = 10 * 1024; // 10 KB

        // Create an empty MerkleRef for the future file content
        MerkleDataImpl emptyTree = MerkleRefFactory.createEmpty(fileSize);

        // Verify the empty tree was created
        assertNotNull(emptyTree, "MerkleRef should not be null");
        assertTrue(emptyTree.getShape().getChunkSize() > 0, "Chunk size should be positive");
        assertEquals(fileSize, emptyTree.getShape().getTotalContentSize(), "Total size should match expected file size");

        // Now create the file with random data
        byte[] data = new byte[fileSize];
        new Random(42).nextBytes(data); // Use fixed seed for reproducibility
        Files.write(nonExistentFile, data, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

        // Verify the file now exists
        assertTrue(Files.exists(nonExistentFile), "File should now exist");

        // Create a new MerkleRef for the now-existing file
        var newProgress = MerkleRefFactory.fromData(nonExistentFile);
        MerkleDataImpl newTree = newProgress.getFuture().get();

        // Verify the new tree was created
        assertNotNull(newTree, "New MerkleRef should not be null");
        assertEquals(emptyTree.getShape().getChunkSize(), newTree.getShape().getChunkSize(), "Chunk size should match between trees");
        assertEquals(fileSize, newTree.getShape().getTotalContentSize(), "Total size should match");

        // Create a MerkleRef from the data buffer for comparison
        ByteBuffer buffer = ByteBuffer.wrap(data);
        var bufferFuture = MerkleRefFactory.fromData(buffer);
        MerkleDataImpl bufferTree = bufferFuture.get();

        // Print debug information
        System.out.println("[DEBUG_LOG] Buffer tree root hash: " + java.util.Arrays.toString(bufferTree.getHashForIndex(0)));
        System.out.println("[DEBUG_LOG] New tree root hash: " + java.util.Arrays.toString(newTree.getHashForIndex(0)));
        System.out.println("[DEBUG_LOG] Empty tree root hash: " + java.util.Arrays.toString(emptyTree.getHashForIndex(0)));

        // Create another tree from the file to verify consistency
        var anotherProgress = MerkleRefFactory.fromData(nonExistentFile);
        MerkleDataImpl anotherTree = anotherProgress.getFuture().get();

        // Verify that trees created from the same file are consistent
        assertArrayEquals(newTree.getHashForIndex(0), anotherTree.getHashForIndex(0), "Trees created from the same file should have identical hashes");

        // Verify the empty tree's root hash is different from the new tree's
        // This is because the empty tree has no actual data, while the new one contains actual data
        assertFalse(
            java.util.Arrays.equals(emptyTree.getHashForIndex(0), newTree.getHashForIndex(0)), 
            "Empty and populated tree root hashes should be different"
        );

        // Note: MerkleDataImpl doesn't need explicit closing like the old MerkleTree
    }
}
