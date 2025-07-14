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

        // Create a MerkleTree for the non-existent file (chunk size ignored, calculated automatically)
        MerkleTreeBuildProgress progress = MerkleTree.fromData(nonExistentFile);
        MerkleTree tree = progress.getFuture().get();

        // Verify the tree was created
        assertNotNull(tree, "MerkleTree should not be null");
        assertTrue(tree.getChunkSize() > 0, "Chunk size should be positive");
        assertEquals(0, tree.totalSize(), "Total size should be 0 for non-existent file");

        // Now create the file with random data
        byte[] data = new byte[fileSize];
        new Random(42).nextBytes(data); // Use fixed seed for reproducibility
        Files.write(nonExistentFile, data, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

        // Verify the file now exists
        assertTrue(Files.exists(nonExistentFile), "File should now exist");

        // Create a new MerkleTree for the now-existing file
        MerkleTreeBuildProgress newProgress = MerkleTree.fromData(nonExistentFile);
        MerkleTree newTree = newProgress.getFuture().get();

        // Verify the new tree was created
        assertNotNull(newTree, "New MerkleTree should not be null");
        assertEquals(tree.getChunkSize(), newTree.getChunkSize(), "Chunk size should match between trees");
        assertEquals(fileSize, newTree.totalSize(), "Total size should match");

        // Create a MerkleTree from the data buffer for comparison (using auto-calculated chunk size)
        ByteBuffer buffer = ByteBuffer.wrap(data);
        MerkleTree bufferTree = MerkleTree.fromData(buffer);

        // Print debug information
        System.out.println("[DEBUG_LOG] Buffer tree root hash: " + java.util.Arrays.toString(bufferTree.getHash(0)));
        System.out.println("[DEBUG_LOG] New tree root hash: " + java.util.Arrays.toString(newTree.getHash(0)));
        System.out.println("[DEBUG_LOG] Original tree root hash: " + java.util.Arrays.toString(tree.getHash(0)));

        // Create another tree from the file to verify consistency
        MerkleTreeBuildProgress anotherProgress = MerkleTree.fromData(nonExistentFile);
        MerkleTree anotherTree = anotherProgress.getFuture().get();

        // Verify that trees created from the same file are consistent
        assertArrayEquals(newTree.getHash(0), anotherTree.getHash(0), "Trees created from the same file should have identical hashes");

        // Close the additional tree
        anotherTree.close();

        // Verify the original tree's root hash is different from the new tree's
        // This is because the original tree was empty, while the new one contains actual data
        assertFalse(
            java.util.Arrays.equals(tree.getHash(0), newTree.getHash(0)), 
            "Original and new tree root hashes should be different"
        );

        // Clean up
        tree.close();
        newTree.close();
        bufferTree.close();
    }
}
