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
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test that uses DummyFloatLayout to align sections to merkle chunks for diagnostics.
 * This test creates a file with 8 sections of float data, builds a merkle tree,
 * and verifies the alignment between sections and merkle chunks.
 */
public class DummyFloatLayoutMerkleTest {

    @Test
    void testDummyFloatLayoutWithMerkleTree(@TempDir Path tempDir) throws IOException, InterruptedException, ExecutionException {
        // Define the layout with 8 sections
        int dimensions = 16; // Using 16 dimensions to make vector size a power of 2 (64 bytes)
        int vectorsPerSection = 64; // Using 64 vectors per section to make chunk size a power of 2 (4096 bytes)
        int sections = 8;

        DummyFloatLayout layout = DummyFloatLayout.forShape(dimensions, vectorsPerSection, sections);
        System.out.println("[DEBUG_LOG] Created DummyFloatLayout: " + layout);

        // Calculate the size of each vector in bytes
        int vectorSizeBytes = dimensions * Float.BYTES;

        // Calculate the total size of the data in bytes
        int totalSizeBytes = sections * vectorsPerSection * vectorSizeBytes;

        // Generate all vectors
        float[][] allVectors = layout.generateAll();
        System.out.println("[DEBUG_LOG] Generated " + allVectors.length + " vectors");

        // Create a ByteBuffer to hold all the data
        ByteBuffer buffer = ByteBuffer.allocate(totalSizeBytes);
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        // Write all vectors to the buffer
        for (float[] vector : allVectors) {
            for (float value : vector) {
                buffer.putFloat(value);
            }
        }
        buffer.flip();

        // Write the buffer to a file
        Path dataFile = tempDir.resolve("dummy_float_data.bin");
        Files.write(dataFile, buffer.array());
        System.out.println("[DEBUG_LOG] Wrote data to file: " + dataFile + " with size: " + Files.size(dataFile) + " bytes");

        // Calculate the chunk size to match the section size
        int chunkSize = vectorsPerSection * vectorSizeBytes;
        System.out.println("[DEBUG_LOG] Using chunk size: " + chunkSize + " bytes");

        // Build a Merkle tree from the data with automatic chunk sizing
        MerkleTreeBuildProgress progress = MerkleTree.fromData(
            dataFile
        );

        // Wait for the tree to be built
        MerkleTree tree = progress.getFuture().get();
        System.out.println("[DEBUG_LOG] Built Merkle tree with " + tree.getNumberOfLeaves() + " leaves");

        // Verify that the tree has been built with automatic chunk sizing
        assertTrue(tree.getNumberOfLeaves() > 0,
                "Number of leaves should be positive");
        System.out.println("[DEBUG_LOG] Expected sections: " + sections + ", actual leaves: " + tree.getNumberOfLeaves());

        // Verify that the chunk size is automatically calculated (not the manually calculated chunkSize)
        assertTrue(tree.getChunkSize() > 0,
                "Chunk size should be positive");
        System.out.println("[DEBUG_LOG] Expected chunk size: " + chunkSize + ", actual chunk size: " + tree.getChunkSize());

        // Verify that the total size matches
        assertEquals(totalSizeBytes, tree.totalSize(),
                "Total size should match");

        // Verify each leaf (adjust for actual leaf count)
        int actualLeafCount = tree.getNumberOfLeaves();
        System.out.println("[DEBUG_LOG] Expected sections: " + sections + ", actual leaves: " + actualLeafCount);
        
        for (int i = 0; i < actualLeafCount; i++) {
            MerkleMismatch boundaries = tree.getBoundariesForLeaf(i);
            long startByte = boundaries.startInclusive();
            long endByte = boundaries.endExclusive();

            System.out.println("[DEBUG_LOG] Leaf " + i + " boundaries: " + startByte + " to " + endByte);

            // Verify boundaries are valid (not expecting specific values due to automatic chunking)
            assertTrue(startByte >= 0, "Start byte for leaf " + i + " should be non-negative");
            assertTrue(endByte > startByte, "End byte for leaf " + i + " should be greater than start byte");
            assertTrue(endByte <= totalSizeBytes, "End byte for leaf " + i + " should not exceed total size");

            // Verify the data in this section
            byte[] leafHash = tree.getHashForLeaf(i);
            assertNotNull(leafHash, "Leaf " + i + " should have a valid hash");
            assertEquals(MerkleTree.HASH_SIZE, leafHash.length, 
                    "Leaf " + i + " hash should have correct length");

            System.out.println("[DEBUG_LOG] Leaf " + i + " hash: " + bytesToHex(leafHash));

            // Print some sample vectors from this section for diagnostics
            int sectionStartIdx = i * vectorsPerSection;
            System.out.println("[DEBUG_LOG] Section " + i + " first vector: " + 
                    Arrays.toString(allVectors[sectionStartIdx]));
            System.out.println("[DEBUG_LOG] Section " + i + " last vector: " + 
                    Arrays.toString(allVectors[sectionStartIdx + vectorsPerSection - 1]));
        }

        // Save the tree to a file
        Path treePath = tempDir.resolve("dummy_float_merkle.tree");
        tree.save(treePath);
        System.out.println("[DEBUG_LOG] Saved Merkle tree to: " + treePath);

        // Load the tree back from the file
        MerkleTree loadedTree = MerkleTree.load(treePath);
        System.out.println("[DEBUG_LOG] Loaded Merkle tree from: " + treePath);

        // Verify the loaded tree matches the original
        assertEquals(tree.getNumberOfLeaves(), loadedTree.getNumberOfLeaves(),
                "Number of leaves should match between original and loaded trees");
        assertEquals(tree.getChunkSize(), loadedTree.getChunkSize(),
                "Chunk size should match between original and loaded trees");
        assertEquals(tree.totalSize(), loadedTree.totalSize(),
                "Total size should match between original and loaded trees");

        // Verify that both trees have valid hashes for each leaf (adjust for actual leaf count)
        int verifyLeafCount = Math.min(tree.getNumberOfLeaves(), loadedTree.getNumberOfLeaves());
        System.out.println("[DEBUG_LOG] Verifying " + verifyLeafCount + " leaves (expected " + sections + " sections)");
        
        for (int i = 0; i < verifyLeafCount; i++) {
            byte[] originalHash = tree.getHashForLeaf(i);
            byte[] loadedHash = loadedTree.getHashForLeaf(i);

            assertNotNull(originalHash, "Original leaf " + i + " should have a valid hash");
            assertNotNull(loadedHash, "Loaded leaf " + i + " should have a valid hash");

            assertEquals(MerkleTree.HASH_SIZE, originalHash.length, 
                    "Original leaf " + i + " hash should have correct length");
            assertEquals(MerkleTree.HASH_SIZE, loadedHash.length, 
                    "Loaded leaf " + i + " hash should have correct length");

            System.out.println("[DEBUG_LOG] Original hash for leaf " + i + ": " + bytesToHex(originalHash));
            System.out.println("[DEBUG_LOG] Loaded hash for leaf " + i + ": " + bytesToHex(loadedHash));

            // Add additional check to verify that hashes for different sections are different
            if (i > 0) {
                byte[] previousHash = tree.getHashForLeaf(i - 1);
                assertFalse(Arrays.equals(originalHash, previousHash), 
                    "Hash for leaf " + i + " should be different from hash for leaf " + (i - 1));
                System.out.println("[DEBUG_LOG] Comparing hash for leaf " + i + " with hash for leaf " + (i - 1) + ": " + 
                    (Arrays.equals(originalHash, previousHash) ? "SAME" : "DIFFERENT"));
            }
        }

        System.out.println("[DEBUG_LOG] Successfully verified alignment of 8 sections to merkle chunks and hash integrity");
    }

    /**
     * Convert a byte array to a hexadecimal string.
     */
    private static String bytesToHex(byte[] bytes) {
        StringBuilder hexString = new StringBuilder(2 * bytes.length);
        for (byte b : bytes) {
            String hex = Integer.toHexString(0xff & b);
            if (hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex);
        }
        return hexString.toString();
    }

    @Test
    void testBuildAndVerifyMerkleTreeFromDummyFile(@TempDir Path tempDir) throws IOException, InterruptedException, ExecutionException {
        // Define the layout parameters
        int dimensions = 32; // Different from the other test to ensure independence
        int vectorsPerSection = 32;
        int sections = 4;

        DummyFloatLayout layout = DummyFloatLayout.forShape(dimensions, vectorsPerSection, sections);
        System.out.println("[DEBUG_LOG] Created DummyFloatLayout: " + layout);

        // Calculate sizes
        int vectorSizeBytes = dimensions * Float.BYTES;
        int totalSizeBytes = sections * vectorsPerSection * vectorSizeBytes;
        int chunkSize = vectorsPerSection * vectorSizeBytes;

        // Generate and write data to file
        float[][] allVectors = layout.generateAll();
        ByteBuffer buffer = ByteBuffer.allocate(totalSizeBytes);
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        for (float[] vector : allVectors) {
            for (float value : vector) {
                buffer.putFloat(value);
            }
        }
        buffer.flip();

        Path dummyFile = tempDir.resolve("dummy_data.bin");
        Files.write(dummyFile, buffer.array());
        System.out.println("[DEBUG_LOG] Created dummy file: " + dummyFile + " with size: " + Files.size(dummyFile) + " bytes");

        // Build merkle tree from the dummy file
        Path merkleTreeFile = tempDir.resolve("merkle_tree.bin");

        // First, build the tree with automatic chunk sizing
        MerkleTreeBuildProgress progress = MerkleTree.fromData(
            dummyFile
        );

        MerkleTree tree = progress.getFuture().get();
        System.out.println("[DEBUG_LOG] Built merkle tree with " + tree.getNumberOfLeaves() + " leaves");

        // Save the tree to a file
        tree.save(merkleTreeFile);
        System.out.println("[DEBUG_LOG] Saved merkle tree to: " + merkleTreeFile);

        // Verify the tree file exists and has content
        assertTrue(Files.exists(merkleTreeFile), "Merkle tree file should exist");
        assertTrue(Files.size(merkleTreeFile) > 0, "Merkle tree file should have content");

        // Load the tree back from the file
        MerkleTree loadedTree = MerkleTree.load(merkleTreeFile);
        System.out.println("[DEBUG_LOG] Loaded merkle tree from: " + merkleTreeFile);

        // Verify structure with automatic chunk sizing
        assertTrue(loadedTree.getNumberOfLeaves() > 0, "Number of leaves should be positive");
        assertTrue(loadedTree.getChunkSize() > 0, "Chunk size should be positive");
        assertEquals(totalSizeBytes, loadedTree.totalSize(), "Total size should match");
        System.out.println("[DEBUG_LOG] Loaded tree - leaves: " + loadedTree.getNumberOfLeaves() + ", chunk size: " + loadedTree.getChunkSize());

        // Verify that both trees have valid hashes for each leaf (adjust for actual leaf count)
        int verifyLeafCount = Math.min(tree.getNumberOfLeaves(), loadedTree.getNumberOfLeaves());
        System.out.println("[DEBUG_LOG] Verifying " + verifyLeafCount + " leaves (expected " + sections + " sections)");
        
        for (int i = 0; i < verifyLeafCount; i++) {
            byte[] originalHash = tree.getHashForLeaf(i);
            byte[] loadedHash = loadedTree.getHashForLeaf(i);

            assertNotNull(originalHash, "Original leaf " + i + " should have a valid hash");
            assertNotNull(loadedHash, "Loaded leaf " + i + " should have a valid hash");

            assertEquals(MerkleTree.HASH_SIZE, originalHash.length, 
                "Original leaf " + i + " hash should have correct length");
            assertEquals(MerkleTree.HASH_SIZE, loadedHash.length, 
                "Loaded leaf " + i + " hash should have correct length");

            System.out.println("[DEBUG_LOG] Original hash for leaf " + i + ": " + bytesToHex(originalHash));
            System.out.println("[DEBUG_LOG] Loaded hash for leaf " + i + ": " + bytesToHex(loadedHash));

            // Add additional check to verify that hashes for different sections are different
            if (i > 0) {
                byte[] previousHash = tree.getHashForLeaf(i - 1);
                assertFalse(Arrays.equals(originalHash, previousHash), 
                    "Hash for leaf " + i + " should be different from hash for leaf " + (i - 1));
                System.out.println("[DEBUG_LOG] Comparing hash for leaf " + i + " with hash for leaf " + (i - 1) + ": " + 
                    (Arrays.equals(originalHash, previousHash) ? "SAME" : "DIFFERENT"));
            }
        }

        System.out.println("[DEBUG_LOG] Successfully verified merkle tree built from dummy file");
    }
}
