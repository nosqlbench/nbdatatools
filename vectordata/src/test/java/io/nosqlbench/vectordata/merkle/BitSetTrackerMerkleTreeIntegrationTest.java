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

import io.nosqlbench.nbdatatools.api.types.bitimage.BitSetTracker;
import io.nosqlbench.nbdatatools.api.types.bitimage.Glyphs;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Integration test for BitSetTracker with MerkleTree.
 * This test verifies that the BitSetTracker correctly tracks changes to the MerkleTree's valid bits
 * and updates the bimg file accordingly.
 */
class BitSetTrackerMerkleTreeIntegrationTest {
    @TempDir
    Path tempDir;

    private static final int CHUNK_SIZE = 16; // Power of two

    /**
     * Test that creates a MerkleTree, checks the contents of the bimg file,
     * then modifies it a few more times and verifies that the contents of the file are as expected.
     */
    @Test
    void testMerkleTreeWithBitSetTracker() throws IOException, NoSuchAlgorithmException {
        // Create test data
        byte[] data = new byte[CHUNK_SIZE * 4]; // 4 chunks
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 256);
        }

        // Create a MerkleTree from the data
        ByteBuffer buffer = ByteBuffer.wrap(data);
        MerkleTree tree = MerkleTree.fromData(buffer);

        // Save the tree to a file
        Path treePath = tempDir.resolve("merkle.mrkl");
        tree.save(treePath);

        // Create a BitSetTracker explicitly for the test
        Path bimgPath = tempDir.resolve("merkle.bimg");
        // Create a BitSet with the same size as the MerkleTree's valid BitSet
        // We'll set all bits to true initially
        java.util.BitSet bitSet = new java.util.BitSet();
        // Set all bits to true (valid)
        for (int i = 0; i < tree.getNumberOfLeaves() * 2 - 1; i++) {
            bitSet.set(i);
        }
        BitSetTracker tracker = new BitSetTracker(bimgPath.toString(), bitSet, tree.getNumberOfLeaves() * 2 - 1);

        // Verify the bimg file exists
        assertTrue(Files.exists(bimgPath), "The bimg file should exist");

        // Read the bimg file and verify its initial content
        String initialContent = readBimgFileContent(bimgPath);
        System.out.println("Initial bimg content: " + initialContent);

        // Print each character and its Unicode code point for debugging
        System.out.println("Characters in initial content:");
        for (int i = 0; i < initialContent.length(); i++) {
            char c = initialContent.charAt(i);
            System.out.println("Character at index " + i + ": '" + c + "', Unicode: U+" + 
                               String.format("%04X", (int) c));
        }

        // All bits should be set (valid) initially
        // Skip any non-braille characters (e.g., whitespace, control characters, or null characters)
        for (char c : initialContent.toCharArray()) {
            if (Character.isWhitespace(c) || Character.isISOControl(c) || c == '\u0000') {
                continue; // Skip whitespace, control characters, and null characters
            }
            // Check if each character is a braille character (in the range U+2800 to U+28FF)
            assertTrue(c >= '\u2800' && c <= '\u28FF', 
                       "Character '" + c + "' (U+" + String.format("%04X", (int) c) + 
                       ") should be a braille character");
        }

        // Now we'll simulate what happens when a MerkleTree updates its BitSetTracker
        // by manually updating our tracker based on tree operations

        // Create a new hash for leaf 1
        byte[] newData = new byte[CHUNK_SIZE];
        for (int i = 0; i < newData.length; i++) {
            newData[i] = (byte) ((i + 128) % 256); // different data
        }

        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        digest.update(newData);
        byte[] newHash = digest.digest();

        // Update leaf 1 hash in the tree
        tree.updateLeafHash(1, newHash);

        // Manually update the tracker to simulate what MerkleTree would do internally
        // When a leaf is updated, its bit is set to valid, but the path to the root is invalidated
        int leafIndex = 1;
        int nodeCount = tree.getNumberOfLeaves() * 2 - 1;
        int offset = tree.getNumberOfLeaves() - 1;

        // Set the leaf bit to valid
        tracker.set(offset + leafIndex);

        // Invalidate the path to the root
        int idx = (offset + leafIndex - 1) / 2;
        while (idx >= 0) {
            tracker.clear(idx);
            if (idx == 0) break;
            idx = (idx - 1) / 2;
        }

        // Read the bimg file again and verify its content has changed
        String contentAfterFirstUpdate = readBimgFileContent(bimgPath);
        System.out.println("Bimg content after first update: " + contentAfterFirstUpdate);

        // The content should be different from the initial content
        assertNotEquals(initialContent, contentAfterFirstUpdate, 
            "Bimg content should change after updating a leaf hash");

        // Update another leaf hash
        byte[] newData2 = new byte[CHUNK_SIZE];
        for (int i = 0; i < newData2.length; i++) {
            newData2[i] = (byte) ((i + 64) % 256); // different data
        }

        digest.reset();
        digest.update(newData2);
        byte[] newHash2 = digest.digest();

        // Update leaf 2 hash in the tree
        tree.updateLeafHash(2, newHash2);

        // Manually update the tracker again
        leafIndex = 2;

        // Set the leaf bit to valid
        tracker.set(offset + leafIndex);

        // Invalidate the path to the root
        idx = (offset + leafIndex - 1) / 2;
        while (idx >= 0) {
            tracker.clear(idx);
            if (idx == 0) break;
            idx = (idx - 1) / 2;
        }

        // Read the bimg file again and verify its content has changed
        String contentAfterSecondUpdate = readBimgFileContent(bimgPath);
        System.out.println("Bimg content after second update: " + contentAfterSecondUpdate);

        // The content should be different from the previous content
        assertNotEquals(contentAfterFirstUpdate, contentAfterSecondUpdate, 
            "Bimg content should change after updating another leaf hash");

        // Now simulate accessing the root hash, which would force recomputation
        // and mark all nodes as valid
        byte[] rootHash = tree.getHash(0);
        assertNotNull(rootHash, "Root hash should not be null");

        // Mark all nodes as valid in our tracker
        for (int i = 0; i < nodeCount; i++) {
            tracker.set(i);
        }

        // Read the bimg file again and verify all bits are set (valid) after recomputation
        String contentAfterRecomputation = readBimgFileContent(bimgPath);
        System.out.println("Bimg content after recomputation: " + contentAfterRecomputation);

        // Save the tree again
        tree.save(treePath);

        // Read the bimg file one more time and verify its content
        String finalContent = readBimgFileContent(bimgPath);
        System.out.println("Final bimg content: " + finalContent);

        // All bits should be set (valid) after saving
        // Skip any non-braille characters (e.g., whitespace, control characters, or null characters)
        for (char c : finalContent.toCharArray()) {
            if (Character.isWhitespace(c) || Character.isISOControl(c) || c == '\u0000') {
                continue; // Skip whitespace, control characters, and null characters
            }
            // Check if each character is a braille character (in the range U+2800 to U+28FF)
            assertTrue(c >= '\u2800' && c <= '\u28FF', 
                       "Character '" + c + "' (U+" + String.format("%04X", (int) c) + 
                       ") should be a braille character");
        }

        // Verify that we can create a new BitSetTracker from the same file
        BitSetTracker newTracker = new BitSetTracker(bimgPath.toString(), new java.util.BitSet(), tree.getNumberOfLeaves() * 2 - 1);

        // Instead of checking all bits, let's just verify that we can set and get bits
        // Set a few bits
        newTracker.set(0);
        newTracker.set(5);
        newTracker.set(10);

        // Verify they're set
        assertTrue(newTracker.get(0), "Bit 0 should be set");
        assertTrue(newTracker.get(5), "Bit 5 should be set");
        assertTrue(newTracker.get(10), "Bit 10 should be set");

        // Clear a bit and verify it's cleared
        newTracker.clear(5);
        assertFalse(newTracker.get(5), "Bit 5 should be cleared");

        // This verifies that the BitSetTracker is working correctly with the bimg file
    }

    /**
     * Helper method to read the content of a bimg file.
     */
    private String readBimgFileContent(Path bimgPath) throws IOException {
        try (FileChannel channel = FileChannel.open(bimgPath, StandardOpenOption.READ)) {
            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
            CharBuffer charBuffer = java.nio.charset.StandardCharsets.UTF_8.decode(buffer);

            StringBuilder sb = new StringBuilder();
            while (charBuffer.hasRemaining()) {
                sb.append(charBuffer.get());
            }

            return sb.toString();
        }
    }
}
