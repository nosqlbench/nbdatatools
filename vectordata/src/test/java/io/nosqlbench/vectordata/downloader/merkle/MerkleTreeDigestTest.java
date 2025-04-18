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


import io.nosqlbench.vectordata.merkle.MerkleRange;
import io.nosqlbench.vectordata.merkle.MerkleTree;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.junit.jupiter.api.Assertions.*;

class MerkleTreeDigestTest {

    @TempDir
    Path tempDir;

    private static final int CHUNK_SIZE = 16; // Small chunk size for testing
    private static final int TEST_DATA_SIZE = 128; // Multiple of chunk size

    /**
     * Creates test data with a specific pattern
     */
    private byte[] createTestData(int size) {
        byte[] data = new byte[size];
        for (int i = 0; i < size; i++) {
            data[i] = (byte) (i % 256);
        }
        return data;
    }

    /**
     * Creates a test file with the given data
     */
    private Path createTestFile(byte[] data) throws IOException {
        Path file = tempDir.resolve("test.dat");
        try (FileChannel channel = FileChannel.open(file,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            channel.write(ByteBuffer.wrap(data));
        }
        return file;
    }

    @Test
    void testSaveAndLoadWithDigest() throws IOException {
        // Create test data
        byte[] data = createTestData(TEST_DATA_SIZE);
        ByteBuffer buffer = ByteBuffer.wrap(data);

        // Create a merkle tree
        MerkleTree tree = MerkleTree.fromData(
                buffer,
                CHUNK_SIZE,
                new MerkleRange(0, data.length)
        );

        // Save the tree to a file
        Path merkleFile = tempDir.resolve("test.mrkl");
        tree.save(merkleFile);

        // Load the tree from the file
        MerkleTree loadedTree = MerkleTree.load(merkleFile);

        // Verify the loaded tree has the same properties
        assertEquals(tree.chunkSize(), loadedTree.chunkSize());
        assertEquals(tree.totalSize(), loadedTree.totalSize());
        assertEquals(tree.getNumberOfLeaves(), loadedTree.getNumberOfLeaves());
        assertArrayEquals(tree.root().hash(), loadedTree.root().hash());
    }

    @Test
    void testDigestVerification() throws IOException {
        // Create test data
        byte[] data = createTestData(TEST_DATA_SIZE);
        ByteBuffer buffer = ByteBuffer.wrap(data);

        // Create a merkle tree
        MerkleTree tree = MerkleTree.fromData(
                buffer,
                CHUNK_SIZE,
                new MerkleRange(0, data.length)
        );

        // Save the tree to a file
        Path merkleFile = tempDir.resolve("test.mrkl");
        tree.save(merkleFile);

        // Get the file size to determine where the footer starts
        long fileSize = Files.size(merkleFile);

        // Read the footer length (last byte)
        byte footerLength;
        try (FileChannel channel = FileChannel.open(merkleFile, StandardOpenOption.READ)) {
            ByteBuffer lengthBuffer = ByteBuffer.allocate(1);
            channel.position(fileSize - 1);
            channel.read(lengthBuffer);
            lengthBuffer.flip();
            footerLength = lengthBuffer.get();
        }

        // Calculate where the tree data ends and footer begins
        long treeDataSize = fileSize - footerLength;

        // Corrupt the file by modifying a leaf hash
        try (FileChannel channel = FileChannel.open(merkleFile, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            // Modify the first byte of the first leaf hash
            channel.position(0);
            byte[] corruptByte = new byte[1];
            channel.read(ByteBuffer.wrap(corruptByte));
            corruptByte[0] = (byte) ~corruptByte[0]; // Flip bits
            channel.position(0);
            channel.write(ByteBuffer.wrap(corruptByte));
        }

        // Try to load the corrupted tree
        assertThrows(IOException.class, () -> MerkleTree.load(merkleFile),
                "Loading a corrupted merkle tree should throw an exception");
    }

    @Test
    void testCreateEmptyTreeLike() throws IOException {
        // Create test data
        byte[] data = createTestData(TEST_DATA_SIZE);
        ByteBuffer buffer = ByteBuffer.wrap(data);

        // Create a merkle tree
        MerkleTree tree = MerkleTree.fromData(
                buffer,
                CHUNK_SIZE,
                new MerkleRange(0, data.length)
        );

        // Save the tree to a file
        Path merkleFile = tempDir.resolve("test.mrkl");
        tree.save(merkleFile);

        // Create an empty tree like the original
        Path emptyMerkleFile = tempDir.resolve("empty.mrkl");
        MerkleTree.createEmptyTreeLike(merkleFile, emptyMerkleFile);

        // Manually read the empty tree file to verify its structure
        long fileSize = Files.size(emptyMerkleFile);

        // Read the footer length (last byte)
        byte footerLength;
        try (FileChannel channel = FileChannel.open(emptyMerkleFile, StandardOpenOption.READ)) {
            ByteBuffer lengthBuffer = ByteBuffer.allocate(1);
            channel.position(fileSize - 1);
            channel.read(lengthBuffer);
            lengthBuffer.flip();
            footerLength = lengthBuffer.get();
        }

        // Read the footer
        ByteBuffer footerBuffer = ByteBuffer.allocate(footerLength);
        try (FileChannel channel = FileChannel.open(emptyMerkleFile, StandardOpenOption.READ)) {
            channel.position(fileSize - footerLength);
            channel.read(footerBuffer);
            footerBuffer.flip();
        }

        // Extract chunk size and total size from the footer
        long chunkSize = footerBuffer.getLong();
        long totalSize = footerBuffer.getLong();

        // Verify the empty tree has the same properties
        assertEquals(tree.chunkSize(), chunkSize);
        assertEquals(tree.totalSize(), totalSize);

        // Calculate number of leaves
        int numLeaves = (int)((totalSize + chunkSize - 1) / chunkSize);
        assertEquals(tree.getNumberOfLeaves(), numLeaves);
    }
}
