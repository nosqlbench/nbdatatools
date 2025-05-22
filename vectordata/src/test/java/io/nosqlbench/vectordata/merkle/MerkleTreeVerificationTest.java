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
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test class for verifying Merkle tree file verification functionality.
 * Tests both successful verification and detection of corrupted files.
 */
public class MerkleTreeVerificationTest {

    @TempDir
    Path tempDir;

    private static final int CHUNK_SIZE = 1024; // 1KB chunks
    private static final int TEST_DATA_SIZE = CHUNK_SIZE * 16; // 16KB test data
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
     * Creates and saves a test Merkle tree file.
     *
     * @param filePath The path where the file should be saved
     * @return The MerkleTree object that was created
     * @throws IOException If there's an error creating or saving the file
     */
    private MerkleTree createAndSaveMerkleTree(Path filePath) throws IOException {
        byte[] data = createTestData(TEST_DATA_SIZE);
        ByteBuffer buffer = ByteBuffer.wrap(data);

        MerkleTree tree = MerkleTree.fromData(
                buffer,
                CHUNK_SIZE,
                new MerkleRange(0, data.length)
        );

        tree.save(filePath);
        return tree;
    }

    /**
     * Tests that a valid Merkle tree file passes verification.
     */
    @Test
    void testValidFileVerification() throws IOException {
        Path merkleFile = tempDir.resolve("valid.mrkl");

        // Create and save the tree
        MerkleTree originalTree = createAndSaveMerkleTree(merkleFile);

        // The save method should have verified the file automatically
        assertTrue(Files.exists(merkleFile), "Merkle file should exist after save");
        assertFalse(Files.exists(tempDir.resolve("valid.mrkl.corrupted")), 
                   "No corrupted file should exist for valid tree");

        // Verify the tree can be loaded without errors
        MerkleTree loadedTree = MerkleTree.load(merkleFile);

        // Verify tree properties match
        assertEquals(originalTree.getChunkSize(), loadedTree.getChunkSize());
        assertEquals(originalTree.totalSize(), loadedTree.totalSize());

        // Verify the root hashes match (get hash from index 0 which is root)
        assertArrayEquals(originalTree.getHash(0), loadedTree.getHash(0),
                         "Root hashes should match for valid tree");
    }

    /**
     * Note: This test has been updated to reflect the removal of the footer digest.
     * Since the footer digest has been removed, the MerkleTree no longer verifies
     * the integrity of the data in the same way, so corruptions in the data region
     * may not be detected. This test now focuses on verifying that the verifyWrittenMerkleFile
     * method can detect basic structural issues like empty files.
     */
    @Test
    void testDirectVerificationBasics() throws Exception {
        Path merkleFile = tempDir.resolve("direct_verify.mrkl");
        createAndSaveMerkleTree(merkleFile);

        // Use reflection to access the private static verifyWrittenMerkleFile method
        Method verifyMethod = MerkleTree.class.getDeclaredMethod("verifyWrittenMerkleFile", Path.class);
        verifyMethod.setAccessible(true);

        // Test that verification succeeds for a valid file
        assertDoesNotThrow(() -> verifyMethod.invoke(null, merkleFile));

        // Create an empty file
        Path emptyFile = tempDir.resolve("empty.mrkl");
        Files.createFile(emptyFile);

        // Test that verification fails for an empty file
        Exception exception = assertThrows(Exception.class, 
            () -> verifyMethod.invoke(null, emptyFile));

        // Unwrap the InvocationTargetException to get the actual exception
        Throwable cause = exception.getCause();
        assertTrue(cause instanceof IOException, 
                  "Verification should throw IOException for empty file");
        assertTrue(cause.getMessage().contains("File is empty"), 
                  "Exception message should indicate empty file");
    }

    /**
     * Corrupts a file by replacing a byte at the specified position.
     *
     * @param filePath The path to the file to corrupt
     * @param position The position where corruption should occur
     * @param corruptByte The byte value to write at the position
     * @throws IOException If there's an error corrupting the file
     */
    private void corruptFile(Path filePath, long position, byte corruptByte) throws IOException {
        try (FileChannel channel = FileChannel.open(filePath, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            ByteBuffer buffer = ByteBuffer.allocate(1);
            channel.position(position);
            channel.read(buffer);
            buffer.flip();

            // Replace with corrupt byte
            buffer.clear();
            buffer.put(corruptByte);
            buffer.flip();

            channel.position(position);
            channel.write(buffer);
        }
    }

    /**
     * Note: This test has been updated to reflect the removal of the footer digest.
     * Since the footer digest has been removed, the MerkleTree no longer verifies
     * the integrity of the data in the same way, so corruptions in the data region
     * may not be detected. This test now focuses on verifying that a valid file
     * can be loaded successfully.
     */
    @Test
    void testValidFileLoading() throws IOException {
        // Create a valid Merkle tree file
        Path merkleFile = tempDir.resolve("valid_file.mrkl");

        // Create and save a valid Merkle tree
        byte[] data = createTestData(TEST_DATA_SIZE);
        ByteBuffer buffer = ByteBuffer.wrap(data);
        MerkleTree tree = MerkleTree.fromData(
                buffer,
                CHUNK_SIZE,
                new MerkleRange(0, data.length)
        );

        // Save the tree
        tree.save(merkleFile);

        // Verify we can load it successfully
        assertDoesNotThrow(() -> MerkleTree.load(merkleFile));

        // Load the tree and verify its properties
        MerkleTree loadedTree = MerkleTree.load(merkleFile);
        assertEquals(tree.getChunkSize(), loadedTree.getChunkSize(), "Chunk size should match");
        assertEquals(tree.totalSize(), loadedTree.totalSize(), "Total size should match");
    }

    /**
     * Note: This test has been updated to reflect the removal of the footer digest.
     * Since the footer digest has been removed, the MerkleTree no longer verifies
     * the integrity of the data in the same way, so corruptions in the data region
     * may not be detected. This test now focuses on verifying that corruptions in
     * the footer are still detected.
     */
    @Test
    void testFooterCorruption() throws IOException {
        Path merkleFile = tempDir.resolve("corrupt_footer.mrkl");
        createAndSaveMerkleTree(merkleFile);

        // Get the file size
        long fileSize = Files.size(merkleFile);

        // Calculate corruption position for the footer length byte
        long corruptPosition = fileSize - 1;

        // Create a backup before corruption
        Path backupFile = tempDir.resolve("backup_footer.mrkl");
        Files.copy(merkleFile, backupFile, StandardCopyOption.REPLACE_EXISTING);

        // Corrupt the file's footer length byte
        corruptFile(merkleFile, corruptPosition, (byte) 0xFF);

        // Try to load the corrupted file, which should throw
        // When the footer length byte is corrupted with 0xFF, it's interpreted as -1,
        // which causes an IllegalArgumentException when trying to allocate a ByteBuffer
        Exception exception = assertThrows(IllegalArgumentException.class, 
            () -> MerkleTree.load(merkleFile));

        // We expect a specific error message about negative capacity
        assertTrue(exception.getMessage().contains("capacity < 0"),
                  "Exception should indicate negative capacity issue for corrupted footer length byte");
    }

    /**
     * Note: This test has been updated to reflect the removal of the footer digest.
     * Since the footer digest has been removed, the MerkleTree no longer verifies
     * the integrity of the data in the same way, so corruptions in the data region
     * may not be detected. This test now focuses on verifying that the rename-on-corruption
     * behavior works when there's a structural issue with the file.
     */
    @Test
    void testFileRenamedOnStructuralCorruption() throws Exception {
        Path merkleFile = tempDir.resolve("to_be_renamed.mrkl");
        Path expectedCorruptedFile = tempDir.resolve("to_be_renamed.mrkl.corrupted");

        // Create a valid Merkle tree
        byte[] data = createTestData(TEST_DATA_SIZE);
        ByteBuffer buffer = ByteBuffer.wrap(data);
        MerkleTree tree = MerkleTree.fromData(
                buffer,
                CHUNK_SIZE,
                new MerkleRange(0, data.length)
        );

        // Save the tree (will be valid)
        tree.save(merkleFile);

        // Truncate the file to create a structural issue
        try (FileChannel channel = FileChannel.open(merkleFile, StandardOpenOption.WRITE)) {
            // Truncate to remove the footer
            channel.truncate(Files.size(merkleFile) - 5);
        }

        // Create a new tree with the same data to use for testing
        final MerkleTree finalTree = MerkleTree.fromData(
                ByteBuffer.wrap(data),
                CHUNK_SIZE,
                new MerkleRange(0, data.length)
        );

        // Save and expect failure
        try {
            finalTree.save(merkleFile);
            fail("Save should have failed due to structural corruption");
        } catch (IOException e) {
            // Expected - now check the file has been renamed
            assertFalse(Files.exists(merkleFile), "Original file should have been moved");
            assertTrue(Files.exists(expectedCorruptedFile), "Corrupted file should exist with .corrupted extension");
            // The error message could vary, but should indicate some kind of issue
            assertTrue(e.getMessage().contains("failed") || e.getMessage().contains("error") || 
                       e.getMessage().contains("invalid") || e.getMessage().contains("unexpected"),
                      "Exception message should indicate an issue with the file");
        }
    }

    /**
     * Tests that an empty file fails verification properly.
     */
    @Test
    void testEmptyFileVerification() throws Exception {
        Path emptyFile = tempDir.resolve("empty.mrkl");

        // Create an empty file
        Files.createFile(emptyFile);

        // Use reflection to access the private verifyWrittenMerkleFile method
        Method verifyMethod = MerkleTree.class.getDeclaredMethod("verifyWrittenMerkleFile", Path.class);
        verifyMethod.setAccessible(true);

        // Verify that the method throws an IOException for an empty file
        Exception exception = assertThrows(Exception.class, 
            () -> verifyMethod.invoke(null, emptyFile));

        // Unwrap the InvocationTargetException to get the actual exception
        Throwable cause = exception.getCause();
        assertTrue(cause instanceof IOException, 
                  "Verification should throw IOException for empty file");
        assertTrue(cause.getMessage().contains("File is empty"),
                  "Exception message should indicate empty file");
    }

    /**
     * Tests that a truncated file (with incomplete footer) fails verification properly.
     */
    @Test
    void testTruncatedFileVerification() throws IOException, Exception {
        Path merkleFile = tempDir.resolve("truncated.mrkl");
        createAndSaveMerkleTree(merkleFile);

        // Get the file size
        long fileSize = Files.size(merkleFile);

        // Truncate the file to remove part of the footer
        try (FileChannel channel = FileChannel.open(merkleFile, StandardOpenOption.WRITE)) {
            channel.truncate(fileSize - 5); // Remove last 5 bytes
        }

        // Use reflection to access the private verifyWrittenMerkleFile method
        Method verifyMethod = MerkleTree.class.getDeclaredMethod("verifyWrittenMerkleFile", Path.class);
        verifyMethod.setAccessible(true);

        // Verify that the method throws an IOException for a truncated file
        Exception exception = assertThrows(Exception.class, 
            () -> verifyMethod.invoke(null, merkleFile));

        // Unwrap the InvocationTargetException to get the actual exception
        Throwable cause = exception.getCause();
        assertTrue(cause instanceof IOException, 
                  "Verification should throw IOException for truncated file");
    }
}
