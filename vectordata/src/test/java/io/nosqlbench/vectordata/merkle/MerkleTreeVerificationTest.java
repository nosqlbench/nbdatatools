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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;


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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

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
     * Directly tests the verifyWrittenMerkleFile method using reflection.
     */
    @Test
    void testDirectVerification() throws Exception {
        Path merkleFile = tempDir.resolve("direct_verify.mrkl");
        createAndSaveMerkleTree(merkleFile);
        
        // Use reflection to access the private static verifyWrittenMerkleFile method
        Method verifyMethod = MerkleTree.class.getDeclaredMethod("verifyWrittenMerkleFile", Path.class);
        verifyMethod.setAccessible(true);
        
        // Test that verification succeeds for a valid file
        assertDoesNotThrow(() -> verifyMethod.invoke(null, merkleFile));
        
        // Corrupt the file
        corruptFile(merkleFile, 0, (byte) 0xFF);
        
        // Test that verification fails for a corrupted file
        Exception exception = assertThrows(Exception.class, 
            () -> verifyMethod.invoke(null, merkleFile));
        
        // Unwrap the InvocationTargetException to get the actual exception
        Throwable cause = exception.getCause();
        assertTrue(cause instanceof IOException, 
                  "Verification should throw IOException for corrupted file");
        assertTrue(cause.getMessage().contains("Verification failed"), 
                  "Exception message should indicate verification failure");
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
     * Tests that the load method detects corruption in Merkle tree files.
     */
    @Test
    void testCorruptionDetectionDuringSave() throws IOException {
        // Create a valid Merkle tree file
        Path merkleFile = tempDir.resolve("valid_then_corrupt.mrkl");
        
        // Create and save a valid Merkle tree
        byte[] data = createTestData(TEST_DATA_SIZE);
        ByteBuffer buffer = ByteBuffer.wrap(data);
        MerkleTree tree = MerkleTree.fromData(
                buffer,
                CHUNK_SIZE,
                new MerkleRange(0, data.length)
        );
        
        // First, save the tree (this should work fine)
        tree.save(merkleFile);
        
        // Verify we can load it successfully
        assertDoesNotThrow(() -> MerkleTree.load(merkleFile));
        
        // Now corrupt the file
        corruptFile(merkleFile, 0, (byte) 0xFF);
        
        // Try to load the corrupted file, which should throw
        Exception exception = assertThrows(IOException.class, 
            () -> MerkleTree.load(merkleFile));
        
        // Verify the exception contains the expected message
        assertTrue(exception.getMessage().contains("Merkle tree digest verification failed"),
                  "Exception should indicate digest verification failure");
    }

    /**
     * Tests that corrupted files cause verification failures in various scenarios.
     */
    @ParameterizedTest
    @CsvSource({
        "0, Beginning of file (tree data)",
        "10, Middle of tree data",
        "-2, Footer length byte"
    })
    void testCorruptionInDifferentPositions(long positionOffset, String description) throws IOException {
        Path merkleFile = tempDir.resolve("corrupt_" + positionOffset + ".mrkl");
        createAndSaveMerkleTree(merkleFile);
        
        // Get the file size
        long fileSize = Files.size(merkleFile);
        
        // Calculate corruption position
        long corruptPosition = positionOffset < 0 ? fileSize + positionOffset : positionOffset;
        
        // Create a backup before corruption
        Path backupFile = tempDir.resolve("backup_" + positionOffset + ".mrkl");
        Files.copy(merkleFile, backupFile, StandardCopyOption.REPLACE_EXISTING);
        
        // Corrupt the file
        corruptFile(merkleFile, corruptPosition, (byte) 0xFF);
        
        // Try to load the corrupted file, which should throw
        Exception exception = assertThrows(IOException.class, 
            () -> MerkleTree.load(merkleFile));
        
        assertTrue(exception.getMessage().contains("Merkle tree digest verification failed"),
                  "Exception should indicate digest verification failure for corruption at " + description);
        
        // Original save method would have moved the corrupted file
        // The load method throws a different exception since it doesn't rename files
    }

    /**
     * Tests that the MerkleTree rename-on-corruption behavior works.
     * This test uses reflection to simulate a verification failure during save().
     */
    @Test
    void testFileRenamedOnCorruption() throws Exception {
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
        
        // Corrupt the file after saving
        corruptFile(merkleFile, 0, (byte) 0xFF);
        
        // Create a new tree with the same data to use for testing
        final MerkleTree finalTree = MerkleTree.fromData(
                ByteBuffer.wrap(data),
                CHUNK_SIZE,
                new MerkleRange(0, data.length)
        );
        
        // Reflection setup for verification testing
        final Method verifyMethod = MerkleTree.class.getDeclaredMethod("verifyWrittenMerkleFile", Path.class);
        verifyMethod.setAccessible(true);
        
        // Save and expect failure
        try {
            finalTree.save(merkleFile);
            fail("Save should have failed due to corruption");
        } catch (IOException e) {
            // Expected - now check the file has been renamed
            assertFalse(Files.exists(merkleFile), "Original file should have been moved");
            assertTrue(Files.exists(expectedCorruptedFile), "Corrupted file should exist with .corrupted extension");
            assertTrue(e.getMessage().contains("Merkle tree digest verification failed"), 
                      "Exception message should indicate verification failure");
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
