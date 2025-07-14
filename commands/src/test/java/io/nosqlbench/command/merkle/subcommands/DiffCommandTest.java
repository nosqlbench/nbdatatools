package io.nosqlbench.command.merkle.subcommands;

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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link CMD_merkle_diff}.
 */
public class DiffCommandTest {

    @TempDir
    Path tempDir;

    private Path testFile1;
    private Path testFile2;
    private Path testFile3;
    private Path merkleFile1;
    private Path merkleFile2;
    private Path merkleFile3;
    @BeforeEach
    public void setUp() throws IOException {
        // Create test files with known content, including class name to avoid collisions
        testFile1 = tempDir.resolve("DiffCommandTest_test_data1.bin");
        testFile2 = tempDir.resolve("DiffCommandTest_test_data2.bin");
        testFile3 = tempDir.resolve("DiffCommandTest_test_data3.bin");

        // Create first test file (128KB)
        createTestFile(testFile1, 1024 * 128, 0);

        // Create second test file with some differences (128KB)
        createTestFile(testFile2, 1024 * 128, 10);

        // Create third test file with different size (64KB)
        createTestFile(testFile3, 1024 * 64, 0);

        // Create merkle tree files for the test files
        merkleFile1 = createMerkleFile(testFile1);
        merkleFile2 = createMerkleFile(testFile2);
        merkleFile3 = createMerkleFile(testFile3);
    }

    @AfterEach
    public void tearDown() {
        // No cleanup needed
    }

    /**
     * Creates a test file with the specified size filled with pattern data.
     *
     * @param filePath The path where the test file will be created
     * @param size The size of the test file in bytes
     * @param offset An offset to add to each byte value to create differences between files
     * @return The path to the created test file
     * @throws IOException If an I/O error occurs
     */
    private Path createTestFile(Path filePath, int size, int offset) throws IOException {
        byte[] data = new byte[size];
        // Fill with some pattern data
        for (int i = 0; i < size; i++) {
            data[i] = (byte)((i % 256) + offset);
        }
        Files.write(filePath, data);
        return filePath;
    }

    /**
     * Creates a merkle tree file for the specified file.
     *
     * @param filePath The path to the file for which to create a merkle tree
     * @return The path to the created merkle tree file
     * @throws IOException If an I/O error occurs
     */
    private Path createMerkleFile(Path filePath) throws IOException {
        // Read the file into a ByteBuffer
        long fileSize = Files.size(filePath);
        ByteBuffer fileData = ByteBuffer.allocate((int)fileSize);
        Files.newByteChannel(filePath).read(fileData);
        fileData.flip();

        // Create a MerkleTree from the file data
        MerkleTree merkleTree = MerkleTree.fromData(fileData);

        // Save the MerkleTree to a file
        Path merklePath = filePath.resolveSibling(filePath.getFileName() + ".mrkl");
        merkleTree.save(merklePath);

        return merklePath;
    }

    @Test
    public void testExecuteWithIdenticalFiles() throws Exception {
        // Create a DiffCommand instance
        CMD_merkle_diff diffCommand = new CMD_merkle_diff();

        // Test with identical files (comparing a file with itself)
        boolean success = diffCommand.execute(merkleFile1, merkleFile1);

        // Verify the command succeeded
        assertTrue(success, "Diff command should succeed with identical files");
    }

    @Test
    public void testExecuteWithDifferentFiles() throws Exception {
        // Create a DiffCommand instance
        CMD_merkle_diff diffCommand = new CMD_merkle_diff();

        // Test with different files
        boolean success = diffCommand.execute(merkleFile1, merkleFile2);

        // Verify the command succeeded
        assertTrue(success, "Diff command should succeed with different files");
    }

    @Test
    public void testExecuteWithDifferentSizedFiles() throws Exception {
        // Create a DiffCommand instance
        CMD_merkle_diff diffCommand = new CMD_merkle_diff();

        // Test with files of different sizes
        boolean success = diffCommand.execute(merkleFile1, merkleFile3);

        // Verify the command succeeded
        assertTrue(success, "Diff command should succeed with different sized files");
    }

    @Test
    public void testExecuteWithNonExistentFile() throws Exception {
        // Create a DiffCommand instance
        CMD_merkle_diff diffCommand = new CMD_merkle_diff();

        // Test with a non-existent file, including class name to avoid collisions
        Path nonExistentFile = tempDir.resolve("DiffCommandTest_non_existent_file.txt");
        boolean success = diffCommand.execute(merkleFile1, nonExistentFile);

        // Verify the command failed
        assertFalse(success, "Diff command should fail with non-existent file");
    }

    @Test
    public void testCallMethod() throws Exception {
        // Create a DiffCommand instance with test parameters
        CMD_merkle_diff diffCommand = new CMD_merkle_diff();

        // Set the file fields using reflection
        Field file1Field = CMD_merkle_diff.class.getDeclaredField("file1");
        file1Field.setAccessible(true);
        file1Field.set(diffCommand, merkleFile1);

        Field file2Field = CMD_merkle_diff.class.getDeclaredField("file2");
        file2Field.setAccessible(true);
        file2Field.set(diffCommand, merkleFile2);

        // Call the call() method
        Integer result = diffCommand.call();

        // Verify the result is 0 (success)
        assertEquals(0, result.intValue(), "call() method should return 0 for success");
    }
}
