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


import io.nosqlbench.vectordata.merkle.MerkleTree;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link CMD_merkle_summary}.
 */
public class SummaryCommandTest {

    @TempDir
    Path tempDir;

    private Path testFile;
    private Path merkleFile;
    private Path refFile;

    // For capturing stdout and stderr
    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
    private final PrintStream originalOut = System.out;
    private final PrintStream originalErr = System.err;
    @BeforeEach
    public void setUp() throws IOException {
        // Redirect stdout and stderr
        System.setOut(new PrintStream(outContent));
        System.setErr(new PrintStream(errContent));

        // Create a small test file with known content, including class name to avoid collisions
        testFile = tempDir.resolve("SummaryCommandTest_test_data.bin");
        createTestFile(testFile, 1024 * 128); // 128KB file

        // Create a merkle tree file for the test file
        merkleFile = createMerkleFile(testFile);

        // Create a reference file by copying the merkle file
        refFile = testFile.resolveSibling(testFile.getFileName() + ".mref");
        Files.copy(merkleFile, refFile);
    }

    @AfterEach
    public void tearDown() {
        // Restore original stdout and stderr
        System.setOut(originalOut);
        System.setErr(originalErr);
    }

    /**
     * Creates a test file with the specified size filled with pattern data.
     *
     * @param filePath The path where the test file will be created
     * @param size The size of the test file in bytes
     * @return The path to the created test file
     * @throws IOException If an I/O error occurs
     */
    private Path createTestFile(Path filePath, int size) throws IOException {
        byte[] data = new byte[size];
        // Fill with some pattern data
        for (int i = 0; i < size; i++) {
            data[i] = (byte)(i % 256);
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
    public void testExecuteWithOriginalFile() throws Exception {
        // Create a SummaryCommand instance
        CMD_merkle_summary summaryCommand = new CMD_merkle_summary();

        // Test with the original file in dryrun mode
        boolean success = summaryCommand.execute(List.of(testFile), 1024 * 16, false, true);

        // Verify the command succeeded
        assertTrue(success, "Summary command should succeed with original file in dryrun mode");

        // Note: We can't verify logger output directly in this test
        // The SummaryCommand uses Log4j2 logger, not System.out
    }

    @Test
    public void testExecuteWithMerkleFile() throws Exception {
        // Create a SummaryCommand instance
        CMD_merkle_summary summaryCommand = new CMD_merkle_summary();

        // Clear previous output
        outContent.reset();

        // Test with the merkle file directly in dryrun mode
        boolean success = summaryCommand.execute(List.of(merkleFile), 1024 * 16, false, true);

        // Verify the command succeeded
        assertTrue(success, "Summary command should succeed with merkle file in dryrun mode");

        // Note: We can't verify logger output directly in this test
        // The SummaryCommand uses Log4j2 logger, not System.out
    }

    @Test
    public void testExecuteWithReferenceFile() throws Exception {
        // Create a SummaryCommand instance
        CMD_merkle_summary summaryCommand = new CMD_merkle_summary();

        // Clear previous output
        outContent.reset();

        // Test with the reference file directly in dryrun mode
        boolean success = summaryCommand.execute(List.of(refFile), 1024 * 16, false, true);

        // Verify the command succeeded
        assertTrue(success, "Summary command should succeed with reference file in dryrun mode");

        // Note: We can't verify logger output directly in this test
        // The SummaryCommand uses Log4j2 logger, not System.out
    }

    @Test
    public void testExecuteWithNonExistentFile() throws Exception {
        // Create a SummaryCommand instance
        CMD_merkle_summary summaryCommand = new CMD_merkle_summary();

        // Clear previous output
        outContent.reset();
        errContent.reset();

        // Test with a non-existent file, including class name to avoid collisions
        Path nonExistentFile = tempDir.resolve("SummaryCommandTest_non_existent_file.txt");
        boolean success = summaryCommand.execute(List.of(nonExistentFile), 1024 * 16, false, false);

        // Verify the command failed
        assertFalse(success, "Summary command should fail with non-existent file");

        // Note: We can't verify logger output directly in this test
        // The SummaryCommand uses Log4j2 logger, not System.err
    }

    @Test
    public void testExecuteWithActualSummaryDisplay() throws Exception {
        // Create a SummaryCommand instance
        CMD_merkle_summary summaryCommand = new CMD_merkle_summary();

        // Clear previous output
        outContent.reset();

        // Test with the merkle file without dryrun mode to actually display the summary
        boolean success = summaryCommand.execute(List.of(merkleFile), 1024 * 16, false, false);

        // Verify the command succeeded
        assertTrue(success, "Summary command should succeed with merkle file");

        // Verify that some output was produced to System.out
        // The displayMerkleFileSummary method uses System.out.println
        String output = outContent.toString();
        assertFalse(output.isEmpty(), "Summary output should not be empty");
        assertTrue(output.contains("MERKLE TREE FILE SUMMARY"), "Summary should contain the header");
    }

    @Test
    public void testCallMethod() throws Exception {
        // Create a SummaryCommand instance with test parameters
        CMD_merkle_summary summaryCommand = new CMD_merkle_summary();
        // Set the files field using reflection
        java.lang.reflect.Field filesField = CMD_merkle_summary.class.getDeclaredField("files");
        filesField.setAccessible(true);
        filesField.set(summaryCommand, List.of(merkleFile));

        // Call the call() method
        Integer result = summaryCommand.call();

        // Verify the result is 0 (success)
        assertEquals(0, result.intValue(), "call() method should return 0 for success");
    }
}
