package io.nosqlbench.command.analyze.subcommands;

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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class Test_CMD_analyze_describe {

    @TempDir
    Path tempDir;

    // For capturing stdout and stderr
    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
    private final PrintStream originalOut = System.out;
    private final PrintStream originalErr = System.err;

    @BeforeEach
    public void setUp() {
        // Redirect stdout and stderr
        System.setOut(new PrintStream(outContent));
        System.setErr(new PrintStream(errContent));
    }

    @AfterEach
    public void tearDown() {
        // Restore original stdout and stderr
        System.setOut(originalOut);
        System.setErr(originalErr);
    }

    @Test
    public void testDescribeFvecFile() throws IOException {
        // Create a test fvec file with known properties
        int totalVectors = 10;
        int dimension = 4;
        Path fvecFile = createTestFvecFile(totalVectors, dimension);

        // Clear previous output
        outContent.reset();

        // Run the command
        CMD_analyze_describe cmd = new CMD_analyze_describe();
        int exitCode = new CommandLine(cmd).execute(fvecFile.toString());

        // Verify exit code
        assertEquals(0, exitCode, "Command should exit with code 0");

        // Verify output contains expected information
        String output = outContent.toString();
        assertTrue(output.contains("File Description:"), "Output should contain file description header");
        assertTrue(output.contains("- File Type: xvec"), "Output should contain file type");
        assertTrue(output.contains("- Data Type: float"), "Output should contain data type");
        assertTrue(output.contains("- Dimensions: " + dimension), "Output should contain correct dimensions");
        assertTrue(output.contains("- Vector Count: " + totalVectors), "Output should contain correct vector count");
        // Verify record size: 4 bytes for dimension + (dimension * 4 bytes for float values)
        int expectedRecordSize = 4 + (dimension * 4);
        assertTrue(output.contains("- Record Size: " + expectedRecordSize + " bytes"), "Output should contain correct record size");
    }

    @Test
    public void testDescribeIvecFile() throws IOException {
        // Create a test ivec file with known properties
        int totalVectors = 15;
        int dimension = 8;
        Path ivecFile = createTestIvecFile(totalVectors, dimension);

        // Clear previous output
        outContent.reset();

        // Run the command
        CMD_analyze_describe cmd = new CMD_analyze_describe();
        int exitCode = new CommandLine(cmd).execute(ivecFile.toString());

        // Verify exit code
        assertEquals(0, exitCode, "Command should exit with code 0");

        // Verify output contains expected information
        String output = outContent.toString();
        assertTrue(output.contains("File Description:"), "Output should contain file description header");
        assertTrue(output.contains("- File Type: xvec"), "Output should contain file type");
        assertTrue(output.contains("- Data Type: int"), "Output should contain data type");
        assertTrue(output.contains("- Dimensions: " + dimension), "Output should contain correct dimensions");
        assertTrue(output.contains("- Vector Count: " + totalVectors), "Output should contain correct vector count");
        // Verify record size: 4 bytes for dimension + (dimension * 4 bytes for int values)
        int expectedRecordSize = 4 + (dimension * 4);
        assertTrue(output.contains("- Record Size: " + expectedRecordSize + " bytes"), "Output should contain correct record size");
    }

    @Test
    public void testDescribeNonExistentFile() {
        // Clear previous output
        outContent.reset();
        errContent.reset();

        // Run the command with a non-existent file
        String nonExistentFileName = this.getClass().getSimpleName()+"__non-existent-file.fvec";
        CMD_analyze_describe cmd = new CMD_analyze_describe();
        int exitCode = new CommandLine(cmd).execute(nonExistentFileName);

        // Verify exit code
        assertEquals(1, exitCode, "Command should exit with code 1 for non-existent file");

        // Note: The error message is logged using Log4j2 logger, not System.err
        // We can't directly verify the logger output in this test
        // But we can verify that no output was produced to System.out
        String output = outContent.toString();
        assertTrue(output.isEmpty() || !output.contains("File Description:"), 
            "No file description should be output for non-existent file");
    }

    /**
     * Creates a test fvec file with the specified number of vectors and dimensions
     * @param totalVectors Total number of vectors in the file
     * @param dimension Number of dimensions per vector
     * @return Path to the created file
     * @throws IOException If an error occurs creating the file
     */
    private Path createTestFvecFile(int totalVectors, int dimension) throws IOException {
        Path testFile = tempDir.resolve(this.getClass().getSimpleName()+"__test-file.fvec");

        try (java.io.FileOutputStream fos = new java.io.FileOutputStream(testFile.toFile())) {
            ByteBuffer buffer = ByteBuffer.allocate(4 + dimension * 4); // dimension + data
            buffer.order(ByteOrder.LITTLE_ENDIAN);

            for (int i = 0; i < totalVectors; i++) {
                buffer.clear();
                buffer.putInt(dimension); // Write dimension

                // Write vector data
                for (int j = 0; j < dimension; j++) {
                    buffer.putFloat(i + j + 1.0f);
                }

                buffer.flip();
                fos.write(buffer.array(), 0, buffer.limit());
            }
        }

        return testFile;
    }

    /**
     * Creates a test ivec file with the specified number of vectors and dimensions
     * @param totalVectors Total number of vectors in the file
     * @param dimension Number of dimensions per vector
     * @return Path to the created file
     * @throws IOException If an error occurs creating the file
     */
    private Path createTestIvecFile(int totalVectors, int dimension) throws IOException {
        Path testFile = tempDir.resolve(this.getClass().getSimpleName()+"__test-file.ivec");

        try (java.io.FileOutputStream fos = new java.io.FileOutputStream(testFile.toFile())) {
            ByteBuffer buffer = ByteBuffer.allocate(4 + dimension * 4); // dimension + data
            buffer.order(ByteOrder.LITTLE_ENDIAN);

            for (int i = 0; i < totalVectors; i++) {
                buffer.clear();
                buffer.putInt(dimension); // Write dimension

                // Write vector data
                for (int j = 0; j < dimension; j++) {
                    buffer.putInt(i + j + 1);
                }

                buffer.flip();
                fos.write(buffer.array(), 0, buffer.limit());
            }
        }

        return testFile;
    }
}
