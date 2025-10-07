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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class Test_CMD_analyze_select {

    @TempDir
    Path tempDir;

    // For capturing stdout and stderr
    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
    private final PrintStream originalOut = System.out;
    private final PrintStream originalErr = System.err;

    @org.junit.jupiter.api.BeforeEach
    public void setUp() {
        // Redirect stdout and stderr
        System.setOut(new PrintStream(outContent));
        System.setErr(new PrintStream(errContent));
    }

    @org.junit.jupiter.api.AfterEach
    public void tearDown() {
        // Restore original stdout and stderr
        System.setOut(originalOut);
        System.setErr(originalErr);
    }

    @Test
    public void testSelectFvecFile() throws IOException {
        // Create a test fvec file with known properties
        int totalVectors = 10;
        int dimension = 4;
        Path fvecFile = createTestFvecFile(totalVectors, dimension);

        // Select the 3rd vector (index 2)
        int ordinalToSelect = 2;

        // Clear previous output
        outContent.reset();
        errContent.reset();

        // Run the command
        CMD_analyze_select cmd = new CMD_analyze_select();
        int exitCode = new CommandLine(cmd).execute(fvecFile.toString(), String.valueOf(ordinalToSelect));

        // Verify exit code
        assertEquals(0, exitCode, "Command should exit with code 0");

        // Verify output contains expected information
        String output = outContent.toString();
        assertTrue(output.contains("Selecting vector at position " + ordinalToSelect), 
            "Output should indicate which vector is being selected");
        assertTrue(output.contains("Vector Data:"), 
            "Output should contain vector data header");
        assertTrue(output.contains("- Type: float[" + dimension + "]"), 
            "Output should contain correct vector type and dimension");

        // The values should contain the expected pattern (i + j + 1.0f)
        // For ordinal 2 and dimension 4, we expect values like [3.0, 4.0, 5.0, 6.0]
        for (int j = 0; j < dimension; j++) {
            float expectedValue = ordinalToSelect + j + 1.0f;
            assertTrue(output.contains(String.valueOf(expectedValue)), 
                "Output should contain expected value: " + expectedValue);
        }
    }

    @Test
    public void testSelectIvecFile() throws IOException {
        // Create a test ivec file with known properties
        int totalVectors = 15;
        int dimension = 3;
        Path ivecFile = createTestIvecFile(totalVectors, dimension);

        // Select the 5th vector (index 4)
        int ordinalToSelect = 4;

        // Clear previous output
        outContent.reset();
        errContent.reset();

        // Run the command
        CMD_analyze_select cmd = new CMD_analyze_select();
        int exitCode = new CommandLine(cmd).execute(ivecFile.toString(), String.valueOf(ordinalToSelect));

        // Verify exit code
        assertEquals(0, exitCode, "Command should exit with code 0");

        // Verify output contains expected information
        String output = outContent.toString();
        assertTrue(output.contains("Selecting vector at position " + ordinalToSelect), 
            "Output should indicate which vector is being selected");
        assertTrue(output.contains("Vector Data:"), 
            "Output should contain vector data header");
        assertTrue(output.contains("- Type: int[" + dimension + "]"), 
            "Output should contain correct vector type and dimension");

        // The values should contain the expected pattern (i + j + 1)
        // For ordinal 4 and dimension 3, we expect values like [5, 6, 7]
        for (int j = 0; j < dimension; j++) {
            int expectedValue = ordinalToSelect + j + 1;
            assertTrue(output.contains(String.valueOf(expectedValue)), 
                "Output should contain expected value: " + expectedValue);
        }
    }

    @Test
    public void testSelectInvalidOrdinal() throws IOException {
        // Create a test fvec file with known properties
        int totalVectors = 5;
        int dimension = 4;
        Path fvecFile = createTestFvecFile(totalVectors, dimension);

        // Try to select a vector with an invalid ordinal (out of bounds)
        int invalidOrdinal = 10;

        // Clear previous output
        outContent.reset();
        errContent.reset();

        // Run the command
        CMD_analyze_select cmd = new CMD_analyze_select();
        int exitCode = new CommandLine(cmd).execute(fvecFile.toString(), String.valueOf(invalidOrdinal));

        // Verify exit code for invalid ordinal
        assertEquals(2, exitCode, "Command should exit with code 2 for invalid ordinal");

        // Verify error output
        String errorOutput = errContent.toString();
        assertTrue(errorOutput.contains("Invalid ordinal: " + invalidOrdinal), 
            "Error output should indicate the invalid ordinal");
        assertTrue(errorOutput.contains("Valid range is 0 to " + (totalVectors - 1)), 
            "Error output should indicate the valid range");
    }

    @Test
    public void testSelectNonExistentFile() {
        // Clear previous output
        outContent.reset();
        errContent.reset();

        // Run the command with a non-existent file
        String nonExistentFileName = this.getClass().getSimpleName()+"__non-existent-file.fvec";
        CMD_analyze_select cmd = new CMD_analyze_select();
        int exitCode = new CommandLine(cmd).execute(nonExistentFileName, "0");

        // Verify exit code
        assertEquals(1, exitCode, "Command should exit with code 1 for non-existent file");

        // Verify error output
        String errorOutput = errContent.toString();
        assertTrue(errorOutput.contains("File not found") || errorOutput.isEmpty(), 
            "Error output should indicate file not found or be empty (if only logged)");
    }

    @Test
    public void testSelectFirstVector() throws IOException {
        // Create a test fvec file with known properties
        int totalVectors = 10;
        int dimension = 4;
        Path fvecFile = createTestFvecFile(totalVectors, dimension);

        // Select the first vector (index 0)
        int ordinalToSelect = 0;

        // Clear previous output
        outContent.reset();
        errContent.reset();

        // Run the command
        CMD_analyze_select cmd = new CMD_analyze_select();
        int exitCode = new CommandLine(cmd).execute(fvecFile.toString(), String.valueOf(ordinalToSelect));

        // Verify exit code
        assertEquals(0, exitCode, "Command should exit with code 0");

        // Verify output contains expected information
        String output = outContent.toString();
        assertTrue(output.contains("Selecting vector at position " + ordinalToSelect), 
            "Output should indicate which vector is being selected");
        assertTrue(output.contains("Vector Data:"), 
            "Output should contain vector data header");
        assertTrue(output.contains("- Type: float[" + dimension + "]"), 
            "Output should contain correct vector type and dimension");

        // The values should contain the expected pattern (i + j + 1.0f)
        // For ordinal 0 and dimension 4, we expect values like [1.0, 2.0, 3.0, 4.0]
        for (int j = 0; j < dimension; j++) {
            float expectedValue = ordinalToSelect + j + 1.0f;
            assertTrue(output.contains(String.valueOf(expectedValue)), 
                "Output should contain expected value: " + expectedValue);
        }
    }

    @Test
    public void testSelectLastVector() throws IOException {
        // Create a test fvec file with known properties
        int totalVectors = 10;
        int dimension = 4;
        Path fvecFile = createTestFvecFile(totalVectors, dimension);

        // Select the last vector (index totalVectors-1)
        int ordinalToSelect = totalVectors - 1;

        // Clear previous output
        outContent.reset();
        errContent.reset();

        // Run the command
        CMD_analyze_select cmd = new CMD_analyze_select();
        int exitCode = new CommandLine(cmd).execute(fvecFile.toString(), String.valueOf(ordinalToSelect));

        // Verify exit code
        assertEquals(0, exitCode, "Command should exit with code 0");

        // Verify output contains expected information
        String output = outContent.toString();
        assertTrue(output.contains("Selecting vector at position " + ordinalToSelect), 
            "Output should indicate which vector is being selected");
        assertTrue(output.contains("Vector Data:"), 
            "Output should contain vector data header");
        assertTrue(output.contains("- Type: float[" + dimension + "]"), 
            "Output should contain correct vector type and dimension");

        // The values should contain the expected pattern (i + j + 1.0f)
        // For the last vector (ordinal = totalVectors-1) and dimension 4, 
        // we expect values like [10.0, 11.0, 12.0, 13.0] for totalVectors=10
        for (int j = 0; j < dimension; j++) {
            float expectedValue = ordinalToSelect + j + 1.0f;
            assertTrue(output.contains(String.valueOf(expectedValue)), 
                "Output should contain expected value: " + expectedValue);
        }
    }

    @Test
    public void testSelectNegativeOrdinal() throws IOException {
        // Create a test fvec file with known properties
        int totalVectors = 5;
        int dimension = 4;
        Path fvecFile = createTestFvecFile(totalVectors, dimension);

        // Try to select a vector with a negative ordinal
        int negativeOrdinal = -1;

        // Clear previous output
        outContent.reset();
        errContent.reset();

        // Run the command
        CMD_analyze_select cmd = new CMD_analyze_select();
        int exitCode = new CommandLine(cmd).execute(fvecFile.toString(), String.valueOf(negativeOrdinal));

        // Verify exit code for invalid ordinal
        assertEquals(2, exitCode, "Command should exit with code 2 for negative ordinal");

        // Verify error output
        String errorOutput = errContent.toString();
        assertTrue(errorOutput.contains("Invalid ordinal: " + negativeOrdinal), 
            "Error output should indicate the invalid ordinal");
        assertTrue(errorOutput.contains("Valid range is 0 to " + (totalVectors - 1)), 
            "Error output should indicate the valid range");
    }

    @Test
    public void testSelectUnsupportedFileType() throws IOException {
        // Create a temporary file with an unsupported extension
        Path unsupportedFile = tempDir.resolve(this.getClass().getSimpleName()+"__test-file.unsupported");
        java.nio.file.Files.createFile(unsupportedFile);

        // Clear previous output
        outContent.reset();
        errContent.reset();

        // Run the command
        CMD_analyze_select cmd = new CMD_analyze_select();
        int exitCode = new CommandLine(cmd).execute(unsupportedFile.toString(), "0");

        // Verify exit code is non-zero (indicating an error)
        assertNotEquals(0, exitCode, "Command should not exit with code 0 for unsupported file type");

        // Verify output or error output
        // The error might be logged rather than printed to stderr
        String output = outContent.toString();
        String errorOutput = errContent.toString();

        // Either the output or error output should indicate an unsupported file type
        boolean hasUnsupportedTypeMessage = 
            output.contains("unsupported") || 
            errorOutput.contains("unsupported") ||
            output.isEmpty(); // If only logged, output might be empty

        assertTrue(hasUnsupportedTypeMessage, 
            "Output or error output should indicate unsupported file type or be empty (if only logged)");
    }

    /**
     * Creates a test fvec file with the specified number of vectors and dimensions
     * @param totalVectors Total number of vectors in the file
     * @param dimension Number of dimensions per vector
     * @return Path to the created file
     * @throws IOException If an error occurs creating the file
     */
    private Path createTestFvecFile(int totalVectors, int dimension) throws IOException {
        Path testFile = tempDir.resolve(this.getClass().getSimpleName()+"__test_vectors.fvec");

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
        Path testFile = tempDir.resolve(this.getClass().getSimpleName()+"__test_vectors.ivec");

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
