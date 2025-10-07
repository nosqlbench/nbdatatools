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

public class Test_CMD_analyze_zeros {

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
    public void testCountZerosInFvecFile() throws IOException {
        // Create a test fvec file with known zero vectors
        int totalVectors = 10;
        int zeroVectors = 3;
        Path fvecFile = createTestFvecFile(totalVectors, zeroVectors); // 10 vectors, 3 zero vectors

        // Clear previous output
        outContent.reset();
        errContent.reset();

        // Run the command
        CMD_analyze_zeros cmd = new CMD_analyze_zeros();
        int exitCode = new CommandLine(cmd).execute(fvecFile.toString());

        // Verify exit code
        assertEquals(0, exitCode, "Command should exit with code 0");

        // Verify output contains expected information
        String output = outContent.toString();
        assertTrue(output.contains("Processing file"), 
            "Output should indicate file processing");
        assertTrue(output.contains("Summary for"), 
            "Output should contain summary information");
        assertTrue(output.contains(zeroVectors + " zero vectors out of " + totalVectors), 
            "Output should contain correct zero vector count");

        // Calculate expected percentage
        double expectedPercentage = (zeroVectors * 100.0 / totalVectors);
        String percentageStr = String.format("%.1f%%", expectedPercentage);
        assertTrue(output.contains(percentageStr) || output.contains(String.format("%.2f%%", expectedPercentage)), 
            "Output should contain correct percentage");
    }

    @Test
    public void testCountZerosInIvecFile() throws IOException {
        // Create a test ivec file with known zero vectors
        int totalVectors = 10;
        int zeroVectors = 4;
        Path ivecFile = createTestIvecFile(totalVectors, zeroVectors); // 10 vectors, 4 zero vectors

        // Clear previous output
        outContent.reset();
        errContent.reset();

        // Run the command
        CMD_analyze_zeros cmd = new CMD_analyze_zeros();
        int exitCode = new CommandLine(cmd).execute(ivecFile.toString());

        // Verify exit code
        assertEquals(0, exitCode, "Command should exit with code 0");

        // Verify output contains expected information
        String output = outContent.toString();
        assertTrue(output.contains("Processing file"), 
            "Output should indicate file processing");
        assertTrue(output.contains("Summary for"), 
            "Output should contain summary information");
        assertTrue(output.contains(zeroVectors + " zero vectors out of " + totalVectors), 
            "Output should contain correct zero vector count");

        // Calculate expected percentage
        double expectedPercentage = (zeroVectors * 100.0 / totalVectors);
        String percentageStr = String.format("%.1f%%", expectedPercentage);
        assertTrue(output.contains(percentageStr) || output.contains(String.format("%.2f%%", expectedPercentage)), 
            "Output should contain correct percentage");
    }

    @Test
    public void testCountZerosInMultipleFiles() throws IOException {
        // Create test files with known zero vectors
        int totalVectorsFvec = 10;
        int zeroVectorsFvec = 3;
        Path fvecFile = createTestFvecFile(totalVectorsFvec, zeroVectorsFvec); // 10 vectors, 3 zero vectors

        int totalVectorsIvec = 10;
        int zeroVectorsIvec = 4;
        Path ivecFile = createTestIvecFile(totalVectorsIvec, zeroVectorsIvec); // 10 vectors, 4 zero vectors

        // Clear previous output
        outContent.reset();
        errContent.reset();

        // Run the command with multiple files
        CMD_analyze_zeros cmd = new CMD_analyze_zeros();
        int exitCode = new CommandLine(cmd).execute(fvecFile.toString(), ivecFile.toString());

        // Verify exit code
        assertEquals(0, exitCode, "Command should exit with code 0");

        // Verify output contains expected information
        String output = outContent.toString();
        assertTrue(output.contains("Processing file 1/2"), 
            "Output should indicate processing first file");
        assertTrue(output.contains("Processing file 2/2"), 
            "Output should indicate processing second file");

        // Verify fvec file summary
        assertTrue(output.contains(zeroVectorsFvec + " zero vectors out of " + totalVectorsFvec), 
            "Output should contain correct zero vector count for fvec file");

        // Verify ivec file summary
        assertTrue(output.contains(zeroVectorsIvec + " zero vectors out of " + totalVectorsIvec), 
            "Output should contain correct zero vector count for ivec file");

        // Calculate expected percentages
        double expectedPercentageFvec = (zeroVectorsFvec * 100.0 / totalVectorsFvec);
        double expectedPercentageIvec = (zeroVectorsIvec * 100.0 / totalVectorsIvec);

        // Check for either format of percentage (%.1f%% or %.2f%%)
        boolean hasCorrectPercentages = 
            (output.contains(String.format("%.1f%%", expectedPercentageFvec)) || 
             output.contains(String.format("%.2f%%", expectedPercentageFvec))) &&
            (output.contains(String.format("%.1f%%", expectedPercentageIvec)) || 
             output.contains(String.format("%.2f%%", expectedPercentageIvec)));

        assertTrue(hasCorrectPercentages, "Output should contain correct percentages for both files");
    }

    /**
     * Creates a test fvec file with the specified number of vectors and zero vectors
     * @param totalVectors Total number of vectors in the file
     * @param zeroVectors Number of zero vectors in the file
     * @return Path to the created file
     * @throws IOException If an error occurs creating the file
     */
    private Path createTestFvecFile(int totalVectors, int zeroVectors) throws IOException {
        Path testFile = tempDir.resolve(this.getClass().getSimpleName()+"__test-file.fvec");

        // Each vector has 4 dimensions
        int dimension = 4;

        try (java.io.FileOutputStream fos = new java.io.FileOutputStream(testFile.toFile())) {
            ByteBuffer buffer = ByteBuffer.allocate(4 + dimension * 4); // dimension + data
            buffer.order(ByteOrder.LITTLE_ENDIAN);

            for (int i = 0; i < totalVectors; i++) {
                buffer.clear();
                buffer.putInt(dimension); // Write dimension

                if (i < zeroVectors) {
                    // Write zero vector
                    for (int j = 0; j < dimension; j++) {
                        buffer.putFloat(0.0f);
                    }
                } else {
                    // Write non-zero vector
                    for (int j = 0; j < dimension; j++) {
                        buffer.putFloat(i + j + 1.0f);
                    }
                }

                buffer.flip();
                fos.write(buffer.array(), 0, buffer.limit());
            }
        }

        return testFile;
    }

    /**
     * Creates a test ivec file with the specified number of vectors and zero vectors
     * @param totalVectors Total number of vectors in the file
     * @param zeroVectors Number of zero vectors in the file
     * @return Path to the created file
     * @throws IOException If an error occurs creating the file
     */
    private Path createTestIvecFile(int totalVectors, int zeroVectors) throws IOException {
        Path testFile = tempDir.resolve(this.getClass().getSimpleName()+"__test-file.ivec");

        // Each vector has 4 dimensions
        int dimension = 4;

        try (java.io.FileOutputStream fos = new java.io.FileOutputStream(testFile.toFile())) {
            ByteBuffer buffer = ByteBuffer.allocate(4 + dimension * 4); // dimension + data
            buffer.order(ByteOrder.LITTLE_ENDIAN);

            for (int i = 0; i < totalVectors; i++) {
                buffer.clear();
                buffer.putInt(dimension); // Write dimension

                if (i < zeroVectors) {
                    // Write zero vector
                    for (int j = 0; j < dimension; j++) {
                        buffer.putInt(0);
                    }
                } else {
                    // Write non-zero vector
                    for (int j = 0; j < dimension; j++) {
                        buffer.putInt(i + j + 1);
                    }
                }

                buffer.flip();
                fos.write(buffer.array(), 0, buffer.limit());
            }
        }

        return testFile;
    }
}
