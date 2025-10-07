package io.nosqlbench.command.generate;

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

import io.nosqlbench.command.generate.subcommands.CMD_generate_vectors;
import io.nosqlbench.nbdatatools.api.services.VectorFileIO;
import io.nosqlbench.nbdatatools.api.fileio.VectorFileArray;
import io.nosqlbench.nbdatatools.api.services.FileType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/// Unit tests for the VectorGenerate command
class VectorGenerateTest {

    @TempDir
    Path tempDir;

    private Path outputPath;

    @BeforeEach
    void setUp() {
        outputPath = tempDir.resolve("test_vectors.xvec");
    }

    @AfterEach
    void tearDown() {
        try {
            Files.deleteIfExists(outputPath);
        } catch (Exception e) {
            // Ignore cleanup errors
        }
    }

    /// Test generating float vectors
    @Test
    void testGenerateFloatVectors() throws Exception {
        // Arrange
        String[] args = {
            "--type", "float[]",
            "--dimension", "10",
            "--count", "100",
            "--format", "xvec",
            "--output", outputPath.toString(),
            "--seed", "42"  // Use fixed seed for reproducibility
        };

        CMD_generate_vectors cmd = new CMD_generate_vectors();
        CommandLine commandLine = new CommandLine(cmd);

        // Act
        int exitCode = commandLine.execute(args);

        // Assert
        assertEquals(0, exitCode, "Command should execute successfully");
        assertTrue(Files.exists(outputPath), "Output file should be created");

        // Verify the generated vectors
        VectorFileArray<float[]> vectors = VectorFileIO.randomAccess(FileType.xvec, float[].class, outputPath);
        assertEquals(100, vectors.getSize(), "Should generate 100 vectors");

        // Check dimensions of the first vector
        float[] firstVector = vectors.get(0);
        assertEquals(10, firstVector.length, "Vector should have 10 dimensions");

        // Check that vectors contain valid float values
        for (int i = 0; i < 5; i++) {
            float[] vector = vectors.get(i);
            for (float value : vector) {
                assertTrue(value >= 0.0f && value <= 1.0f, 
                    "Vector values should be within the default range [0.0, 1.0]");
            }
        }
    }

    /// Test generating integer vectors
    @Test
    void testGenerateIntVectors() throws Exception {
        // Arrange
        String[] args = {
            "--type", "int[]",
            "--dimension", "5",
            "--count", "50",
            "--format", "xvec",
            "--output", outputPath.toString(),
            "--int-min", "10",
            "--int-max", "100",
            "--seed", "42"  // Use fixed seed for reproducibility
        };

        CMD_generate_vectors cmd = new CMD_generate_vectors();
        CommandLine commandLine = new CommandLine(cmd);

        // Act
        int exitCode = commandLine.execute(args);

        // Assert
        assertEquals(0, exitCode, "Command should execute successfully");
        assertTrue(Files.exists(outputPath), "Output file should be created");

        // Verify the generated vectors
        VectorFileArray<int[]> vectors = VectorFileIO.randomAccess(FileType.xvec, int[].class, outputPath);
        assertEquals(50, vectors.getSize(), "Should generate 50 vectors");

        // Check dimensions of the first vector
        int[] firstVector = vectors.get(0);
        assertEquals(5, firstVector.length, "Vector should have 5 dimensions");

        // Check that vectors contain valid integer values
        for (int i = 0; i < 5; i++) {
            int[] vector = vectors.get(i);
            for (int value : vector) {
                assertTrue(value >= 10 && value <= 100, 
                    "Vector values should be within the specified range [10, 100]");
            }
        }
    }

    /// Test generating double vectors with custom range
    @Test
    void testGenerateDoubleVectorsWithCustomRange() throws Exception {
        // Arrange
        String[] args = {
            "--type", "double[]",
            "--dimension", "3",
            "--count", "30",
            "--format", "xvec",
            "--output", outputPath.toString(),
            "--min", "-1.0",
            "--max", "1.0",
            "--seed", "42"  // Use fixed seed for reproducibility
        };

        CMD_generate_vectors cmd = new CMD_generate_vectors();
        CommandLine commandLine = new CommandLine(cmd);

        // Act
        int exitCode = commandLine.execute(args);

        // Assert
        assertEquals(0, exitCode, "Command should execute successfully");
        assertTrue(Files.exists(outputPath), "Output file should be created");

        // Verify the generated vectors
        VectorFileArray<double[]> vectors = VectorFileIO.randomAccess(FileType.xvec, double[].class, outputPath);
        assertEquals(30, vectors.getSize(), "Should generate 30 vectors");

        // Check dimensions of the first vector
        double[] firstVector = vectors.get(0);
        assertEquals(3, firstVector.length, "Vector should have 3 dimensions");

        // Check that vectors contain valid double values
        for (int i = 0; i < 5; i++) {
            double[] vector = vectors.get(i);
            for (double value : vector) {
                assertTrue(value >= -1.0 && value <= 1.0, 
                    "Vector values should be within the specified range [-1.0, 1.0]");
            }
        }
    }

    /// Test error handling when output file already exists
    @Test
    void testErrorWhenOutputFileExists() throws Exception {
        // Arrange - create the output file first
        Files.createFile(outputPath);

        String[] args = {
            "--type", "float[]",
            "--dimension", "10",
            "--count", "100",
            "--format", "xvec",
            "--output", outputPath.toString()
        };

        CMD_generate_vectors cmd = new CMD_generate_vectors();
        CommandLine commandLine = new CommandLine(cmd);

        // Act
        int exitCode = commandLine.execute(args);

        // Assert
        assertEquals(1, exitCode, "Command should fail with exit code 1 when file exists");
    }

    /// Test force overwrite when output file already exists
    @Test
    void testForceOverwriteWhenOutputFileExists() throws Exception {
        // Arrange - create the output file first
        Files.createFile(outputPath);

        String[] args = {
            "--type", "float[]",
            "--dimension", "10",
            "--count", "100",
            "--format", "xvec",
            "--output", outputPath.toString(),
            "--force"  // Force overwrite
        };

        CMD_generate_vectors cmd = new CMD_generate_vectors();
        CommandLine commandLine = new CommandLine(cmd);

        // Act
        int exitCode = commandLine.execute(args);

        // Assert
        assertEquals(0, exitCode, "Command should succeed with force option");
        assertTrue(Files.exists(outputPath), "Output file should exist");
    }

    /// Test error handling for invalid vector type
    @Test
    void testErrorForInvalidVectorType() {
        // Arrange
        String[] args = {
            "--type", "invalid[]",  // Invalid type
            "--dimension", "10",
            "--count", "100",
            "--format", "xvec",
            "--output", outputPath.toString()
        };

        CMD_generate_vectors cmd = new CMD_generate_vectors();
        CommandLine commandLine = new CommandLine(cmd);

        // Act
        int exitCode = commandLine.execute(args);

        // Assert
        assertEquals(2, exitCode, "Command should fail with exit code 2 for invalid type");
        assertFalse(Files.exists(outputPath), "Output file should not be created");
    }
}
