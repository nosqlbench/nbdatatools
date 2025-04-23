package io.nosqlbench.nbvectors.commands.generate.commands;

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

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// Tests for the IvecShuffle command
public class IvecShuffleTest {

    @TempDir
    Path tempDir;

    /// Test basic functionality - creates a shuffled ivec file with correct content
    @Test
    void testSuccessfulGeneration() throws IOException {
        // Arrange
        Path outputPath = tempDir.resolve("test_output.ivec");
        String[] args = {
            outputPath.toString(),
            "--interval", "100",
            "--seed", "42"
        };
        
        // Act
        int exitCode = new CommandLine(new IvecShuffle()).execute(args);
        
        // Assert
        assertEquals(0, exitCode, "Command should execute successfully");
        assertTrue(Files.exists(outputPath), "Output file should be created");
        
        // Read and verify file content
        List<Integer> values = readIvecFile(outputPath);
        
        // Check that we have exactly the expected number of values
        assertEquals(100, values.size(), "Should generate exactly 100 values");
        
        // Check that all values in the range 0-99 are present
        Set<Integer> uniqueValues = new HashSet<>(values);
        assertEquals(100, uniqueValues.size(), "All values should be unique");
        for (int i = 0; i < 100; i++) {
            assertTrue(uniqueValues.contains(i), "Value " + i + " should be present");
        }
        
        // Check that the values are not in sequential order (shuffled)
        boolean isOrdered = true;
        for (int i = 0; i < values.size() - 1; i++) {
            if (values.get(i) + 1 != values.get(i + 1)) {
                isOrdered = false;
                break;
            }
        }
        assertFalse(isOrdered, "Values should be shuffled, not in sequential order");
    }
    
    /// Test that using the same seed produces identical shuffled sequences
    @Test
    void testDeterministicShuffle() throws IOException {
        // Arrange
        Path outputPath1 = tempDir.resolve("output1.ivec");
        Path outputPath2 = tempDir.resolve("output2.ivec");
        long seed = 12345;
        
        // Act - Generate two files with the same seed
        new CommandLine(new IvecShuffle()).execute(
            outputPath1.toString(), "--interval", "50", "--seed", String.valueOf(seed));
        new CommandLine(new IvecShuffle()).execute(
            outputPath2.toString(), "--interval", "50", "--seed", String.valueOf(seed));
        
        // Assert
        List<Integer> values1 = readIvecFile(outputPath1);
        List<Integer> values2 = readIvecFile(outputPath2);
        
        assertThat(values1).isEqualTo(values2)
            .withFailMessage("Using the same seed should produce identical shuffled sequences");
    }
    
    /// Test that using different seeds produces different shuffled sequences
    @Test
    void testDifferentSeedsProduceDifferentSequences() throws IOException {
        // Arrange
        Path outputPath1 = tempDir.resolve("seed1.ivec");
        Path outputPath2 = tempDir.resolve("seed2.ivec");
        
        // Act - Generate two files with different seeds
        new CommandLine(new IvecShuffle()).execute(
            outputPath1.toString(), "--interval", "100", "--seed", "1000");
        new CommandLine(new IvecShuffle()).execute(
            outputPath2.toString(), "--interval", "100", "--seed", "2000");
        
        // Assert
        List<Integer> values1 = readIvecFile(outputPath1);
        List<Integer> values2 = readIvecFile(outputPath2);
        
        assertThat(values1).isNotEqualTo(values2)
            .withFailMessage("Different seeds should produce different shuffled sequences");
    }
    
    /// Test that the command refuses to overwrite existing files without the force flag
    @Test
    void testOverwriteProtection() throws IOException {
        // Arrange
        Path outputPath = tempDir.resolve("protected.ivec");
        
        // First execution to create the file
        int firstExitCode = new CommandLine(new IvecShuffle()).execute(
            outputPath.toString(), "--interval", "10", "--seed", "42");
        assertEquals(0, firstExitCode, "First execution should succeed");
        
        // Act - Try to overwrite without force flag
        int secondExitCode = new CommandLine(new IvecShuffle()).execute(
            outputPath.toString(), "--interval", "10", "--seed", "42");
        
        // Assert
        assertEquals(1, secondExitCode, "Should return exit code 1 when file exists without force flag");
    }
    
    /// Test that the force flag allows overwriting existing files
    @Test
    void testForceOverwrite() throws IOException {
        // Arrange
        Path outputPath = tempDir.resolve("forced.ivec");
        
        // First execution
        new CommandLine(new IvecShuffle()).execute(
            outputPath.toString(), "--interval", "10", "--seed", "42");
        List<Integer> firstValues = readIvecFile(outputPath);
        
        // Act - Second execution with force flag and different seed
        int exitCode = new CommandLine(new IvecShuffle()).execute(
            outputPath.toString(), "--interval", "10", "--seed", "999", "--force");
        
        // Assert
        assertEquals(0, exitCode, "Command should execute successfully with force flag");
        List<Integer> secondValues = readIvecFile(outputPath);
        assertThat(secondValues).isNotEqualTo(firstValues)
            .withFailMessage("File should be overwritten with new content");
    }
    
    /// Test that the command creates necessary parent directories
    @Test
    void testDirectoryCreation() {
        // Arrange
        Path nestedPath = tempDir.resolve("nested/dirs/output.ivec");
        assertFalse(Files.exists(nestedPath.getParent()), "Parent directories should not exist yet");
        
        // Act
        int exitCode = new CommandLine(new IvecShuffle()).execute(
            nestedPath.toString(), "--interval", "5", "--seed", "42");
        
        // Assert
        assertEquals(0, exitCode, "Command should execute successfully");
        assertTrue(Files.exists(nestedPath), "Output file should be created");
        assertTrue(Files.exists(nestedPath.getParent()), "Parent directories should be created");
    }
    
    /// Helper method to read ivec file content
    private List<Integer> readIvecFile(Path path) throws IOException {
        List<Integer> values = new ArrayList<>();
        
        try (DataInputStream dis = new DataInputStream(Files.newInputStream(path))) {
            while (dis.available() > 0) {
                int dimension = dis.readInt();
                assertEquals(1, dimension, "Each vector should have dimension of 1");
                int value = dis.readInt();
                values.add(value);
            }
        }
        
        return values;
    }
}
