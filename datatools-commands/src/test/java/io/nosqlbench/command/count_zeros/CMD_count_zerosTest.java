package io.nosqlbench.command.count_zeros;

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

import io.nosqlbench.command.analyze.subcommands.count_zeros.CMD_count_zeros;
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

public class CMD_count_zerosTest {

    @TempDir
    Path tempDir;

    @Test
    public void testCountZerosInFvecFile() throws IOException {
        // Create a test fvec file with known zero vectors
        Path fvecFile = createTestFvecFile(10, 3); // 10 vectors, 3 zero vectors
        
        // Capture stdout to verify output
        ByteArrayOutputStream outContent = new ByteArrayOutputStream();
        PrintStream originalOut = System.out;
        System.setOut(new PrintStream(outContent));
        
        try {
            // Run the command
            CMD_count_zeros cmd = new CMD_count_zeros();
            int exitCode = new CommandLine(cmd).execute(fvecFile.toString());
            
            // Verify exit code
            assertEquals(0, exitCode, "Command should exit with code 0");
            
            // Verify output contains the correct summary
            String output = outContent.toString();
            assertTrue(output.contains("3 zero vectors out of 10 total vectors"), 
                "Output should contain correct zero vector count");
        } finally {
            System.setOut(originalOut);
        }
    }
    
    @Test
    public void testCountZerosInIvecFile() throws IOException {
        // Create a test ivec file with known zero vectors
        Path ivecFile = createTestIvecFile(10, 4); // 10 vectors, 4 zero vectors
        
        // Capture stdout to verify output
        ByteArrayOutputStream outContent = new ByteArrayOutputStream();
        PrintStream originalOut = System.out;
        System.setOut(new PrintStream(outContent));
        
        try {
            // Run the command
            CMD_count_zeros cmd = new CMD_count_zeros();
            int exitCode = new CommandLine(cmd).execute(ivecFile.toString());
            
            // Verify exit code
            assertEquals(0, exitCode, "Command should exit with code 0");
            
            // Verify output contains the correct summary
            String output = outContent.toString();
            assertTrue(output.contains("4 zero vectors out of 10 total vectors"), 
                "Output should contain correct zero vector count");
        } finally {
            System.setOut(originalOut);
        }
    }
    
    @Test
    public void testCountZerosInMultipleFiles() throws IOException {
        // Create test files with known zero vectors
        Path fvecFile = createTestFvecFile(10, 3); // 10 vectors, 3 zero vectors
        Path ivecFile = createTestIvecFile(10, 4); // 10 vectors, 4 zero vectors
        
        // Capture stdout to verify output
        ByteArrayOutputStream outContent = new ByteArrayOutputStream();
        PrintStream originalOut = System.out;
        System.setOut(new PrintStream(outContent));
        
        try {
            // Run the command with multiple files
            CMD_count_zeros cmd = new CMD_count_zeros();
            int exitCode = new CommandLine(cmd).execute(fvecFile.toString(), ivecFile.toString());
            
            // Verify exit code
            assertEquals(0, exitCode, "Command should exit with code 0");
            
            // Verify output contains the correct summaries for both files
            String output = outContent.toString();
            assertTrue(output.contains("3 zero vectors out of 10 total vectors"), 
                "Output should contain correct zero vector count for fvec file");
            assertTrue(output.contains("4 zero vectors out of 10 total vectors"), 
                "Output should contain correct zero vector count for ivec file");
        } finally {
            System.setOut(originalOut);
        }
    }
    
    /**
     * Creates a test fvec file with the specified number of vectors and zero vectors
     * @param totalVectors Total number of vectors in the file
     * @param zeroVectors Number of zero vectors in the file
     * @return Path to the created file
     * @throws IOException If an error occurs creating the file
     */
    private Path createTestFvecFile(int totalVectors, int zeroVectors) throws IOException {
        Path testFile = tempDir.resolve("test-file.fvec");
        
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
        Path testFile = tempDir.resolve("test-file.ivec");
        
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