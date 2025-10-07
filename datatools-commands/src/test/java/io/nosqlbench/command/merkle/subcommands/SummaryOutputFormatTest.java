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

import io.nosqlbench.vectordata.merklev2.MerkleDataImpl;
import io.nosqlbench.vectordata.merklev2.MerkleRefFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test to verify the new fractional output format for merkle summary command.
 */
public class SummaryOutputFormatTest {

    @TempDir
    Path tempDir;

    private Path testFile;
    private Path merkleFile;

    // For capturing stdout
    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    private final PrintStream originalOut = System.out;

    @BeforeEach
    public void setUp() throws IOException {
        // Redirect stdout
        System.setOut(new PrintStream(outContent));

        // Create a test file with multiple chunks to get meaningful counts
        testFile = tempDir.resolve("format_test_data.bin");
        createTestFile(testFile, 1024 * 1024 * 3); // 3MB file (should create multiple chunks)

        // Create a merkle tree file for the test file
        merkleFile = createMerkleFile(testFile);
    }

    @AfterEach
    public void tearDown() {
        // Restore original stdout
        System.setOut(originalOut);
    }

    private Path createTestFile(Path filePath, int size) throws IOException {
        byte[] data = new byte[size];
        // Fill with some pattern data
        for (int i = 0; i < size; i++) {
            data[i] = (byte)(i % 256);
        }
        Files.write(filePath, data);
        return filePath;
    }

    private Path createMerkleFile(Path filePath) throws IOException {
        try {
            MerkleDataImpl merkleRef = MerkleRefFactory.fromDataSimple(filePath).get();
            Path merklePath = filePath.resolveSibling(filePath.getFileName() + ".mref");
            merkleRef.save(merklePath);
            return merklePath;
        } catch (InterruptedException | ExecutionException e) {
            throw new IOException("Failed to create merkle file for " + filePath, e);
        }
    }

    @Test
    public void testFractionalOutputFormat() throws Exception {
        // Create a SummaryCommand instance
        CMD_merkle_summary summaryCommand = new CMD_merkle_summary();

        // Clear output
        outContent.reset();

        // Run the summary command
        boolean success = summaryCommand.execute(List.of(merkleFile), 1024 * 1024, false, false);

        // Verify the command succeeded
        assertTrue(success, "Summary command should succeed");

        // Get the output
        String output = outContent.toString();
        
        // Verify the output is not empty
        assertFalse(output.isEmpty(), "Summary output should not be empty");
        
        // Verify the new fractional format is present
        assertTrue(output.contains("Leaf Nodes: (valid/total)="), 
            "Output should contain fractional format for leaf nodes");
        assertTrue(output.contains("Parent Nodes: (valid/total)="), 
            "Output should contain fractional format for parent nodes");
        assertTrue(output.contains("All Nodes: (valid/total)="), 
            "Output should contain fractional format for all nodes");
        
        // Verify the old format is NOT present
        assertFalse(output.contains("Valid Leaf Nodes:"), 
            "Output should not contain old 'Valid Leaf Nodes:' format");
        assertFalse(output.contains("Total Leaf Nodes:"), 
            "Output should not contain old 'Total Leaf Nodes:' format");
        
        // Print the actual output for manual verification
        System.setOut(originalOut);
        System.out.println("=== MERKLE SUMMARY OUTPUT ===");
        System.out.println(output);
        System.out.println("=== END OUTPUT ===");
    }
}