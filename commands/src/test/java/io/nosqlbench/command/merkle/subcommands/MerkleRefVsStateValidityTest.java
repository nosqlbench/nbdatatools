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
import io.nosqlbench.vectordata.merklev2.MerkleRef;
import io.nosqlbench.vectordata.merklev2.MerkleRefFactory;
import io.nosqlbench.vectordata.merklev2.MerkleState;
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
import java.util.Random;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test that specifically verifies the difference between MerkleRef and MerkleState
 * validity reporting in the summary command.
 */
public class MerkleRefVsStateValidityTest {

    @TempDir
    Path tempDir;

    // For capturing stdout
    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    private final PrintStream originalOut = System.out;

    @BeforeEach
    public void setUp() {
        // Redirect stdout to capture summary output
        System.setOut(new PrintStream(outContent));
    }

    @AfterEach
    public void tearDown() {
        // Restore original stdout
        System.setOut(originalOut);
    }

    @Test
    public void testMrefVsMrkl_ValidityDifference() throws Exception {
        // Create test data
        Path testFile = createTestFile(tempDir.resolve("validity_test.bin"), 3 * 1024 * 1024); // 3MB = 3 chunks
        
        // Create MerkleRef file (.mref)
        MerkleRef merkleRef = MerkleRefFactory.fromDataSimple(testFile).get();
        Path mrefFile = testFile.resolveSibling(testFile.getFileName() + ".mref");
        ((MerkleDataImpl) merkleRef).save(mrefFile);
        
        // Create MerkleState file (.mrkl) from the reference
        Path mrklFile = tempDir.resolve("validity_test.mrkl");
        MerkleState merkleState = MerkleState.fromRef(merkleRef, mrklFile);
        
        try {
            // Only validate the first chunk (chunk 0) in the state
            byte[] chunk0Data = new byte[1024 * 1024];
            byte[] fileData = Files.readAllBytes(testFile);
            System.arraycopy(fileData, 0, chunk0Data, 0, chunk0Data.length);
            
            boolean chunk0Valid = merkleState.saveIfValid(0, java.nio.ByteBuffer.wrap(chunk0Data), data -> {});
            assertTrue(chunk0Valid, "Chunk 0 should be valid");
            
            // Verify state before summaries
            assertEquals(1, merkleState.getValidChunks().cardinality(), "Should have 1 valid chunk");
            merkleState.flush();
            
        } finally {
            merkleState.close();
        }
        
        // Test 1: Summary of .mref file should show all chunks as valid
        CMD_merkle_summary summaryCommand = new CMD_merkle_summary();
        outContent.reset();
        boolean success1 = summaryCommand.execute(List.of(mrefFile), 1024 * 1024, false, false);
        
        assertTrue(success1, "MerkleRef summary should succeed");
        String mrefOutput = outContent.toString();
        
        // Test 2: Summary of .mrkl file should show actual valid chunks
        outContent.reset();
        boolean success2 = summaryCommand.execute(List.of(mrklFile), 1024 * 1024, false, false);
        
        assertTrue(success2, "MerkleState summary should succeed");
        String mrklOutput = outContent.toString();
        
        // Restore stdout for assertions and debug output
        System.setOut(originalOut);
        
        // Verify .mref file shows all chunks as valid
        assertTrue(mrefOutput.contains("Leaf Nodes: (valid/total)=(3/3)"), 
            "MerkleRef file should show all 3 chunks as valid in: " + mrefOutput);
        assertTrue(mrefOutput.contains("MERKLE REFERENCE FILE SUMMARY"),
            "Should be identified as reference file: " + mrefOutput);
            
        // Verify .mrkl file shows actual valid chunks
        assertTrue(mrklOutput.contains("Leaf Nodes: (valid/total)=(1/3)"), 
            "MerkleState file should show 1 out of 3 chunks as valid in: " + mrklOutput);
        assertTrue(mrklOutput.contains("MERKLE TREE FILE SUMMARY"),
            "Should be identified as tree file (state): " + mrklOutput);
        
        System.out.println("=== MERKLE REF (.mref) SUMMARY ===");
        System.out.println(mrefOutput);
        System.out.println("=== MERKLE STATE (.mrkl) SUMMARY ===");
        System.out.println(mrklOutput);
        
        // Clean up
        try {
            if (merkleRef instanceof AutoCloseable) {
                ((AutoCloseable) merkleRef).close();
            }
        } catch (Exception e) { /* ignore */ }
    }

    private Path createTestFile(Path filePath, int size) throws IOException {
        byte[] data = new byte[size];
        Random random = new Random(12345); // Fixed seed for reproducible content
        random.nextBytes(data);
        Files.write(filePath, data);
        return filePath;
    }
}