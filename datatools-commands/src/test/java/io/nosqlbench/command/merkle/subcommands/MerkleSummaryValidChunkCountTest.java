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
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test to verify that merkle summary shows actual valid chunk counts,
 * not just assumes all chunks are valid.
 */
public class MerkleSummaryValidChunkCountTest {

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
    public void testMerkleRefSummary_AllChunksValid() throws Exception {
        // Create test data with multiple chunks
        Path testFile = createTestFile(tempDir.resolve("ref_test.bin"), 3 * 1024 * 1024); // 3MB = 3 chunks
        
        // Create MerkleRef file (.mref) - all chunks should be valid
        MerkleRef merkleRef = MerkleRefFactory.fromDataSimple(testFile).get();
        Path mrefFile = testFile.resolveSibling(testFile.getFileName() + ".mref");
        ((MerkleDataImpl) merkleRef).save(mrefFile);
        
        try {
            // Run summary command on .mref file
            CMD_merkle_summary summaryCommand = new CMD_merkle_summary();
            outContent.reset();
            boolean success = summaryCommand.execute(List.of(mrefFile), 1024 * 1024, false, false);
            
            assertTrue(success, "Summary command should succeed");
            
            String output = outContent.toString();
            System.setOut(originalOut); // Restore for debug output
            
            // For .mref files, all chunks should be valid
            assertTrue(output.contains("Leaf Nodes: (valid/total)=(3/3)"), 
                "MerkleRef summary should show all 3 chunks as valid: " + output);
            assertTrue(output.contains("MERKLE REFERENCE FILE SUMMARY"),
                "Should identify as reference file: " + output);
                
        } finally {
            if (merkleRef instanceof AutoCloseable) {
                try { ((AutoCloseable) merkleRef).close(); } catch (Exception e) { /* ignore */ }
            }
        }
    }
    
    @Test
    public void testMerkleStateSummary_PartiallyValidChunks() throws Exception {
        // Create test data with multiple chunks
        Path testFile = createTestFile(tempDir.resolve("state_test.bin"), 4 * 1024 * 1024); // 4MB = 4 chunks
        
        // Create MerkleRef first
        MerkleRef merkleRef = MerkleRefFactory.fromDataSimple(testFile).get();
        
        try {
            // Create MerkleState from the ref (starts with no valid chunks)
            Path stateFile = tempDir.resolve("test_state.mrkl");
            MerkleState merkleState = MerkleState.fromRef(merkleRef, stateFile);
            
            try {
                // Validate only chunks 0 and 2 (leaving 1 and 3 invalid)
                byte[] chunk0Data = new byte[1024 * 1024];
                byte[] chunk2Data = new byte[1024 * 1024];
                
                // Read actual chunk data from test file
                byte[] fileData = Files.readAllBytes(testFile);
                System.arraycopy(fileData, 0, chunk0Data, 0, chunk0Data.length);
                System.arraycopy(fileData, 2 * 1024 * 1024, chunk2Data, 0, 
                    Math.min(chunk2Data.length, fileData.length - 2 * 1024 * 1024));
                
                // Validate chunks 0 and 2
                boolean chunk0Valid = merkleState.saveIfValid(0, ByteBuffer.wrap(chunk0Data), data -> {});
                boolean chunk2Valid = merkleState.saveIfValid(2, ByteBuffer.wrap(chunk2Data), data -> {});
                
                assertTrue(chunk0Valid, "Chunk 0 should be valid");
                assertTrue(chunk2Valid, "Chunk 2 should be valid");
                
                // Verify state before summary
                assertEquals(2, merkleState.getValidChunks().cardinality(), "Should have 2 valid chunks");
                assertTrue(merkleState.isValid(0), "Chunk 0 should be valid");
                assertFalse(merkleState.isValid(1), "Chunk 1 should be invalid");
                assertTrue(merkleState.isValid(2), "Chunk 2 should be valid");
                assertFalse(merkleState.isValid(3), "Chunk 3 should be invalid");
                
                merkleState.flush();
                
            } finally {
                merkleState.close();
            }
            
            // Run summary command on .mrkl file
            CMD_merkle_summary summaryCommand = new CMD_merkle_summary();
            outContent.reset();
            boolean success = summaryCommand.execute(List.of(stateFile), 1024 * 1024, false, false);
            
            assertTrue(success, "Summary command should succeed");
            
            String output = outContent.toString();
            System.setOut(originalOut); // Restore for debug output
            
            System.out.println("=== MERKLE STATE SUMMARY OUTPUT ===");
            System.out.println(output);
            System.out.println("=== END OUTPUT ===");
            
            // For .mrkl files, should show actual valid chunk count (2 out of 4)
            assertTrue(output.contains("Leaf Nodes: (valid/total)=(2/4)"), 
                "MerkleState summary should show 2 out of 4 chunks as valid, but got: " + output);
            assertTrue(output.contains("MERKLE TREE FILE SUMMARY"),
                "Should identify as tree file (state): " + output);
                
        } finally {
            if (merkleRef instanceof AutoCloseable) {
                try { ((AutoCloseable) merkleRef).close(); } catch (Exception e) { /* ignore */ }
            }
        }
    }
    
    @Test
    public void testMerkleStateSummary_NoValidChunks() throws Exception {
        // Create test data
        Path testFile = createTestFile(tempDir.resolve("empty_state_test.bin"), 2 * 1024 * 1024); // 2MB = 2 chunks
        
        // Create MerkleRef first
        MerkleRef merkleRef = MerkleRefFactory.fromDataSimple(testFile).get();
        
        try {
            // Create MerkleState from the ref (starts with no valid chunks)
            Path stateFile = tempDir.resolve("empty_state.mrkl");
            MerkleState merkleState = MerkleState.fromRef(merkleRef, stateFile);
            
            try {
                // Don't validate any chunks - leave them all invalid
                assertEquals(0, merkleState.getValidChunks().cardinality(), "Should have 0 valid chunks");
                assertFalse(merkleState.isValid(0), "Chunk 0 should be invalid");
                assertFalse(merkleState.isValid(1), "Chunk 1 should be invalid");
                
                merkleState.flush();
                
            } finally {
                merkleState.close();
            }
            
            // Run summary command on .mrkl file
            CMD_merkle_summary summaryCommand = new CMD_merkle_summary();
            outContent.reset();
            boolean success = summaryCommand.execute(List.of(stateFile), 1024 * 1024, false, false);
            
            assertTrue(success, "Summary command should succeed");
            
            String output = outContent.toString();
            System.setOut(originalOut); // Restore for debug output
            
            // For .mrkl files with no validated chunks, should show 0 out of total
            assertTrue(output.contains("Leaf Nodes: (valid/total)=(0/2)"), 
                "MerkleState summary should show 0 out of 2 chunks as valid, but got: " + output);
                
        } finally {
            if (merkleRef instanceof AutoCloseable) {
                try { ((AutoCloseable) merkleRef).close(); } catch (Exception e) { /* ignore */ }
            }
        }
    }

    private Path createTestFile(Path filePath, int size) throws IOException {
        byte[] data = new byte[size];
        Random random = new Random(42); // Fixed seed for reproducible content
        random.nextBytes(data);
        Files.write(filePath, data);
        return filePath;
    }
}