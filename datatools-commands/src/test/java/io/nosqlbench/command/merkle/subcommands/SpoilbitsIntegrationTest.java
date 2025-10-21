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
import io.nosqlbench.vectordata.merklev2.MerkleRefBuildProgress;
import io.nosqlbench.vectordata.merklev2.MerkleRefFactory;
import io.nosqlbench.vectordata.merklev2.MerkleState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.BitSet;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class SpoilbitsIntegrationTest {

    @TempDir
    Path tempDir;
    
    private Path testDataFile;
    private Path mrklFile;

    @BeforeEach
    void setUp() throws Exception {
        testDataFile = tempDir.resolve("test_data.bin");
        mrklFile = tempDir.resolve("test_state.mrkl");
        
        // Create test data file with enough content for multiple chunks (2MB)
        byte[] testData = new byte[2 * 1024 * 1024];
        new Random(42).nextBytes(testData);
        Files.write(testDataFile, testData);
        
        // Create merkle reference file
        MerkleRefBuildProgress progress = MerkleRefFactory.fromData(testDataFile);
        MerkleDataImpl merkleRef = progress.getFuture().get();
        
        // Save as .mref first, then copy to .mrkl for state file testing
        Path mrefFile = tempDir.resolve("test_ref.mref");
        merkleRef.save(mrefFile);
        
        // Create a state file from the reference (all chunks start as invalid)
        MerkleState state = MerkleState.fromRef(merkleRef, mrklFile);
        
        // For testing spoilbits, we need some chunks marked as valid
        // Since we can't easily mark chunks as valid through the normal API,
        // let's create a modified BitSet directly
        createMrklFileWithValidChunks(mrklFile, merkleRef);
        
        state.close();
        merkleRef.close();
    }

    /**
     * Creates a .mrkl file with some chunks marked as valid for testing spoilbits.
     */
    private void createMrklFileWithValidChunks(Path mrklFile, MerkleDataImpl merkleRef) throws Exception {
        // Get the shape and create a BitSet with all chunks marked as valid
        var shape = merkleRef.getShape();
        BitSet allValidChunks = new BitSet(shape.getTotalChunks());
        allValidChunks.set(0, shape.getTotalChunks()); // Mark all chunks as valid
        
        // Get all hashes from the reference
        byte[][] hashes = new byte[shape.getNodeCount()][];
        for (int i = 0; i < shape.getTotalChunks(); i++) {
            hashes[shape.getOffset() + i] = merkleRef.getHashForLeaf(i);
        }
        
        // Create a new MerkleDataImpl with all chunks marked as valid
        MerkleDataImpl stateWithValidChunks = MerkleDataImpl.createFromHashesAndBitSet(shape, hashes, allValidChunks);
        stateWithValidChunks.save(mrklFile);
        stateWithValidChunks.close();
    }

    @Test
    void testSpoilbitsActuallyModifiesBitset() throws Exception {
        // Capture output
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PrintStream originalOut = System.out;
        System.setOut(new PrintStream(outputStream));
        
        try {
            // First, verify all chunks are valid before spoiling
            CMD_merkle_summary summaryBefore = new CMD_merkle_summary();
            summaryBefore.displayMerkleSummary(mrklFile);
            String outputBefore = outputStream.toString();
            assertTrue(outputBefore.contains("Leaf Nodes: (valid/total)=(2/2)"), 
                      "Should show all chunks as valid before spoiling");
            
            outputStream.reset();
            
            // Run spoilbits to invalidate 50% of chunks
            CMD_merkle_spoilbits spoilbits = new CMD_merkle_spoilbits();
            CommandLine cmd = new CommandLine(spoilbits);
            String[] args = {mrklFile.toString(), "--percentage", "50", "--force"};
            int result = cmd.execute(args);
            
            assertEquals(0, result, "Spoilbits command should succeed");
            
            // Now check the summary again to see if chunks were actually spoiled
            CMD_merkle_summary summaryAfter = new CMD_merkle_summary();
            summaryAfter.displayMerkleSummary(mrklFile);
            String outputAfter = outputStream.toString();
            
            // Should show fewer valid chunks after spoiling
            assertTrue(outputAfter.contains("Leaf Nodes: (valid/total)=(1/2)") || 
                      outputAfter.contains("Leaf Nodes: (valid/total)=(0/2)"), 
                      "Should show fewer valid chunks after spoiling. Output: " + outputAfter);
            
        } finally {
            System.setOut(originalOut);
        }
    }

    @Test 
    void testSpoilbitsWithRange() throws Exception {
        // Test range-based spoiling
        CMD_merkle_spoilbits spoilbits = new CMD_merkle_spoilbits();
        CommandLine cmd = new CommandLine(spoilbits);
        String[] args = {mrklFile.toString(), "--start", "0", "--end", "1", "--force"};
        int result = cmd.execute(args);
        
        assertEquals(0, result, "Range-based spoilbits should succeed");
        
        // Verify the file was modified by loading it and checking the bitset
        MerkleDataImpl modifiedData = MerkleRefFactory.load(mrklFile);
        BitSet validChunks = modifiedData.getValidChunks();
        
        // Chunk 0 should be invalid, chunk 1 should still be valid
        assertFalse(validChunks.get(0), "Chunk 0 should be invalid after range spoiling");
        assertTrue(validChunks.get(1), "Chunk 1 should still be valid after range spoiling (0,1)");
        
        modifiedData.close();
    }
}
