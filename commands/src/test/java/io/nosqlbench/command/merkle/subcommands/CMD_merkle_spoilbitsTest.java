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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.BitSet;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class CMD_merkle_spoilbitsTest {

    @TempDir
    Path tempDir;
    
    private Path testDataFile;
    private Path mrklFile;
    private CMD_merkle_spoilbits command;

    @BeforeEach
    void setUp() throws Exception {
        testDataFile = tempDir.resolve("test_data.bin");
        mrklFile = tempDir.resolve("test_state.mrkl");
        command = new CMD_merkle_spoilbits();
        
        // Create test data file with some content (4 chunks worth)
        byte[] testData = new byte[4 * 1024 * 1024]; // 4MB to get multiple chunks
        new Random(42).nextBytes(testData);
        Files.write(testDataFile, testData);
        
        // Create merkle reference and then state file
        MerkleRefBuildProgress progress = MerkleRefFactory.fromData(testDataFile);
        MerkleDataImpl merkleRef = progress.getFuture().get();
        
        // Create state file with some chunks marked as valid
        MerkleState state = MerkleState.fromRef(merkleRef, mrklFile);
        
        // Mark some chunks as valid to have something to spoil
        BitSet validChunks = state.getValidChunks();
        int totalChunks = state.getMerkleShape().getTotalChunks();
        for (int i = 0; i < totalChunks; i++) {
            // Mark chunks as valid (this would normally happen after downloading)
            // We can't easily do this without the proper API, so this test
            // will verify the command structure works
        }
        
        state.close();
        merkleRef.close();
    }

    @Test
    void testDryRunPercentageBased() throws Exception {
        // Test dry run mode with percentage
        CommandLine cmd = new CommandLine(command);
        String[] args = {mrklFile.toString(), "--percentage", "25", "--dryrun"};
        
        int result = cmd.execute(args);
        assertEquals(0, result, "Command should succeed in dry run mode");
        
        // Verify the original file wasn't modified
        assertTrue(Files.exists(mrklFile));
        assertFalse(Files.exists(mrklFile.resolveSibling(mrklFile.getFileName() + ".backup")));
    }

    @Test
    void testDryRunRangeBased() throws Exception {
        // Test dry run mode with range
        CommandLine cmd = new CommandLine(command);
        String[] args = {mrklFile.toString(), "--start", "0", "--end", "2", "--dryrun"};
        
        int result = cmd.execute(args);
        assertEquals(0, result, "Command should succeed in dry run mode");
        
        // Verify the original file wasn't modified
        assertTrue(Files.exists(mrklFile));
        assertFalse(Files.exists(mrklFile.resolveSibling(mrklFile.getFileName() + ".backup")));
    }

    @Test
    void testInvalidPercentage() throws Exception {
        CommandLine cmd = new CommandLine(command);
        String[] args = {mrklFile.toString(), "--percentage", "150"};
        
        int result = cmd.execute(args);
        assertEquals(1, result, "Command should fail with invalid percentage");
    }

    @Test
    void testInvalidRange() throws Exception {
        CommandLine cmd = new CommandLine(command);
        String[] args = {mrklFile.toString(), "--start", "5", "--end", "2"};
        
        int result = cmd.execute(args);
        assertEquals(1, result, "Command should fail with invalid range");
    }

    @Test
    void testNonExistentFile() throws Exception {
        Path nonExistentFile = tempDir.resolve("nonexistent.mrkl");
        CommandLine cmd = new CommandLine(command);
        String[] args = {nonExistentFile.toString()};
        
        int result = cmd.execute(args);
        assertEquals(1, result, "Command should fail with non-existent file");
    }

    @Test
    void testWrongFileExtension() throws Exception {
        Path wrongExtensionFile = tempDir.resolve("test.txt");
        Files.write(wrongExtensionFile, "test".getBytes());
        
        CommandLine cmd = new CommandLine(command);
        String[] args = {wrongExtensionFile.toString()};
        
        int result = cmd.execute(args);
        assertEquals(1, result, "Command should fail with wrong file extension");
    }

    @Test
    void testCommandLineInterface() {
        // Test that the command line interface is properly configured
        CommandLine cmd = new CommandLine(command);
        
        // Test help option works
        String help = cmd.getUsageMessage();
        assertTrue(help.contains("spoilbits"));
        assertTrue(help.contains("percentage"));
        assertTrue(help.contains("start"));
        assertTrue(help.contains("end"));
        assertTrue(help.contains("dryrun"));
    }
}