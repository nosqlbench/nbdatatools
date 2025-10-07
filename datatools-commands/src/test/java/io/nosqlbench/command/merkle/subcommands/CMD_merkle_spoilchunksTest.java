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

class CMD_merkle_spoilchunksTest {

    @TempDir
    Path tempDir;
    
    private Path testDataFile;
    private Path mrklFile;
    private Path cacheFile;
    private CMD_merkle_spoilchunks command;

    @BeforeEach
    void setUp() throws Exception {
        testDataFile = tempDir.resolve("test_data.bin");
        mrklFile = tempDir.resolve("test_data.mrkl");
        cacheFile = tempDir.resolve("test_data"); // Cache file name without extension
        command = new CMD_merkle_spoilchunks();
        
        // Create test data file with multiple chunks (4MB to get multiple chunks)
        byte[] testData = new byte[4 * 1024 * 1024];
        new Random(42).nextBytes(testData);
        Files.write(testDataFile, testData);
        
        // Create cache file (same data as test file for simplicity)
        Files.write(cacheFile, testData);
        
        // Create merkle reference and then state file
        MerkleRefBuildProgress progress = MerkleRefFactory.fromData(testDataFile);
        MerkleDataImpl merkleRef = progress.getFuture().get();
        
        // Create state file with some chunks marked as valid
        // Copy the .mref to .mrkl for testing (all chunks start as valid in ref file)
        Path mrefFile = tempDir.resolve("test_ref.mref");
        merkleRef.save(mrefFile);
        Files.copy(mrefFile, mrklFile);
        
        merkleRef.close();
    }

    @Test
    void testDryRunPercentageBased() throws Exception {
        CommandLine cmd = new CommandLine(command);
        String[] args = {mrklFile.toString(), "--percentage", "25", "--bytes-to-corrupt", "5", "--dryrun"};
        
        int result = cmd.execute(args);
        assertEquals(0, result, "Command should succeed in dry run mode");
        
        // Verify files weren't modified
        assertTrue(Files.exists(mrklFile));
        assertTrue(Files.exists(cacheFile));
        assertFalse(Files.exists(mrklFile.resolveSibling(mrklFile.getFileName() + ".backup")));
        assertFalse(Files.exists(cacheFile.resolveSibling(cacheFile.getFileName() + ".backup")));
    }

    @Test 
    void testDryRunRangeBased() throws Exception {
        CommandLine cmd = new CommandLine(command);
        String[] args = {mrklFile.toString(), "--start", "0", "--end", "2", "--bytes-to-corrupt", "1-5", "--dryrun"};
        
        int result = cmd.execute(args);
        assertEquals(0, result, "Command should succeed in dry run mode");
        
        // Verify files weren't modified
        assertTrue(Files.exists(mrklFile));
        assertTrue(Files.exists(cacheFile));
    }

    @Test
    void testActualCorruptionFixedBytes() throws Exception {
        // Store original cache data for comparison
        byte[] originalCacheData = Files.readAllBytes(cacheFile);
        
        CommandLine cmd = new CommandLine(command);
        String[] args = {mrklFile.toString(), "--percentage", "50", "--bytes-to-corrupt", "3", "--seed", "12345"};
        
        int result = cmd.execute(args);
        assertEquals(0, result, "Command should succeed");
        
        // Verify backups were created
        assertTrue(Files.exists(mrklFile.resolveSibling(mrklFile.getFileName() + ".backup")));
        assertTrue(Files.exists(cacheFile.resolveSibling(cacheFile.getFileName() + ".backup")));
        
        // Verify cache data was modified
        byte[] modifiedCacheData = Files.readAllBytes(cacheFile);
        assertFalse(java.util.Arrays.equals(originalCacheData, modifiedCacheData), 
                   "Cache data should have been corrupted");
        
        // Verify merkle file was modified
        MerkleDataImpl modifiedMerkleData = MerkleRefFactory.load(mrklFile);
        BitSet validChunks = modifiedMerkleData.getValidChunks();
        int validCount = validChunks.cardinality();
        int totalChunks = modifiedMerkleData.getShape().getTotalChunks();
        
        assertTrue(validCount < totalChunks, "Some chunks should have been invalidated");
        modifiedMerkleData.close();
    }

    @Test
    void testPercentageBasedCorruption() throws Exception {
        CommandLine cmd = new CommandLine(command);
        String[] args = {mrklFile.toString(), "--percentage", "25", "--bytes-to-corrupt", "5%", "--seed", "54321"};
        
        int result = cmd.execute(args);
        assertEquals(0, result, "Command should succeed with percentage-based corruption");
    }

    @Test
    void testRangeBasedCorruption() throws Exception {
        CommandLine cmd = new CommandLine(command);
        String[] args = {mrklFile.toString(), "--start", "0", "--end", "1", "--bytes-to-corrupt", "2..10", "--seed", "67890"};
        
        int result = cmd.execute(args);
        assertEquals(0, result, "Command should succeed with range-based corruption");
    }

    @Test
    void testPercentageRangeCorruption() throws Exception {
        CommandLine cmd = new CommandLine(command);
        String[] args = {mrklFile.toString(), "--percentage", "10", "--bytes-to-corrupt", "1%-5%", "--seed", "11111"};
        
        int result = cmd.execute(args);
        assertEquals(0, result, "Command should succeed with percentage range corruption");
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
    void testNonExistentMrklFile() throws Exception {
        Path nonExistentFile = tempDir.resolve("nonexistent.mrkl");
        CommandLine cmd = new CommandLine(command);
        String[] args = {nonExistentFile.toString()};
        
        int result = cmd.execute(args);
        assertEquals(1, result, "Command should fail with non-existent merkle file");
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
    void testMissingCacheFile() throws Exception {
        // Delete cache file
        Files.delete(cacheFile);
        
        CommandLine cmd = new CommandLine(command);
        String[] args = {mrklFile.toString(), "--percentage", "10"};
        
        int result = cmd.execute(args);
        assertEquals(1, result, "Command should fail when cache file is missing");
    }

    @Test
    void testInvalidBytesCorruptionSpec() throws Exception {
        CommandLine cmd = new CommandLine(command);
        String[] args = {mrklFile.toString(), "--percentage", "10", "--bytes-to-corrupt", "invalid"};
        
        int result = cmd.execute(args);
        assertEquals(1, result, "Command should fail with invalid bytes corruption spec");
    }
}

class ByteCorruptionSpecTest {
    
    @Test
    void testParseFixedCount() {
        var spec = CMD_merkle_spoilchunks.ByteCorruptionSpec.parse("5");
        assertEquals(CMD_merkle_spoilchunks.ByteCorruptionSpec.Type.FIXED_COUNT, spec.type);
        assertEquals(5, spec.getBytesToCorrupt(1000, new Random()));
        assertEquals(5, spec.getBytesToCorrupt(10, new Random())); // 5 bytes requested, chunk has 10
        assertEquals(3, spec.getBytesToCorrupt(3, new Random())); // Should cap at chunk size when chunk is smaller
    }

    @Test
    void testParseInclusiveRange() {
        var spec = CMD_merkle_spoilchunks.ByteCorruptionSpec.parse("5-10");
        assertEquals(CMD_merkle_spoilchunks.ByteCorruptionSpec.Type.RANGE_COUNT_INCLUSIVE, spec.type);
        
        Random random = new Random(12345);
        for (int i = 0; i < 100; i++) {
            int result = spec.getBytesToCorrupt(1000, random);
            assertTrue(result >= 5 && result <= 10, "Result " + result + " not in range 5-10");
        }
    }

    @Test
    void testParseExclusiveRange() {
        var spec = CMD_merkle_spoilchunks.ByteCorruptionSpec.parse("5..10");
        assertEquals(CMD_merkle_spoilchunks.ByteCorruptionSpec.Type.RANGE_COUNT_EXCLUSIVE, spec.type);
        
        Random random = new Random(12345);
        for (int i = 0; i < 100; i++) {
            int result = spec.getBytesToCorrupt(1000, random);
            assertTrue(result >= 5 && result < 10, "Result " + result + " not in range 5..10");
        }
    }

    @Test
    void testParseFixedPercent() {
        var spec = CMD_merkle_spoilchunks.ByteCorruptionSpec.parse("10%");
        assertEquals(CMD_merkle_spoilchunks.ByteCorruptionSpec.Type.FIXED_PERCENT, spec.type);
        assertEquals(10, spec.getBytesToCorrupt(100, new Random()));
        assertEquals(5, spec.getBytesToCorrupt(50, new Random()));
    }

    @Test
    void testParsePercentageRange() {
        var spec = CMD_merkle_spoilchunks.ByteCorruptionSpec.parse("5%-15%");
        assertEquals(CMD_merkle_spoilchunks.ByteCorruptionSpec.Type.RANGE_PERCENT, spec.type);
        
        Random random = new Random(12345);
        for (int i = 0; i < 100; i++) {
            int result = spec.getBytesToCorrupt(100, random);
            assertTrue(result >= 5 && result <= 15, "Result " + result + " not in expected percentage range");
        }
    }

    @Test
    void testInvalidSpecs() {
        assertThrows(IllegalArgumentException.class, () -> CMD_merkle_spoilchunks.ByteCorruptionSpec.parse(""));
        assertThrows(IllegalArgumentException.class, () -> CMD_merkle_spoilchunks.ByteCorruptionSpec.parse("invalid"));
        assertThrows(IllegalArgumentException.class, () -> CMD_merkle_spoilchunks.ByteCorruptionSpec.parse("-5"));
        assertThrows(IllegalArgumentException.class, () -> CMD_merkle_spoilchunks.ByteCorruptionSpec.parse("10-5"));
        assertThrows(IllegalArgumentException.class, () -> CMD_merkle_spoilchunks.ByteCorruptionSpec.parse("10..10"));
        assertThrows(IllegalArgumentException.class, () -> CMD_merkle_spoilchunks.ByteCorruptionSpec.parse("150%"));
        assertThrows(IllegalArgumentException.class, () -> CMD_merkle_spoilchunks.ByteCorruptionSpec.parse("10%-5%"));
    }

    @Test
    void testToString() {
        assertEquals("5 bytes", CMD_merkle_spoilchunks.ByteCorruptionSpec.parse("5").toString());
        assertEquals("5-10 bytes (inclusive)", CMD_merkle_spoilchunks.ByteCorruptionSpec.parse("5-10").toString());
        assertEquals("5..10 bytes (exclusive)", CMD_merkle_spoilchunks.ByteCorruptionSpec.parse("5..10").toString());
        assertEquals("10.0% of chunk", CMD_merkle_spoilchunks.ByteCorruptionSpec.parse("10%").toString());
        assertEquals("5.0%-15.0% of chunk", CMD_merkle_spoilchunks.ByteCorruptionSpec.parse("5%-15%").toString());
    }
}