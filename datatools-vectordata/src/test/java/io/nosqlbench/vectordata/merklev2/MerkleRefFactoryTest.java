package io.nosqlbench.vectordata.merklev2;

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

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the MerkleRefFactory and the enhanced merklev2 functionality.
 */
public class MerkleRefFactoryTest {

    @Test
    void testCreateFromDataWithProgress(@TempDir Path tempDir) throws Exception {
        // Create test data file
        byte[] testData = "Hello, World! This is test data for merkle tree creation.".getBytes();
        Path testFile = tempDir.resolve("test.dat");
        
        try (FileChannel channel = FileChannel.open(testFile, 
                StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            channel.write(ByteBuffer.wrap(testData));
        }
        
        // Create merkle ref with progress tracking
        MerkleRefBuildProgress progress = MerkleRefFactory.fromData(testFile);
        
        assertNotNull(progress);
        assertTrue(progress.getTotalChunks() > 0);
        assertEquals(testData.length, progress.getTotalBytes());
        
        // Wait for completion
        MerkleDataImpl merkleRef = progress.getFuture().get();
        
        assertNotNull(merkleRef);
        assertEquals(testData.length, merkleRef.getShape().getTotalContentSize());
        
        // Verify we can get hashes
        byte[] leafHash = merkleRef.getHashForLeaf(0);
        assertNotNull(leafHash);
        assertEquals(32, leafHash.length); // SHA-256
        
        merkleRef.close();
    }

    @Test
    void testSaveAndLoadWorkflow(@TempDir Path tempDir) throws Exception {
        // Create test data
        byte[] testData = "Test data for save/load workflow.".getBytes();
        Path testFile = tempDir.resolve("test.dat");
        
        try (FileChannel channel = FileChannel.open(testFile, 
                StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            channel.write(ByteBuffer.wrap(testData));
        }
        
        // Create merkle ref
        MerkleRefBuildProgress progress = MerkleRefFactory.fromData(testFile);
        MerkleDataImpl originalRef = progress.getFuture().get();
        
        // Save to .mref file
        Path mrefFile = tempDir.resolve("test.mref");
        originalRef.save(mrefFile);
        
        assertTrue(Files.exists(mrefFile));
        assertTrue(Files.size(mrefFile) > 0);
        
        // Test loading
        MerkleDataImpl loadedRef = MerkleRefFactory.load(mrefFile);
        assertTrue(originalRef.equals(loadedRef));
        
        originalRef.close();
        loadedRef.close();
    }

    @Test
    void testCreateEmpty() {
        long contentSize = 1024 * 1024; // 1MB
        MerkleDataImpl emptyRef = MerkleRefFactory.createEmpty(contentSize);
        
        assertNotNull(emptyRef);
        assertEquals(contentSize, emptyRef.getShape().getTotalContentSize());
        
        // All hashes should be null for empty ref
        assertNull(emptyRef.getHashForLeaf(0));
        
        emptyRef.close();
    }

    @Test
    void testComparison(@TempDir Path tempDir) throws Exception {
        // Create two identical test files
        byte[] testData = "Identical test data.".getBytes();
        
        Path testFile1 = tempDir.resolve("test1.dat");
        Path testFile2 = tempDir.resolve("test2.dat");
        
        try (FileChannel channel = FileChannel.open(testFile1, 
                StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            channel.write(ByteBuffer.wrap(testData));
        }
        
        try (FileChannel channel = FileChannel.open(testFile2, 
                StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            channel.write(ByteBuffer.wrap(testData));
        }
        
        // Create merkle refs from both files
        MerkleDataImpl ref1 = MerkleRefFactory.fromDataSimple(testFile1).get();
        MerkleDataImpl ref2 = MerkleRefFactory.fromDataSimple(testFile2).get();
        
        // They should be equal
        assertTrue(ref1.equals(ref2));
        
        // Should have no mismatched chunks
        assertTrue(ref1.findMismatchedChunks(ref2).isEmpty());
        
        ref1.close();
        ref2.close();
    }
}