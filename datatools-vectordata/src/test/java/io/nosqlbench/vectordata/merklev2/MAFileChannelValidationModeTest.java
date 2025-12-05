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

import io.nosqlbench.vectordata.merklev2.schedulers.DefaultChunkScheduler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.BitSet;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class MAFileChannelValidationModeTest {

    @TempDir
    Path tempDir;
    
    private Path testDataFile;
    private Path cacheFile;
    private Path stateFile;

    @BeforeEach
    void setUp() throws IOException {
        testDataFile = tempDir.resolve("test_data.bin");
        cacheFile = tempDir.resolve("test_cache.bin");
        stateFile = tempDir.resolve("test_state.mrkl");
        
        // Create test data file with some content
        byte[] testData = new byte[2048]; // 2KB of test data
        new Random(42).nextBytes(testData);
        Files.write(testDataFile, testData);
    }

    @Test
    void testValidationModeConstructor() throws Exception {
        // First, create a merkle reference file from our test data
        MerkleRefBuildProgress progress = MerkleRefFactory.fromData(testDataFile);
        MerkleDataImpl merkleRef = progress.getFuture().get();
        
        // Create a state file from the reference
        MerkleState state = MerkleState.fromRef(merkleRef, stateFile);
        
        // Create some partial cache data (simulate previously downloaded chunks)
        try (AsynchronousFileChannel cache = AsynchronousFileChannel.open(cacheFile, 
                StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            
            // Write some data to the cache to simulate partial downloads
            ByteBuffer testBuffer = ByteBuffer.wrap("test data content".getBytes());
            cache.write(testBuffer, 0).get();
        }
        
        state.close();
        merkleRef.close();
        
        // Now test the validation mode constructor
        try (MAFileChannel channel = new MAFileChannel(cacheFile, stateFile, 
                "http://example.com/test", new DefaultChunkScheduler(), true, -1)) {
            
            // The constructor should have run validation mode checks
            assertNotNull(channel);
            assertTrue(channel.isOpen());
            
            // Verify we can get the size (indicates successful initialization)
            long size = channel.size();
            assertEquals(2048, size);
        }
    }

    @Test
    void testValidationModeWithoutValidationFlag() throws Exception {
        // Create test setup similar to above
        MerkleRefBuildProgress progress = MerkleRefFactory.fromData(testDataFile);
        MerkleDataImpl merkleRef = progress.getFuture().get();
        MerkleState state = MerkleState.fromRef(merkleRef, stateFile);
        
        try (AsynchronousFileChannel cache = AsynchronousFileChannel.open(cacheFile, 
                StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            ByteBuffer testBuffer = ByteBuffer.wrap("test data content".getBytes());
            cache.write(testBuffer, 0).get();
        }
        
        state.close();
        merkleRef.close();
        
        // Test without validation mode (should work normally)
        try (MAFileChannel channel = new MAFileChannel(cacheFile, stateFile, 
                "http://example.com/test", new DefaultChunkScheduler(), false, -1)) {
            
            assertNotNull(channel);
            assertTrue(channel.isOpen());
            long size = channel.size();
            assertEquals(2048, size);
        }
    }

    @Test
    void testBackwardsCompatibilityConstructor() throws Exception {
        // Setup
        MerkleRefBuildProgress progress = MerkleRefFactory.fromData(testDataFile);
        MerkleDataImpl merkleRef = progress.getFuture().get();
        MerkleState state = MerkleState.fromRef(merkleRef, stateFile);
        
        try (AsynchronousFileChannel cache = AsynchronousFileChannel.open(cacheFile, 
                StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            ByteBuffer testBuffer = ByteBuffer.wrap("test data content".getBytes());
            cache.write(testBuffer, 0).get();
        }
        
        state.close();
        merkleRef.close();
        
        // Test backwards compatibility (old constructor should work)
        try (MAFileChannel channel = new MAFileChannel(cacheFile, stateFile, 
                "http://example.com/test", new DefaultChunkScheduler())) {
            
            assertNotNull(channel);
            assertTrue(channel.isOpen());
            long size = channel.size();
            assertEquals(2048, size);
        }
    }
}
