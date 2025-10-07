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

import io.nosqlbench.vectordata.merklev2.BaseMerkleShape;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.BitSet;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the clean MerkleDataImpl implementation.
 */
public class MerkleDataImplTest {

    @Test
    void testCreateFromRefAndBasicOperations(@TempDir Path tempDir) throws Exception {
        // Create test data
        byte[] testData = "Hello, World! This is test data for merkle tree validation.".getBytes();
        MerkleShape shape = new BaseMerkleShape(testData.length);
        
        // Create a mock reference tree
        Path refPath = tempDir.resolve("test.mref");
        MerkleRef mockRef = createMockRef(shape, testData);
        
        // Create merkle data from reference
        Path statePath = tempDir.resolve("test.mrkl");
        MerkleDataImpl merkleData = MerkleDataImpl.createFromRef(mockRef, statePath);
        
        try {
            // Test basic properties
            assertEquals(shape.getTotalContentSize(), merkleData.getShape().getTotalContentSize());
            assertEquals(shape.getLeafCount(), merkleData.getShape().getLeafCount());
            
            // Initially no chunks should be valid
            BitSet validChunks = merkleData.getValidChunks();
            assertEquals(0, validChunks.cardinality());
            
            // Test hash retrieval
            byte[] leafHash = merkleData.getHashForLeaf(0);
            assertNotNull(leafHash);
            assertEquals(32, leafHash.length); // SHA-256
            
            // Test hash retrieval for validation
            ByteBuffer chunkData = ByteBuffer.wrap(testData);
            // Instead of isChunkValid, verify hashes are accessible
            assertNotNull(merkleData.getHashForLeaf(0));
            
        } finally {
            merkleData.close();
        }
    }

    @Test
    void testSaveIfValidWorkflow(@TempDir Path tempDir) throws Exception {
        // Create test data
        byte[] testData = "Test data for save validation workflow.".getBytes();
        MerkleShape shape = new BaseMerkleShape(testData.length);
        
        // Create mock reference and state
        MerkleRef mockRef = createMockRef(shape, testData);
        Path statePath = tempDir.resolve("test.mrkl");
        MerkleDataImpl merkleData = MerkleDataImpl.createFromRef(mockRef, statePath);
        
        try {
            // Verify initially no chunks are valid
            assertFalse(merkleData.isValid(0));
            
            // Test successful save
            AtomicBoolean saveCallbackCalled = new AtomicBoolean(false);
            ByteBuffer chunkData = ByteBuffer.wrap(testData);
            
            boolean saved = merkleData.saveIfValid(0, chunkData, data -> {
                saveCallbackCalled.set(true);
                assertEquals(testData.length, data.remaining());
            });
            
            assertTrue(saved);
            assertTrue(saveCallbackCalled.get());
            assertTrue(merkleData.isValid(0));
            
            // Test save with invalid data should fail
            byte[] invalidData = "Invalid data that won't match hash".getBytes();
            ByteBuffer invalidBuffer = ByteBuffer.wrap(invalidData);
            
            AtomicBoolean invalidSaveCallbackCalled = new AtomicBoolean(false);
            boolean invalidSaved = merkleData.saveIfValid(0, invalidBuffer, data -> {
                invalidSaveCallbackCalled.set(true);
            });
            
            assertFalse(invalidSaved);
            assertFalse(invalidSaveCallbackCalled.get());
            
        } finally {
            merkleData.close();
        }
    }

    @Test
    void testPersistenceAndReload(@TempDir Path tempDir) throws Exception {
        // Create test data
        byte[] testData = "Persistence test data for merkle tree.".getBytes();
        MerkleShape shape = new BaseMerkleShape(testData.length);
        
        // Create and save initial state
        MerkleRef mockRef = createMockRef(shape, testData);
        Path statePath = tempDir.resolve("test.mrkl");
        
        // Create, modify, and close first instance
        MerkleDataImpl merkleData1 = MerkleDataImpl.createFromRef(mockRef, statePath);
        ByteBuffer chunkData = ByteBuffer.wrap(testData);
        
        merkleData1.saveIfValid(0, chunkData, data -> {
            // Simulate saving data
        });
        merkleData1.close();
        
        // Load second instance and verify state persisted
        MerkleDataImpl merkleData2 = MerkleDataImpl.load(statePath);
        
        try {
            assertTrue(merkleData2.isValid(0));
            assertEquals(1, merkleData2.getValidChunks().cardinality());
            
            // Verify hash is still available
            byte[] hash = merkleData2.getHashForLeaf(0);
            assertNotNull(hash);
            
            // Verify hash retrieval still works
            assertNotNull(merkleData2.getHashForLeaf(0));
            
        } finally {
            merkleData2.close();
        }
    }

    @Test
    void testConcurrentAccess(@TempDir Path tempDir) throws Exception {
        // Create test data
        byte[] testData = "Concurrent access test data.".getBytes();
        MerkleShape shape = new BaseMerkleShape(testData.length);
        
        MerkleRef mockRef = createMockRef(shape, testData);
        Path statePath = tempDir.resolve("test.mrkl");
        MerkleDataImpl merkleData = MerkleDataImpl.createFromRef(mockRef, statePath);
        
        try {
            // Test concurrent reads
            Thread[] readers = new Thread[5];
            for (int i = 0; i < readers.length; i++) {
                readers[i] = new Thread(() -> {
                    for (int j = 0; j < 100; j++) {
                        byte[] hash = merkleData.getHashForLeaf(0);
                        assertNotNull(hash);
                        
                        // Verify hash access instead of validation
                        byte[] ahash = merkleData.getHashForLeaf(0);
                        assertNotNull(ahash);
                    }
                });
                readers[i].start();
            }
            
            // Wait for all readers to complete
            for (Thread reader : readers) {
                reader.join();
            }
            
        } finally {
            merkleData.close();
        }
    }

    @Test
    void testErrorHandling(@TempDir Path tempDir) throws Exception {
        byte[] testData = "Error handling test data.".getBytes();
        MerkleShape shape = new BaseMerkleShape(testData.length);
        
        MerkleRef mockRef = createMockRef(shape, testData);
        Path statePath = tempDir.resolve("test.mrkl");
        MerkleDataImpl merkleData = MerkleDataImpl.createFromRef(mockRef, statePath);
        
        try {
            // Test invalid chunk index
            assertThrows(IllegalArgumentException.class, () -> {
                merkleData.getHashForLeaf(-1);
            });
            
            assertThrows(IllegalArgumentException.class, () -> {
                merkleData.getHashForLeaf(shape.getLeafCount());
            });
            
            // Test operations after close
            merkleData.close();
            
            assertNull(merkleData.getHashForLeaf(0));
            assertNull(merkleData.getHashForLeaf(0)); // After close, should return null
            
        } finally {
            if (!merkleData.closed()) {
                merkleData.close();
            }
        }
    }

    // Helper method to create a mock reference tree
    private MerkleRef createMockRef(MerkleShape shape, byte[] data) throws Exception {
        return new MerkleRef() {
            private final byte[] hash = computeHash(data);
            
            // Note: isChunkValid removed from interface as MerkleRef files are always valid
            
            @Override
            public MerkleShape getShape() {
                return shape;
            }
            
            @Override
            public MerkleState createEmptyState(Path path) throws IOException {
                return MerkleDataImpl.createFromRef(this, path);
            }
            
            @Override
            public byte[] getHashForLeaf(int leafIndex) {
                return leafIndex == 0 ? hash.clone() : null;
            }
            
            @Override
            public byte[] getHashForIndex(int index) {
                // Simplified - in real implementation would have full tree
                return index < shape.getNodeCount() ? hash.clone() : null;
            }
            
            @Override
            public java.util.List<byte[]> getPathToRoot(int leafIndex) {
                // Simplified mock implementation
                if (leafIndex < 0 || leafIndex >= shape.getLeafCount()) {
                    throw new IllegalArgumentException("Invalid leaf index: " + leafIndex);
                }
                java.util.List<byte[]> path = new java.util.ArrayList<>();
                path.add(hash.clone()); // Just return the leaf hash for simplicity
                return path;
            }
            
            private byte[] computeHash(byte[] data) {
                try {
                    MessageDigest digest = MessageDigest.getInstance("SHA-256");
                    return digest.digest(data);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }
}