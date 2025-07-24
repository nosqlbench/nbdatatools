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
// ChunkBoundary is now in the same package
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.BitSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Integration tests for the merklev2 package demonstrating the complete workflow.
 */
class MerkleV2IntegrationTest {

    @Test
    void testCompleteWorkflow(@TempDir Path tempDir) throws Exception {
        // Test data
        String testContent = "This is a comprehensive integration test for the merklev2 package. " +
                            "It demonstrates the complete workflow from reference creation to " +
                            "state management and chunk verification. The content should be " +
                            "large enough to span multiple chunks for meaningful testing.";
        byte[] contentBytes = testContent.getBytes();
        
        // Step 1: Create a reference merkle tree (simulating .mref file)
        MerkleShape shape = new BaseMerkleShape(contentBytes.length);
        MockMerkleRef reference = new MockMerkleRef(shape, contentBytes);
        
        // Step 2: Create merkle state from reference (simulating .mrkl file creation)
        Path statePath = tempDir.resolve("test.mrkl");
        MerkleState merkleState = MerkleState.fromRef(reference, statePath);
        
        try {
            // Step 3: Verify initial state
            assertEquals(shape.getTotalContentSize(), merkleState.getMerkleShape().getTotalContentSize());
            assertEquals(shape.getLeafCount(), merkleState.getMerkleShape().getLeafCount());
            
            // Initially, no chunks should be verified
            BitSet validChunks = merkleState.getValidChunks();
            assertEquals(0, validChunks.cardinality());
            
            // Step 4: Simulate downloading and verifying chunks
            int totalChunks = shape.getLeafCount();
            System.out.println("Total chunks to verify: " + totalChunks);
            
            for (int chunkIndex = 0; chunkIndex < totalChunks; chunkIndex++) {
                // Get chunk boundary
                ChunkBoundary boundary = shape.getChunkBoundary(chunkIndex);
                int chunkStart = (int) boundary.startInclusive();
                int chunkLength = (int) boundary.size();
                int chunkEnd = Math.min(chunkStart + chunkLength, contentBytes.length);
                
                // Extract chunk data (simulating download)
                byte[] chunkData = new byte[chunkEnd - chunkStart];
                System.arraycopy(contentBytes, chunkStart, chunkData, 0, chunkData.length);
                
                // Verify and save chunk
                ByteBuffer chunkBuffer = ByteBuffer.wrap(chunkData);
                int finalChunkIndex = chunkIndex;
                boolean saved = merkleState.saveIfValid(chunkIndex, chunkBuffer, data -> {
                    // Simulate saving to local cache
                    System.out.println("Saved chunk " + finalChunkIndex + " (" + data.remaining() + " bytes)");
                });
                
                assertTrue(saved, "Chunk " + chunkIndex + " should be valid and saved");
                assertTrue(merkleState.isValid(chunkIndex), 
                          "Chunk " + chunkIndex + " should be marked as valid");
            }
            
            // Step 5: Verify all chunks are now valid
            BitSet finalValidChunks = merkleState.getValidChunks();
            assertEquals(totalChunks, finalValidChunks.cardinality());
            
            // Step 6: Test persistence by closing and reloading
            merkleState.close();
            
            MerkleState reloadedState = MerkleState.load(statePath);
            try {
                // Verify state persisted correctly
                BitSet reloadedValidChunks = reloadedState.getValidChunks();
                assertEquals(totalChunks, reloadedValidChunks.cardinality());
                
                // Verify all chunks are still valid after reload
                for (int i = 0; i < totalChunks; i++) {
                    assertTrue(reloadedValidChunks.get(i), "Chunk " + i + " should remain valid after reload");
                }
                
                // Test chunk validation after reload
                var firstBoundary = shape.getChunkBoundary(0);
                int firstChunkLength = (int) firstBoundary.size();
                byte[] firstChunkData = new byte[firstChunkLength];
                System.arraycopy(contentBytes, 0, firstChunkData, 0, 
                               Math.min(firstChunkLength, contentBytes.length));
                
                ByteBuffer firstChunkBuffer = ByteBuffer.wrap(firstChunkData);
                // Verify state is still functional after reload
                assertTrue(reloadedState.isValid(0),
                          "First chunk should still be valid after reload");
                
            } finally {
                reloadedState.close();
            }
            
        } finally {
            // Close merkleState - AutoCloseable interface doesn't expose a closed() method
            try {
                merkleState.close();
            } catch (Exception e) {
                // Already closed or error closing
            }
        }
    }

    @Test
    void testInvalidChunkRejection(@TempDir Path tempDir) throws Exception {
        // Test that invalid chunks are properly rejected
        String testContent = "Test content for invalid chunk rejection.";
        byte[] contentBytes = testContent.getBytes();
        
        MerkleShape shape = new BaseMerkleShape(contentBytes.length);
        MockMerkleRef reference = new MockMerkleRef(shape, contentBytes);
        
        Path statePath = tempDir.resolve("rejection_test.mrkl");
        MerkleState merkleState = MerkleState.fromRef(reference, statePath);
        
        try {
            // Try to save invalid data
            byte[] invalidData = "This is completely different invalid data".getBytes();
            ByteBuffer invalidBuffer = ByteBuffer.wrap(invalidData);
            
            boolean saved = merkleState.saveIfValid(0, invalidBuffer, data -> {
                fail("Save callback should not be called for invalid data");
            });
            
            assertFalse(saved, "Invalid chunk should be rejected");
            assertFalse(merkleState.isValid(0), "Chunk should not be marked valid");
            
        } finally {
            merkleState.close();
        }
    }

    @Test
    void testMerkleShapeProperties(@TempDir Path tempDir) throws Exception {
        // Test various content sizes to verify merkle shape calculations
        long[] testSizes = {512, 1024, 4096, 1024 * 1024, 5 * 1024 * 1024};
        
        for (long contentSize : testSizes) {
            MerkleShape shape = new BaseMerkleShape(contentSize);
            
            // Basic validations
            assertEquals(contentSize, shape.getTotalContentSize());
            assertTrue(shape.getChunkSize() > 0);
            assertTrue(shape.getLeafCount() > 0);
            assertTrue(shape.getNodeCount() > 0);
            
            // Verify chunk coverage
            long totalCoverage = 0;
            for (int i = 0; i < shape.getLeafCount(); i++) {
                var boundary = shape.getChunkBoundary(i);
                totalCoverage += boundary.size();
                
                // Verify position calculations
                int calculatedIndex = shape.getChunkIndexForPosition(boundary.startInclusive());
                assertEquals(i, calculatedIndex);
            }
            
            // Total coverage should equal or exceed content size
            assertTrue(totalCoverage >= contentSize, 
                      "Total chunk coverage should cover all content for size " + contentSize);
            
            System.out.println("Content size: " + contentSize + 
                             ", Chunk size: " + shape.getChunkSize() + 
                             ", Leaf count: " + shape.getLeafCount() +
                             ", Total coverage: " + totalCoverage);
        }
    }

    /**
     * Mock implementation of MerkleRef for testing purposes.
     */
    private static class MockMerkleRef implements MerkleRef {
        private final MerkleShape shape;
        private final byte[] content;
        private final byte[][] hashes;

        public MockMerkleRef(MerkleShape shape, byte[] content) throws Exception {
            this.shape = shape;
            this.content = content;
            this.hashes = computeHashes(shape, content);
        }

        // Note: isChunkValid method removed from MerkleRef interface
        // MerkleRef files are always valid after creation
        // Validation logic moved to MerkleState implementations

        @Override
        public MerkleState createEmptyState(Path path) throws IOException {
            return MerkleDataImpl.createFromRef(this, path);
        }

        @Override
        public MerkleShape getShape() {
            return shape;
        }

        @Override
        public byte[] getHashForLeaf(int leafIndex) {
            return leafIndex >= 0 && leafIndex < hashes.length ? 
                   hashes[leafIndex].clone() : null;
        }

        @Override
        public byte[] getHashForIndex(int index) {
            // Simplified - return leaf hash for valid indices
            if (index >= shape.getOffset() && index < shape.getOffset() + shape.getLeafCount()) {
                int leafIndex = index - shape.getOffset();
                return getHashForLeaf(leafIndex);
            }
            
            // For internal nodes, return a computed hash (simplified)
            if (index >= 0 && index < shape.getNodeCount()) {
                try {
                    MessageDigest digest = MessageDigest.getInstance("SHA-256");
                    digest.update(("internal_" + index).getBytes());
                    return digest.digest();
                } catch (Exception e) {
                    return null;
                }
            }
            
            return null;
        }

        @Override
        public java.util.List<byte[]> getPathToRoot(int leafIndex) {
            // Simplified mock implementation for testing
            if (leafIndex < 0 || leafIndex >= shape.getLeafCount()) {
                throw new IllegalArgumentException("Invalid leaf index: " + leafIndex);
            }
            
            java.util.List<byte[]> path = new java.util.ArrayList<>();
            
            // Add leaf hash
            byte[] leafHash = getHashForLeaf(leafIndex);
            if (leafHash != null) {
                path.add(leafHash.clone());
            }
            
            // For simplicity, just add one parent hash (would be more complex in real implementation)
            try {
                MessageDigest digest = MessageDigest.getInstance("SHA-256");
                digest.update(("parent_of_" + leafIndex).getBytes());
                path.add(digest.digest());
            } catch (Exception e) {
                // Ignore for test
            }
            
            return path;
        }

        private byte[][] computeHashes(MerkleShape shape, byte[] content) throws Exception {
            byte[][] hashes = new byte[shape.getLeafCount()][];
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            
            for (int i = 0; i < shape.getLeafCount(); i++) {
                var boundary = shape.getChunkBoundary(i);
                int start = (int) boundary.startInclusive();
                int length = (int) boundary.length();
                int end = Math.min(start + length, content.length);
                
                digest.reset();
                digest.update(content, start, end - start);
                hashes[i] = digest.digest();
            }
            
            return hashes;
        }
    }
}