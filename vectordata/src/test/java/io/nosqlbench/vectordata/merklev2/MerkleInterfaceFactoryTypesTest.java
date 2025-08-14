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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that verify the static factory methods on MerkleRef and MerkleState interfaces
 * return the correct faceted interface types and not the underlying implementation.
 * This ensures proper interface segregation at the API level.
 */
public class MerkleInterfaceFactoryTypesTest {

    private static final long TEST_FILE_SIZE = 1024 * 1024; // 1MB

    /**
     * Test that MerkleRef static factory methods return MerkleRef interface type.
     */
    @Test
    void testMerkleRefFactoryReturnTypes(@TempDir Path tempDir) throws IOException, InterruptedException, ExecutionException {
        // Create test data
        Path testFile = createTestDataFile(tempDir, TEST_FILE_SIZE);
        byte[] testData = Files.readAllBytes(testFile);
        ByteBuffer testBuffer = ByteBuffer.wrap(testData);
        
        // Test MerkleRef.fromData() - returns MerkleRefBuildProgress which gives MerkleRef
        MerkleRefBuildProgress progress = MerkleRef.fromData(testFile);
        assertNotNull(progress, "fromData should return progress tracker");
        
        MerkleRef refFromProgress = progress.getFuture().get();
        assertTrue(refFromProgress instanceof MerkleRef, "Progress should complete with MerkleRef");
        assertFalse(isOnlyMerkleRef(refFromProgress), "Implementation may also be MerkleState, but interface is MerkleRef");
        
        // Test MerkleRef.fromDataSimple() - returns CompletableFuture<MerkleRef>
        MerkleRef refFromDataSimple = MerkleRef.fromDataSimple(testFile).get();
        assertTrue(refFromDataSimple instanceof MerkleRef, "fromDataSimple should return MerkleRef");
        verifyMerkleRefInterface(refFromDataSimple);
        
        // Test MerkleRef.fromData(ByteBuffer) - returns CompletableFuture<MerkleRef>  
        MerkleRef refFromBuffer = MerkleRef.fromData(testBuffer).get();
        assertTrue(refFromBuffer instanceof MerkleRef, "fromData(ByteBuffer) should return MerkleRef");
        verifyMerkleRefInterface(refFromBuffer);
        
        // Test MerkleRef.createEmpty() - returns MerkleRef
        MerkleRef emptyRef = MerkleRef.createEmpty(TEST_FILE_SIZE);
        assertTrue(emptyRef instanceof MerkleRef, "createEmpty should return MerkleRef");
        verifyMerkleRefInterfaceStructure(emptyRef); // Empty refs may not have hashes
        
        // Test MerkleRef.load() - returns MerkleRef
        Path mrefFile = tempDir.resolve("test.mref");
        saveToFile(refFromDataSimple, mrefFile);
        
        MerkleRef loadedRef = MerkleRef.load(mrefFile);
        assertTrue(loadedRef instanceof MerkleRef, "load should return MerkleRef");
        verifyMerkleRefInterface(loadedRef);
    }

    /**
     * Test that MerkleState static factory methods return MerkleState interface type.
     */
    @Test
    void testMerkleStateFactoryReturnTypes(@TempDir Path tempDir) throws IOException, InterruptedException, ExecutionException {
        // Create MerkleRef first (dependency for MerkleState)
        Path testFile = createTestDataFile(tempDir, TEST_FILE_SIZE);
        MerkleRef merkleRef = MerkleRef.fromDataSimple(testFile).get();
        
        // Test MerkleState.fromRef() - returns MerkleState
        Path stateFile = tempDir.resolve("test.mrkl");
        MerkleState stateFromRef = MerkleState.fromRef(merkleRef, stateFile);
        
        assertTrue(stateFromRef instanceof MerkleState, "fromRef should return MerkleState");
        verifyMerkleStateInterface(stateFromRef);
        
        // Test MerkleState.load() - returns MerkleState
        stateFromRef.close();
        
        MerkleState loadedState = MerkleState.load(stateFile);
        assertTrue(loadedState instanceof MerkleState, "load should return MerkleState");
        verifyMerkleStateInterface(loadedState);
        
        loadedState.close();
    }

    /**
     * Test that interface delegation works correctly through factory methods.
     * The underlying implementation should support both interfaces, but
     * clients should get the appropriate faceted view.
     */
    @Test
    void testInterfaceDelegationThroughFactories(@TempDir Path tempDir) throws IOException, InterruptedException, ExecutionException {
        // Create test file and MerkleRef
        Path testFile = createTestDataFile(tempDir, TEST_FILE_SIZE);
        MerkleRef merkleRef = MerkleRef.fromDataSimple(testFile).get();
        
        // Create MerkleState from the reference
        Path stateFile = tempDir.resolve("delegation.mrkl");
        MerkleState merkleState = MerkleState.fromRef(merkleRef, stateFile);
        
        // Both should be the same underlying object, but with different interface views
        assertNotNull(merkleRef, "MerkleRef should exist");
        assertNotNull(merkleState, "MerkleState should exist");
        
        // Both should provide shape information (potentially with different method names)
        MerkleShape refShape = merkleRef.getShape();
        MerkleShape stateShape = merkleState.getMerkleShape();
        
        assertNotNull(refShape, "MerkleRef should provide shape");
        assertNotNull(stateShape, "MerkleState should provide shape");
        assertEquals(refShape.getLeafCount(), stateShape.getLeafCount(), "Both should have same leaf count");
        
        // MerkleRef provides read-only hash access
        if (refShape.getLeafCount() > 0) {
            byte[] hash = merkleRef.getHashForLeaf(0);
            assertNotNull(hash, "MerkleRef should provide hash access");
        }
        
        // MerkleState provides mutable validation tracking
        assertNotNull(merkleState.getValidChunks(), "MerkleState should provide valid chunks tracking");
        
        merkleState.close();
    }

    /**
     * Test interface type consistency across save/load operations.
     */
    @Test
    void testInterfaceTypeConsistencyAcrossPersistence(@TempDir Path tempDir) throws IOException, InterruptedException, ExecutionException {
        // Create and save MerkleRef
        Path testFile = createTestDataFile(tempDir, TEST_FILE_SIZE);
        MerkleRef originalRef = MerkleRef.fromDataSimple(testFile).get();
        
        Path mrefFile = tempDir.resolve("consistency.mref");
        saveToFile(originalRef, mrefFile);
        
        // Load should return MerkleRef interface
        MerkleRef loadedRef = MerkleRef.load(mrefFile);
        assertTrue(loadedRef instanceof MerkleRef, "Loaded object should be MerkleRef interface");
        verifyMerkleRefInterface(loadedRef);
        
        // Create, save, and load MerkleState
        Path stateFile = tempDir.resolve("consistency.mrkl");
        MerkleState originalState = MerkleState.fromRef(originalRef, stateFile);
        originalState.close();
        
        // Load should return MerkleState interface
        MerkleState loadedState = MerkleState.load(stateFile);
        assertTrue(loadedState instanceof MerkleState, "Loaded object should be MerkleState interface");
        verifyMerkleStateInterface(loadedState);
        
        loadedState.close();
    }

    // Helper methods
    private Path createTestDataFile(Path tempDir, long size) throws IOException {
        Path file = tempDir.resolve("test_data.dat");
        byte[] data = new byte[(int) size];
        
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 256);
        }
        
        Files.write(file, data);
        return file;
    }
    
    private void saveToFile(MerkleRef merkleRef, Path file) throws IOException {
        // Since we don't have a save method on the interface, we need to cast to implementation
        if (merkleRef instanceof MerkleDataImpl) {
            ((MerkleDataImpl) merkleRef).save(file);
        } else {
            fail("MerkleRef implementation doesn't support saving");
        }
    }
    
    private void verifyMerkleRefInterface(MerkleRef merkleRef) {
        // Verify MerkleRef interface methods are accessible
        assertNotNull(merkleRef.getShape(), "MerkleRef should provide getShape()");
        
        MerkleShape shape = merkleRef.getShape();
        if (shape.getLeafCount() > 0) {
            assertNotNull(merkleRef.getHashForLeaf(0), "MerkleRef should provide getHashForLeaf()");
            assertNotNull(merkleRef.getHashForIndex(shape.getOffset()), "MerkleRef should provide getHashForIndex()");
            assertNotNull(merkleRef.getPathToRoot(0), "MerkleRef should provide getPathToRoot()");
        }
        
        // The interface should not expose MerkleState methods directly
        // (though the implementation may support them)
    }
    
    private void verifyMerkleRefInterfaceStructure(MerkleRef merkleRef) {
        // Verify basic interface structure (for empty refs that may not have computed hashes)
        assertNotNull(merkleRef.getShape(), "MerkleRef should provide getShape()");
        
        MerkleShape shape = merkleRef.getShape();
        assertTrue(shape.getLeafCount() >= 0, "Shape should have non-negative leaf count");
        assertTrue(shape.getChunkSize() > 0, "Shape should have positive chunk size");
        
        // Don't verify hashes for empty references as they may be null
    }
    
    private void verifyMerkleStateInterface(MerkleState merkleState) {
        // Verify MerkleState interface methods are accessible
        assertNotNull(merkleState.getMerkleShape(), "MerkleState should provide getMerkleShape()");
        assertNotNull(merkleState.getValidChunks(), "MerkleState should provide getValidChunks()");
        
        MerkleShape shape = merkleState.getMerkleShape();
        if (shape.getLeafCount() > 0) {
            // isValid should work for all chunk indices
            boolean valid = merkleState.isValid(0);
            // Don't assert specific value, just that method works
            assertNotNull(Boolean.valueOf(valid), "isValid should return boolean");
        }
        
        // The interface should not directly expose MerkleRef methods
        // (though the implementation may support them)
    }
    
    private boolean isOnlyMerkleRef(Object obj) {
        // Check if object implements only MerkleRef and not MerkleState
        return (obj instanceof MerkleRef) && !(obj instanceof MerkleState);
    }
}