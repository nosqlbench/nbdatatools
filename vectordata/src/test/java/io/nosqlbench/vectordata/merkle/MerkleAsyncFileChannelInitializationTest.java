package io.nosqlbench.vectordata.merkle;

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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import io.nosqlbench.vectordata.status.NoOpDownloadEventSink;

import static org.junit.jupiter.api.Assertions.*;

/// Test class for verifying the initial state of a newly initialized MerkleAsyncFileChannel instance.
/// This test ensures that the MerkleAsyncFileChannel works correctly with the modern API that uses
/// automatic chunk size calculation via ChunkGeometryDescriptor.
public class MerkleAsyncFileChannelInitializationTest {

    // Using instance fields instead of static fields to avoid test fixture collisions
    // Note: Chunk size is now automatically calculated based on content size
    private final int TEST_DATA_SIZE = 4 * 1024; // 4KB test data
    // Create a new Random instance with a unique seed for each test to avoid collisions
    private final Random random = new Random(System.nanoTime() + Thread.currentThread().getId());

    /**
     * Creates random test data of the specified size.
     *
     * @param size The size of the test data in bytes
     * @return A byte array containing the test data
     */
    private byte[] createTestData(int size) {
        byte[] data = new byte[size];
        random.nextBytes(data);
        return data;
    }

    /**
     * Test that a newly initialized MerkleAsyncFileChannel instance works correctly
     * with the new API that uses automatic chunk size calculation.
     * 
     * This test will:
     * 1. Create a test file with known content
     * 2. Create a reference tree using the modern API
     * 3. Create an empty tree with the same geometry
     * 4. Verify that both trees have consistent structure
     * 5. Test basic functionality with MerklePane
     */
    @Test
    void testMerkleAsyncFileChannelInitialState(@TempDir Path tempDir) throws IOException, ExecutionException, InterruptedException {
        // Generate a unique identifier for this test run to avoid collisions
        String uniqueId = "MerkleAsyncFileChannelInitializationTest_" + System.nanoTime() + "_" + Thread.currentThread().getId();

        // Create test data
        byte[] testData = createTestData(TEST_DATA_SIZE);

        // Create a test file with a unique name to avoid collisions
        Path testDataFile = tempDir.resolve(uniqueId + "_test_data.bin");
        Files.write(testDataFile, testData);

        // Create a merkle tree for the test file to serve as the reference tree
        // Use unique file names to avoid collisions
        Path refMerkleFile = tempDir.resolve(uniqueId + "_test_data.bin.mref");

        // Use try-with-resources to ensure the MerkleTree is properly closed
        // Use the modern API that automatically calculates chunk size
        try (MerkleTree refTree = MerkleTree.fromData(testDataFile).getFuture().get()) {
            refTree.save(refMerkleFile);

            // Get the number of leaves for verification
            int refLeafCount = refTree.getNumberOfLeaves();

            // Create a MerkleAsyncFileChannel instance with the test file
            // Use a local file URL and the testing constructor
            String localUrl = "file://" + testDataFile.toAbsolutePath();

            // Create an empty tree with the same geometry as the reference tree
            Path merklePath = testDataFile.resolveSibling(testDataFile.getFileName().toString() + ".mrkl");
            if (Files.exists(merklePath)) {
                Files.delete(merklePath);
            }
            
            // Create a proper empty tree by using the ChunkGeometryDescriptor from the reference tree
            ChunkGeometryDescriptor geometry = ChunkGeometryDescriptor.fromContentSize(TEST_DATA_SIZE);
            try (MerkleTree emptyTree = MerkleTree.createEmpty(geometry, new NoOpDownloadEventSink())) {
                emptyTree.save(merklePath);
            }

            // Load the reference tree directly with try-with-resources
            try (MerkleTree refTreeDirect = MerkleTree.load(refMerkleFile);
                 // Load the merkle tree directly with try-with-resources
                 MerkleTree merkleTree = MerkleTree.load(merklePath)) {

                // Verify that the reference tree exists
                assertNotNull(refTreeDirect, "Reference tree should not be null");

                // Verify that the merkle tree exists
                assertNotNull(merkleTree, "Merkle tree should not be null");

                // Get the number of leaves in the trees for verification
                int refLeafCountDirect = refTreeDirect.getNumberOfLeaves();
                int merkleLeafCount = merkleTree.getNumberOfLeaves();
                
                assertEquals(refLeafCountDirect, merkleLeafCount, "Both trees should have the same number of leaves");

                // Verify that chunk sizes match (since they should be calculated the same way)
                assertEquals(refTreeDirect.getChunkSize(), merkleTree.getChunkSize(), "Chunk sizes should match");
                
                // Verify that total sizes match
                assertEquals(refTreeDirect.totalSize(), merkleTree.totalSize(), "Total sizes should match");

                // Verify that all nodes in the reference tree are valid
                for (int i = 0; i < refLeafCountDirect; i++) {
                    assertTrue(refTreeDirect.isLeafValid(i), "Leaf " + i + " in reference tree should be valid");
                }

                // Test basic functionality with MerklePane
                Path refPath = refMerkleFile;

                // Use try-with-resources to ensure the MerklePane is properly closed
                try (MerkleAsyncFileChannel merkleChannel = new MerkleAsyncFileChannel(testDataFile, localUrl, new NoOpDownloadEventSink(), true);
                     MerklePane pane = new MerklePane(testDataFile, merklePath, refPath, localUrl)) {
                    // Verify that the pane was created successfully
                    assertNotNull(pane, "MerklePane should not be null");
                    
                    // Verify that the merkle tree in the pane has the expected structure
                    assertEquals(merkleLeafCount, pane.merkleTree().getNumberOfLeaves(), 
                                "MerklePane tree should have same number of leaves as loaded tree");
                    
                    // Basic functionality test - check initial state and then invalidate chunks
                    if (merkleLeafCount > 0) {
                        // Check that the chunk is initially not intact since we created an empty tree
                        // (empty tree has no valid hashes, so chunks should not be intact)
                        assertFalse(pane.isChunkIntact(0), "Chunk 0 should not be intact initially with empty tree");
                        
                        // Invalidate first chunk (this should be a no-op since it's already invalid)
                        pane.merkleTree().invalidateLeaf(0);
                        
                        // Verify it's still not intact
                        assertFalse(pane.isChunkIntact(0), "Chunk 0 should still not be intact after invalidation");
                    }
                }
            }
        }
    }
}
