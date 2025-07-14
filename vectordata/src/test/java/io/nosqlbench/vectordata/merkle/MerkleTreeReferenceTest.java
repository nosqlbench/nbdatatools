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

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test to diagnose reference tree loading issues
 */
public class MerkleTreeReferenceTest {

    @Test
    public void testReferenceTreeHashUniqueness() throws Exception {
        // Find the cohere reference tree file
        String homeDir = System.getProperty("user.home");
        Path referenceTreePath = Paths.get(homeDir, ".cache", "jvector", "cohere", "10m", "cohere_wiki_en_flat_base_10m_norm.fvecs.mref");
        
        if (!referenceTreePath.toFile().exists()) {
            // Skip test if reference tree doesn't exist
            System.out.println("Reference tree not found at: " + referenceTreePath);
            return;
        }
        
        System.out.println("Loading reference tree from: " + referenceTreePath);
        
        // Load the reference tree
        MerkleTree refTree = MerkleTree.load(referenceTreePath);
        
        assertNotNull(refTree, "Reference tree should load successfully");
        
        int leafCount = refTree.getNumberOfLeaves();
        System.out.println("Number of leaves in reference tree: " + leafCount);
        
        // Test multiple chunks to see if they have the same hash
        Set<String> seenHashes = new HashSet<>();
        int duplicateCount = 0;
        int testChunks = Math.min(100, leafCount); // Test first 100 chunks
        
        for (int i = 0; i < testChunks; i++) {
            byte[] hash = refTree.getHashForLeaf(i);
            if (hash != null) {
                String hashStr = bytesToHex(hash);
                if (seenHashes.contains(hashStr)) {
                    duplicateCount++;
                    System.out.println("Duplicate hash found for chunk " + i + ": " + hashStr);
                } else {
                    seenHashes.add(hashStr);
                }
            }
        }
        
        System.out.println("Tested " + testChunks + " chunks, found " + duplicateCount + " duplicates");
        System.out.println("Unique hashes: " + seenHashes.size());
        
        // Check specific chunks that are failing
        int[] testChunkIndices = {20527, 20577, 36889};
        for (int chunkIndex : testChunkIndices) {
            if (chunkIndex < leafCount) {
                byte[] hash = refTree.getHashForLeaf(chunkIndex);
                if (hash != null) {
                    String hashStr = bytesToHex(hash);
                    System.out.println("Chunk " + chunkIndex + " hash: " + hashStr);
                }
            }
        }
        
        refTree.close();
    }
    
    private String bytesToHex(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return "";
        }
        StringBuilder hexString = new StringBuilder(2 * bytes.length);
        for (byte b : bytes) {
            String hex = Integer.toHexString(0xff & b);
            if (hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex);
        }
        return hexString.toString();
    }
}
