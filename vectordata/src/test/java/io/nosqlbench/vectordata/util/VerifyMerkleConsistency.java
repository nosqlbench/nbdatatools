package io.nosqlbench.vectordata.util;

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


import io.nosqlbench.vectordata.merklev2.MerkleRefFactory;
import io.nosqlbench.vectordata.merklev2.MerkleRefBuildProgress;
import io.nosqlbench.vectordata.merklev2.MerkleDataImpl;

import java.nio.file.Path;
import java.util.HexFormat;

/**
 * Utility to verify merkle file consistency by comparing stored hashes with freshly computed ones.
 */
public class VerifyMerkleConsistency {
    
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: VerifyMerkleConsistency <data_file>");
            System.exit(1);
        }
        
        Path dataPath = Path.of(args[0]);
        Path merklePath = Path.of(args[0] + ".mref");
        
        System.out.println("Verifying merkle consistency for: " + dataPath);
        System.out.println();
        
        // Load existing merkle tree
        System.out.println("Loading existing merkle tree from: " + merklePath);
        MerkleDataImpl existingTree = MerkleRefFactory.load(merklePath);
        
        // Create new merkle tree from data
        System.out.println("Computing fresh merkle tree from data...");
        MerkleRefBuildProgress progress = MerkleRefFactory.fromData(dataPath);
        MerkleDataImpl freshTree = progress.getFuture().get();
        
        try {
            // Compare properties
            System.out.println("\nTree properties:");
            System.out.println("Chunk count: " + existingTree.getShape().getLeafCount());
            System.out.println("Chunk size: " + existingTree.getShape().getChunkSize());
            System.out.println("Total size: " + existingTree.getShape().getTotalContentSize());
            
            // Compare chunk hashes
            System.out.println("\nComparing chunk hashes:");
            boolean allMatch = true;
            
            for (int i = 0; i < existingTree.getShape().getLeafCount(); i++) {
                byte[] existingHash = existingTree.getHashForLeaf(i);
                byte[] freshHash = freshTree.getHashForLeaf(i);
                
                String existingHex = HexFormat.of().formatHex(existingHash);
                String freshHex = HexFormat.of().formatHex(freshHash);
                
                boolean matches = java.util.Arrays.equals(existingHash, freshHash);
                
                if (!matches || i < 3 || i == existingTree.getShape().getLeafCount() - 1) {
                    System.out.println("Chunk " + i + ":");
                    System.out.println("  Stored hash: " + existingHex);
                    System.out.println("  Fresh hash:  " + freshHex);
                    System.out.println("  Match: " + (matches ? "YES" : "NO"));
                }
                
                if (!matches) {
                    allMatch = false;
                }
            }
            
            System.out.println("\nOverall result: " + (allMatch ? "ALL HASHES MATCH" : "HASH MISMATCH DETECTED"));
            
        } finally {
            existingTree.close();
            freshTree.close();
        }
    }
}
