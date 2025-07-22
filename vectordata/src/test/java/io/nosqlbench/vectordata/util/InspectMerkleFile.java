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
import io.nosqlbench.vectordata.merklev2.MerkleDataImpl;

import java.nio.file.Path;
import java.util.HexFormat;

/**
 * Utility to inspect merkle file contents and show hashes for specific chunks.
 */
public class InspectMerkleFile {
    
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: InspectMerkleFile <merkle_file> [chunk_index]");
            System.exit(1);
        }
        
        Path merklePath = Path.of(args[0]);
        int chunkIndex = args.length > 1 ? Integer.parseInt(args[1]) : -1;
        
        System.out.println("Loading merkle file: " + merklePath);
        
        MerkleDataImpl tree = MerkleRefFactory.load(merklePath);
        
        try {
            System.out.println("Total size: " + tree.getShape().getTotalContentSize() + " bytes");
            System.out.println("Chunk count: " + tree.getShape().getLeafCount());
            System.out.println("Chunk size: " + tree.getShape().getChunkSize() + " bytes");
            System.out.println();
            
            if (chunkIndex >= 0) {
                // Show specific chunk
                if (chunkIndex >= tree.getShape().getLeafCount()) {
                    System.err.println("Error: Chunk index " + chunkIndex + " is out of range (0-" + (tree.getShape().getLeafCount() - 1) + ")");
                    System.exit(1);
                }
                
                byte[] hash = tree.getHashForLeaf(chunkIndex);
                String hexHash = HexFormat.of().formatHex(hash);
                
                System.out.println("Chunk " + chunkIndex + " hash: " + hexHash);
                System.out.println("Chunk " + chunkIndex + " valid: " + tree.isValid(chunkIndex));
            } else {
                // Show first few chunks
                int numToShow = Math.min(5, tree.getShape().getLeafCount());
                System.out.println("First " + numToShow + " chunk hashes:");
                
                for (int i = 0; i < numToShow; i++) {
                    byte[] hash = tree.getHashForLeaf(i);
                    String hexHash = HexFormat.of().formatHex(hash);
                    System.out.println("  Chunk " + i + ": " + hexHash + " (valid: " + tree.isValid(i) + ")");
                }
                
                if (tree.getShape().getLeafCount() > numToShow) {
                    System.out.println("  ... (" + (tree.getShape().getLeafCount() - numToShow) + " more chunks)");
                }
            }
        } finally {
            tree.close();
        }
    }
}
