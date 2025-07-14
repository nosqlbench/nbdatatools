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


import io.nosqlbench.vectordata.merkle.MerkleTree;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class TraceHashTest {
    
    @Test
    void traceHashMismatch() throws IOException, NoSuchAlgorithmException, InterruptedException, java.util.concurrent.ExecutionException {
        Path dataPath = Path.of("src/test/resources/testserver/rawdatasets/testxvec/testxvec_base.fvec");
        
        System.out.println("=== Tracing Hash Computation ===");
        
        // First, manually read chunk 1 and hash it
        long chunkSize = 1048576; // 1MB
        long chunk1Offset = chunkSize; // Chunk 1 starts at 1MB
        
        StringBuilder manualHex = new StringBuilder();
        try (FileChannel channel = FileChannel.open(dataPath, StandardOpenOption.READ)) {
            ByteBuffer buffer = ByteBuffer.allocate((int) chunkSize);
            channel.position(chunk1Offset);
            int bytesRead = channel.read(buffer);
            
            buffer.flip();
            
            // Create a byte array copy as MerkleTree does
            byte[] chunkData = new byte[buffer.remaining()];
            buffer.get(chunkData);
            
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(chunkData);
            byte[] manualHash = digest.digest();
            
            for (byte b : manualHash) {
                manualHex.append(String.format("%02x", b));
            }
            System.out.println("Manual hash for chunk 1: " + manualHex.toString());
        }
        
        // Now create a MerkleTree and see what it produces
        System.out.println("\n=== MerkleTree Hash ===");
        MerkleTree tree = MerkleTree.fromData(dataPath).getFuture().get();
        
        byte[] merkleHash = tree.getHashForLeaf(1);
        StringBuilder merkleHex = new StringBuilder();
        for (byte b : merkleHash) {
            merkleHex.append(String.format("%02x", b));
        }
        System.out.println("MerkleTree hash for chunk 1: " + merkleHex.toString());
        
        // Check if they match
        if (manualHex.toString().equals(merkleHex.toString())) {
            System.out.println("*** HASHES MATCH ***");
        } else {
            System.out.println("*** HASHES DO NOT MATCH ***");
            
            // Test if the MerkleTree hash is the hash of "1"
            MessageDigest testDigest = MessageDigest.getInstance("SHA-256");
            testDigest.update("1".getBytes());
            byte[] string1Hash = testDigest.digest();
            
            StringBuilder string1Hex = new StringBuilder();
            for (byte b : string1Hash) {
                string1Hex.append(String.format("%02x", b));
            }
            System.out.println("Hash of string '1': " + string1Hex.toString());
            
            if (merkleHex.toString().equals(string1Hex.toString())) {
                System.out.println("*** MERKLE TREE IS HASHING STRING '1' INSTEAD OF FILE DATA ***");
            }
        }
        
        tree.close();
    }
}
