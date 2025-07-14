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

public class DebugChunkHashTest {
    
    @Test
    void debugChunk1Hash() throws IOException, NoSuchAlgorithmException, InterruptedException, java.util.concurrent.ExecutionException {
        Path dataPath = Path.of("src/test/resources/testserver/rawdatasets/testxvec/testxvec_base.fvec");
        
        System.out.println("=== Debug Chunk 1 Hash ===");
        System.out.println("Data file: " + dataPath);
        System.out.println("File exists: " + java.nio.file.Files.exists(dataPath));
        System.out.println("File size: " + java.nio.file.Files.size(dataPath) + " bytes");
        
        // Read chunk 1 directly from file (assuming 1MB chunk size, chunk 1 starts at offset 1048576)
        long chunkSize = 1048576; // 1MB
        long chunk1Offset = chunkSize; // Chunk 1 starts at 1MB
        
        try (FileChannel channel = FileChannel.open(dataPath, StandardOpenOption.READ)) {
            ByteBuffer buffer = ByteBuffer.allocate((int) chunkSize);
            channel.position(chunk1Offset);
            int bytesRead = channel.read(buffer);
            
            System.out.println("Bytes read for chunk 1: " + bytesRead);
            System.out.println("Expected chunk size: " + chunkSize);
            
            buffer.flip();
            
            // Show first few bytes
            buffer.mark();
            byte[] firstBytes = new byte[Math.min(16, buffer.remaining())];
            buffer.get(firstBytes);
            buffer.reset();
            
            System.out.print("First 16 bytes of chunk 1: ");
            for (byte b : firstBytes) {
                System.out.printf("%02x ", b);
            }
            System.out.println();
            
            // Check if it's the string "1"
            buffer.mark();
            if (buffer.remaining() >= 1) {
                byte firstByte = buffer.get();
                buffer.reset();
                if (firstByte == '1' && buffer.remaining() == 1) {
                    System.out.println("*** CHUNK 1 CONTAINS ONLY THE CHARACTER '1' ***");
                } else if (firstByte == '1') {
                    System.out.println("*** CHUNK 1 STARTS WITH CHARACTER '1' ***");
                }
            }
            
            // Compute hash
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(buffer);
            byte[] hash = digest.digest();
            
            StringBuilder hex = new StringBuilder();
            for (byte b : hash) {
                hex.append(String.format("%02x", b));
            }
            String hashStr = hex.toString();
            
            System.out.println("Computed hash for chunk 1: " + hashStr);
            System.out.println("Expected hash (from error): 6b86b273ff34fce19d6b804eff5a3f5747ada4eaa22f1d49c01e52ddb7875b4b");
            System.out.println("Actual hash (from error):   0bfaab5e9ecb1eaf6038e96e952cae85e7719382637149c13c520977b329e85a");
            
            if (hashStr.equals("6b86b273ff34fce19d6b804eff5a3f5747ada4eaa22f1d49c01e52ddb7875b4b")) {
                System.out.println("*** MATCH: Computed hash matches 'expected' (string '1') hash");
            } else if (hashStr.equals("0bfaab5e9ecb1eaf6038e96e952cae85e7719382637149c13c520977b329e85a")) {
                System.out.println("*** MATCH: Computed hash matches 'actual' hash from server");
            } else {
                System.out.println("*** NO MATCH: Computed hash is different from both");
            }
        }
        
        // Also check what MerkleTree.fromData produces
        System.out.println("\n=== MerkleTree Hash Comparison ===");
        MerkleTree tree = MerkleTree.fromData(dataPath).getFuture().get();
        
        byte[] merkleHash = tree.getHashForLeaf(1);
        StringBuilder merkleHex = new StringBuilder();
        for (byte b : merkleHash) {
            merkleHex.append(String.format("%02x", b));
        }
        String merkleHashStr = merkleHex.toString();
        
        System.out.println("MerkleTree hash for chunk 1: " + merkleHashStr);
        System.out.println("Chunk size from MerkleTree: " + tree.getChunkSize());
        System.out.println("Total chunks from MerkleTree: " + tree.getNumberOfLeaves());
        
        tree.close();
    }
    
    @Test
    void verifyString1Hash() throws NoSuchAlgorithmException {
        // Verify that "1" indeed hashes to the expected value
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        digest.update("1".getBytes());
        byte[] hash = digest.digest();
        
        StringBuilder hex = new StringBuilder();
        for (byte b : hash) {
            hex.append(String.format("%02x", b));
        }
        
        System.out.println("Hash of string '1': " + hex.toString());
        System.out.println("Expected from test: 6b86b273ff34fce19d6b804eff5a3f5747ada4eaa22f1d49c01e52ddb7875b4b");
    }
}
