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


import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.security.MessageDigest;

/**
 * Utility to compute the hash of a specific chunk from a data file.
 */
public class ComputeChunkHash {
    
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: ComputeChunkHash <data_file> <chunk_index> <chunk_size>");
            System.exit(1);
        }
        
        Path dataPath = Path.of(args[0]);
        int chunkIndex = Integer.parseInt(args[1]);
        int chunkSize = Integer.parseInt(args[2]);
        
        System.out.println("Computing hash for chunk " + chunkIndex + " from: " + dataPath);
        System.out.println("Chunk size: " + chunkSize + " bytes");
        
        // Read the chunk data
        long chunkOffset = (long) chunkIndex * chunkSize;
        
        try (RandomAccessFile file = new RandomAccessFile(dataPath.toFile(), "r")) {
            long fileSize = file.length();
            System.out.println("File size: " + fileSize + " bytes");
            
            if (chunkOffset >= fileSize) {
                System.err.println("Error: Chunk offset " + chunkOffset + " is beyond file size " + fileSize);
                System.exit(1);
            }
            
            // Calculate actual chunk size (might be smaller for last chunk)
            int actualChunkSize = (int) Math.min(chunkSize, fileSize - chunkOffset);
            System.out.println("Chunk offset: " + chunkOffset);
            System.out.println("Actual chunk size: " + actualChunkSize + " bytes");
            
            // Read chunk data
            ByteBuffer buffer = ByteBuffer.allocate(actualChunkSize);
            file.seek(chunkOffset);
            file.readFully(buffer.array());
            
            // Compute SHA-256 hash
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(buffer.array());
            
            String hexHash = bytesToHex(hash);
            System.out.println("\nChunk " + chunkIndex + " hash: " + hexHash);
            
            // Show first few bytes of chunk data for debugging
            System.out.println("\nFirst 32 bytes of chunk data (hex):");
            int bytesToShow = Math.min(32, actualChunkSize);
            byte[] firstBytes = new byte[bytesToShow];
            System.arraycopy(buffer.array(), 0, firstBytes, 0, bytesToShow);
            System.out.println(bytesToHex(firstBytes));
        }
    }
    
    private static String bytesToHex(byte[] bytes) {
        StringBuilder hex = new StringBuilder();
        for (byte b : bytes) {
            hex.append(String.format("%02x", b));
        }
        return hex.toString();
    }
}
