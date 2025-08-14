package io.nosqlbench.command.merkle;

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


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * A mock implementation of CMD_merkle for testing purposes.
 * This class provides simplified versions of the methods in CMD_merkle
 * that can be used in tests without requiring the actual implementation.
 */
public class MockCMD_merkle {
    
    public static final String MRKL = ".mrkl";
    
    /**
     * Creates a simple Merkle file for the given file.
     * This is a mock implementation that just creates an empty file with the .mrkl extension.
     * 
     * @param file The file to create a Merkle file for
     * @param chunkSize The chunk size to use
     * @throws Exception If an error occurs
     */
    public void createMerkleFile(Path file, long chunkSize) throws Exception {
        // Create a simple Merkle file with the same name as the source file plus .mrkl
        Path merkleFile = file.resolveSibling(file.getFileName() + MRKL);
        
        // Create a simple hash of the file content
        byte[] fileContent = Files.readAllBytes(file);
        byte[] hash = createHash(fileContent);
        
        // Write the hash to the Merkle file
        Files.write(merkleFile, hash);
    }
    
    /**
     * Verifies a file against its Merkle file.
     * This is a mock implementation that just checks if the Merkle file exists.
     * 
     * @param file The file to verify
     * @param merklePath The path to the Merkle file
     * @param chunkSize The chunk size to use
     * @throws Exception If verification fails
     */
    public void verifyFile(Path file, Path merklePath, long chunkSize) throws Exception {
        // Check if the Merkle file exists
        if (!Files.exists(merklePath)) {
            throw new RuntimeException("Merkle file not found: " + merklePath);
        }
        
        // Read the original hash from the Merkle file
        byte[] originalHash = Files.readAllBytes(merklePath);
        
        // Create a hash of the current file content
        byte[] fileContent = Files.readAllBytes(file);
        byte[] currentHash = createHash(fileContent);
        
        // Compare the hashes
        if (!compareHashes(originalHash, currentHash)) {
            throw new RuntimeException("File verification failed: hashes do not match");
        }
    }
    
    /**
     * Creates a SHA-256 hash of the given data.
     * 
     * @param data The data to hash
     * @return The hash
     */
    private byte[] createHash(byte[] data) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return digest.digest(data);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 not available", e);
        }
    }
    
    /**
     * Compares two hashes for equality.
     * 
     * @param hash1 The first hash
     * @param hash2 The second hash
     * @return true if the hashes are equal, false otherwise
     */
    private boolean compareHashes(byte[] hash1, byte[] hash2) {
        if (hash1.length != hash2.length) {
            return false;
        }
        
        for (int i = 0; i < hash1.length; i++) {
            if (hash1[i] != hash2[i]) {
                return false;
            }
        }
        
        return true;
    }
}
