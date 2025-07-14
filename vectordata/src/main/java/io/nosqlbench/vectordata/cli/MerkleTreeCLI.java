package io.nosqlbench.vectordata.cli;

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


import io.nosqlbench.vectordata.merklev2.MerkleData;
import io.nosqlbench.vectordata.merklev2.MerkleDataImpl;
import io.nosqlbench.vectordata.merklev2.MerkleDataImpl;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.BitSet;
import java.util.concurrent.CompletableFuture;

/**
 * Command-line interface for merkle tree operations.
 * 
 * This utility provides commands to create, verify, and inspect Merkle trees for data files.
 * 
 * Usage: java -cp ... io.nosqlbench.vectordata.cli.MerkleTreeCLI &lt;command&gt; &lt;file_path&gt;
 * 
 * Commands:
 * - create &lt;data_file&gt;   - Create a .mrkl file for the given data file
 * - verify &lt;data_file&gt;   - Verify the merkle tree for the given data file
 * - info &lt;merkle_file&gt;   - Show information about the merkle tree file
 */
public class MerkleTreeCLI {

    /**
     * Default constructor for MerkleTreeCLI.
     */
    public MerkleTreeCLI() {
        // Default constructor
    }

    /**
     * Main entry point for the MerkleTreeCLI application.
     * 
     * @param args Command line arguments: [command] [file_path]
     */
    public static void main(String[] args) {
        if (args.length < 2) {
            printUsage();
            System.exit(1);
        }

        String command = args[0];
        String filePath = args[1];

        try {
            switch (command.toLowerCase()) {
                case "create":
                    createMerkleTree(filePath);
                    break;
                case "verify":
                    verifyMerkleTree(filePath);
                    break;
                case "info":
                    showMerkleInfo(filePath);
                    break;
                default:
                    System.err.println("Unknown command: " + command);
                    printUsage();
                    System.exit(1);
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void createMerkleTree(String dataFilePath) throws Exception {
        Path dataPath = Path.of(dataFilePath);
        if (!Files.exists(dataPath)) {
            throw new IllegalArgumentException("Data file not found: " + dataFilePath);
        }

        Path merklePath = Path.of(dataFilePath + ".mrkl");

        System.out.println("Creating merkle tree for: " + dataPath);
        System.out.println("Output file: " + merklePath);

        // Remove existing merkle file if it exists
        if (Files.exists(merklePath)) {
            Files.delete(merklePath);
            System.out.println("Removed existing merkle file");
        }

        // Create merkle reference from data
        CompletableFuture<MerkleDataImpl> refFuture = MerkleDataImpl.fromData(dataPath);
        MerkleDataImpl ref = refFuture.get();

        // Create merkle data from reference
        MerkleDataImpl data = MerkleDataImpl.createFromRef(ref, merklePath);

        long fileSize = Files.size(dataPath);
        long merkleSize = Files.size(merklePath);
        int chunkCount = ref.getShape().getLeafCount();
        long chunkSize = ref.getShape().getChunkSize();

        System.out.println("Merkle tree created successfully!");
        System.out.println("Data file size: " + fileSize + " bytes");
        System.out.println("Merkle file size: " + merkleSize + " bytes");
        System.out.println("Chunk count: " + chunkCount);
        System.out.println("Chunk size: " + chunkSize + " bytes");

        // Close the ref and data
        ref.close();
        data.close();
    }

    private static void verifyMerkleTree(String dataFilePath) throws Exception {
        Path dataPath = Path.of(dataFilePath);
        Path merklePath = Path.of(dataFilePath + ".mrkl");

        if (!Files.exists(dataPath)) {
            throw new IllegalArgumentException("Data file not found: " + dataFilePath);
        }

        if (!Files.exists(merklePath)) {
            throw new IllegalArgumentException("Merkle file not found: " + merklePath);
        }

        System.out.println("Verifying merkle tree for: " + dataPath);

        // Load existing merkle data
        MerkleData existingData = MerkleDataImpl.load(merklePath);

        // Create new merkle reference from data
        CompletableFuture<MerkleDataImpl> refFuture = MerkleDataImpl.fromData(dataPath);
        MerkleDataImpl newRef = refFuture.get();

        // Compare basic properties
        boolean valid = true;
        if (existingData.getShape().getLeafCount() != newRef.getShape().getLeafCount()) {
            System.err.println("ERROR: Chunk count mismatch");
            valid = false;
        }

        if (existingData.getShape().getChunkSize() != newRef.getShape().getChunkSize()) {
            System.err.println("ERROR: Chunk size mismatch");
            valid = false;
        }

        if (existingData.getShape().getTotalContentSize() != newRef.getShape().getTotalContentSize()) {
            System.err.println("ERROR: Total size mismatch");
            valid = false;
        }

        // Compare hashes for each chunk
        int chunkCount = existingData.getShape().getLeafCount();
        for (int i = 0; i < chunkCount; i++) {
            byte[] existingHash = existingData.getHashForLeaf(i);
            byte[] newHash = newRef.getHashForLeaf(i);

            if (!java.util.Arrays.equals(existingHash, newHash)) {
                System.err.println("ERROR: Hash mismatch for chunk " + i);
                valid = false;
            }
        }

        if (valid) {
            System.out.println("Merkle tree is VALID");
        } else {
            System.err.println("Merkle tree is INVALID");
            System.exit(1);
        }

        // Close data and ref
        existingData.close();
        newRef.close();
    }

    private static void showMerkleInfo(String merkleFilePath) throws Exception {
        Path merklePath = Path.of(merkleFilePath);

        if (!Files.exists(merklePath)) {
            throw new IllegalArgumentException("Merkle file not found: " + merkleFilePath);
        }

        System.out.println("Merkle tree information for: " + merklePath);

        MerkleData data = MerkleDataImpl.load(merklePath);

        System.out.println("File size: " + Files.size(merklePath) + " bytes");
        System.out.println("Total content size: " + data.getShape().getTotalContentSize() + " bytes");
        System.out.println("Chunk count: " + data.getShape().getLeafCount());
        System.out.println("Chunk size: " + data.getShape().getChunkSize() + " bytes");

        // Show valid chunks
        BitSet validChunks = data.getValidChunks();
        int validCount = validChunks.cardinality();
        System.out.println("Valid chunks: " + validCount + "/" + data.getShape().getLeafCount());

        data.close();
    }

    private static void printUsage() {
        System.out.println("Usage: java -cp ... io.nosqlbench.vectordata.cli.MerkleTreeCLI <command> <file_path>");
        System.out.println();
        System.out.println("Commands:");
        System.out.println("  create <data_file>   - Create a .mrkl file for the given data file");
        System.out.println("  verify <data_file>   - Verify the merkle tree for the given data file");  
        System.out.println("  info <merkle_file>   - Show information about the merkle tree file");
        System.out.println();
        System.out.println("Examples:");
        System.out.println("  java -cp ... io.nosqlbench.vectordata.cli.MerkleTreeCLI create data.bin");
        System.out.println("  java -cp ... io.nosqlbench.vectordata.cli.MerkleTreeCLI verify data.bin");
        System.out.println("  java -cp ... io.nosqlbench.vectordata.cli.MerkleTreeCLI info data.bin.mrkl");
    }
}
