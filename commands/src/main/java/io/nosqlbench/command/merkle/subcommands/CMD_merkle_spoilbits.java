package io.nosqlbench.command.merkle.subcommands;

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

import io.nosqlbench.vectordata.merklev2.MerkleDataImpl;
import io.nosqlbench.vectordata.merklev2.MerkleRefFactory;
import io.nosqlbench.vectordata.merklev2.MerkleShape;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;

/// Command to invalidate bitset entries in a .mrkl file by spoiling bits.
/// This command supports invalidating all non-leaf bits and a configurable 
/// number of random leaf bits by percentage or range.
@Command(
    name = "spoilbits",
    description = "Invalidate bitset entries in a .mrkl file"
)
public class CMD_merkle_spoilbits implements Callable<Integer> {
    private static final Logger logger = LogManager.getLogger(CMD_merkle_spoilbits.class);

    @Parameters(index = "0", description = "Path to the .mrkl file")
    private Path mrklFile;

    @Option(names = {"--percentage"}, 
            description = "Percentage of leaf nodes to invalidate (0-100, default: ${DEFAULT-VALUE})",
            defaultValue = "10")
    private double percentage = 10.0;

    @Option(names = {"--start"}, 
            description = "Start index for range-based invalidation (0-based)")
    private Integer startIndex;

    @Option(names = {"--end"}, 
            description = "End index for range-based invalidation (exclusive, 0-based)")
    private Integer endIndex;

    @Option(names = {"--seed"}, 
            description = "Random seed for reproducible results (default: current time)")
    private Long seed;

    @Option(names = {"--dryrun", "-n"}, 
            description = "Show which bits would be invalidated without actually modifying the file")
    private boolean dryrun = false;

    @Override
    public Integer call() throws Exception {
        try {
            if (!Files.exists(mrklFile)) {
                logger.error("Merkle file not found: {}", mrklFile);
                return 1;
            }

            if (!mrklFile.toString().endsWith(".mrkl")) {
                logger.error("File must have .mrkl extension: {}", mrklFile);
                return 1;
            }

            if (percentage < 0 || percentage > 100) {
                logger.error("Percentage must be between 0 and 100: {}", percentage);
                return 1;
            }

            if (startIndex != null && endIndex != null && startIndex >= endIndex) {
                logger.error("Start index must be less than end index: {} >= {}", startIndex, endIndex);
                return 1;
            }

            // Load the merkle state file
            MerkleDataImpl merkleData = MerkleRefFactory.load(mrklFile);
            MerkleShape shape = merkleData.getShape();
            BitSet validChunks = merkleData.getValidChunks();

            int totalLeafNodes = shape.getTotalChunks();
            int totalNodes = shape.getNodeCount();

            logger.info("Processing merkle file: {}", mrklFile);
            logger.info("Total leaf nodes (chunks): {}", totalLeafNodes);
            logger.info("Total nodes: {}", totalNodes);
            logger.info("Currently valid leaf nodes: {}", validChunks.cardinality());

            // Note: The BitSet only tracks leaf chunks, not internal nodes
            // BitSet indices 0..totalLeafNodes-1 correspond to chunk indices 0..totalLeafNodes-1
            logger.info("BitSet size matches leaf count: {}", validChunks.size() >= totalLeafNodes);

            // Determine which leaf chunks to invalidate
            List<Integer> leafChunksToInvalidate = new ArrayList<>();

            if (startIndex != null && endIndex != null) {
                // Range-based invalidation
                int safeStart = Math.max(0, startIndex);
                int safeEnd = Math.min(totalLeafNodes, endIndex);
                
                for (int chunkIndex = safeStart; chunkIndex < safeEnd; chunkIndex++) {
                    if (validChunks.get(chunkIndex)) {
                        leafChunksToInvalidate.add(chunkIndex);
                    }
                }
                logger.info("Range-based invalidation: chunks {} to {} (found {} valid chunks to invalidate)", 
                           safeStart, safeEnd - 1, leafChunksToInvalidate.size());
            } else {
                // Percentage-based invalidation
                List<Integer> validLeafChunks = new ArrayList<>();
                for (int chunkIndex = 0; chunkIndex < totalLeafNodes; chunkIndex++) {
                    if (validChunks.get(chunkIndex)) {
                        validLeafChunks.add(chunkIndex);
                    }
                }

                int chunksToInvalidate = (int) Math.round(validLeafChunks.size() * percentage / 100.0);
                
                Random random = new Random(seed != null ? seed : System.currentTimeMillis());
                Collections.shuffle(validLeafChunks, random);
                
                leafChunksToInvalidate = validLeafChunks.subList(0, Math.min(chunksToInvalidate, validLeafChunks.size()));
                logger.info("Percentage-based invalidation: {}% of {} valid leaf chunks = {} chunks to invalidate", 
                           percentage, validLeafChunks.size(), leafChunksToInvalidate.size());
            }

            if (dryrun) {
                logger.info("DRY RUN: Would invalidate {} leaf chunks", leafChunksToInvalidate.size());
                
                // Show some examples of chunks that would be invalidated
                int showCount = Math.min(10, leafChunksToInvalidate.size());
                for (int i = 0; i < showCount; i++) {
                    int chunkIndex = leafChunksToInvalidate.get(i);
                    logger.info("DRY RUN:   Chunk {}", chunkIndex);
                }
                if (leafChunksToInvalidate.size() > showCount) {
                    logger.info("DRY RUN:   ... and {} more", leafChunksToInvalidate.size() - showCount);
                }
            } else {
                // Create backup before modification
                Path backupPath = mrklFile.resolveSibling(mrklFile.getFileName() + ".backup");
                Files.copy(mrklFile, backupPath);
                logger.info("Created backup at: {}", backupPath);

                // Create modified bitset
                BitSet modifiedBitSet = (BitSet) validChunks.clone();

                // Invalidate selected leaf chunks
                for (int chunkIndex : leafChunksToInvalidate) {
                    modifiedBitSet.clear(chunkIndex);
                }

                logger.info("Invalidated {} leaf chunks", leafChunksToInvalidate.size());

                // Close the original merkle data
                merkleData.close();

                // Directly modify the bitset in the original file
                try {
                    modifyBitSetInFile(mrklFile, modifiedBitSet, shape);
                    logger.info("Successfully spoiled {} chunks in merkle file", 
                               leafChunksToInvalidate.size());
                               
                } catch (Exception e) {
                    logger.error("Failed to modify merkle file: {}", e.getMessage());
                    // Restore from backup
                    Files.copy(backupPath, mrklFile);
                    logger.info("Restored original file from backup");
                    throw e;
                }
            }

            return 0;

        } catch (Exception e) {
            logger.error("Error spoiling bits in merkle file: {}", e.getMessage(), e);
            return 1;
        }
    }

    /**
     * Directly modifies the BitSet in a .mrkl file without recreating the entire file.
     * This method writes the modified BitSet directly to the correct location in the file.
     */
    private void modifyBitSetInFile(Path mrklFile, BitSet modifiedBitSet, MerkleShape shape) throws IOException {
        // Constants from MerkleDataImpl
        final int HASH_SIZE = 32; // SHA-256
        
        // Calculate the position where the BitSet is stored in the file
        long bitsetPosition = (long) shape.getNodeCount() * HASH_SIZE;
        
        // Convert BitSet to byte array
        byte[] bitsetBytes = modifiedBitSet.toByteArray();
        int requiredBytes = (shape.getLeafCount() + 7) / 8; // Round up to byte boundary
        
        // Ensure the byte array is the correct size
        byte[] paddedBitsetBytes = new byte[requiredBytes];
        System.arraycopy(bitsetBytes, 0, paddedBitsetBytes, 0, Math.min(bitsetBytes.length, requiredBytes));
        
        // Open the file for writing and modify the BitSet region
        try (var channel = java.nio.channels.FileChannel.open(mrklFile, 
                java.nio.file.StandardOpenOption.WRITE)) {
            
            java.nio.ByteBuffer bitsetBuffer = java.nio.ByteBuffer.wrap(paddedBitsetBytes);
            channel.write(bitsetBuffer, bitsetPosition);
            channel.force(false); // Ensure changes are written to disk
        }
    }

    /// Main method to run the spoilbits command directly
    /// @param args Command line arguments
    public static void main(String[] args) {
        int exitCode = new picocli.CommandLine(new CMD_merkle_spoilbits()).execute(args);
        System.exit(exitCode);
    }
}