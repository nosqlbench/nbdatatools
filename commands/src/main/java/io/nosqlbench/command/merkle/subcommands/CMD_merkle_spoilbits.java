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
            logger.info("Total leaf nodes: {}", totalLeafNodes);
            logger.info("Total nodes: {}", totalNodes);
            logger.info("Currently valid leaf nodes: {}", validChunks.cardinality());

            // Count non-leaf valid bits
            int nonLeafValidBits = 0;
            int leafOffset = shape.getOffset();
            for (int i = 0; i < leafOffset; i++) {
                if (validChunks.get(i)) {
                    nonLeafValidBits++;
                }
            }

            logger.info("Currently valid non-leaf nodes: {}", nonLeafValidBits);

            // Determine which leaf nodes to invalidate
            List<Integer> leafNodesToInvalidate = new ArrayList<>();

            if (startIndex != null && endIndex != null) {
                // Range-based invalidation
                int safeStart = Math.max(0, startIndex);
                int safeEnd = Math.min(totalLeafNodes, endIndex);
                
                for (int i = safeStart; i < safeEnd; i++) {
                    int nodeIndex = leafOffset + i;
                    if (validChunks.get(nodeIndex)) {
                        leafNodesToInvalidate.add(nodeIndex);
                    }
                }
                logger.info("Range-based invalidation: {} to {} (found {} valid nodes to invalidate)", 
                           safeStart, safeEnd - 1, leafNodesToInvalidate.size());
            } else {
                // Percentage-based invalidation
                List<Integer> validLeafNodes = new ArrayList<>();
                for (int i = 0; i < totalLeafNodes; i++) {
                    int nodeIndex = leafOffset + i;
                    if (validChunks.get(nodeIndex)) {
                        validLeafNodes.add(nodeIndex);
                    }
                }

                int nodesToInvalidate = (int) Math.round(validLeafNodes.size() * percentage / 100.0);
                
                Random random = new Random(seed != null ? seed : System.currentTimeMillis());
                Collections.shuffle(validLeafNodes, random);
                
                leafNodesToInvalidate = validLeafNodes.subList(0, Math.min(nodesToInvalidate, validLeafNodes.size()));
                logger.info("Percentage-based invalidation: {}% of {} valid leaf nodes = {} nodes to invalidate", 
                           percentage, validLeafNodes.size(), leafNodesToInvalidate.size());
            }

            if (dryrun) {
                logger.info("DRY RUN: Would invalidate {} non-leaf nodes", nonLeafValidBits);
                logger.info("DRY RUN: Would invalidate {} leaf nodes:", leafNodesToInvalidate.size());
                
                // Show some examples of leaf nodes that would be invalidated
                int showCount = Math.min(10, leafNodesToInvalidate.size());
                for (int i = 0; i < showCount; i++) {
                    int nodeIndex = leafNodesToInvalidate.get(i);
                    int chunkIndex = nodeIndex - leafOffset;
                    logger.info("DRY RUN:   Leaf node {} (chunk {})", nodeIndex, chunkIndex);
                }
                if (leafNodesToInvalidate.size() > showCount) {
                    logger.info("DRY RUN:   ... and {} more", leafNodesToInvalidate.size() - showCount);
                }
            } else {
                // Create backup before modification
                Path backupPath = mrklFile.resolveSibling(mrklFile.getFileName() + ".backup");
                Files.copy(mrklFile, backupPath);
                logger.info("Created backup at: {}", backupPath);

                // Create modified bitset
                BitSet modifiedBitSet = (BitSet) validChunks.clone();

                // Invalidate all non-leaf bits
                for (int i = 0; i < leafOffset; i++) {
                    modifiedBitSet.clear(i);
                }

                // Invalidate selected leaf bits
                for (int nodeIndex : leafNodesToInvalidate) {
                    modifiedBitSet.clear(nodeIndex);
                }

                logger.info("Invalidated {} non-leaf nodes", nonLeafValidBits);
                logger.info("Invalidated {} leaf nodes", leafNodesToInvalidate.size());

                // Close the original merkle data
                merkleData.close();

                // Create new MerkleDataImpl with modified BitSet by copying hashes and applying new BitSet
                try {
                    // Load the merkle data again to get access to hashes
                    MerkleDataImpl originalData = MerkleRefFactory.load(backupPath);
                    
                    // Create hashes array and copy from original
                    byte[][] hashes = new byte[shape.getNodeCount()][];
                    for (int i = 0; i < shape.getNodeCount(); i++) {
                        if (shape.isLeafNode(i)) {
                            int leafIndex = i - leafOffset;
                            if (leafIndex >= 0 && leafIndex < shape.getTotalChunks()) {
                                hashes[i] = originalData.getHashForLeaf(leafIndex);
                            }
                        } else {
                            // For internal nodes, we need to get the hash differently
                            // This is a limitation of the current API
                            // For now, we'll copy whatever we can access
                        }
                    }
                    
                    originalData.close();
                    
                    // Create new MerkleDataImpl with modified BitSet
                    MerkleDataImpl modifiedData = MerkleDataImpl.createFromHashesAndBitSet(shape, hashes, modifiedBitSet);
                    
                    // Save the modified data to the original file
                    modifiedData.save(mrklFile);
                    modifiedData.close();
                    
                    logger.info("Successfully spoiled {} total bits in merkle file", 
                               nonLeafValidBits + leafNodesToInvalidate.size());
                               
                } catch (Exception e) {
                    logger.error("Failed to create modified merkle file: {}", e.getMessage());
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
}