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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/// Command to invalidate bitset entries in a .mrkl file and corrupt corresponding cache data.
/// This command supports invalidating random leaf bits and corrupting the corresponding 
/// data chunks in the cache file with configurable byte corruption patterns.
@Command(
    name = "spoilchunks",
    description = "Invalidate bitset entries in a .mrkl file and corrupt corresponding cache data"
)
public class CMD_merkle_spoilchunks implements Callable<Integer> {
    private static final Logger logger = LogManager.getLogger(CMD_merkle_spoilchunks.class);

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
            description = "Show which bits would be invalidated without actually modifying files")
    private boolean dryrun = false;

    @Option(names = {"--bytes-to-corrupt"}, 
            description = "Bytes to corrupt per chunk: number (5), range (5-20, 5..20), percent (5%%, 5%%-7%%)",
            defaultValue = "1")
    private String bytesToCorrupt = "1";

    @Option(names = {"-f", "--force"},
            description = "Force modification of Merkle and cache files instead of requiring --dryrun")
    private boolean force = false;

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

            // Parse byte corruption specification
            ByteCorruptionSpec corruptionSpec;
            try {
                corruptionSpec = ByteCorruptionSpec.parse(bytesToCorrupt);
                logger.info("Byte corruption specification: {}", corruptionSpec);
            } catch (IllegalArgumentException e) {
                logger.error("Invalid bytes-to-corrupt format '{}': {}", bytesToCorrupt, e.getMessage());
                return 1;
            }

            // Determine cache file path
            String mrklFileName = mrklFile.getFileName().toString();
            String baseName = mrklFileName.substring(0, mrklFileName.length() - 5); // Remove .mrkl
            Path cacheFile = mrklFile.resolveSibling(baseName);

            if (!Files.exists(cacheFile)) {
                logger.error("Cache file not found: {}", cacheFile);
                return 1;
            }

            // Load the merkle state file
            MerkleDataImpl merkleData = MerkleRefFactory.load(mrklFile);
            MerkleShape shape = merkleData.getShape();
            BitSet validChunks = merkleData.getValidChunks();

            int totalLeafNodes = shape.getTotalChunks();
            int totalNodes = shape.getNodeCount();

            logger.info("Processing merkle file: {}", mrklFile);
            logger.info("Cache file: {}", cacheFile);
            logger.info("Total leaf nodes (chunks): {}", totalLeafNodes);
            logger.info("Total nodes: {}", totalNodes);
            logger.info("Currently valid leaf nodes: {}", validChunks.cardinality());

            // Determine which leaf chunks to invalidate (same logic as spoilbits)
            List<Integer> leafChunksToInvalidate = new ArrayList<>();
            Random random = new Random(seed != null ? seed : System.currentTimeMillis());

            if (startIndex != null && endIndex != null) {
                // Range-based invalidation
                int safeStart = Math.max(0, startIndex);
                int safeEnd = Math.min(totalLeafNodes, endIndex);
                
                for (int i = safeStart; i < safeEnd; i++) {
                    if (validChunks.get(i)) {
                        leafChunksToInvalidate.add(i);
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
                    long chunkStart = shape.getChunkStartPosition(chunkIndex);
                    long chunkEnd = shape.getChunkEndPosition(chunkIndex);
                    long chunkSize = chunkEnd - chunkStart;
                    
                    int bytesToCorruptForChunk = corruptionSpec.getBytesToCorrupt((int) chunkSize, random);
                    logger.info("DRY RUN:   Chunk {} (pos {}-{}, size {}, would corrupt {} bytes)", 
                               chunkIndex, chunkStart, chunkEnd - 1, chunkSize, bytesToCorruptForChunk);
                }
                if (leafChunksToInvalidate.size() > showCount) {
                    logger.info("DRY RUN:   ... and {} more", leafChunksToInvalidate.size() - showCount);
                }
            } else {
                if (!force) {
                    logger.error("Refusing to modify Merkle or cache files without --force. "
                        + "Use --dryrun for a preview or rerun with --force.");
                    return 1;
                }

                // Create backup before modification
                Path backupPath = mrklFile.resolveSibling(mrklFile.getFileName() + ".backup");
                Files.copy(mrklFile, backupPath, StandardCopyOption.REPLACE_EXISTING);
                logger.info("Created merkle backup at: {}", backupPath);

                Path cacheBackupPath = cacheFile.resolveSibling(cacheFile.getFileName() + ".backup");
                Files.copy(cacheFile, cacheBackupPath, StandardCopyOption.REPLACE_EXISTING);
                logger.info("Created cache backup at: {}", cacheBackupPath);

                try {
                    // First corrupt the cache data
                    int totalBytesCorrupted = corruptCacheChunks(cacheFile, shape, leafChunksToInvalidate, corruptionSpec, random);
                    logger.info("Corrupted {} total bytes across {} chunks in cache file", totalBytesCorrupted, leafChunksToInvalidate.size());

                    // Then invalidate the bits (same logic as spoilbits)
                    BitSet modifiedBitSet = (BitSet) validChunks.clone();
                    for (int chunkIndex : leafChunksToInvalidate) {
                        modifiedBitSet.clear(chunkIndex);
                    }

                    // Close the original merkle data
                    merkleData.close();

                    // Directly modify the bitset in the original file
                    modifyBitSetInFile(mrklFile, modifiedBitSet, shape);
                    
                    logger.info("Successfully spoiled {} chunks and corrupted cache data", leafChunksToInvalidate.size());

                } catch (Exception e) {
                    logger.error("Failed to modify files: {}", e.getMessage(), e);
                    // Restore from backups
                    Files.copy(backupPath, mrklFile, StandardCopyOption.REPLACE_EXISTING);
                    Files.copy(cacheBackupPath, cacheFile, StandardCopyOption.REPLACE_EXISTING);
                    logger.info("Restored original files from backups");
                    throw e;
                }
            }

            return 0;

        } catch (Exception e) {
            logger.error("Error spoiling chunks: {}", e.getMessage(), e);
            return 1;
        }
    }

    private int corruptCacheChunks(Path cacheFile, MerkleShape shape, List<Integer> chunkIndices, 
                                   ByteCorruptionSpec corruptionSpec, Random random) throws IOException {
        int totalBytesCorrupted = 0;
        
        try (FileChannel channel = FileChannel.open(cacheFile, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            for (int chunkIndex : chunkIndices) {
                long chunkStart = shape.getChunkStartPosition(chunkIndex);
                long chunkEnd = shape.getChunkEndPosition(chunkIndex);
                long chunkSize = chunkEnd - chunkStart;
                
                if (chunkSize > Integer.MAX_VALUE) {
                    logger.warn("Skipping chunk {} - too large: {} bytes", chunkIndex, chunkSize);
                    continue;
                }
                
                int bytesToCorruptForChunk = corruptionSpec.getBytesToCorrupt((int) chunkSize, random);
                
                if (bytesToCorruptForChunk > 0) {
                    // Read the chunk
                    ByteBuffer chunkData = ByteBuffer.allocate((int) chunkSize);
                    channel.read(chunkData, chunkStart);
                    
                    // Corrupt random bytes
                    chunkData.flip();
                    byte[] chunkArray = chunkData.array();
                    
                    // Select random positions to corrupt
                    List<Integer> positions = new ArrayList<>();
                    for (int i = 0; i < chunkSize; i++) {
                        positions.add(i);
                    }
                    Collections.shuffle(positions, random);
                    
                    for (int i = 0; i < bytesToCorruptForChunk && i < positions.size(); i++) {
                        int pos = positions.get(i);
                        byte originalByte = chunkArray[pos];
                        // Flip a random bit to corrupt the byte
                        int bitToFlip = random.nextInt(8);
                        chunkArray[pos] ^= (1 << bitToFlip);
                        totalBytesCorrupted++;
                    }
                    
                    // Write the corrupted chunk back
                    ByteBuffer corruptedData = ByteBuffer.wrap(chunkArray);
                    channel.write(corruptedData, chunkStart);
                    
                    logger.debug("Corrupted {} bytes in chunk {} at positions {}", 
                                bytesToCorruptForChunk, chunkIndex, 
                                positions.subList(0, Math.min(bytesToCorruptForChunk, positions.size())));
                }
            }
            
            channel.force(false); // Ensure changes are written to disk
        }
        
        return totalBytesCorrupted;
    }

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
        try (FileChannel channel = FileChannel.open(mrklFile, StandardOpenOption.WRITE)) {
            ByteBuffer bitsetBuffer = ByteBuffer.wrap(paddedBitsetBytes);
            channel.write(bitsetBuffer, bitsetPosition);
            channel.force(false); // Ensure changes are written to disk
        }
    }

    /// Main method to run the spoilchunks command directly
    /// @param args Command line arguments
    public static void main(String[] args) {
        int exitCode = new picocli.CommandLine(new CMD_merkle_spoilchunks()).execute(args);
        System.exit(exitCode);
    }

    /// Specification for how many bytes to corrupt in each chunk
    static class ByteCorruptionSpec {
        private static final Pattern PATTERN = Pattern.compile("^(?:(\\d+)(?:-(\\d+)|\\.\\.(\\d+))?|(\\d+(?:\\.\\d+)?)%(?:-(\\d+(?:\\.\\d+)?)%)?|" +
                "(\\d+(?:\\.\\d+)?)%-(\\d+(?:\\.\\d+)?)%)$");

        final Type type;
        final int fixedCount;
        final int minCount;
        final int maxCount; // For .. operator, this is exclusive
        final boolean exclusiveMax;
        final double fixedPercent;
        final double minPercent;
        final double maxPercent;

        enum Type {
            FIXED_COUNT,
            RANGE_COUNT_INCLUSIVE,
            RANGE_COUNT_EXCLUSIVE,
            FIXED_PERCENT,
            RANGE_PERCENT
        }

        private ByteCorruptionSpec(Type type, int fixedCount, int minCount, int maxCount, boolean exclusiveMax,
                                  double fixedPercent, double minPercent, double maxPercent) {
            this.type = type;
            this.fixedCount = fixedCount;
            this.minCount = minCount;
            this.maxCount = maxCount;
            this.exclusiveMax = exclusiveMax;
            this.fixedPercent = fixedPercent;
            this.minPercent = minPercent;
            this.maxPercent = maxPercent;
        }

        public static ByteCorruptionSpec parse(String spec) {
            if (spec == null || spec.trim().isEmpty()) {
                throw new IllegalArgumentException("Corruption spec cannot be empty");
            }

            spec = spec.trim();

            // Handle simple number (e.g., "5")
            try {
                int count = Integer.parseInt(spec);
                if (count < 0) {
                    throw new IllegalArgumentException("Byte count cannot be negative");
                }
                return new ByteCorruptionSpec(Type.FIXED_COUNT, count, 0, 0, false, 0, 0, 0);
            } catch (NumberFormatException ignored) {
                // Continue to pattern matching
            }

            // Handle ranges and percentages
            if (spec.contains("-") || spec.contains("..") || spec.contains("%")) {
                return parseComplexSpec(spec);
            }

            throw new IllegalArgumentException("Invalid format. Use: number (5), range (5-20, 5..20), or percent (5%, 5%-7%)");
        }

        private static ByteCorruptionSpec parseComplexSpec(String spec) {
            // Handle percentage ranges like "5%-7%"
            if (spec.matches("^\\d+(?:\\.\\d+)?%-\\d+(?:\\.\\d+)?%$")) {
                String[] parts = spec.split("-");
                double min = Double.parseDouble(parts[0].replace("%", ""));
                double max = Double.parseDouble(parts[1].replace("%", ""));
                if (min < 0 || max < 0 || min > 100 || max > 100 || min > max) {
                    throw new IllegalArgumentException("Invalid percentage range");
                }
                return new ByteCorruptionSpec(Type.RANGE_PERCENT, 0, 0, 0, false, 0, min, max);
            }

            // Handle single percentage like "5%"
            if (spec.matches("^\\d+(?:\\.\\d+)?%$")) {
                double percent = Double.parseDouble(spec.replace("%", ""));
                if (percent < 0 || percent > 100) {
                    throw new IllegalArgumentException("Percentage must be 0-100");
                }
                return new ByteCorruptionSpec(Type.FIXED_PERCENT, 0, 0, 0, false, percent, 0, 0);
            }

            // Handle inclusive ranges like "5-20"
            if (spec.matches("^\\d+-\\d+$")) {
                String[] parts = spec.split("-");
                int min = Integer.parseInt(parts[0]);
                int max = Integer.parseInt(parts[1]);
                if (min < 0 || max < 0 || min > max) {
                    throw new IllegalArgumentException("Invalid range");
                }
                return new ByteCorruptionSpec(Type.RANGE_COUNT_INCLUSIVE, 0, min, max, false, 0, 0, 0);
            }

            // Handle exclusive ranges like "5..20"
            if (spec.matches("^\\d+\\.\\.\\d+$")) {
                String[] parts = spec.split("\\.\\.");
                int min = Integer.parseInt(parts[0]);
                int max = Integer.parseInt(parts[1]);
                if (min < 0 || max < 0 || min >= max) {
                    throw new IllegalArgumentException("Invalid exclusive range");
                }
                return new ByteCorruptionSpec(Type.RANGE_COUNT_EXCLUSIVE, 0, min, max, true, 0, 0, 0);
            }

            throw new IllegalArgumentException("Invalid format: " + spec);
        }

        public int getBytesToCorrupt(int chunkSize, Random random) {
            switch (type) {
                case FIXED_COUNT:
                    return Math.min(fixedCount, chunkSize);

                case RANGE_COUNT_INCLUSIVE:
                    int countInclusive = random.nextInt(maxCount - minCount + 1) + minCount;
                    return Math.min(countInclusive, chunkSize);

                case RANGE_COUNT_EXCLUSIVE:
                    int countExclusive = random.nextInt(maxCount - minCount) + minCount;
                    return Math.min(countExclusive, chunkSize);

                case FIXED_PERCENT:
                    return (int) Math.round(chunkSize * fixedPercent / 100.0);

                case RANGE_PERCENT:
                    double randomPercent = random.nextDouble() * (maxPercent - minPercent) + minPercent;
                    return (int) Math.round(chunkSize * randomPercent / 100.0);

                default:
                    return 0;
            }
        }

        @Override
        public String toString() {
            switch (type) {
                case FIXED_COUNT:
                    return fixedCount + " bytes";
                case RANGE_COUNT_INCLUSIVE:
                    return minCount + "-" + maxCount + " bytes (inclusive)";
                case RANGE_COUNT_EXCLUSIVE:
                    return minCount + ".." + maxCount + " bytes (exclusive)";
                case FIXED_PERCENT:
                    return fixedPercent + "% of chunk";
                case RANGE_PERCENT:
                    return minPercent + "%-" + maxPercent + "% of chunk";
                default:
                    return "unknown";
            }
        }
    }
}
