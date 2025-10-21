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

import io.nosqlbench.nbdatatools.api.types.bitimage.Glyphs;
import io.nosqlbench.vectordata.merklev2.MerkleMismatch;
import io.nosqlbench.vectordata.merklev2.MerkleRefFactory;
import io.nosqlbench.vectordata.merklev2.MerkleDataImpl;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Command to display a summary of differences between two Merkle tree files.
 */
@Command(
    name = "diff",
    description = "Display a summary of differences between two Merkle tree files"
)
public class CMD_merkle_diff implements Callable<Integer> {
    private static final Logger logger = LogManager.getLogger(CMD_merkle_diff.class);

    // File extensions for merkle tree files
    public static final String MRKL = ".mrkl";
    public static final String MREF = ".mref";

    @Parameters(index = "0", description = "First Merkle tree file")
    private Path file1;

    @Parameters(index = "1", description = "Second Merkle tree file")
    private Path file2;

    @Option(names = {"-f", "--force"},
        description = "Force diff generation even if supplemental outputs already exist")
    private boolean force = false;

    /**
     * Sets the first file path for testing purposes.
     * 
     * @param file1 The first file path
     */
    public void setFile1(Path file1) {
        this.file1 = file1;
    }

    /**
     * Sets the second file path for testing purposes.
     * 
     * @param file2 The second file path
     */
    public void setFile2(Path file2) {
        this.file2 = file2;
    }

    @Override
    public Integer call() throws Exception {
        boolean success = execute(file1, file2);
        return success ? 0 : 1;
    }

    /**
     * Execute the diff command on the specified files.
     *
     * @param file1 The first Merkle tree file
     * @param file2 The second Merkle tree file
     * @return true if the operation was successful, false otherwise
     */
    public boolean execute(Path file1, Path file2) {
        try {
            // Validate input files
            if (!Files.exists(file1)) {
                logger.error("First file not found: {}", file1);
                return false;
            }

            if (!Files.exists(file2)) {
                logger.error("Second file not found: {}", file2);
                return false;
            }

            // Determine the appropriate Merkle file paths based on the file extensions
            Path merklePath1 = determineMerklePath(file1);
            Path merklePath2 = determineMerklePath(file2);

            if (!Files.exists(merklePath1)) {
                logger.error("Merkle file not found for: {}", file1);
                return false;
            }

            if (!Files.exists(merklePath2)) {
                logger.error("Merkle file not found for: {}", file2);
                return false;
            }

            // Load the Merkle references
            MerkleDataImpl ref1 = MerkleRefFactory.load(merklePath1);
            MerkleDataImpl ref2 = MerkleRefFactory.load(merklePath2);

            // Display the diff summary
            displayDiffSummary(ref1, ref2, merklePath1, merklePath2);

            return true;
        } catch (Exception e) {
            logger.error("Error comparing Merkle trees: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Displays a summary of differences between two Merkle references.
     *
     * @param ref1 The first Merkle reference
     * @param ref2 The second Merkle reference
     * @param path1 The path to the first Merkle reference file
     * @param path2 The path to the second Merkle reference file
     */
    private void displayDiffSummary(MerkleDataImpl ref1, MerkleDataImpl ref2, Path path1, Path path2) {
        StringBuilder summary = new StringBuilder();

        // Add header
        summary.append("\nMERKLE REFERENCE DIFF SUMMARY\n");
        summary.append("============================\n");
        summary.append(String.format("File 1: %s\n", path1.toAbsolutePath()));
        summary.append(String.format("File 2: %s\n", path2.toAbsolutePath()));
        summary.append("\n");

        // Compare basic info
        summary.append("Basic Information Comparison:\n");
        summary.append("----------------------------\n");

        // Compare chunk sizes
        long chunkSize1 = ref1.getChunkSize();
        long chunkSize2 = ref2.getChunkSize();
        boolean chunkSizesMatch = chunkSize1 == chunkSize2;
        summary.append(String.format("Chunk Size: %s vs %s %s\n", 
            formatByteSize(chunkSize1), 
            formatByteSize(chunkSize2),
            chunkSizesMatch ? "(MATCH)" : "(MISMATCH)"));

        // Compare total sizes
        long totalSize1 = ref1.totalSize();
        long totalSize2 = ref2.totalSize();
        boolean totalSizesMatch = totalSize1 == totalSize2;
        summary.append(String.format("Total Size: %s vs %s %s\n", 
            formatByteSize(totalSize1), 
            formatByteSize(totalSize2),
            totalSizesMatch ? "(MATCH)" : "(MISMATCH)"));

        // Compare number of leaves
        int leafCount1 = ref1.getNumberOfLeaves();
        int leafCount2 = ref2.getNumberOfLeaves();
        boolean leafCountsMatch = leafCount1 == leafCount2;
        summary.append(String.format("Leaf Count: %d vs %d %s\n", 
            leafCount1, 
            leafCount2,
            leafCountsMatch ? "(MATCH)" : "(MISMATCH)"));

        // If chunk sizes don't match, we can't compare the references
        if (!chunkSizesMatch) {
            summary.append("\nCannot compare references with different chunk sizes.\n");
            System.out.println(summary);
            return;
        }

        // Find mismatched chunks
        List<MerkleMismatch> mismatches;
        try {
            mismatches = ref1.findMismatchedChunks(ref2);
        } catch (IllegalArgumentException e) {
            summary.append("\nError comparing references: ").append(e.getMessage()).append("\n");
            System.out.println(summary);
            return;
        }

        // Add leaf node differences summary
        summary.append("\nLeaf Node Differences:\n");
        summary.append("---------------------\n");
        summary.append(String.format("Total Mismatched Chunks: %d\n", mismatches.size()));

        if (mismatches.size() > 0) {
            // Calculate percentage of mismatched chunks
            double mismatchPercentage = (double) mismatches.size() / Math.min(leafCount1, leafCount2) * 100;
            summary.append(String.format("Percentage of Mismatched Chunks: %.2f%%\n", mismatchPercentage));

            // List the first few mismatches
            int mismatchesToShow = Math.min(5, mismatches.size());
            summary.append("\nFirst ").append(mismatchesToShow).append(" mismatches:\n");
            for (int i = 0; i < mismatchesToShow; i++) {
                MerkleMismatch mismatch = mismatches.get(i);
                summary.append(String.format("  Chunk %d: offset %d, length %d\n", 
                    mismatch.chunkIndex(), mismatch.startInclusive(), mismatch.length()));
            }

            // Create a BitSet to represent the mismatched chunks
            BitSet mismatchBits = new BitSet(Math.max(leafCount1, leafCount2));
            for (MerkleMismatch mismatch : mismatches) {
                mismatchBits.set(mismatch.chunkIndex());
            }

            // Generate the braille-formatted image
            String brailleImage = Glyphs.braille(mismatchBits);

            // Add visual representation
            summary.append("\nVisual Representation of Differences (Braille Format):\n");
            summary.append("Each dot represents a mismatched chunk\n");
            summary.append(brailleImage).append("\n\n");
        } else {
            summary.append("The Merkle references are identical.\n");
        }

        // Print the complete summary
        System.out.println(summary);
    }

    /**
     * Determines the appropriate Merkle file path based on the file extension.
     *
     * @param file The input file path
     * @return The path to the Merkle file
     */
    private Path determineMerklePath(Path file) {
        String fileName = file.getFileName().toString();

        // If the file is already a Merkle file (.mrkl or .mref), use it directly
        if (fileName.endsWith(MRKL) || fileName.endsWith(MREF)) {
            return file;
        }

        // Otherwise, look for an associated Merkle file
        Path merklePath = file.resolveSibling(fileName + MRKL);
        if (Files.exists(merklePath)) {
            return merklePath;
        }

        // If .mrkl doesn't exist, try .mref
        Path mrefPath = file.resolveSibling(fileName + MREF);
        if (Files.exists(mrefPath)) {
            return mrefPath;
        }

        // Default to .mrkl if neither exists
        return merklePath;
    }

    /**
     * Formats a byte size into a human-readable string.
     *
     * @param bytes The size in bytes
     * @return A human-readable string representation
     */
    private String formatByteSize(long bytes) {
        if (bytes < 1024) {
            return bytes + " bytes";
        } else if (bytes < 1024 * 1024) {
            return String.format("%.2f KB", bytes / 1024.0);
        } else if (bytes < 1024 * 1024 * 1024) {
            return String.format("%.2f MB", bytes / (1024.0 * 1024.0));
        } else {
            return String.format("%.2f GB", bytes / (1024.0 * 1024.0 * 1024.0));
        }
    }
}
