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

import io.nosqlbench.command.merkle.MerkleUtils;
import io.nosqlbench.nbdatatools.api.types.bitimage.Glyphs;
import io.nosqlbench.vectordata.merkle.MerkleFooter;
import io.nosqlbench.vectordata.merklev2.MerkleRefFactory;
import io.nosqlbench.vectordata.merklev2.MerkleDataImpl;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Command to display summary information about existing Merkle tree files.
 */
@Command(
    name = "summary",
    description = "Display summary information about Merkle trees"
)
public class CMD_merkle_summary implements Callable<Integer> {
    private static final Logger logger = LogManager.getLogger(CMD_merkle_summary.class);

    // File extensions for merkle tree files - using constants from MerkleUtils
    public static final String MRKL = MerkleUtils.MRKL;
    public static final String MREF = MerkleUtils.MREF;

    @Parameters(index = "0..*", description = "Files to process")
    private List<Path> files = new ArrayList<>();

    /**
     * Sets the files list for testing purposes.
     * 
     * @param files The list of files to process
     */
    public void setFiles(List<Path> files) {
        this.files = files;
    }

    @Option(names = {"--chunk-size"},
        description = "Chunk size in bytes (must be a power of 2, default: ${DEFAULT-VALUE})",
        defaultValue = "1048576" // 1MB
    )
    private long chunkSize;

    @Option(names = {"-f", "--force"}, description = "Overwrite existing Merkle files, even if they are up-to-date")
    private boolean force = false;

    @Option(names = {"--dryrun", "-n"}, description = "Show which files would be processed without actually displaying summaries")
    private boolean dryrun = false;

    @Override
    public Integer call() throws Exception {
        boolean success = execute(files, chunkSize, force, dryrun);
        return success ? 0 : 1;
    }

    /**
     * Execute the summary command on the specified files.
     *
     * @param files     The list of files to process
     * @param chunkSize The chunk size to use for Merkle tree operations (not used in summary)
     * @param force     Whether to force overwrite of existing files (not used in summary)
     * @param dryrun    Whether to only show what would be done without actually displaying summaries
     * @return true if the operation was successful, false otherwise
     */
    public boolean execute(List<Path> files, long chunkSize, boolean force, boolean dryrun) {
        boolean success = true;
        for (Path file : files) {
            try {
                if (!Files.exists(file)) {
                    logger.error("File not found: {}", file);
                    success = false;
                    continue;
                }

                // Determine the appropriate Merkle file path based on the file extension
                Path merklePath = determineMerklePath(file);

                if (!Files.exists(merklePath)) {
                    logger.error("Merkle file not found for: {}", file);
                    success = false;
                    continue;
                }

                if (dryrun) {
                    logger.info("DRY RUN: Would display summary for Merkle file: {}", merklePath);
                } else {
                    // Call the displayMerkleSummary method
                    displayMerkleSummary(merklePath);
                }
            } catch (Exception e) {
                logger.error("Error displaying summary for: {}", file, e);
                success = false;
            }
        }
        return success;
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

    /// Main method to run the SummaryCommand directly
    /// @param args Command line arguments
    public static void main(String[] args) {
        int exitCode = new CommandLine(new CMD_merkle_summary()).execute(args);
        System.exit(exitCode);
    }

    /// Displays a summary of the Merkle tree file.
    ///
    /// @param merklePath The path to the Merkle tree file
    /// @throws IOException If there's an error reading the file
    public void displayMerkleSummary(Path merklePath) throws IOException {
        logger.info("Displaying summary for Merkle tree file: {}", merklePath);

        String fileName = merklePath.getFileName().toString();
        boolean isMerkleFile = fileName.endsWith(".mrkl") || fileName.endsWith(".mref");

        // If this is a Merkle file, display its summary directly
        if (isMerkleFile) {
            displayMerkleFileSummary(merklePath);
        } else {
            // Otherwise, look for an associated Merkle file
            Path mrklPath = merklePath.resolveSibling(fileName + ".mrkl");
            Path mrefPath = merklePath.resolveSibling(fileName + ".mref");

            if (Files.exists(mrklPath)) {
                displayMerkleFileSummary(mrklPath);
            } else if (Files.exists(mrefPath)) {
                displayMerkleFileSummary(mrefPath);
            } else {
                logger.error("No Merkle file found for: {}", merklePath);
            }
        }
    }

    /// Displays a summary of a Merkle tree file.
    ///
    /// @param merklePath The path to the Merkle tree file
    /// @throws IOException If there's an error reading the file
    private void displayMerkleFileSummary(Path merklePath) throws IOException {
        // Load the Merkle tree from the file
        MerkleDataImpl merkleRef = MerkleRefFactory.load(merklePath);

        // Get the file size
        long fileSize = Files.size(merklePath);

        // Read the footer directly from the file to get all footer data
        MerkleFooter footer = MerkleUtils.readMerkleFooter(merklePath);

        // Determine the file type
        String fileType = merklePath.getFileName().toString().endsWith(".mref") ? "MERKLE REFERENCE FILE" : "MERKLE TREE FILE";

        // Get the content file path and size
        String contentFileName = merklePath.getFileName().toString();
        if (contentFileName.endsWith(".mrkl")) {
            contentFileName = contentFileName.substring(0, contentFileName.length() - 5);
        } else if (contentFileName.endsWith(".mref")) {
            contentFileName = contentFileName.substring(0, contentFileName.length() - 5);
        }
        Path contentFilePath = merklePath.resolveSibling(contentFileName);
        long contentFileSize = Files.exists(contentFilePath) ? Files.size(contentFilePath) : 0;

        // Get the number of chunks
        int numberOfChunks = merkleRef.getShape().getLeafCount();

        // Count valid and total nodes
        int totalLeafNodes = numberOfChunks;
        int totalParentNodes;

        // Get total parent nodes from the shape
        totalParentNodes = merkleRef.getShape().getInternalNodeCount();
        int totalAllNodes = totalLeafNodes + totalParentNodes;

        // For reference trees, all leaves are considered valid
        int validLeafNodes = totalLeafNodes;

        // For parent nodes, we can't directly check if they're valid
        // We'll estimate based on the tree structure
        int validParentNodes = 0;
        // If all leaf nodes are valid, all parent nodes are valid
        if (validLeafNodes == totalLeafNodes) {
            validParentNodes = totalParentNodes;
        } else {
            // Estimate: If a leaf node is valid, its parent is likely valid
            // This is a rough estimate and may not be accurate
            validParentNodes = Math.min(totalParentNodes, validLeafNodes / 2);
        }

        int validAllNodes = validLeafNodes + validParentNodes;

        // For reference trees, all leaf nodes are valid
        BitSet leafStatus = new BitSet(totalLeafNodes);
        leafStatus.set(0, totalLeafNodes);

        // Generate the braille-formatted image
        String brailleImage = Glyphs.braille(leafStatus);

        // Build the header for the summary
        StringBuilder summary = new StringBuilder();
        summary.append("\n").append(fileType).append(" SUMMARY\n");
        summary.append("=======================\n");
        summary.append(String.format("File: %s\n", merklePath.toAbsolutePath()));
        summary.append(String.format("File Size: %s\n", MerkleUtils.formatByteSize(fileSize)));

        if (Files.exists(contentFilePath)) {
            summary.append(String.format("Content File: %s\n", contentFilePath.toAbsolutePath()));
            summary.append(String.format("Content File Size: %s\n", MerkleUtils.formatByteSize(contentFileSize)));
        } else {
            summary.append("Content File: Not found\n");
        }

        summary.append(String.format("Number of Chunks: %d\n\n", numberOfChunks));

        // Append the Merkle shape information
        summary.append(String.format("Shape: %s\n", merkleRef.getShape().toString()));

        // Add node count information
        summary.append("Node Counts:\n");
        summary.append(String.format("Valid Leaf Nodes: %d\n", validLeafNodes));
        summary.append(String.format("Valid Parent Nodes: %d\n", validParentNodes));
        summary.append(String.format("Valid Total Nodes: %d\n\n", validAllNodes));

        summary.append(String.format("Total Leaf Nodes: %d\n", totalLeafNodes));
        summary.append(String.format("Total Parent Nodes: %d\n", totalParentNodes));
        summary.append(String.format("Total All Nodes: %d\n\n", totalAllNodes));

        // Add footer information
        summary.append("Footer Information:\n");
        summary.append(String.format("Chunk Size: %s\n", MerkleUtils.formatByteSize(footer.chunkSize())));
        summary.append(String.format("Total Size: %s\n", MerkleUtils.formatByteSize(footer.totalSize())));
        summary.append(String.format("Footer Length: %d bytes\n\n", footer.footerLength()));

        // Add braille-formatted image
        summary.append("Leaf Node Status (Braille Format):\n");
        summary.append(brailleImage).append("\n\n");

        // If this is a reference file, add information about the original file
        if (merklePath.getFileName().toString().endsWith(".mref")) {
            String originalFileName = merklePath.getFileName().toString().replace(".mref", "");
            Path originalPath = merklePath.resolveSibling(originalFileName);

            summary.append("Reference Information:\n");
            summary.append(String.format("Original File: %s\n", originalPath.toAbsolutePath()));

            if (Files.exists(originalPath)) {
                long originalSize = Files.size(originalPath);
                summary.append(String.format("Original File Size: %s\n", MerkleUtils.formatByteSize(originalSize)));
                summary.append(String.format("Size Ratio: %.2f%%\n", (double) fileSize / originalSize * 100));
            } else {
                summary.append("Original File: Not found\n");
            }
        }

        // Print the complete summary
        System.out.println(summary);
        
        // Close the merkle reference
        merkleRef.close();
    }
}
