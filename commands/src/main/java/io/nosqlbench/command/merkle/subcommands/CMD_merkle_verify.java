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
import io.nosqlbench.command.merkle.console.MerkleConsoleDisplay;
import io.nosqlbench.vectordata.merklev2.MerkleMismatch;
import io.nosqlbench.vectordata.merklev2.MerkleRange;
import io.nosqlbench.vectordata.merklev2.MerkleRefFactory;
import io.nosqlbench.vectordata.merklev2.MerkleDataImpl;
import io.nosqlbench.vectordata.merklev2.MerkleRefBuildProgress;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/// Command to verify files against their existing Merkle tree files.
@Command(
    name = "verify",
    description = "Verify files against their Merkle trees"
)
public class CMD_merkle_verify implements Callable<Integer> {
    private static final Logger logger = LogManager.getLogger(CMD_merkle_verify.class);

    // File extensions for merkle tree files - using constants from MerkleUtils
    public static final String MRKL = MerkleUtils.MRKL;
    public static final String MREF = MerkleUtils.MREF;

    @Parameters(index = "0..*", description = "Files to process")
    private List<Path> files = new ArrayList<>();

    @Option(names = {"--chunk-size"},
        description = "Chunk size in bytes (must be a power of 2, default: ${DEFAULT-VALUE})",
        defaultValue = "1048576" // 1MB
    )
    private long chunkSize;

    @Option(names = {"-f", "--force"}, description = "Overwrite existing Merkle files, even if they are up-to-date")
    private boolean force = false;

    @Option(names = {"--dryrun", "-n"}, description = "Show which files would be processed without actually verifying files")
    private boolean dryrun = false;

    /// Executes the command with the specified options.
    /// This method is called by the command line framework when the command is executed.
    /// It delegates to the execute method with the options specified on the command line.
    ///
    /// @return 0 if the operation was successful, 1 otherwise
    /// @throws Exception if an error occurs during execution
    @Override
    public Integer call() throws Exception {
        boolean success = execute(files, chunkSize, force, dryrun);
        return success ? 0 : 1;
    }

    /// Execute the verify command on the specified files.
    ///
    /// @param files     The list of files to process
    /// @param chunkSize The chunk size to use for Merkle tree operations
    /// @param force     Whether to force overwrite of existing files (not used in verify)
    /// @param dryrun    Whether to only show what would be done without actually verifying files
    /// @return true if the operation was successful, false otherwise
    public boolean execute(List<Path> files, long chunkSize, boolean force, boolean dryrun) {
        boolean success = true;
        for (Path file : files) {
            try {
                if (!Files.exists(file)) {
                    logger.error("File not found: {}", file);
                    success = false;
                    continue;
                }

                Path merklePath = file.resolveSibling(file.getFileName() + MRKL);
                if (!Files.exists(merklePath)) {
                    logger.error("Merkle file not found for: {}", file);
                    success = false;
                    continue;
                }

                if (dryrun) {
                    logger.info("DRY RUN: Would verify file against its Merkle tree: {}", file);
                } else {
                    // Call the verifyFile method
                    verifyFile(file, merklePath, chunkSize);
                }
            } catch (Exception e) {
                logger.error("Error verifying file: {}", file, e);
                success = false;
            }
        }
        return success;
    }

    /// Verifies a file against its Merkle tree.
    /// This method builds a new Merkle tree from the file and compares it with the existing Merkle tree.
    /// If the trees match, the file is considered verified. Otherwise, mismatches are reported.
    ///
    /// The verification process includes:
    /// 1. Loading the original Merkle tree from the specified path
    /// 2. Building a new Merkle tree from the current file content
    /// 3. Comparing the two trees to detect any differences
    /// 4. Reporting the results of the comparison
    ///
    /// ```
    /// File      Merkle Tree
    /// ┌────┐    ┌─────┐
    /// │Data│───>│Build│
    /// └────┘    └─────┘
    ///             │
    ///             ▼
    /// ┌────────┐ ┌─────────┐
    /// │Original│ │Generated│
    /// │Tree    │ │Tree     │
    /// └────────┘ └─────────┘
    ///      │         │
    ///      └────┬────┘
    ///           ▼
    ///      ┌─────────┐
    ///      │Compare  │
    ///      └─────────┘
    /// ```
    ///
    /// @param file The file to verify
    /// @param merklePath The path to the Merkle tree file
    /// @param chunkSize The chunk size to use for Merkle tree operations
    /// @throws Exception If an error occurs during verification, including if the file doesn't match its Merkle tree
    public void verifyFile(Path file, Path merklePath, long chunkSize) throws Exception {
        try (MerkleConsoleDisplay display = new MerkleConsoleDisplay(file)) {
            display.setStatus("Preparing to verify file against Merkle tree");
            display.startProgressThread();

            // Get file size and compute initial range
            long fileSize = Files.size(file);
            display.log("File size: %d bytes", fileSize);

            // Calculate number of chunks to process
            int numChunks = (int) ((fileSize + chunkSize - 1) / chunkSize);
            display.log("Processing file in %d chunks", numChunks);

            // Load the original Merkle reference from file
            display.setStatus("Loading original Merkle reference");
            MerkleDataImpl originalRef = MerkleRefFactory.load(merklePath);
            display.log("Original Merkle reference loaded successfully");

            // Create a new Merkle reference from the current file content using the Path version of fromData
            display.setStatus("Building verification Merkle reference");
            display.log("Using MerkleRefFactory.fromData for direct file processing");

            // Start the Merkle reference creation process
            MerkleRefBuildProgress progress = MerkleRefFactory.fromData(file);

            // Set up a thread to monitor progress
            Thread progressMonitorThread = new Thread(() -> {
                try {
                    while (!Thread.currentThread().isInterrupted()) {
                        // Update the display with current progress
                        int processedChunks = progress.getProcessedChunks();
                        int totalChunks = progress.getTotalChunks();
                        long totalBytes = progress.getTotalBytes();

                        // Calculate bytes processed based on chunks
                        long bytesProcessed = (long) processedChunks * chunkSize;
                        if (bytesProcessed > totalBytes) {
                            bytesProcessed = totalBytes;
                        }

                        // Update the display
                        display.updateProgress(bytesProcessed, totalBytes, processedChunks, totalChunks);
                        display.setStatus("Building verification Merkle reference: " + progress.getPhase());

                        // Check if the future is done
                        if (progress.getFuture().isDone()) {
                            break;
                        }

                        // Sleep for a short time before updating again
                        Thread.sleep(100);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            // Start the progress monitor thread
            progressMonitorThread.start();

            try {
                // Wait for the Merkle reference to be built
                MerkleDataImpl currentRef = progress.getFuture().get();

                // Stop the progress monitor thread
                progressMonitorThread.interrupt();
                progressMonitorThread.join();

                // Compare the references
                display.setStatus("Comparing Merkle references");
                boolean isEqual = originalRef.equals(currentRef);

                if (isEqual) {
                    display.log("Verification successful: %s matches its Merkle reference", file);
                    logger.info("Verification successful: {} matches its Merkle reference", file);
                } else {
                    // Find mismatches between the references
                    display.setStatus("Finding mismatches between references");
                    List<MerkleMismatch> mismatches = originalRef.findMismatchedChunks(currentRef);
                    display.log("Verification failed: %s does not match its Merkle reference", file);
                    display.log("Found %d mismatched sections", mismatches.size());
                    logger.error("Verification failed: {} does not match its Merkle reference", file);
                    logger.error("Found {} mismatched sections", mismatches.size());

                    // Log details of the first few mismatches
                    int mismatchesToShow = Math.min(5, mismatches.size());
                    for (int i = 0; i < mismatchesToShow; i++) {
                        MerkleMismatch mismatch = mismatches.get(i);
                        display.log("  Mismatch at offset %d (length: %d)", mismatch.startInclusive(), mismatch.length());
                        logger.error("  Mismatch at offset {} (length: {})", mismatch.startInclusive(), mismatch.length());
                    }

                    throw new RuntimeException("File verification failed");
                }
            } catch (Exception e) {
                // Stop the progress monitor thread
                progressMonitorThread.interrupt();
                progressMonitorThread.join();

                display.log("Error verifying file: %s", e.getMessage());
                throw e;
            }
        }
    }
}
