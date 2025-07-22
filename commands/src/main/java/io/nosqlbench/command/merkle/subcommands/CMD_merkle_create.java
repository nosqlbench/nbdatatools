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
import io.nosqlbench.command.merkle.console.SimpleProgressReporter;
import io.nosqlbench.common.types.VectorFileExtension;
import io.nosqlbench.vectordata.merkle.MerkleRange;
import io.nosqlbench.vectordata.merklev2.MerkleRefBuildProgress;
import io.nosqlbench.vectordata.merklev2.MerkleRefFactory;
import io.nosqlbench.vectordata.merklev2.MerkleDataImpl;
import io.nosqlbench.vectordata.merklev2.MerkleShape;
import io.nosqlbench.vectordata.merklev2.BaseMerkleShape;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/// Command to create Merkle tree files for specified files.
/// 
/// This command supports the following options:
/// - `-f, --force`: Overwrite existing Merkle files, even if they are up-to-date
/// - `-u, --update`: Update only existing Merkle files (implies -f)
/// - `-m, --match-extensions`: Create Merkle files for files with matching extensions only if they don't already have one
/// - `--dryrun, -n`: Show which files would be processed without actually creating Merkle files
/// - `--chunk-size`: Chunk size in bytes (must be a power of 2, default: 1MB)
@Command(
    name = "create",
    description = "Create new Merkle tree files"
)
public class CMD_merkle_create implements Callable<Integer> {
    private static final Logger logger = LogManager.getLogger(CMD_merkle_create.class);

    /**
     * Default constructor for CMD_merkle_create.
     */
    public CMD_merkle_create() {
        // Default constructor  
    }

    /** Merkle reference file extension */
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

    @Option(names = {"-u", "--update"}, description = "Update only existing Merkle files (implies -f)")
    private boolean update = false;

    @Option(names = {"-m", "--match-extensions"}, description = "Create Merkle files for files with matching extensions only if they don't already have one")
    private boolean matchExtensions = false;

    @Option(names = {"--dryrun", "-n"}, description = "Show which files would be processed without actually creating Merkle files")
    private boolean dryrun = false;

    @Option(names = {"--no-tui"}, description = "Disable TUI view and use simple progress reporting")
    private boolean noTui = false;

    @Option(names = {"--progress-interval"}, description = "Progress reporting interval in seconds (default: ${DEFAULT-VALUE})", defaultValue = "5")
    private int progressInterval = 5;

    /// Executes the command with the specified options.
    /// This method is called by the command line framework when the command is executed.
    /// It delegates to the execute method with the options specified on the command line.
    ///
    /// @return 0 if the operation was successful, 1 otherwise
    /// @throws Exception if an error occurs during execution
    @Override
    public Integer call() throws Exception {
        // If update is specified, force is implied
        if (update) {
            force = true;
        }
        boolean success = execute(files, chunkSize, force, update, matchExtensions, dryrun, noTui, progressInterval);
        return success ? 0 : 1;
    }

    /**
     * Execute the create command on the specified files.
     * This is a backward-compatible version of the execute method for existing tests.
     *
     * @param files     The list of files to process
     * @param chunkSize The chunk size to use for Merkle tree operations
     * @param force     Whether to force overwrite of existing files
     * @param dryrun    Whether to only show what would be done without actually creating files
     * @return true if the operation was successful, false otherwise
     */
    public boolean execute(List<Path> files, long chunkSize, boolean force, boolean dryrun) {
        // Call the new version with default values for the new parameters
        return execute(files, chunkSize, force, false, false, dryrun, false, 5);
    }

    /**
     * Execute the create command on the specified files.
     *
     * @param files           The list of files to process
     * @param chunkSize       The chunk size to use for Merkle tree operations
     * @param force           Whether to force overwrite of existing files
     * @param update          Whether to update only existing Merkle files
     * @param matchExtensions Whether to create Merkle files for files with matching extensions only
     * @param dryrun          Whether to only show what would be done without actually creating files
     * @param noTui           Whether to disable TUI and use simple progress reporting
     * @param progressInterval Progress reporting interval in seconds
     * @return true if the operation was successful, false otherwise
     */
    public boolean execute(List<Path> files, long chunkSize, boolean force, boolean update, boolean matchExtensions, boolean dryrun, boolean noTui, int progressInterval) {
        boolean success = true;
        try {
            // Expand directories with extensions
            List<Path> expandedFiles = MerkleUtils.expandDirectoriesWithExtensions(files);

            // Filter out Merkle files (.mrkl and .mref) when creating Merkle files
            expandedFiles = expandedFiles.stream()
                .filter(path -> {
                    String fileName = path.getFileName().toString().toLowerCase();
                    return !fileName.endsWith(MerkleUtils.MRKL) && !fileName.endsWith(MREF);
                })
                .collect(Collectors.toList());

            // If update mode is enabled, only process files that already have a .mref file
            if (update) {
                expandedFiles = expandedFiles.stream()
                    .filter(path -> {
                        Path merklePath = path.resolveSibling(path.getFileName() + MREF);
                        return Files.exists(merklePath);
                    })
                    .collect(Collectors.toList());

                if (expandedFiles.isEmpty()) {
                    logger.warn("No files with existing Merkle files found to update");
                    return true;
                }
            }

            // If match-extensions mode is enabled, only process files with matching extensions
            // that don't already have a .mref file
            if (matchExtensions && !update) {
                // Get the set of extensions from the files parameter
                Set<String> extensions = new HashSet<>();
                for (Path path : files) {
                    String pathStr = path.toString();
                    if (pathStr.startsWith(".")) {
                        extensions.add(pathStr.toLowerCase());
                    }
                }

                // If no extensions were specified, use the default extensions from CMD_merkle
                if (extensions.isEmpty()) {
                    // Use the default extensions from VectorFileExtension enum
                    extensions.addAll(VectorFileExtension.getAllExtensions());
                }

                // Filter files by extension and only include those without existing .mref files
                expandedFiles = expandedFiles.stream()
                    .filter(path -> {
                        String fileName = path.getFileName().toString().toLowerCase();
                        Path merklePath = path.resolveSibling(path.getFileName() + MREF);

                        // Check if the file has one of the specified extensions
                        boolean hasMatchingExtension = extensions.stream()
                            .anyMatch(fileName::endsWith);

                        // Only include files with matching extensions that don't already have a .mref file
                        return hasMatchingExtension && !Files.exists(merklePath);
                    })
                    .collect(Collectors.toList());

                if (expandedFiles.isEmpty()) {
                    logger.warn("No files with matching extensions found without existing Merkle files");
                    return true;
                }
            }

            if (expandedFiles.isEmpty()) {
                logger.warn("No files found to process");
                return true;
            }

            // Pre-walk phase: enumerate all files and calculate total blocks
            List<Path> filesToProcess = new ArrayList<>();
            long totalBlocks = 0;

            logger.info("Starting pre-walk phase to calculate total blocks for {} files", expandedFiles.size());

            for (Path file : expandedFiles) {
                try {
                    if (!Files.exists(file) || !Files.isRegularFile(file)) {
                        logger.error("File not found or not a regular file: {}", file);
                        success = false;
                        continue;
                    }

                    Path merklePath = file.resolveSibling(file.getFileName() + MREF);
                    boolean shouldProcess = true;

                    if (Files.exists(merklePath)) {
                        if (!force) {
                            // Try to verify the existing MerkleRef file using the new API
                            boolean isValid = false;
                            try {
                                MerkleDataImpl existingMerkle = MerkleRefFactory.load(merklePath);
                                isValid = (existingMerkle != null);
                                if (existingMerkle != null) {
                                    existingMerkle.close(); // Clean up resources
                                }
                            } catch (Exception e) {
                                // File exists but can't be loaded, treat as invalid
                                logger.debug("Existing merkle file could not be loaded: {}", e.getMessage());
                            }

                            if (!isValid) {
                                // Merkle file is invalid or incompatible, we need to recreate it
                                if (dryrun) {
                                    logger.info("DRY RUN: Would recreate invalid Merkle file for: {}", file);
                                } else {
                                    logger.warn("Merkle file is invalid and will be recreated: {}", file);
                                }
                            } else {
                                // Merkle file is valid, now check if it's up-to-date
                                long sourceLastModified = Files.getLastModifiedTime(file).toMillis();
                                long merkleLastModified = Files.getLastModifiedTime(merklePath).toMillis();

                                if (merkleLastModified >= sourceLastModified) {
                                    // Merkle file is up-to-date, skip this file
                                    if (dryrun) {
                                        logger.info("DRY RUN: Would skip file as Merkle file is up-to-date: {}", file);
                                    } else {
                                        logger.info("Skipping file as Merkle file is up-to-date: {}", file);
                                    }
                                    shouldProcess = false;
                                } else {
                                    // Source file is newer than merkle file, recreate it
                                    if (dryrun) {
                                        logger.info("DRY RUN: Would recreate outdated Merkle file for: {}", file);
                                    } else {
                                        logger.info("Recreating outdated Merkle file for: {}", file);
                                    }
                                }
                            }
                        } else if (dryrun) {
                            // If force is true and dryrun is true
                            logger.info("DRY RUN: Would overwrite existing Merkle file for: {}", file);
                        } else {
                            // If force is true and dryrun is false
                            logger.info("Overwriting existing Merkle file for: {}", file);
                        }
                    } else if (dryrun) {
                        // No existing Merkle file and dryrun is true
                        logger.info("DRY RUN: Would create new Merkle file for: {}", file);
                        continue;
                    }

                    if (shouldProcess) {
                        // Calculate the number of blocks for this file
                        long fileSize = Files.size(file);
                        MerkleRange fullRange = new MerkleRange(0, fileSize);
                        MerkleShape geometry = BaseMerkleShape.fromContentSize(fileSize);
                        // Only count leaf chunks in progress since internal nodes are processed very quickly
                        int leafChunks = geometry.getTotalChunks();
                        int internalNodes = geometry.getInternalNodeCount();
                        int fileBlocks = leafChunks; // Only count leaf chunks for progress tracking

                        // Add to total blocks (leaf chunks only)
                        totalBlocks += fileBlocks;

                        // Add to files to process
                        filesToProcess.add(file);

                        if (!dryrun) {
                            logger.info("File: {} - Size: {} bytes, Leaf chunks: {}, Internal nodes: {}, Progress blocks: {}", 
                                       file, fileSize, leafChunks, internalNodes, fileBlocks);
                        }
                    }
                } catch (Exception e) {
                    logger.error("Error calculating blocks for file: {}", file, e);
                    success = false;
                }
            }

            if (filesToProcess.isEmpty()) {
                logger.info("No files need processing");
                return true;
            }

            if (dryrun) {
                logger.info("DRY RUN: Would process {} files with a total of {} leaf chunks", filesToProcess.size(), totalBlocks);
            } else {
                logger.info("Processing {} files with a total of {} leaf chunks", filesToProcess.size(), totalBlocks);
            }

            // Create an atomic counter to track total leaf chunks processed across all files
            AtomicLong totalBlocksProcessed = new AtomicLong(0);

            // Process each file
            for (Path file : filesToProcess) {
                try {
                    // Call the createMerkleFile method if not in dryrun mode
                    if (!dryrun) {
                        createMerkleFile(file, chunkSize, totalBlocksProcessed, totalBlocks, noTui, progressInterval);
                    }
                } catch (Exception e) {
                    logger.error("Error {} Merkle file for: {}", dryrun ? "analyzing" : "creating", file, e);
                    success = false;
                }
            }
        } catch (IOException e) {
            logger.error("Error expanding directories: {}", e.getMessage(), e);
            success = false;
        }
        return success;
    }

    /**
     * Creates a Merkle tree file for the specified file.
     * This is a convenience method that delegates to the more detailed version with progress tracking.
     * 
     * The Merkle tree creation process involves:
     * 1. Calculating the file size and determining the number of chunks
     * 2. Creating a Merkle tree structure based on the file content
     * 3. Computing hashes for each chunk of the file
     * 4. Building the tree by combining hashes in a binary tree structure
     * 5. Saving the resulting Merkle tree to a file with the same name as the input file plus the .mref extension
     *
     * ```
     * File          Merkle Tree Creation
     * ┌────┐        ┌─────────────┐
     * │Data│───────>│Divide into  │
     * └────┘        │chunks       │
     *               └─────────────┘
     *                      │
     *                      ▼
     *               ┌─────────────┐
     *               │Compute hash │
     *               │for each     │
     *               │chunk        │
     *               └─────────────┘
     *                      │
     *                      ▼
     *               ┌─────────────┐
     *               │Build binary │
     *               │tree         │
     *               └─────────────┘
     *                      │
     *                      ▼
     *               ┌─────────────┐
     *               │Save to .mref│
     *               │file         │
     *               └─────────────┘
     * ```
     *
     * @param file The file to create a Merkle tree for
     * @param chunkSize The chunk size to use for Merkle tree operations (must be a power of 2)
     * @throws Exception If an error occurs during Merkle tree creation, such as I/O errors or invalid parameters
     */
    public void createMerkleFile(Path file, long chunkSize) throws Exception {
        createMerkleFile(file, chunkSize, null, 0, false, 5);
    }

    /**
     * Creates a Merkle tree file for the specified file with session-wide progress tracking.
     * This is the primary implementation that handles both individual file progress and session-wide progress tracking.
     * 
     * This method extends the basic Merkle tree creation process by adding:
     * - Session-wide progress tracking across multiple files
     * - Detailed console display with progress information
     * - Estimation of Merkle tree file size
     * - Parallel processing of file chunks using virtual threads
     *
     * The progress tracking works at two levels:
     * 1. File-level: Tracks chunks processed within the current file
     * 2. Session-level: Tracks total blocks processed across all files in the session
     *
     * ```
     * Session Progress Tracking
     * ┌───────────┐     ┌───────────┐     ┌───────────┐
     * │File 1     │     │File 2     │     │File 3     │
     * │Progress   │────>│Progress   │────>│Progress   │
     * └───────────┘     └───────────┘     └───────────┘
     *       │                 │                 │
     *       ▼                 ▼                 ▼
     * ┌─────────────────────────────────────────────┐
     * │             Session Progress                │
     * │  [====================----------] 67%       │
     * └─────────────────────────────────────────────┘
     * ```
     *
     * @param file The file to create a Merkle tree for
     * @param chunkSize The chunk size to use for Merkle tree operations (must be a power of 2)
     * @param totalBlocksProcessed Counter for tracking total leaf chunks processed across all files (can be null if session tracking is not needed)
     * @param totalBlocks Total number of leaf chunks across all files (used for session-wide progress calculation)
     * @param noTui Whether to disable TUI and use simple progress reporting
     * @param progressInterval Progress reporting interval in seconds (for non-TUI mode)
     * @throws Exception If an error occurs during Merkle tree creation, such as I/O errors, invalid parameters, or if the file cannot be read
     */
    public void createMerkleFile(Path file, long chunkSize, AtomicLong totalBlocksProcessed, long totalBlocks, boolean noTui, int progressInterval) throws Exception {
        // Calculate total blocks for this file (only count leaf chunks for progress tracking)
        long fileSize = Files.size(file);
        MerkleShape geometry = BaseMerkleShape.fromContentSize(fileSize);
        int leafChunks = geometry.getTotalChunks();
        int internalNodes = geometry.getInternalNodeCount();
        int totalFileBlocks = leafChunks; // Only count leaf chunks for progress
        
        if (noTui) {
            // Use simple progress reporting  
            try (SimpleProgressReporter reporter = new SimpleProgressReporter(file, progressInterval, totalBlocksProcessed, totalBlocks, totalFileBlocks)) {
                createMerkleFileWithSimpleReporter(file, chunkSize, totalBlocksProcessed, totalBlocks, totalFileBlocks, leafChunks, internalNodes, reporter);
            }
        } else {
            // Use TUI display
            try (MerkleConsoleDisplay display = new MerkleConsoleDisplay(file)) {
                display.setStatus("Preparing to compute Merkle tree");
                display.startProgressThread();
                createMerkleFileWithTuiDisplay(file, chunkSize, totalBlocksProcessed, totalBlocks, totalFileBlocks, leafChunks, internalNodes, display);
            }
        }
    }

    /**
     * Creates a Merkle tree file using the TUI display interface.
     * This method contains the original TUI-based implementation.
     */
    private void createMerkleFileWithTuiDisplay(Path file, long chunkSize, AtomicLong totalBlocksProcessed, long totalBlocks, int totalFileBlocks, int leafChunks, int internalNodes, MerkleConsoleDisplay display) throws Exception {
        // Get file size for logging
        long fileSize = Files.size(file);
        display.log("File size: %d bytes", fileSize);
        display.log("Processing file: %d leaf chunks + %d internal nodes (tracking %d leaf chunks for progress)", 
                   leafChunks, internalNodes, totalFileBlocks);

        // Use virtual threads for per-task execution
        display.log("Using virtual threads per task executor");

        // Estimate Merkle tree file size
        // Each leaf node has a 32-byte hash, and we need one leaf per chunk
        // Plus internal nodes and metadata overhead
        long estimatedLeafNodesSize = leafChunks * 32L; // 32 bytes per SHA-256 hash
        long estimatedInternalNodesSize = internalNodes * 32L; // Internal nodes also have 32-byte hashes
        long estimatedMetadataSize = 1024L; // Rough estimate for metadata
        long estimatedTotalSize = estimatedLeafNodesSize + estimatedInternalNodesSize + estimatedMetadataSize;

        // Display the estimate in human-readable form
        String sizeUnit;
        double displaySize;
        if (estimatedTotalSize > 1024 * 1024) {
          sizeUnit = "MB";
          displaySize = estimatedTotalSize / (1024.0 * 1024.0);
        } else if (estimatedTotalSize > 1024) {
          sizeUnit = "KB";
          displaySize = estimatedTotalSize / 1024.0;
        } else {
          sizeUnit = "bytes";
          displaySize = estimatedTotalSize;
        }

        display.log("Estimated Merkle tree file size: %.2f %s", displaySize, sizeUnit);

        // Create a Merkle tree directly from the file using the fromData method
        display.setStatus("Building Merkle tree");
        display.log("Using MerkleRefFactory.fromData for direct file processing");

        // Create the merkle tree file path
        Path merkleFile = file.resolveSibling(file.getFileName() + MerkleUtils.MREF);

        // Start the Merkle tree creation process
        MerkleRefBuildProgress progress = MerkleRefFactory.fromData(file);

        // Set up a thread to monitor progress
        Thread progressMonitorThread = new Thread(() -> {
          try {
            while (!Thread.currentThread().isInterrupted()) {
              // Update the display with current progress
              int processedChunks = progress.getProcessedChunks();
              int totalChunks = progress.getTotalChunks();
              long totalBytesForFile = progress.getTotalBytes();

              // Calculate bytes processed more accurately based on file progress
              // Note: processedChunks only tracks leaf chunks, not internal nodes
              long bytesProcessed = totalBytesForFile > 0 ? 
                (totalBytesForFile * processedChunks) / leafChunks : 0;

              // Update the display with file-specific progress (using leaf chunks only for consistency)
              display.updateProgress(bytesProcessed, totalBytesForFile, processedChunks, leafChunks);

              // If we're tracking session-wide progress, update that too
              if (totalBlocksProcessed != null && totalBlocks > 0) {
                // Calculate current file progress in terms of leaf chunks only
                // Since we only track leaf chunks, use processed chunks directly
                int currentFileProgress = processedChunks;
                
                // Session progress = already completed + current file progress
                long currentSessionProgress = totalBlocksProcessed.get() + currentFileProgress;
                
                // Update the display with session-wide progress
                display.updateSessionProgress(currentSessionProgress, totalBlocks);
              }

              // Get the current stage and phase
              String currentPhase = progress.getPhase();
              String stageInfo = "";

              try {
                // Get the current stage if available (this is a new feature)
                if (progress.getCurrentStage() != null) {
                  stageInfo = "[" + progress.getCurrentStage().name() + "] ";
                }
              } catch (Exception e) {
                // Ignore any errors if getCurrentStage() is not available
              }

              // Update the display with the current stage and phase
              display.setStatus("Building Merkle tree: " + stageInfo + currentPhase);

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
          // Wait for the Merkle tree to be built
          MerkleDataImpl merkleRef = progress.getFuture().get();

          // Stop the progress monitor thread
          progressMonitorThread.interrupt();
          progressMonitorThread.join();

          // Save the Merkle tree to the file
          display.setStatus("Saving Merkle tree to file");
          merkleRef.save(merkleFile);

          // Update the session-wide counter with the total blocks from this file
          if (totalBlocksProcessed != null) {
            totalBlocksProcessed.set(totalBlocksProcessed.get() + totalFileBlocks);
          }

          display.log("Merkle tree creation completed successfully");
        } catch (Exception e) {
          // Stop the progress monitor thread
          progressMonitorThread.interrupt();
          progressMonitorThread.join();

          display.log("Error creating Merkle tree: %s", e.getMessage());
          throw e;
        }
    }

    /**
     * Creates a Merkle tree file using simple progress reporting.
     * This method provides a simpler, log-based progress reporting that doesn't use TUI.
     * It's suitable for headless environments, CI/CD pipelines, or when TUI is not desired.
     */
    private void createMerkleFileWithSimpleReporter(Path file, long chunkSize, AtomicLong totalBlocksProcessed, long totalBlocks, int totalFileBlocks, int leafChunks, int internalNodes, SimpleProgressReporter reporter) throws Exception {
        // Get file size for logging
        long fileSize = Files.size(file);
        reporter.log("File size: %d bytes", fileSize);
        reporter.log("Processing file: %d leaf chunks + %d internal nodes (tracking %d leaf chunks for progress)", 
                   leafChunks, internalNodes, totalFileBlocks);

        // Use virtual threads for per-task execution
        reporter.log("Using virtual threads per task executor");

        // Estimate Merkle tree file size
        // Each leaf node has a 32-byte hash, and we need one leaf per chunk
        // Plus internal nodes and metadata overhead
        long estimatedLeafNodesSize = leafChunks * 32L; // 32 bytes per SHA-256 hash
        long estimatedInternalNodesSize = internalNodes * 32L; // Internal nodes also have 32-byte hashes
        long estimatedMetadataSize = 1024L; // Rough estimate for metadata
        long estimatedTotalSize = estimatedLeafNodesSize + estimatedInternalNodesSize + estimatedMetadataSize;

        // Display the estimate in human-readable form
        String sizeUnit;
        double displaySize;
        if (estimatedTotalSize > 1024 * 1024) {
          sizeUnit = "MB";
          displaySize = estimatedTotalSize / (1024.0 * 1024.0);
        } else if (estimatedTotalSize > 1024) {
          sizeUnit = "KB";
          displaySize = estimatedTotalSize / 1024.0;
        } else {
          sizeUnit = "bytes";
          displaySize = estimatedTotalSize;
        }

        reporter.log("Estimated Merkle tree file size: %.2f %s", displaySize, sizeUnit);

        // Create a Merkle tree directly from the file using the fromData method
        reporter.setStatus("Building Merkle tree");
        reporter.log("Using MerkleRefFactory.fromData for direct file processing");

        // Create the merkle tree file path
        Path merkleFile = file.resolveSibling(file.getFileName() + MerkleUtils.MREF);

        // Start the Merkle tree creation process
        MerkleRefBuildProgress progress = MerkleRefFactory.fromData(file);

        // Start the progress reporting
        reporter.startReporting(progress);

        try {
          // Wait for the Merkle tree to be built
          MerkleDataImpl merkleRef = progress.getFuture().get();

          // Save the Merkle tree to the file
          reporter.setStatus("Saving Merkle tree to file");
          merkleRef.save(merkleFile);

          // Update the session-wide counter with the total blocks from this file
          if (totalBlocksProcessed != null) {
            totalBlocksProcessed.set(totalBlocksProcessed.get() + totalFileBlocks);
          }

          reporter.log("Merkle tree creation completed successfully");
        } catch (Exception e) {
          reporter.log("Error creating Merkle tree: %s", e.getMessage());
          throw e;
        }
    }
}
