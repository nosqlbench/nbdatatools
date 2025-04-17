package io.nosqlbench.nbvectors.commands.merkle;

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

import io.nosqlbench.vectordata.download.merkle.MerkleNode;
import io.nosqlbench.vectordata.download.merkle.MerkleMismatch;
import io.nosqlbench.vectordata.download.merkle.MerkleRange;
import io.nosqlbench.vectordata.download.merkle.MerkleTree;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static io.nosqlbench.vectordata.download.merkle.MerklePane.MRKL;

@Command(name = "merkle",
    headerHeading = "Usage:%n%n",
    synopsisHeading = "%n",
    descriptionHeading = "%nDescription%n%n",
    parameterListHeading = "%nParameters:%n%",
    optionListHeading = "%nOptions:%n",
    header = "create or verify Merkle tree files for data integrity",
    description = """
        Creates or verifies Merkle tree files for specified files.
        These Merkle tree files can be used later to efficiently
        verify file integrity or identify changed portions of
        files for partial downloads/updates.

        The Merkle tree file is created with the same name as the
        source file plus a .mrkl extension.

        Examples:

        # Create Merkle files for multiple files
        nbvectors merkle file1.hdf5 file2.hdf5

        # Create with custom chunk size (must be power of 2)
        nbvectors merkle --chunk-size 1048576 bigfile.hdf5

        # Verify files against their Merkle trees
        nbvectors merkle -v file1.hdf5 file2.hdf5

        # Force overwrite of existing Merkle files
        nbvectors merkle -f file1.hdf5

        # Dump details of merkle trees for related files
        nbvectors merkle -d file1.hdf5 file2.hdf5
        """,
    exitCodeListHeading = "Exit Codes:%n",
    exitCodeList = {
        "0: Success", "1: Error creating Merkle tree file", "2: Error verifying Merkle tree file"
    },
    subcommands = {CommandLine.HelpCommand.class})
public class CMD_merkle implements Callable<Integer> {
  private static final Logger logger = LogManager.getLogger(CMD_merkle.class);

  @Parameters(description = "Files to process", arity = "1..*")
  private List<Path> files = new ArrayList<>();

  @Option(names = {"-v", "--verify"},
      description = "Verify existing Merkle files instead of creating new ones")
  private boolean verify = false;

  @Option(names = {"-s", "--summary"},
      description = "Display summary information about existing Merkle files instead of creating new ones")
  private boolean summary = false;

  @Option(names = {"--chunk-size"},
      description = "Chunk size in bytes (must be a power of 2, default: ${DEFAULT-VALUE})",
      defaultValue = "1048576" // 1MB
  )
  private long chunkSize;

  @Option(names = {"-f", "--force"}, description = "Overwrite existing Merkle files")
  private boolean force = false;

  @Override
  public Integer call() {
    // Validate chunk size is a power of 2
    if (!isPowerOfTwo(chunkSize)) {
      logger.error("Chunk size must be a power of two, got: {}", chunkSize);
      return 1;
    }

    boolean hasErrors = false;

    for (Path file : files) {
      try {
        if (!Files.exists(file)) {
          logger.error("File not found: {}", file);
          hasErrors = true;
          continue;
        }

        // Construct the merkle file path by appending .merkle to the original file name
        Path merklePath = file.resolveSibling(file.getFileName() + MRKL);

        if (summary) {
          if (!Files.exists(merklePath)) {
            logger.error("Merkle file not found for: {}", file);
            hasErrors = true;
            continue;
          }
          displayMerkleSummary(merklePath);
        } else if (verify) {
          if (!Files.exists(merklePath)) {
            logger.error("Merkle file not found for: {}", file);
            hasErrors = true;
            continue;
          }
          verifyFile(file, merklePath);
        } else {
          if (Files.exists(merklePath) && !force) {
            logger.error("Merkle file already exists for: {} (use --force to overwrite)", file);
            hasErrors = true;
            continue;
          }
          createMerkleFile(file);
        }
      } catch (Exception e) {
        logger.error("Error processing file: {}", file, e);
        hasErrors = true;
      }
    }

    return hasErrors ? 1 : 0;
  }

  private void createMerkleFile(Path file) throws Exception {
    try (MerkleConsoleDisplay display = new MerkleConsoleDisplay(file)) {
      display.setStatus("Preparing to compute Merkle tree");
      display.startProgressThread();

      // Get file size and compute initial range
      long fileSize = Files.size(file);
      display.log("File size: %d bytes", fileSize);

      // Calculate number of chunks to process
      int numChunks = (int) ((fileSize + chunkSize - 1) / chunkSize);
      display.log("Processing file in %d chunks", numChunks);

      // Create a thread pool with a reasonable number of threads
      int numThreads = Math.min(Runtime.getRuntime().availableProcessors(), numChunks);
      display.log("Using %d threads for parallel processing", numThreads);

      // Estimate Merkle tree file size
      // Each leaf node has a 32-byte hash, and we need one leaf per chunk
      // Plus some overhead for the tree structure and metadata
      long estimatedLeafNodesSize = numChunks * 32L; // 32 bytes per SHA-256 hash
      long estimatedInternalNodesSize = estimatedLeafNodesSize; // Internal nodes are roughly the same size as leaf nodes
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

      // Create a shared atomic counter for progress tracking
      AtomicLong bytesProcessed = new AtomicLong(0);

      // Process chunks in parallel
      try (ExecutorService executor = Executors.newFixedThreadPool(numThreads)) {
        // Create a list to hold all the chunk processing futures
        List<Future<ChunkResult>> futures = new ArrayList<>(numChunks);

        // Submit tasks for each chunk
        for (int i = 0; i < numChunks; i++) {
          long startOffset = (long) i * chunkSize;
          long endOffset = Math.min(startOffset + chunkSize, fileSize);
          MerkleRange chunkRange = new MerkleRange(startOffset, endOffset);

          // Submit the chunk processing task
          futures.add(executor.submit(() -> processChunk(file, chunkRange, bytesProcessed, fileSize, display)));
        }

        // Collect results from all futures
        List<ChunkResult> chunkResults = new ArrayList<>(numChunks);
        for (Future<ChunkResult> future : futures) {
          chunkResults.add(future.get());
        }

        // Sort results by chunk index to ensure correct order
        chunkResults.sort((a, b) -> Long.compare(a.range.start(), b.range.start()));

        // Build the final Merkle tree from all chunk results
        display.setStatus("Building final Merkle tree");
        MerkleTree merkleTree = buildMerkleTreeFromChunks(chunkResults, chunkSize, fileSize);

        // Save the Merkle tree
        display.setStatus("Saving Merkle tree");
        saveMerkleTree(file, merkleTree);
        display.log("Merkle tree creation completed successfully");
      }
    }
  }

  /// Processes a single chunk of the file and returns the result.
  ///
  /// @param file The file to process
  /// @param range The range of the file to process
  /// @param bytesProcessed Atomic counter for tracking progress
  /// @param totalSize Total file size
  /// @param display Console display for progress updates
  /// @return The chunk processing result
  private ChunkResult processChunk(Path file, MerkleRange range, AtomicLong bytesProcessed,
                                  long totalSize, MerkleConsoleDisplay display) throws IOException {
    // Calculate chunk size
    long chunkSize = range.size();
    int bufferSize = (int) Math.min(chunkSize, Integer.MAX_VALUE);

    // Read the chunk from the file
    ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
    try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
      channel.position(range.start());
      channel.read(buffer);
      buffer.flip();

      // Create a MerkleTree from this chunk
      MerkleTree chunkTree = MerkleTree.fromData(buffer, chunkSize, range);

      // Update progress
      long newBytesProcessed = bytesProcessed.addAndGet(chunkSize);
      int sectionsCompleted = (int) (newBytesProcessed / chunkSize);
      int totalSections = (int) ((totalSize + chunkSize - 1) / chunkSize);
      display.updateProgress(newBytesProcessed, totalSize, sectionsCompleted, totalSections);

      // Return the result
      return new ChunkResult(range, chunkTree);
    }
  }

  /// Builds a complete Merkle tree from individual chunk results.
  ///
  /// @param chunkResults The results from processing each chunk
  /// @param chunkSize The chunk size
  /// @param totalSize The total file size
  /// @return The complete Merkle tree
  private MerkleTree buildMerkleTreeFromChunks(List<ChunkResult> chunkResults, long chunkSize, long totalSize) {
    // Create a full range for the entire file
    MerkleRange fullRange = new MerkleRange(0, totalSize);

    // If there's only one chunk, return its tree directly
    if (chunkResults.size() == 1) {
      return chunkResults.get(0).tree;
    }

    // Extract all leaf nodes from the chunk trees
    List<MerkleNode> allLeaves = new ArrayList<>();
    for (ChunkResult result : chunkResults) {
      collectLeafNodes(result.tree.root(), allLeaves);
    }

    // Build a new tree from all leaves
    // Use the MerkleTree's static buildTree method
    MerkleNode root = buildTreeFromLeaves(allLeaves);
    return new MerkleTree(root, chunkSize, totalSize, fullRange);
  }

  /// Recursively collects all leaf nodes from a tree.
  ///
  /// @param node The current node
  /// @param leaves The list to collect leaves into
  private void collectLeafNodes(MerkleNode node, List<MerkleNode> leaves) {
    if (node == null) {
      return;
    }

    if (node.isLeaf()) {
      leaves.add(node);
    } else {
      collectLeafNodes(node.left(), leaves);
      collectLeafNodes(node.right(), leaves);
    }
  }

  /// Builds a tree from a list of leaf nodes.
  /// This is a recursive implementation that builds a balanced binary tree.
  ///
  /// @param leaves The list of leaf nodes
  /// @return The root node of the built tree
  private MerkleNode buildTreeFromLeaves(List<MerkleNode> leaves) {
    if (leaves == null || leaves.isEmpty()) {
      return null;
    }

    if (leaves.size() == 1) {
      return leaves.get(0);
    }

    // Create parent nodes by combining pairs of leaves
    List<MerkleNode> parents = new ArrayList<>((leaves.size() + 1) / 2);

    for (int i = 0; i < leaves.size(); i += 2) {
      MerkleNode left = leaves.get(i);
      MerkleNode right = (i + 1 < leaves.size()) ? leaves.get(i + 1) : null;

      // Create a parent node that combines these two children
      parents.add(MerkleNode.internal(
          parents.size(),
          left.hash(),
          right != null ? right.hash() : null,
          left,
          right
      ));
    }

    // Recursively build the tree from the parent nodes
    return buildTreeFromLeaves(parents);
  }

  /// Represents the result of processing a single chunk of the file.
  private static class ChunkResult {
    final MerkleRange range;
    final MerkleTree tree;

    ChunkResult(MerkleRange range, MerkleTree tree) {
      this.range = range;
      this.tree = tree;
    }
  }

  /// Saves a Merkle tree to a file using the current MerkleTree implementation
  /// @param file
  ///     The source file path
  /// @param merkleTree
  ///     The MerkleTree to save
  /// @throws IOException
  ///     If there's an error writing to the file
  private void saveMerkleTree(Path file, MerkleTree merkleTree) throws IOException {
    Path merkleFile = file.resolveSibling(file.getFileName() + MRKL);
    merkleTree.save(merkleFile);
    logger.info("Saved Merkle tree to {}", merkleFile);
  }

  private String createProgressBar(double percent, int width) {
    int completed = (int) (width * (percent / 100.0));
    StringBuilder bar = new StringBuilder();
    for (int i = 0; i < width; i++) {
      if (i < completed) {
        bar.append("=");
      } else if (i == completed) {
        bar.append(">");
      } else {
        bar.append(" ");
      }
    }
    return bar.toString();
  }

  /// Displays a summary of the Merkle tree file.
  ///
  /// @param merklePath The path to the Merkle tree file
  /// @throws IOException If there's an error reading the file
  private void displayMerkleSummary(Path merklePath) throws IOException {
    logger.info("Displaying summary for Merkle tree file: {}", merklePath);

    // Load the Merkle tree from the file
    MerkleTree merkleTree = MerkleTree.load(merklePath);

    // Get the file size
    long fileSize = Files.size(merklePath);

    // Build the header for the summary
    StringBuilder summary = new StringBuilder();
    summary.append("\nMERKLE TREE FILE SUMMARY\n");
    summary.append("=======================\n");
    summary.append(String.format("File: %s\n", merklePath.toAbsolutePath()));
    summary.append(String.format("File Size: %s\n\n", formatByteSize(fileSize)));

    // Append the Merkle tree's toString output
    summary.append(merkleTree.toString());

    // Print the complete summary
    System.out.println(summary);
  }

  /// Formats a byte size into a human-readable string.
  ///
  /// @param bytes The size in bytes
  /// @return A human-readable string representation
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

  private void verifyFile(Path file, Path merklePath) throws Exception {
    logger.info("Verifying file against Merkle tree: {}", file);

    // Load the original Merkle tree from file
    MerkleTree originalTree = MerkleTree.load(merklePath);

    // Create a new Merkle tree from the current file content
    long fileSize = Files.size(file);
    MerkleRange fullRange = new MerkleRange(0, fileSize);

    // Create initial MerkleTree instance
    MerkleTree currentTree = new MerkleTree(null, chunkSize, fileSize, fullRange);

    // Process the file in chunks
    long bytesProcessed = 0;
    while (bytesProcessed < fileSize) {
      long endOffset = Math.min(bytesProcessed + chunkSize, fileSize);
      MerkleRange nextRange = new MerkleRange(bytesProcessed, endOffset);

      // Process this range of the file
      try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
        // Calculate buffer size for this chunk
        int bufferSize = (int) Math.min(chunkSize, Integer.MAX_VALUE);
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize);

        // Position channel at the start of this range
        channel.position(bytesProcessed);

        // Read the chunk
        int bytesRead = channel.read(buffer);
        buffer.flip();

        // Update the tree with this chunk
        if (bytesRead > 0) {
          currentTree = MerkleTree.fromData(buffer, chunkSize, nextRange);
        }
      }

      bytesProcessed = endOffset;
    }

    // Compare the root hashes of both trees
    if (Arrays.equals(originalTree.root().hash(), currentTree.root().hash())) {
      logger.info("Verification successful: {} matches its Merkle tree", file);
    } else {
      // Find mismatches between the trees
      List<MerkleMismatch> mismatches = originalTree.findMismatchedChunks(currentTree);
      logger.error("Verification failed: {} does not match its Merkle tree", file);
      logger.error("Found {} mismatched sections", mismatches.size());

      // Log details of the first few mismatches
      int mismatchesToShow = Math.min(5, mismatches.size());
      for (int i = 0; i < mismatchesToShow; i++) {
        MerkleMismatch mismatch = mismatches.get(i);
        logger.error("  Mismatch at offset {} (length: {})", mismatch.start(), mismatch.length());
      }

      throw new RuntimeException("File verification failed");
    }
  }

  private boolean isPowerOfTwo(long n) {
    return n > 0 && (n & (n - 1)) == 0;
  }
}
