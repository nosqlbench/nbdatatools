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
        
        # Create with custom section sizes
        nbvectors merkle --min-section 2097152 --max-section 33554432 bigfile.hdf5
        
        # Verify files against their Merkle trees
        nbvectors merkle -v file1.hdf5 file2.hdf5
        
        # Force overwrite of existing Merkle files
        nbvectors merkle -f file1.hdf5
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

  @Option(names = {"--min-section"},
      description = "Minimum section size in bytes (default: ${DEFAULT-VALUE})",
      defaultValue = "1048576"
      // 1MB
  )
  private long minSection;

  @Option(names = {"--max-section"},
      description = "Maximum section size in bytes (default: ${DEFAULT-VALUE})",
      defaultValue = "16777216"
      // 16MB
  )
  private long maxSection;

  @Option(names = {"-f", "--force"}, description = "Overwrite existing Merkle files")
  private boolean force = false;

  @Override
  public Integer call() {
    boolean hasErrors = false;

    for (Path file : files) {
      try {
        if (!Files.exists(file)) {
          logger.error("File not found: {}", file);
          hasErrors = true;
          continue;
        }

        // Construct the merkle file path by appending .merkle to the original file name
        Path merklePath = file.resolveSibling(file.getFileName() + ".merkle");

        if (verify) {
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
      MerkleRange initialRange = new MerkleRange(0, fileSize);

      // Create initial MerkleTree instance
      MerkleTree merkleTree = new MerkleTree(null, minSection, fileSize, initialRange);

      // Update progress as we process the file
      long bytesProcessed = 0;
      while (bytesProcessed < fileSize) {
        long endOffset = Math.min(bytesProcessed + maxSection, fileSize);
        MerkleRange nextRange = new MerkleRange(bytesProcessed, endOffset);

        // Process this range of the file
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
          // Calculate buffer size for this chunk
          int bufferSize = (int) Math.min(maxSection, Integer.MAX_VALUE);
          ByteBuffer buffer = ByteBuffer.allocate(bufferSize);

          // Position channel at the start of this range
          channel.position(bytesProcessed);

          // Read the chunk
          int bytesRead = channel.read(buffer);
          buffer.flip();

          // Update the tree with this chunk
          if (bytesRead > 0) {
            merkleTree = MerkleTree.fromData(buffer, minSection, nextRange);
          }
        }

        bytesProcessed = endOffset;

        display.updateProgress(
            bytesProcessed,
            fileSize,
            (int) (bytesProcessed / maxSection),
            (int) ((fileSize + maxSection - 1) / maxSection)
        );
      }

      // Save the Merkle tree using our utility method
      saveMerkleTree(file, merkleTree);
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
    Path merkleFile = file.resolveSibling(file.getFileName() + ".merkle");
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

  private void verifyFile(Path file, Path merklePath) throws Exception {
    logger.info("Verifying file against Merkle tree: {}", file);

    // Load the original Merkle tree from file
    MerkleTree originalTree = MerkleTree.load(merklePath);

    // Create a new Merkle tree from the current file content
    long fileSize = Files.size(file);
    MerkleRange fullRange = new MerkleRange(0, fileSize);

    // Create initial MerkleTree instance
    MerkleTree currentTree = new MerkleTree(null, minSection, fileSize, fullRange);

    // Process the file in chunks
    long bytesProcessed = 0;
    while (bytesProcessed < fileSize) {
      long endOffset = Math.min(bytesProcessed + maxSection, fileSize);
      MerkleRange nextRange = new MerkleRange(bytesProcessed, endOffset);

      // Process this range of the file
      try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
        // Calculate buffer size for this chunk
        int bufferSize = (int) Math.min(maxSection, Integer.MAX_VALUE);
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize);

        // Position channel at the start of this range
        channel.position(bytesProcessed);

        // Read the chunk
        int bytesRead = channel.read(buffer);
        buffer.flip();

        // Update the tree with this chunk
        if (bytesRead > 0) {
          currentTree = MerkleTree.fromData(buffer, minSection, nextRange);
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
}