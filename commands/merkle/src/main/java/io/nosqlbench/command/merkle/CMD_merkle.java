package io.nosqlbench.command.merkle;

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

import io.nosqlbench.nbvectors.api.commands.BundledCommand;
import io.nosqlbench.vectordata.merkle.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.MessageDigest;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static io.nosqlbench.command.merkle.MerkleCommand.MRKL;

@Command(name = "merkle",
    header = "create or verify Merkle tree files for data integrity",
    description = """
        Creates or verifies Merkle tree files for specified files.
        These Merkle tree files can be used later to efficiently
        verify file integrity or identify changed portions of
        files for partial downloads/updates.""",
        subcommands = {HelpCommand.class})
public class CMD_merkle implements Callable<Integer>, BundledCommand {
  // Default extensions to use when a single directory is provided and no extensions are specified
  private static final Set<String> DEFAULT_EXTENSIONS = Set.of(
      ".ivec", ".ivecs", ".fvec", ".fvecs", ".bvec", ".bvecs", ".hdf5", ".mrkl", ".mref"
  );
  private static final Logger logger = LogManager.getLogger(CMD_merkle.class);

  @Parameters(index = "0", description = "Command to execute: create, verify, or summary", defaultValue = "create")
  private String commandName = "create";

  @Parameters(index = "1..*", description = "Files to process")
  private List<Path> files = new ArrayList<>();

  @Option(names = {"--chunk-size"},
      description = "Chunk size in bytes (must be a power of 2, default: ${DEFAULT-VALUE})",
      defaultValue = "1048576" // 1MB
  )
  private long chunkSize;

  @Option(names = {"-f", "--force"}, description = "Overwrite existing Merkle files, even if they are up-to-date")
  private boolean force = false;

  @Option(names = {"--dryrun", "-n"}, description = "Show which files would be processed without actually creating Merkle files")
  private boolean dryrun = false;


  @Override
  public Integer call() throws Exception {
    if (!isPowerOfTwo(chunkSize)) {
      logger.error("Chunk size must be a power of two, got: {}", chunkSize);
      return 1;
    }

    MerkleCommand command = MerkleCommand.findByName(commandName);
    if (command == null) {
      logger.error("Unknown command: {}", commandName);
      logger.info("Available commands: {}", Arrays.stream(MerkleCommand.values())
          .map(MerkleCommand::getName)
          .collect(Collectors.joining(", ")));
      return 1;
    }

    List<Path> expandedFiles = CMD_merkle.expandDirectoriesWithExtensions(files);
    boolean success = command.execute(expandedFiles, chunkSize, force, dryrun);

    return success ? 0 : 1;
  }


  // ... (Other helper methods from previous responses remain the same)


  public void verifyFile(Path file, long chunkSize) throws Exception {
    Path merklePath = file.resolveSibling(file.getFileName() + MRKL);
    if (!Files.exists(merklePath)) {
      logger.error("Merkle file not found for: {}", file);
      return;
    }
    verifyFile(file, merklePath, chunkSize);
  }


  public void createMerkleFile(Path file, long chunkSize) throws Exception {
    // Set the class field to the provided chunk size
    this.chunkSize = chunkSize;
    try (MerkleConsoleDisplay display = new MerkleConsoleDisplay(file)) {
      display.setStatus("Preparing to compute Merkle tree");
      display.startProgressThread();

      // Get file size and compute initial range
      long fileSize = Files.size(file);
      display.log("File size: %d bytes", fileSize);

      // Calculate number of chunks to process
      int numChunks = (int) ((fileSize + chunkSize - 1) / chunkSize);
      display.log("Processing file in %d chunks", numChunks);

      // Use virtual threads for per-task execution
      display.log("Using virtual threads per task executor");

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

      // Create a full Merkle tree from the file directly
      display.setStatus("Building Merkle tree");

      // Create the Merkle range for the entire file
      MerkleRange fullRange = new MerkleRange(0, fileSize);

      // Create an empty Merkle tree for the file size and chunk size
      MerkleTree merkleTree = MerkleTree.createEmpty(fileSize, chunkSize);
      // Concurrently compute leaf hashes using virtual threads
      display.setStatus("Hashing chunks");
      ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
      AtomicInteger chunksDone = new AtomicInteger(0);
      byte[][] leafHashes = new byte[numChunks][];
      for (int i = 0; i < numChunks; i++) {
        final int idx = i;
        executor.submit(() -> {
          try (FileChannel ch2 = FileChannel.open(file, StandardOpenOption.READ)) {
            long pos = (long) idx * chunkSize;
            ch2.position(pos);
            ByteBuffer buf = ByteBuffer.allocate((int) Math.min(chunkSize, fileSize - pos));
            while (buf.hasRemaining()) {
              int r = ch2.read(buf);
              if (r < 0) break;
            }
            buf.flip();
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update(buf);
            byte[] h = md.digest();
            leafHashes[idx] = h;
            int done = chunksDone.incrementAndGet();
            display.setAction(String.format("Hashed %d/%d chunks", done, numChunks));
            // Update byte-level progress: accumulate actual bytes hashed and use file size for total
            long chunkBytes = buf.limit();
            long bp = bytesProcessed.addAndGet(chunkBytes);
            display.updateProgress(bp, fileSize, done, numChunks);
          } catch (Exception e) {
            display.log("Error hashing chunk %d: %s", idx, e.getMessage());
          }
        });
      }
      executor.shutdown();
      executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
      display.setAction("");
      display.setStatus("Building Merkle tree");
      // Update Merkle tree with computed leaf hashes
      for (int idx = 0; idx < numChunks; idx++) {
        display.setAction(String.format("Updating tree %d/%d", idx + 1, numChunks));
        merkleTree.updateLeafHash(idx, leafHashes[idx]);
        // During tree update, show full byte progress and section progress
        display.updateProgress(bytesProcessed.get(), fileSize, idx + 1, numChunks);
      }
      display.setAction("");

      // Save the Merkle tree
      display.setStatus("Saving Merkle tree");
      saveMerkleTree(file, merkleTree);
      display.log("Merkle tree creation completed successfully");
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
    Path merkleFile = file.resolveSibling(file.getFileName() + MerklePane.MRKL);
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
    MerkleTree merkleTree = MerkleTree.load(merklePath);

    // Get the file size
    long fileSize = Files.size(merklePath);

    // Read the footer directly from the file to get all footer data
    MerkleFooter footer = readMerkleFooter(merklePath);

    // Determine the file type
    String fileType = merklePath.getFileName().toString().endsWith(".mref") ? "MERKLE REFERENCE FILE" : "MERKLE TREE FILE";

    // Build the header for the summary
    StringBuilder summary = new StringBuilder();
    summary.append("\n").append(fileType).append(" SUMMARY\n");
    summary.append("=======================\n");
    summary.append(String.format("File: %s\n", merklePath.toAbsolutePath()));
    summary.append(String.format("File Size: %s\n\n", formatByteSize(fileSize)));

    // Append the Merkle tree's toString output
    summary.append(merkleTree.toString());

    // Add footer information
    summary.append("\nFooter Information:\n");
    summary.append(String.format("Chunk Size: %s\n", formatByteSize(footer.chunkSize())));
    summary.append(String.format("Total Size: %s\n", formatByteSize(footer.totalSize())));
    summary.append(String.format("Footer Length: %d bytes\n", footer.footerLength()));
    summary.append(String.format("Digest: %s\n", bytesToHex(footer.digest())));

    // If this is a reference file, add information about the original file
    if (merklePath.getFileName().toString().endsWith(".mref")) {
      String originalFileName = merklePath.getFileName().toString().replace(".mref", "");
      Path originalPath = merklePath.resolveSibling(originalFileName);

      summary.append("\nReference Information:\n");
      summary.append(String.format("Original File: %s\n", originalPath.toAbsolutePath()));

      if (Files.exists(originalPath)) {
        long originalSize = Files.size(originalPath);
        summary.append(String.format("Original File Size: %s\n", formatByteSize(originalSize)));
        summary.append(String.format("Size Ratio: %.2f%%\n", (double) fileSize / originalSize * 100));
      } else {
        summary.append("Original File: Not found\n");
      }
    }

    // Print the complete summary
    System.out.println(summary);
  }

  /**
   * Verifies the integrity of a Merkle tree file by checking its internal digest.
   *
   * @param merklePath The path to the Merkle tree file
   * @return true if the file is valid, false if it's corrupted or invalid
   */
  public boolean verifyMerkleFileIntegrity(Path merklePath) {
    try {
      // Get file size
      long fileSize = Files.size(merklePath);

      // File too small to be valid
      if (fileSize < MerkleFooter.FIXED_FOOTER_SIZE + MerkleFooter.DIGEST_SIZE) {
        logger.debug("Merkle file too small to be valid: {}", merklePath);
        return false;
      }

      try (FileChannel channel = FileChannel.open(merklePath, StandardOpenOption.READ)) {
        // Read the footer length (last byte)
        ByteBuffer footerLengthBuffer = ByteBuffer.allocate(1);
        channel.position(fileSize - 1);
        int bytesRead = channel.read(footerLengthBuffer);
        if (bytesRead != 1) {
          logger.debug("Failed to read footer length from Merkle file: {}", merklePath);
          return false;
        }
        footerLengthBuffer.flip();
        byte footerLength = footerLengthBuffer.get();

        // Validate footer length
        if (footerLength <= 0 || footerLength > fileSize) {
          logger.debug("Invalid footer length in Merkle file: {}", merklePath);
          return false;
        }

        // Read the entire footer
        ByteBuffer footerBuffer = ByteBuffer.allocate(footerLength);
        channel.position(fileSize - footerLength);
        bytesRead = channel.read(footerBuffer);
        if (bytesRead != footerLength) {
          logger.debug("Failed to read footer from Merkle file: {}", merklePath);
          return false;
        }
        footerBuffer.flip();

        // Parse the footer
        MerkleFooter footer = MerkleFooter.fromByteBuffer(footerBuffer);

        // Calculate the tree data size (everything before the footer)
        long treeDataSize = fileSize - footerLength;
        if (treeDataSize <= 0) {
          logger.debug("Invalid tree data size in Merkle file: {}", merklePath);
          return false;
        }

        // Read the tree data
        ByteBuffer treeData = ByteBuffer.allocate((int) Math.min(treeDataSize, Integer.MAX_VALUE));
        channel.position(0);
        bytesRead = channel.read(treeData);
        if (bytesRead <= 0) {
          logger.debug("Failed to read tree data from Merkle file: {}", merklePath);
          return false;
        }
        treeData.flip();

        // Verify the digest
        boolean isValid = footer.verifyDigest(treeData);
        if (!isValid) {
          logger.debug("Digest verification failed for Merkle file: {}", merklePath);
        }
        return isValid;
      }
    } catch (Exception e) {
      logger.debug("Error verifying Merkle file integrity: {}", merklePath, e);
      return false;
    }
  }

  /**
   * Reads the MerkleFooter from a Merkle tree file.
   *
   * @param path The path to the Merkle tree file
   * @return The MerkleFooter object
   * @throws IOException If there's an error reading the file
   */
  private MerkleFooter readMerkleFooter(Path path) throws IOException {
    try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
      // Get file size
      long fileSize = channel.size();

      // Handle empty or very small files
      if (fileSize == 0) {
        // Return a default footer
        return MerkleFooter.create(4096, 0, new byte[MerkleFooter.DIGEST_SIZE]);
      }

      // Try to read the footer length byte (last byte of the file)
      ByteBuffer footerLengthBuffer = ByteBuffer.allocate(1);
      channel.position(fileSize - 1);
      int bytesRead = channel.read(footerLengthBuffer);
      if (bytesRead != 1) {
        // Couldn't read footer length, create a default footer
        return MerkleFooter.create(4096, fileSize, new byte[MerkleFooter.DIGEST_SIZE]);
      }
      footerLengthBuffer.flip();
      byte footerLength = footerLengthBuffer.get();

      // Validate footer length
      if (footerLength <= 0 || footerLength > fileSize) {
        // Invalid footer length, create a default footer
        return MerkleFooter.create(4096, fileSize, new byte[MerkleFooter.DIGEST_SIZE]);
      }

      // Read the entire footer
      ByteBuffer footerBuffer = ByteBuffer.allocate(footerLength);
      channel.position(fileSize - footerLength);
      bytesRead = channel.read(footerBuffer);
      if (bytesRead != footerLength) {
        // Couldn't read full footer, create a default footer
        return MerkleFooter.create(4096, fileSize, new byte[MerkleFooter.DIGEST_SIZE]);
      }
      footerBuffer.flip();

      // Parse and return the footer
      return MerkleFooter.fromByteBuffer(footerBuffer);
    }
  }

  /**
   * Converts a byte array to a hex string
   *
   * @param bytes The byte array to convert
   * @return A hex string representation
   */
  private String bytesToHex(byte[] bytes) {
    StringBuilder sb = new StringBuilder();
    for (byte b : bytes) {
      sb.append(String.format("%02x", b));
    }
    return sb.toString();
  }

  /**
   * Processes a list of paths, expanding directories to find files with matching extensions.
   * If a path is a directory and at least one extension is provided, it will be recursively
   * traversed to find all files with the specified extensions.
   * If a single directory is provided and no extensions are specified, the default extensions
   * will be used automatically.
   *
   * @param paths The list of paths to process
   * @return A list of file paths to process
   * @throws IOException If an error occurs while traversing directories
   */
  public static List<Path> expandDirectoriesWithExtensions(List<Path> paths) throws IOException {
    // Separate directories, files, and extensions
    List<Path> filesToProcess = new ArrayList<>();
    List<Path> directories = new ArrayList<>();
    Set<String> extensions = new HashSet<>();

    // First pass: identify directories, files, and extensions
    for (Path path : paths) {
      String pathStr = path.toString();

      // Check if it's an extension (starts with a dot)
      if (pathStr.startsWith(".")) {
        extensions.add(pathStr.toLowerCase());
        continue;
      }

      // Check if it's a directory or a file
      if (Files.isDirectory(path)) {
        directories.add(path);
      } else if (Files.isRegularFile(path)) {
        filesToProcess.add(path);
      }
    }

    // If we have directories, process them
    if (!directories.isEmpty()) {
      // If no extensions were specified and there's exactly one directory,
      // use the default extensions
      if (extensions.isEmpty() && directories.size() == 1) {
        logger.info("Using default extensions for directory: {}", directories.get(0));
        for (Path directory : directories) {
          List<Path> matchingFiles = findFilesWithExtensions(directory, DEFAULT_EXTENSIONS);
          filesToProcess.addAll(matchingFiles);
        }
      } else if (!extensions.isEmpty()) {
        // If extensions were specified, use them
        for (Path directory : directories) {
          List<Path> matchingFiles = findFilesWithExtensions(directory, extensions);
          filesToProcess.addAll(matchingFiles);
        }
      } else {
        // If multiple directories and no extensions were specified, just add the directories as-is
        filesToProcess.addAll(directories);
      }
    }

    return filesToProcess;
  }

  /**
   * Recursively finds all files with the specified extensions in a directory and its subdirectories.
   *
   * @param directory The directory to search
   * @param extensions The set of file extensions to match (including the dot)
   * @return A list of matching file paths
   * @throws IOException If an error occurs while traversing the directory
   */
  private static List<Path> findFilesWithExtensions(Path directory, Set<String> extensions) throws IOException {
    List<Path> matchingFiles = new ArrayList<>();

    Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
        String fileName = file.getFileName().toString().toLowerCase();

        // Check if the file has one of the specified extensions
        for (String extension : extensions) {
          if (fileName.endsWith(extension)) {
            matchingFiles.add(file);
            break;
          }
        }

        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult visitFileFailed(Path file, IOException exc) {
        // Log the error but continue traversing
        LogManager.getLogger(CMD_merkle.class).warn("Failed to visit file: {}", file, exc);
        return FileVisitResult.CONTINUE;
      }
    });

    return matchingFiles;
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

  public void verifyFile(Path file, Path merklePath, long chunkSize) throws Exception {
    logger.info("Verifying file against Merkle tree: {}", file);

    // Load the original Merkle tree from file
    MerkleTree originalTree = MerkleTree.load(merklePath);

    // Read the entire file content or a reasonable maximum
    long fileSize = Files.size(file);
    MerkleRange fullRange = new MerkleRange(0, fileSize);

    // Read file data
    ByteBuffer fileData = ByteBuffer.allocate((int)Math.min(fileSize, Integer.MAX_VALUE));
    try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
      channel.read(fileData);
      fileData.flip();
    }

    // Create a new Merkle tree from the current file content
    MerkleTree currentTree = MerkleTree.fromData(fileData, chunkSize, fullRange);

    // Compare the trees
    boolean isEqual = originalTree.equals(currentTree);

    if (isEqual) {
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
        logger.error("  Mismatch at offset {} (length: {})", mismatch.startInclusive(), mismatch.length());
      }

      throw new RuntimeException("File verification failed");
    }
  }

  private boolean isPowerOfTwo(long n) {
    return n > 0 && (n & (n - 1)) == 0;
  }
}
