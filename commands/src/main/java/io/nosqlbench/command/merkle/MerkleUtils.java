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


import io.nosqlbench.common.types.VectorFileExtension;
import io.nosqlbench.vectordata.merkle.MerkleFooter;
import io.nosqlbench.vectordata.merklev2.MerkleRefFactory;
import io.nosqlbench.vectordata.merklev2.MerkleDataImpl;
import io.nosqlbench.vectordata.merklev2.MerkleRefBuildProgress;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/// Utility class for Merkle tree operations.
/// This class contains shared functionality used by the various Merkle tree subcommands.
public class MerkleUtils {
    private static final Logger logger = LogManager.getLogger(MerkleUtils.class);

    // File extensions for merkle tree files
    public static final String MRKL = ".mrkl";
    public static final String MREF = ".mref";

    // Default extensions to use when a single directory is provided and no extensions are specified
    public static final Set<String> DEFAULT_EXTENSIONS;

    static {
        // Combine vector file extensions from the enum with Merkle-specific extensions
        Set<String> extensions = new HashSet<>(VectorFileExtension.getAllExtensions());
        extensions.add(MRKL);
        extensions.add(MREF);
        DEFAULT_EXTENSIONS = Collections.unmodifiableSet(extensions);
    }

    /// Processes a list of paths, expanding directories to find files with matching extensions.
    /// If a path is a directory and at least one extension is provided, it will be recursively
    /// traversed to find all files with the specified extensions.
    /// If a single directory is provided and no extensions are specified, the default extensions
    /// will be used automatically.
    ///
    /// @param paths The list of paths to process
    /// @return A list of file paths to process
    /// @throws IOException If an error occurs while traversing directories
    public static List<Path> expandDirectoriesWithExtensions(List<Path> paths) throws IOException {
        // Separate directories, files, and extensions
        List<Path> filesToProcess = new ArrayList<>();
        List<Path> directories = new ArrayList<>();
        Set<String> extensions = new HashSet<>();

        // First pass: identify directories, files, and extensions
        for (Path path : paths) {
            String pathStr = path.toString();

            // Check if it's an extension (starts with a dot) and ends with an alphanumeric character
            if (pathStr.startsWith(".") && pathStr.length() > 1 && Character.isLetterOrDigit(pathStr.charAt(1))) {
                extensions.add(pathStr.toLowerCase());
                continue;
            }

            // Check if it's a directory or a file
            if (Files.isDirectory(path)) {
                // If the path is ".", convert it to the actual current directory path
                if (path.toString().equals(".")) {
                    Path currentDir = Path.of("").toAbsolutePath();
                    logger.info("Converting '.' to current directory: {}", currentDir);
                    directories.add(currentDir);
                } else {
                    directories.add(path);
                }
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

    /// Recursively finds all files with the specified extensions in a directory and its subdirectories.
    ///
    /// @param directory The directory to search
    /// @param extensions The set of file extensions to match (including the dot)
    /// @return A list of matching file paths
    /// @throws IOException If an error occurs while traversing the directory
    public static List<Path> findFilesWithExtensions(Path directory, Set<String> extensions) throws IOException {
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
                logger.warn("Failed to visit file: {}", file, exc);
                return FileVisitResult.CONTINUE;
            }
        });

        return matchingFiles;
    }

    /// Formats a byte size into a human-readable string.
    ///
    /// @param bytes The size in bytes
    /// @return A human-readable string representation
    public static String formatByteSize(long bytes) {
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

    /// Verifies the integrity of a Merkle tree file by checking its structure.
    /// This method accounts for the bitset content that is stored in the file after the hash data
    /// and before the footer. The bitset content size is specified in the footer as bitSetSize.
    ///
    /// The file structure is as follows:
    /// Hash Data, BitSet Data, Footer
    ///
    /// @param merklePath The path to the Merkle tree file
    /// @return true if the file is valid, false if it's corrupted or invalid
    public static boolean verifyMerkleFileIntegrity(Path merklePath) {
        try {
            // Get file size
            long fileSize = Files.size(merklePath);
            logger.info("[DEBUG_LOG] Verifying Merkle file: {} (size: {})", merklePath, fileSize);

            // File too small to be valid
            if (fileSize < MerkleFooter.FIXED_FOOTER_SIZE) {
                logger.info("[DEBUG_LOG] Merkle file too small to be valid: {}", merklePath);
                return false;
            }

            try (FileChannel channel = FileChannel.open(merklePath, StandardOpenOption.READ)) {
                // Read the footer length (last byte)
                ByteBuffer footerLengthBuffer = ByteBuffer.allocate(1);
                channel.position(fileSize - 1);
                int bytesRead = channel.read(footerLengthBuffer);
                if (bytesRead != 1) {
                    logger.info("[DEBUG_LOG] Failed to read footer length from Merkle file: {}", merklePath);
                    return false;
                }
                footerLengthBuffer.flip();
                byte footerLength = footerLengthBuffer.get();
                logger.info("[DEBUG_LOG] Footer length: {}", footerLength);

                // Validate footer length
                if (footerLength <= 0 || footerLength > fileSize) {
                    logger.info("[DEBUG_LOG] Invalid footer length in Merkle file: {}", merklePath);
                    return false;
                }

                // Read the entire footer
                ByteBuffer footerBuffer = ByteBuffer.allocate(footerLength);
                channel.position(fileSize - footerLength);
                bytesRead = channel.read(footerBuffer);
                if (bytesRead != footerLength) {
                    logger.info("[DEBUG_LOG] Failed to read footer from Merkle file: {}", merklePath);
                    return false;
                }
                footerBuffer.flip();

                // Parse the footer
                MerkleFooter footer;
                try {
                    footer = MerkleFooter.fromByteBuffer(footerBuffer);
                    logger.info("[DEBUG_LOG] Footer parsed successfully: chunkSize={}, totalSize={}", 
                        footer.chunkSize(), footer.totalSize());
                } catch (IllegalArgumentException e) {
                    logger.info("[DEBUG_LOG] Invalid footer in Merkle file: {}", merklePath);
                    return false;
                }

                // Get the bitSetSize from the footer
                int bitSetSize = footer.bitSetSize();
                logger.info("[DEBUG_LOG] BitSet size: {}", bitSetSize);

                // Calculate the tree data size (hash data only, excluding bitset and footer)
                long treeDataSize = fileSize - footerLength - bitSetSize;
                if (treeDataSize <= 0) {
                    logger.info("[DEBUG_LOG] Invalid tree data size in Merkle file: {}", merklePath);
                    return false;
                }
                logger.info("[DEBUG_LOG] Tree data size: {}", treeDataSize);

                // Check if data size is a multiple of hash size (32 bytes for SHA-256)
                final int HASH_SIZE = 32; // SHA-256 hash size
                if (treeDataSize % HASH_SIZE != 0) {
                    logger.info("[DEBUG_LOG] Tree data size is not a multiple of hash size: {}", merklePath);
                    return false;
                }

                // Calculate the number of hashes in the file
                int hashCount = (int) (treeDataSize / HASH_SIZE);
                int leafCount = (int) Math.ceil((double) footer.totalSize() / footer.chunkSize());
                logger.info("[DEBUG_LOG] Hash count: {}, Leaf count: {}", hashCount, leafCount);

                // Calculate the expected number of nodes in a complete binary tree
                int paddedCap = 1;
                while (paddedCap < leafCount)
                    paddedCap <<= 1;
                int defaultNodeCount = 2 * paddedCap - 1;
                logger.info("[DEBUG_LOG] Padded capacity: {}, Default node count: {}", paddedCap, defaultNodeCount);

                // Check if the number of hashes makes sense for a merkle tree
                // It should be either the full tree (defaultNodeCount), just the leaves (leafCount),
                // or a complete tree (2 * leafCount - 1)
                if (hashCount != defaultNodeCount && hashCount != leafCount && hashCount != (2 * leafCount - 1)) {
                    logger.info("[DEBUG_LOG] Unexpected number of hashes in Merkle file: {}. Expected one of: {}, {}, {}", 
                        hashCount, defaultNodeCount, leafCount, (2 * leafCount - 1));
                    return false;
                }

                // All checks passed
                logger.info("[DEBUG_LOG] Merkle file verification passed: {}", merklePath);
                return true;
            }
        } catch (Exception e) {
            logger.info("[DEBUG_LOG] Error verifying Merkle file integrity: {}", merklePath, e);
            return false;
        }
    }

    /// Reads the MerkleFooter from a Merkle tree file.
    ///
    /// @param path The path to the Merkle tree file
    /// @return The MerkleFooter object
    /// @throws IOException If there's an error reading the file
    public static MerkleFooter readMerkleFooter(Path path) throws IOException {
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            // Get file size
            long fileSize = channel.size();

            // Handle empty or very small files
            if (fileSize == 0) {
                // Return a default footer
                return MerkleFooter.create(4096, 0);
            }

            // Try to read the footer length byte (last byte of the file)
            ByteBuffer footerLengthBuffer = ByteBuffer.allocate(1);
            channel.position(fileSize - 1);
            int bytesRead = channel.read(footerLengthBuffer);
            if (bytesRead != 1) {
                // Couldn't read footer length, create a default footer
                return MerkleFooter.create(4096, fileSize);
            }
            footerLengthBuffer.flip();
            byte footerLength = footerLengthBuffer.get();

            // Validate footer length
            if (footerLength <= 0 || footerLength > fileSize) {
                // Invalid footer length, create a default footer
                return MerkleFooter.create(4096, fileSize);
            }

            // Read the entire footer
            ByteBuffer footerBuffer = ByteBuffer.allocate(footerLength);
            channel.position(fileSize - footerLength);
            bytesRead = channel.read(footerBuffer);
            if (bytesRead != footerLength) {
                // Couldn't read full footer, create a default footer
                return MerkleFooter.create(4096, fileSize);
            }
            footerBuffer.flip();

            // Parse and return the footer
            return MerkleFooter.fromByteBuffer(footerBuffer);
        }
    }

    /// Converts a byte array to a hex string.
    ///
    /// @param bytes The byte array to convert
    /// @return A hex string representation
    public static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    /// Creates a progress bar string for console display.
    ///
    /// @param percent The percentage of completion (0-100)
    /// @param width The width of the progress bar in characters
    /// @return A string representing the progress bar
    public static String createProgressBar(double percent, int width) {
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

    /// Saves a Merkle reference to a file using the current merklev2 implementation.
    ///
    /// @param file The source file path
    /// @throws IOException If there's an error writing to the file
    public static void saveMerkleRef(Path file) throws IOException {
        Path merkleFile = file.resolveSibling(file.getFileName() + MREF);
        try {
            // Create merkle tree with progress tracking
            MerkleRefBuildProgress progress = MerkleRefFactory.fromData(file);
            
            // Wait for completion and get the result
            MerkleDataImpl merkleData = progress.getFuture().get();
            
            // Save to .mref file
            merkleData.save(merkleFile);
            logger.info("Saved Merkle reference to {}", merkleFile);
        } catch (Exception e) {
            logger.error("Error saving Merkle reference for file: {}", file, e);
            if (e instanceof IOException) {
                throw (IOException) e;
            } else {
                throw new IOException("Error creating merkle reference", e);
            }
        }
    }

    /// Checks if a number is a power of two.
    ///
    /// @param n The number to check
    /// @return true if the number is a power of two, false otherwise
    public static boolean isPowerOfTwo(long n) {
        return n > 0 && (n & (n - 1)) == 0;
    }
}
