package io.nosqlbench.vectordata.fvecs;

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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.FileStore;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test class for generating a large fvecs file with deterministic content.
 * This test creates a file of at least 10GB in size, checking for adequate file system space first.
 * The file contains float vectors with a deterministic layout that can be used to verify ordinal position by vector content.
 */

@Tag("performance")
public class LargeFvecsFileGenerationTest {

    private static final long MIN_REQUIRED_SPACE = 11L * 1024 * 1024 * 1024; // 11GB to be safe
    private static final int VECTOR_DIMENSIONS = 100; // Each vector has 100 dimensions
    private static final int VECTOR_SIZE_BYTES = 4 + (VECTOR_DIMENSIONS * 4); // 4 bytes for dimension count + 4 bytes per float
    private static final long TARGET_FILE_SIZE = 50L * 1024 * 1024 * 1024; // 200GB
    private static final int VECTORS_PER_BATCH = 10000; // Write 10,000 vectors at a time
    private static final long TOTAL_VECTORS = TARGET_FILE_SIZE / VECTOR_SIZE_BYTES;

    // File name for the generated fvecs file
    private static final String LARGE_FVECS_FILE_NAME = "nosqlbench_large_test_vectors.fvecs";
    // The path will be determined dynamically to find a suitable non-root filesystem
    private static Path largeFvecsFilePath;

    /**
     * Finds a suitable filesystem with enough space for the large file.
     * First checks if there's enough space in a local tmp directory of the project, and uses that if available.
     * If not, and if the system property "UseLargestFilesystem" is set, it enumerates all local filesystems 
     * using FileSystems and FileStore APIs, ranks them by available space, and chooses a non-root filesystem 
     * with the most available space. If no suitable non-root filesystem is found, it falls back to using a writable directory
     * within the root filesystem (first trying /tmp, then user's home directory, then current directory).
     *
     * @return Path to a directory on a suitable filesystem
     * @throws IOException If there's an error accessing the filesystems
     */
    private static Path findSuitableFilesystem() throws IOException {
        System.out.println("[DEBUG_LOG] Minimum required space: " + (MIN_REQUIRED_SPACE / (1024 * 1024 * 1024)) + " GB");

        // First check if there's enough space in a local tmp directory of the project
        Path projectTmpDir = Path.of("src/test/resources/testserver/temp");
        if (!Files.exists(projectTmpDir)) {
            try {
                Files.createDirectories(projectTmpDir);
                System.out.println("[DEBUG_LOG] Created local project tmp directory: " + projectTmpDir.toAbsolutePath());
            } catch (IOException e) {
                System.out.println("[DEBUG_LOG] Failed to create local project tmp directory: " + e.getMessage());
            }
        }

        if (Files.exists(projectTmpDir) && Files.isWritable(projectTmpDir)) {
            if (hasEnoughDiskSpace(projectTmpDir)) {
                System.out.println("[DEBUG_LOG] SELECTED: Using local project tmp directory with enough space: " + 
                                  projectTmpDir.toAbsolutePath() + " with " + 
                                  (Files.getFileStore(projectTmpDir).getUsableSpace() / (1024.0 * 1024.0 * 1024.0)) + 
                                  " GB available");
                return projectTmpDir;
            } else {
                System.out.println("[DEBUG_LOG] Local project tmp directory doesn't have enough space, checking if fallback is enabled");
            }
        } else {
            System.out.println("[DEBUG_LOG] Local project tmp directory doesn't exist or is not writable, checking if fallback is enabled");
        }

        // Check if the UseLargestFilesystem system property is set
        boolean useLargestFilesystem = Boolean.getBoolean("UseLargestFilesystem");
        if (!useLargestFilesystem) {
            System.out.println("[DEBUG_LOG] Fallback to finding a larger filesystem is disabled. Set -DUseLargestFilesystem=true to enable it.");
            // If the system property is not set, return the project tmp directory even if it doesn't have enough space
            return projectTmpDir;
        }

        // If local tmp directory doesn't have enough space and fallback is enabled, continue with the original behavior
        System.out.println("[DEBUG_LOG] Fallback to finding a larger filesystem is enabled. Enumerating and ranking all filesystems by available space");

        // Variables to track the best non-root and root filesystems
        Path rootLocation = null;
        long rootSpace = 0;
        Path bestNonRootLocation = null;
        long bestNonRootSpace = 0;
        boolean foundNonRoot = false;

        // Parse /proc/mounts to get mount points for all filesystems
        Map<String, String> fsNameToMountPoint = new HashMap<>();
        try {
            System.out.println("[DEBUG_LOG] Parsing /proc/mounts to find mount points");
            File procMounts = new File("/proc/mounts");
            if (procMounts.exists() && procMounts.canRead()) {
                try (BufferedReader reader = new BufferedReader(new FileReader(procMounts))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        // Format: device mountpoint fstype options dump pass
                        String[] parts = line.split("\\s+");
                        if (parts.length >= 3) {
                            String device = parts[0];
                            String mountPoint = parts[1];
                            String fsType = parts[2];

                            // Skip pseudo filesystems
                            if (fsType.equals("proc") || fsType.equals("sysfs") || fsType.equals("devpts") || 
                                fsType.equals("tmpfs") || fsType.equals("devtmpfs") || fsType.equals("debugfs") ||
                                fsType.equals("securityfs") || fsType.equals("cgroup") || fsType.equals("pstore") ||
                                fsType.equals("autofs") || fsType.equals("mqueue") || fsType.equals("hugetlbfs") ||
                                fsType.equals("fusectl") || fsType.equals("configfs") || fsType.equals("binfmt_misc") ||
                                mountPoint.startsWith("/sys/") || mountPoint.startsWith("/proc/") || 
                                mountPoint.startsWith("/dev/") || mountPoint.startsWith("/run/")) {
                                continue;
                            }

                            // Store the mount point for this filesystem
                            // Use the device name as the key since it's unique
                            fsNameToMountPoint.put(device, mountPoint);
                            System.out.println("[DEBUG_LOG] Found mount: " + device + " at " + mountPoint + " (" + fsType + ")");
                        }
                    }
                }
            } else {
                System.out.println("[DEBUG_LOG] Cannot read /proc/mounts, falling back to Java API for mount points");
            }
        } catch (Exception e) {
            System.out.println("[DEBUG_LOG] Error parsing /proc/mounts: " + e.getMessage());
            System.out.println("[DEBUG_LOG] Falling back to Java API for mount points");
        }

        // Get all available filesystems
        System.out.println("[DEBUG_LOG] ===== FILESYSTEM RANKING BY AVAILABLE SPACE =====");
        for (FileStore store : FileSystems.getDefault().getFileStores()) {
            try {
                // Get filesystem information
                String name = store.name();
                String type = store.type();

                // Skip read-only filesystems
                if (store.isReadOnly()) {
                    continue;
                }

                // Identify root filesystem - a filesystem is root if it's mounted at "/"
                boolean isRoot = false;

                // Check if this filesystem is mounted at the root directory
                for (Map.Entry<String, String> entry : fsNameToMountPoint.entrySet()) {
                    if (entry.getValue().equals("/") && entry.getKey().equals(name)) {
                        isRoot = true;
                        break;
                    }
                }

                // Also consider traditional indicators of root filesystems
                if (!isRoot) {
                    isRoot = name.equals("rootfs") || name.contains("boot");
                }

                // Find the mount point for this filesystem
                String mountPoint = null;

                // First check if we found this filesystem in /proc/mounts
                for (Map.Entry<String, String> entry : fsNameToMountPoint.entrySet()) {
                    // Try to match by device name or by checking if the filesystem at the mount point matches
                    if (entry.getKey().equals(name)) {
                        mountPoint = entry.getValue();
                        break;
                    }

                    try {
                        // Check if the filesystem at this mount point matches the current store
                        Path path = Path.of(entry.getValue());
                        if (Files.exists(path) && Files.isWritable(path)) {
                            FileStore pathStore = Files.getFileStore(path);
                            if (pathStore.name().equals(name)) {
                                mountPoint = entry.getValue();
                                break;
                            }
                        }
                    } catch (IOException e) {
                        // Skip this mount point if we can't access it
                        continue;
                    }
                }

                // If we couldn't find the mount point in /proc/mounts, use fallback methods
                if (mountPoint == null) {
                    if (isRoot) {
                        // For root filesystem
                        mountPoint = "/";
                    } else {
                        // Try common mount points
                        String[] possibleMounts = {
                            "/mnt", "/media", "/data", "/home", "/var", "/opt"
                        };

                        for (String mount : possibleMounts) {
                            Path path = Path.of(mount);
                            if (Files.exists(path) && Files.isWritable(path)) {
                                try {
                                    FileStore pathStore = Files.getFileStore(path);
                                    if (pathStore.name().equals(name)) {
                                        mountPoint = mount;
                                        break;
                                    }
                                } catch (IOException e) {
                                    // Skip this mount point if we can't access it
                                    continue;
                                }
                            }
                        }
                    }
                }

                // If we found a valid mount point
                if (mountPoint != null) {
                    Path location = Path.of(mountPoint);
                    long usableSpace = Files.getFileStore(location).getUsableSpace();
                    double usableSpaceGB = usableSpace / (1024.0 * 1024.0 * 1024.0);

                    if (isRoot) {
                        System.out.println(String.format("[DEBUG_LOG] [%s] Root filesystem: %s at %s with %.2f GB available", 
                                          usableSpaceGB >= (MIN_REQUIRED_SPACE / (1024.0 * 1024.0 * 1024.0)) ? "ELIGIBLE" : "TOO SMALL",
                                          name, mountPoint, usableSpaceGB));

                        // Track the root filesystem with the most space
                        if (usableSpace > rootSpace) {
                            rootLocation = location;
                            rootSpace = usableSpace;
                        }
                    } else {
                        System.out.println(String.format("[DEBUG_LOG] [%s] Non-root filesystem: %s at %s with %.2f GB available", 
                                          usableSpaceGB >= (MIN_REQUIRED_SPACE / (1024.0 * 1024.0 * 1024.0)) ? "ELIGIBLE" : "TOO SMALL",
                                          name, mountPoint, usableSpaceGB));

                        // Check if this non-root location has enough space
                        if (usableSpace >= MIN_REQUIRED_SPACE && usableSpace > bestNonRootSpace) {
                            bestNonRootLocation = location;
                            bestNonRootSpace = usableSpace;
                            foundNonRoot = true;
                        }
                    }
                }
            } catch (Exception e) {
                // Skip this filesystem if we encounter an error
                System.out.println("[DEBUG_LOG] Error checking filesystem " + store.name() + ": " + e.getMessage());
            }
        }
        System.out.println("[DEBUG_LOG] ===== END OF FILESYSTEM RANKING =====");

        // Determine the best location based on our findings
        Path bestLocation;
        long bestSpace;

        if (foundNonRoot) {
            // Use the largest non-root filesystem if found
            bestLocation = bestNonRootLocation;
            bestSpace = bestNonRootSpace;
            System.out.println(String.format("[DEBUG_LOG] SELECTED: Largest non-root filesystem at %s with %.2f GB available", 
                              bestLocation, bestSpace / (1024.0 * 1024.0 * 1024.0)));
        } else if (rootLocation != null) {
            // Fall back to root filesystem if no suitable non-root filesystem found
            // But use a writable directory within the root filesystem
            Path tmpDir = Path.of("/tmp");
            if (Files.isWritable(tmpDir)) {
                bestLocation = tmpDir;
            } else {
                // Try user's home directory if /tmp is not writable
                Path homeDir = Path.of(System.getProperty("user.home"));
                if (Files.isWritable(homeDir)) {
                    bestLocation = homeDir;
                } else {
                    // If neither /tmp nor home directory is writable, use current directory
                    bestLocation = Path.of(".");
                }
            }
            bestSpace = Files.getFileStore(bestLocation).getUsableSpace();
            System.out.println(String.format("[DEBUG_LOG] SELECTED: No suitable non-root filesystem found, falling back to writable directory in root filesystem: %s with %.2f GB available", 
                              bestLocation, bestSpace / (1024.0 * 1024.0 * 1024.0)));
        } else {
            // If no filesystems were found at all (unlikely), use /tmp as a last resort
            bestLocation = Path.of("/tmp");
            bestSpace = Files.getFileStore(bestLocation).getUsableSpace();
            System.out.println(String.format("[DEBUG_LOG] SELECTED: No filesystems found, using /tmp as last resort with %.2f GB available", 
                              bestSpace / (1024.0 * 1024.0 * 1024.0)));
        }

        return bestLocation;
    }


    /**
     * Gets the path to the large fvecs file.
     * This method determines the appropriate location for the file based on available filesystems.
     *
     * @return The path to the large fvecs file
     * @throws IOException If there's an error accessing the filesystems
     */
    public static Path getLargeFvecsFilePath() throws IOException {
        // If we've already determined the path, return it
        if (largeFvecsFilePath != null) {
            return largeFvecsFilePath;
        }

        // Find a suitable filesystem and create the path
        Path baseDir = findSuitableFilesystem();
        largeFvecsFilePath = baseDir.resolve(LARGE_FVECS_FILE_NAME);

        return largeFvecsFilePath;
    }

    /**
     * Checks if there's enough disk space available in the temp directory.
     * 
     * @param tempDir The temporary directory to check
     * @return true if there's enough space, false otherwise
     */
    private static boolean hasEnoughDiskSpace(Path tempDir) {
        File file = tempDir.toFile();
        long usableSpace = file.getUsableSpace();
        System.out.println("[DEBUG_LOG] Available disk space: " + (usableSpace / (1024 * 1024)) + " MB");
        System.out.println("[DEBUG_LOG] Required disk space: " + (MIN_REQUIRED_SPACE / (1024 * 1024)) + " MB");
        return usableSpace >= MIN_REQUIRED_SPACE;
    }

    /**
     * Generates a deterministic float value based on the vector index and dimension.
     * This allows verification of vector content based on its position in the file.
     * 
     * @param vectorIndex The index of the vector in the file
     * @param dimension The dimension within the vector
     * @return A deterministic float value
     */
    private static float generateDeterministicValue(long vectorIndex, int dimension) {
        // Simple formula to generate a unique, deterministic value for each position
        // The value is based on the vector index and dimension, making it easy to verify
        return vectorIndex + (dimension / 1000.0f);
    }

    /**
     * Creates a large fvecs file with deterministic content.
     * 
     * @param outputPath The path where the file will be created
     * @throws IOException If there's an error writing to the file
     */
    private static void createLargeFvecsFile(Path outputPath) throws IOException {
        System.out.println("[DEBUG_LOG] Creating large fvecs file at: " + outputPath);
        System.out.println("[DEBUG_LOG] Target file size: " + (TARGET_FILE_SIZE / (1024 * 1024)) + " MB");
        System.out.println("[DEBUG_LOG] Total vectors to write: " + TOTAL_VECTORS);

        // Use RandomAccessFile with FileChannel for better performance
        try (RandomAccessFile file = new RandomAccessFile(outputPath.toFile(), "rw");
             FileChannel channel = file.getChannel()) {

            // Allocate a buffer for batch writing
            ByteBuffer batchBuffer = ByteBuffer.allocateDirect(VECTORS_PER_BATCH * VECTOR_SIZE_BYTES);
            batchBuffer.order(ByteOrder.LITTLE_ENDIAN);

            long totalBytesWritten = 0;
            long vectorsWritten = 0;
            long startTime = System.currentTimeMillis();
            long lastLogTime = startTime;

            // Write vectors in batches
            while (totalBytesWritten < TARGET_FILE_SIZE) {
                batchBuffer.clear();

                // Fill the buffer with vectors
                int vectorsInBatch = (int) Math.min(VECTORS_PER_BATCH, TOTAL_VECTORS - vectorsWritten);
                for (int i = 0; i < vectorsInBatch; i++) {
                    // Write dimension count
                    batchBuffer.putInt(VECTOR_DIMENSIONS);

                    // Write vector values
                    for (int dim = 0; dim < VECTOR_DIMENSIONS; dim++) {
                        float value = generateDeterministicValue(vectorsWritten, dim);
                        batchBuffer.putFloat(value);
                    }

                    vectorsWritten++;
                }

                // Write the buffer to the file
                batchBuffer.flip();
                int bytesWritten = channel.write(batchBuffer);
                totalBytesWritten += bytesWritten;

                // Log progress every 5 seconds
                long currentTime = System.currentTimeMillis();
                if (currentTime - lastLogTime > 5000) {
                    double progressPercent = (double) totalBytesWritten / TARGET_FILE_SIZE * 100;
                    double mbWritten = totalBytesWritten / (1024.0 * 1024.0);
                    double elapsedSeconds = (currentTime - startTime) / 1000.0;
                    double mbPerSecond = mbWritten / elapsedSeconds;

                    System.out.println(String.format(
                        "[DEBUG_LOG] Progress: %.2f%% (%.2f MB / %.2f MB) at %.2f MB/s",
                        progressPercent, mbWritten, TARGET_FILE_SIZE / (1024.0 * 1024.0), mbPerSecond
                    ));

                    lastLogTime = currentTime;
                }

                // Break if we've written all vectors
                if (vectorsWritten >= TOTAL_VECTORS) {
                    break;
                }
            }

            long endTime = System.currentTimeMillis();
            double totalTimeSeconds = (endTime - startTime) / 1000.0;

            System.out.println("[DEBUG_LOG] File creation completed in " + totalTimeSeconds + " seconds");
            System.out.println("[DEBUG_LOG] Final file size: " + (totalBytesWritten / (1024 * 1024)) + " MB");
            System.out.println("[DEBUG_LOG] Total vectors written: " + vectorsWritten);
        }
    }

    /**
     * Verifies that the generated file contains the expected deterministic content.
     * This method samples vectors from the beginning, middle, and end of the file.
     * 
     * @param filePath The path to the generated file
     * @throws IOException If there's an error reading the file
     */
    private static void verifyFileContent(Path filePath) throws IOException {
        System.out.println("[DEBUG_LOG] Verifying file content: " + filePath);

        try (RandomAccessFile file = new RandomAccessFile(filePath.toFile(), "r");
             FileChannel channel = file.getChannel()) {

            long fileSize = channel.size();
            long totalVectors = fileSize / VECTOR_SIZE_BYTES;

            System.out.println("[DEBUG_LOG] File size: " + (fileSize / (1024 * 1024)) + " MB");
            System.out.println("[DEBUG_LOG] Estimated total vectors: " + totalVectors);

            // Verify vectors at the beginning, middle, and end
            verifyVectorAt(channel, 0);
            verifyVectorAt(channel, totalVectors / 2);
            verifyVectorAt(channel, totalVectors - 1);

            System.out.println("[DEBUG_LOG] File content verification successful");
        }
    }

    /**
     * Verifies a single vector at the specified index.
     * 
     * @param channel The file channel to read from
     * @param vectorIndex The index of the vector to verify
     * @throws IOException If there's an error reading the file
     */
    private static void verifyVectorAt(FileChannel channel, long vectorIndex) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(VECTOR_SIZE_BYTES);
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        // Position the channel at the start of the vector
        long position = vectorIndex * VECTOR_SIZE_BYTES;
        channel.position(position);

        // Read the vector
        buffer.clear();
        channel.read(buffer);
        buffer.flip();

        // Verify dimension count
        int dimensions = buffer.getInt();
        assertEquals(VECTOR_DIMENSIONS, dimensions, 
            "Vector at index " + vectorIndex + " has incorrect dimension count");

        // Verify vector values
        for (int dim = 0; dim < dimensions; dim++) {
            float expected = generateDeterministicValue(vectorIndex, dim);
            float actual = buffer.getFloat();
            assertEquals(expected, actual, 0.0001f, 
                "Vector at index " + vectorIndex + ", dimension " + dim + " has incorrect value");
        }

        System.out.println("[DEBUG_LOG] Verified vector at index " + vectorIndex);
    }

    /**
     * Test method that generates a large fvecs file with deterministic content.
     * This test is skipped if there's not enough disk space available.
     * The file is created in a suitable non-root filesystem and left in place
     * for other tests to use.
     * 
     * @throws IOException If there's an error creating or verifying the file
     */
    @Test
    public void testGenerateLargeFvecsFile() throws IOException {
        // Find a suitable filesystem location
        Path outputPath = getLargeFvecsFilePath();

        // Create the large fvecs file
        createLargeFvecsFile(outputPath);

        // Verify the file exists and has the expected size
        assertTrue(Files.exists(outputPath), "The output file was not created");
        long fileSize = Files.size(outputPath);
        // Allow for small variations in file size (at least 99% of target)
        long minimumAcceptableSize = (long)(TARGET_FILE_SIZE * 0.99);
        assertTrue(fileSize >= minimumAcceptableSize, 
            "File size is too small: " + fileSize + " bytes, expected at least " + minimumAcceptableSize + " bytes");

        // Verify the file content
        verifyFileContent(outputPath);

        // Store the file path for use by other tests
        largeFvecsFilePath = outputPath;

        System.out.println("[DEBUG_LOG] Large fvecs file successfully generated at: " + outputPath);
        System.out.println("[DEBUG_LOG] File size: " + (fileSize / (1024 * 1024)) + " MB");
    }


    /**
     * Ensures the large fvecs file exists by creating it if necessary.
     * This method is used by other tests that depend on this file.
     * It prints out the path of the file, which is located on the largest non-root filesystem
     * with enough space, or on the root filesystem if no suitable non-root filesystem is found.
     * 
     * @return The path to the large fvecs file
     * @throws IOException If there's an error creating or verifying the file
     */
    public static Path ensureLargeFvecsFileExists() throws IOException {
        Path filePath = getLargeFvecsFilePath();

        // Check if the file already exists
        if (Files.exists(filePath)) {
            System.out.println("[DEBUG_LOG] ===== LARGE FVECS FILE LOCATION =====");
            System.out.println("[DEBUG_LOG] Large fvecs file already exists at: " + filePath);
            System.out.println("[DEBUG_LOG] File size: " + (Files.size(filePath) / (1024.0 * 1024.0 * 1024.0)) + " GB");
            System.out.println("[DEBUG_LOG] Filesystem: " + Files.getFileStore(filePath).name());
            System.out.println("[DEBUG_LOG] =====================================");
            return filePath;
        }

        // The filesystem check is already done in getLargeFvecsFilePath()

        // Create the large fvecs file
        System.out.println("[DEBUG_LOG] Creating large fvecs file as it doesn't exist");
        createLargeFvecsFile(filePath);

        // Verify the file exists and has the expected size
        if (Files.exists(filePath)) {
            long fileSize = Files.size(filePath);
            double fileSizeGB = fileSize / (1024.0 * 1024.0 * 1024.0);

            System.out.println("[DEBUG_LOG] ===== LARGE FVECS FILE LOCATION =====");
            System.out.println("[DEBUG_LOG] Large fvecs file successfully generated at: " + filePath);
            System.out.println("[DEBUG_LOG] File size: " + String.format("%.2f GB (%.2f MB)", 
                              fileSizeGB, fileSize / (1024.0 * 1024.0)));
            System.out.println("[DEBUG_LOG] Filesystem: " + Files.getFileStore(filePath).name());
            System.out.println("[DEBUG_LOG] =====================================");

            // Verify the file content
            verifyFileContent(filePath);
        }

        return filePath;
    }
}
