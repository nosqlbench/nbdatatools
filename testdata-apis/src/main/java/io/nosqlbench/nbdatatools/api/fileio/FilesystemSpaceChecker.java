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

package io.nosqlbench.nbdatatools.api.fileio;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility class for checking filesystem space availability before performing operations
 * that require disk space. Provides both single-path and multi-path space checking
 * with configurable safety margins.
 * 
 * <p>Usage examples:</p>
 * <pre>
 * // Simple single path check with default 20% margin
 * FilesystemSpaceChecker.checkSpaceAvailable(Paths.get("/tmp"), 1_000_000_000.0);
 * 
 * // Single path check with custom margin
 * FilesystemSpaceChecker.checkSpaceAvailable(Paths.get("/tmp"), 1_000_000_000.0, 0.10);
 * 
 * // Multi-path check with builder pattern
 * FilesystemSpaceChecker.builder()
 *     .withMargin(0.15)
 *     .addPath(Paths.get("/tmp/file1"), 500_000_000.0)
 *     .addPath(Paths.get("/var/file2"), 300_000_000.0)
 *     .checkAll();
 * </pre>
 */
public class FilesystemSpaceChecker {
    
    private static final double DEFAULT_MARGIN = 0.20; // 20% margin

    /**
     * Checks if there is enough disk space available for the specified size at the given path,
     * using the default 20% safety margin.
     * 
     * @param path the filesystem path to check
     * @param sizeBytes the required space in bytes
     * @throws InsufficientSpaceException if there is not enough space available
     */
    public static void checkSpaceAvailable(Path path, double sizeBytes) throws InsufficientSpaceException {
        checkSpaceAvailable(path, sizeBytes, DEFAULT_MARGIN);
    }

    /**
     * Checks if there is enough disk space available for the specified size at the given path,
     * with a custom safety margin.
     * 
     * @param path the filesystem path to check
     * @param sizeBytes the required space in bytes
     * @param marginPercent the safety margin as a percentage (e.g., 0.20 for 20%)
     * @throws InsufficientSpaceException if there is not enough space available
     */
    public static void checkSpaceAvailable(Path path, double sizeBytes, double marginPercent) throws InsufficientSpaceException {
        try {
            FileStore fileStore = getFileStoreForPath(path);
            long usableSpace = fileStore.getUsableSpace();
            double requiredSpace = sizeBytes * (1.0 + marginPercent);
            
            if (usableSpace < requiredSpace) {
                throw new InsufficientSpaceException(String.format(
                    "Insufficient disk space on filesystem %s for file %s. Required: %.2f GB (%"
                    + ".2f GB + %.0f%% margin), Available: %.2f GB",
                    fileStore.name(),
                    (path.startsWith(fileStore.name()) ?
                        path.toString().substring(fileStore.name().length()) :
                        path.toString()),
                    requiredSpace / (1024.0 * 1024.0 * 1024.0),
                    sizeBytes / (1024.0 * 1024.0 * 1024.0),
                    marginPercent * 100,
                    usableSpace / (1024.0 * 1024.0 * 1024.0)
                ));
            }
        } catch (IOException e) {
            throw new InsufficientSpaceException("Failed to check disk space for path: " + path, e);
        }
    }

    /**
     * Gets the FileStore for a path, walking up the directory tree if necessary
     * to find an existing path that can be used to determine the filesystem.
     */
    private static FileStore getFileStoreForPath(Path path) throws IOException {
        Path currentPath = path.toAbsolutePath();
        
        while (currentPath != null) {
            if (Files.exists(currentPath)) {
                return Files.getFileStore(currentPath);
            }
            currentPath = currentPath.getParent();
        }
        
        // If we can't find any existing parent, fall back to the root
        return Files.getFileStore(path.getRoot());
    }

    /**
     * Creates a new builder for checking multiple paths with combined space requirements
     * per filesystem.
     * 
     * @return a new Builder instance
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder class for checking space requirements across multiple paths.
     * Paths that reside on the same filesystem will have their space requirements
     * combined for more accurate space checking.
     */
    public static class Builder {
        private final List<PathSizeRequirement> requirements = new ArrayList<>();
        private double marginPercent = DEFAULT_MARGIN;

        /**
         * Sets the safety margin percentage to use for all space checks.
         * 
         * @param marginPercent the safety margin as a percentage (e.g., 0.15 for 15%)
         * @return this builder instance for method chaining
         */
        public Builder withMargin(double marginPercent) {
            this.marginPercent = marginPercent;
            return this;
        }

        /**
         * Adds a path and its required space to the check list.
         * 
         * @param path the filesystem path
         * @param sizeBytes the required space in bytes for this path
         * @return this builder instance for method chaining
         */
        public Builder addPath(Path path, double sizeBytes) {
            requirements.add(new PathSizeRequirement(path, sizeBytes));
            return this;
        }

        /**
         * Performs the space check for all added paths, combining space requirements
         * for paths that reside on the same filesystem.
         * 
         * @throws InsufficientSpaceException if any filesystem does not have enough space
         */
        public void checkAll() throws InsufficientSpaceException {
            try {
                Map<FileStore, Double> fileStoreRequirements = new HashMap<>();
                
                for (PathSizeRequirement req : requirements) {
                    FileStore fileStore = getFileStoreForPath(req.path);
                    fileStoreRequirements.merge(fileStore, req.sizeBytes, Double::sum);
                }
                
                for (Map.Entry<FileStore, Double> entry : fileStoreRequirements.entrySet()) {
                    FileStore fileStore = entry.getKey();
                    double totalSizeBytes = entry.getValue();
                    long usableSpace = fileStore.getUsableSpace();
                    double requiredSpace = totalSizeBytes * (1.0 + marginPercent);
                    
                    if (usableSpace < requiredSpace) {
                        String pathsInfo = requirements.stream()
                            .filter(req -> {
                                try {
                                    return getFileStoreForPath(req.path).equals(fileStore);
                                } catch (IOException e) {
                                    return false;
                                }
                            })
                            .map(req -> String.format("%s (%.2f GB)", req.path, req.sizeBytes / (1024.0 * 1024.0 * 1024.0)))
                            .reduce((a, b) -> a + ", " + b)
                            .orElse("unknown paths");
                        
                        throw new InsufficientSpaceException(String.format(
                            "Insufficient disk space on filesystem %s. Required: %.2f GB (%.2f GB + %.0f%% margin), Available: %.2f GB. Affected paths: %s",
                            fileStore.name(),
                            requiredSpace / (1024.0 * 1024.0 * 1024.0),
                            totalSizeBytes / (1024.0 * 1024.0 * 1024.0),
                            marginPercent * 100,
                            usableSpace / (1024.0 * 1024.0 * 1024.0),
                            pathsInfo
                        ));
                    }
                }
            } catch (IOException e) {
                throw new InsufficientSpaceException("Failed to check disk space for one or more paths", e);
            }
        }
    }

    private static class PathSizeRequirement {
        final Path path;
        final double sizeBytes;

        PathSizeRequirement(Path path, double sizeBytes) {
            this.path = path;
            this.sizeBytes = sizeBytes;
        }
    }

    /**
     * Exception thrown when there is insufficient disk space for the requested operation.
     */
    public static class InsufficientSpaceException extends RuntimeException {
        /**
         * Constructs a new InsufficientSpaceException with the specified detail message.
         * 
         * @param message the detail message
         */
        public InsufficientSpaceException(String message) {
            super(message);
        }

        /**
         * Constructs a new InsufficientSpaceException with the specified detail message and cause.
         * 
         * @param message the detail message
         * @param cause the cause of the exception
         */
        public InsufficientSpaceException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}