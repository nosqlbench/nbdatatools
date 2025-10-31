/*
 * Copyright (c) nosqlbench
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.nosqlbench.command.common;

import picocli.CommandLine;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;

/**
 * Mixin for distances output file option.
 * Provides an optional option for specifying the distances output file path.
 * If not specified, derives the path from the indices file path.
 */
public class DistancesFileOption {

    @CommandLine.Option(
        names = {"-d", "--distances"},
        description = "Output file path for distances (optional, derived from indices if not specified)"
    )
    private Path distancesPath;

    private Path indicesPath; // Set by the command after parsing

    /**
     * Sets the indices path for deriving distances path when not specified
     * @param indicesPath The indices file path
     */
    public void setIndicesPath(Path indicesPath) {
        this.indicesPath = indicesPath;
    }

    /**
     * Gets the distances file path, deriving from indices if not specified
     * @return The distances file path
     */
    public Path getDistancesPath() {
        if (distancesPath != null) {
            return distancesPath;
        }

        if (indicesPath == null) {
            throw new IllegalStateException("Indices path must be set before getting distances path");
        }

        // Derive from indices path
        return deriveDistancesPath(indicesPath);
    }

    /**
     * Derives a distances file path from an indices file path
     * @param indicesPath The indices file path
     * @return The derived distances file path
     */
    private Path deriveDistancesPath(Path indicesPath) {
        String indicesName = indicesPath.getFileName().toString();

        // Replace common indices-related patterns with distances
        String distancesName;
        if (indicesName.contains("indices")) {
            distancesName = indicesName.replace("indices", "distances");
        } else if (indicesName.contains("neighbors")) {
            distancesName = indicesName.replace("neighbors", "distances");
        } else if (indicesName.contains("idx")) {
            distancesName = indicesName.replace("idx", "dist");
        } else {
            // If no pattern matches, append -distances before the extension
            int dotIndex = indicesName.lastIndexOf('.');
            if (dotIndex > 0) {
                distancesName = indicesName.substring(0, dotIndex) +
                               "-distances" +
                               indicesName.substring(dotIndex);
            } else {
                distancesName = indicesName + "-distances";
            }
        }

        Path parent = indicesPath.getParent();
        return parent != null ? parent.resolve(distancesName) : Paths.get(distancesName);
    }

    /**
     * Gets the normalized distances file path
     * @return The normalized path to the distances file
     */
    public Path getNormalizedDistancesPath() {
        return getDistancesPath().normalize();
    }

    /**
     * Gets the normalized path as a string
     * @return The normalized path string
     */
    public String getNormalizedPath() {
        return getNormalizedDistancesPath().toString();
    }

    /**
     * Checks if distances path was explicitly specified
     * @return true if explicitly specified, false if it will be derived
     */
    public boolean isExplicitlySpecified() {
        return distancesPath != null;
    }

    /**
     * Validates that the output file can be created
     * @param force Whether to force overwrite existing files
     * @throws IllegalArgumentException if the file exists and force is not set
     */
    public void validateDistancesOutput(boolean force) {
        Path normalizedPath = getNormalizedDistancesPath();
        if (Files.exists(normalizedPath) && !force) {
            throw new IllegalArgumentException(
                "Distances output file already exists: " + normalizedPath +
                ". Use --force to overwrite."
            );
        }

        // Check if parent directory exists or can be created
        Path parentDir = normalizedPath.getParent();
        if (parentDir != null && !Files.exists(parentDir)) {
            try {
                Files.createDirectories(parentDir);
            } catch (Exception e) {
                throw new IllegalArgumentException(
                    "Cannot create parent directory for distances output: " + parentDir, e
                );
            }
        }
    }

    /**
     * Creates a DistancesFile record from this option
     * @param force Whether to force overwrite
     * @return A DistancesFile record with path and force flag
     */
    public DistancesFile toDistancesFile(boolean force) {
        return new DistancesFile(getNormalizedDistancesPath(), force);
    }

    @Override
    public String toString() {
        if (distancesPath != null) {
            return "DistancesFileOption{distancesPath=" + distancesPath + " (explicit)}";
        } else {
            return "DistancesFileOption{distancesPath=<derived from indices>}";
        }
    }

    /**
     * Record representing distances file configuration
     */
    public record DistancesFile(Path path, boolean force) implements CharSequence {
        public DistancesFile {
            if (path == null) {
                throw new IllegalArgumentException("Distances path cannot be null");
            }
            path = path.normalize();
        }

        @Override
        public int length() {
            return path.toString().length();
        }

        @Override
        public char charAt(int index) {
            return path.toString().charAt(index);
        }

        @Override
        public CharSequence subSequence(int start, int end) {
            return path.toString().subSequence(start, end);
        }

        @Override
        public String toString() {
            return path.toString();
        }

        public Path normalize() {
            return path.normalize();
        }

        public boolean exists() {
            return Files.exists(path);
        }
    }
}