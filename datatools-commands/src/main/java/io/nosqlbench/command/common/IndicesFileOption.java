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
 * Mixin for indices output file option.
 * Provides a required option for specifying the indices (neighbors) output file path.
 */
public class IndicesFileOption {

    @CommandLine.Option(
        names = {"-i", "--indices"},
        description = "Output file path for neighbor indices",
        required = true
    )
    private Path indicesPath;

    @CommandLine.Option(
        names = {"-f", "--force"},
        description = "Force overwrite of existing output files",
        defaultValue = "false"
    )
    private boolean force;

    /**
     * Gets the indices file path
     * @return The indices file path
     */
    public Path getIndicesPath() {
        return indicesPath;
    }

    /**
     * Gets the normalized indices file path
     * @return The normalized path to the indices file
     */
    public Path getNormalizedIndicesPath() {
        return indicesPath.normalize();
    }

    /**
     * Gets the normalized path as a string
     * @return The normalized path string
     */
    public String getNormalizedPath() {
        return getNormalizedIndicesPath().toString();
    }

    /**
     * Checks if force overwrite is enabled
     * @return true if force is enabled, false otherwise
     */
    public boolean isForce() {
        return force;
    }

    /**
     * Validates that the output file can be created
     * @throws IllegalArgumentException if the file exists and force is not set
     */
    public void validateIndicesOutput() {
        Path normalizedPath = getNormalizedIndicesPath();
        if (Files.exists(normalizedPath) && !force) {
            throw new IllegalArgumentException(
                "Indices output file already exists: " + normalizedPath +
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
                    "Cannot create parent directory for indices output: " + parentDir, e
                );
            }
        }
    }

    /**
     * Creates an IndicesFile record from this option
     * @return An IndicesFile record with path and force flag
     */
    public IndicesFile toIndicesFile() {
        return new IndicesFile(getNormalizedIndicesPath(), force);
    }

    @Override
    public String toString() {
        return "IndicesFileOption{indicesPath=" + indicesPath + ", force=" + force + "}";
    }

    /**
     * Record representing indices file configuration
     */
    public record IndicesFile(Path path, boolean force) implements CharSequence {
        public IndicesFile {
            if (path == null) {
                throw new IllegalArgumentException("Indices path cannot be null");
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