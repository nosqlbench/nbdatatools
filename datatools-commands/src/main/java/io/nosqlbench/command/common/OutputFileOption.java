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

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Shared output file option with force overwrite flag.
 * All supporting types are inner classes for self-contained encapsulation.
 */
public class OutputFileOption {

    /**
     * Immutable output file specification with force-overwrite flag.
     * Implements CharSequence to allow direct use as a string in most contexts.
     *
     * @param path  the output file path (never null)
     * @param force whether to force overwrite if file exists
     */
    public record OutputFile(Path path, boolean force) implements CharSequence {

        /**
         * Compact constructor with validation.
         */
        public OutputFile {
            if (path == null) {
                throw new IllegalArgumentException("Output path cannot be null");
            }
        }

        /**
         * Creates an OutputFile without force overwrite.
         */
        public OutputFile(Path path) {
            this(path, false);
        }

        /**
         * Gets the normalized absolute path.
         */
        public Path normalizedPath() {
            return path.normalize().toAbsolutePath();
        }

        /**
         * Checks if the output file exists and force is not set.
         */
        public boolean existsWithoutForce() {
            return Files.exists(path) && !force;
        }

        // CharSequence implementation - delegates to path string

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

        /**
         * Returns the path as a string with force flag if set.
         */
        @Override
        public String toString() {
            if (force) {
                return path + " (force)";
            }
            return path.toString();
        }
    }

    @CommandLine.Option(
        names = {"-o", "--output"},
        description = "The output file path",
        required = true
    )
    private Path outputPath;

    @CommandLine.Option(
        names = {"-f", "--force"},
        description = "Force overwrite if output file already exists"
    )
    private boolean force = false;

    /**
     * Gets the OutputFile record constructed from the options.
     */
    public OutputFile getOutputFile() {
        return new OutputFile(outputPath, force);
    }

    /**
     * Gets the output file path.
     */
    public Path getOutputPath() {
        return outputPath;
    }

    /**
     * Gets the normalized output file path.
     */
    public Path getNormalizedOutputPath() {
        return outputPath != null ? outputPath.normalize() : null;
    }

    /**
     * Checks if force overwrite is enabled.
     */
    public boolean isForce() {
        return force;
    }

    /**
     * Checks if the output file already exists and force is not enabled.
     */
    public boolean outputExistsWithoutForce() {
        return getOutputFile().existsWithoutForce();
    }

    /**
     * Validates the output file, checking for existence without force flag.
     */
    public void validate() {
        if (outputExistsWithoutForce()) {
            throw new IllegalStateException(
                "Output file already exists: " + outputPath + ". Use --force to overwrite."
            );
        }
    }

    /**
     * Returns string representation of the output file.
     */
    @Override
    public String toString() {
        return getOutputFile().toString();
    }
}
