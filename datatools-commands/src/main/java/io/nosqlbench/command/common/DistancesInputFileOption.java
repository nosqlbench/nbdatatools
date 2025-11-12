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
import java.nio.file.Files;

/**
 * Mixin for distances input file option.
 * Provides an optional option for specifying the neighbor distances input file path.
 */
public class DistancesInputFileOption {

    @CommandLine.Option(
        names = {"--distances"},
        description = "Input file path for neighbor distances (optional, for distance verification)"
    )
    private Path distancesPath;

    /**
     * Gets the distances file path
     * @return The distances file path, or null if not specified
     */
    public Path getDistancesPath() {
        return distancesPath;
    }

    /**
     * Gets the normalized distances file path
     * @return The normalized path to the distances file, or null if not specified
     */
    public Path getNormalizedDistancesPath() {
        return distancesPath != null ? distancesPath.normalize() : null;
    }

    /**
     * Checks if distances path was specified
     * @return true if specified, false otherwise
     */
    public boolean isSpecified() {
        return distancesPath != null;
    }

    /**
     * Validates that the distances file exists
     * @throws IllegalArgumentException if the file doesn't exist
     */
    public void validateDistancesInput() {
        if (distancesPath != null && !Files.exists(distancesPath)) {
            throw new IllegalArgumentException("Distances input file does not exist: " + distancesPath);
        }
    }

    @Override
    public String toString() {
        return "DistancesInputFileOption{distancesPath=" + distancesPath + "}";
    }
}
