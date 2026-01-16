package io.nosqlbench.vshapes.checkpoint;

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

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;

/// Manager for saving and loading checkpoint state during vector analysis.
///
/// ## Purpose
///
/// Provides atomic save/load operations for checkpoint files, enabling
/// long-running analyses to be interrupted and resumed safely.
///
/// ## Atomic Writes
///
/// Saves are performed atomically using write-to-temp-then-rename:
///
/// ```text
/// ┌─────────────────────────────────────────────────────────────────┐
/// │                    ATOMIC SAVE OPERATION                        │
/// └─────────────────────────────────────────────────────────────────┘
///
///   1. Write JSON to temporary file (checkpoint.json.tmp)
///   2. Compute SHA-256 checksum of content
///   3. Rename temp file to final name (atomic on POSIX)
///
///   If interrupted at any point:
///   - Original checkpoint (if any) remains intact
///   - Temp file may exist but won't corrupt state
/// ```
///
/// ## Checksum Validation
///
/// Each checkpoint includes a SHA-256 checksum for verification:
///
/// ```json
/// {
///   "version": 1,
///   "checksum": "sha256:e3b0c442...",
///   ...
/// }
/// ```
///
/// ## Usage
///
/// ```java
/// // Save checkpoint
/// CheckpointState state = new CheckpointState.Builder()
///     .sourcePath("/data/vectors.fvec")
///     .totalDimensions(384)
///     .completedDimensions(150)
///     .dimensionStatistics(stats)
///     .scalarModels(models)
///     .build();
///
/// CheckpointManager.save(Path.of("checkpoint.json"), state);
///
/// // Load checkpoint
/// CheckpointState restored = CheckpointManager.load(Path.of("checkpoint.json"));
/// ```
///
/// @see CheckpointState
/// @see VshapesGsonConfig
public final class CheckpointManager {

    private static final String TEMP_SUFFIX = ".tmp";
    private static final String CHECKSUM_PREFIX = "sha256:";

    private CheckpointManager() {
        // Utility class
    }

    /// Saves checkpoint state to a file atomically.
    ///
    /// The save operation is atomic: the file is first written to a temporary
    /// location, then renamed to the final path. This ensures that an existing
    /// checkpoint is never corrupted by an interrupted save.
    ///
    /// @param path the path to save the checkpoint to
    /// @param state the checkpoint state to save
    /// @throws IOException if writing fails
    /// @throws NullPointerException if path or state is null
    public static void save(Path path, CheckpointState state) throws IOException {
        Objects.requireNonNull(path, "path cannot be null");
        Objects.requireNonNull(state, "state cannot be null");

        Gson gson = VshapesGsonConfig.gson();
        String json = gson.toJson(state);

        // Compute checksum of the JSON content (excluding the checksum field itself)
        String checksum = computeChecksum(json);

        // Re-serialize with checksum included
        CheckpointState stateWithChecksum = new CheckpointState.Builder()
            .sourcePath(state.sourcePath())
            .totalDimensions(state.totalDimensions())
            .completedDimensions(state.completedDimensions())
            .timestamp(state.timestamp())
            .checksum(checksum)
            .dimensionStatistics(state.dimensionStatistics())
            .scalarModels(state.scalarModels())
            .build();

        String finalJson = gson.toJson(stateWithChecksum);

        // Atomic write: temp file then rename
        Path tempPath = path.resolveSibling(path.getFileName() + TEMP_SUFFIX);
        try (Writer writer = Files.newBufferedWriter(tempPath, StandardCharsets.UTF_8)) {
            writer.write(finalJson);
        }

        // Atomic rename (on POSIX systems)
        Files.move(tempPath, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    /// Loads checkpoint state from a file.
    ///
    /// Validates the checkpoint version and optionally verifies the checksum.
    ///
    /// @param path the path to load the checkpoint from
    /// @return the loaded checkpoint state
    /// @throws IOException if reading fails
    /// @throws CheckpointException if the checkpoint is invalid or corrupted
    /// @throws NullPointerException if path is null
    public static CheckpointState load(Path path) throws IOException, CheckpointException {
        return load(path, true);
    }

    /// Loads checkpoint state from a file with optional checksum verification.
    ///
    /// @param path the path to load the checkpoint from
    /// @param verifyChecksum whether to verify the checksum
    /// @return the loaded checkpoint state
    /// @throws IOException if reading fails
    /// @throws CheckpointException if the checkpoint is invalid or corrupted
    /// @throws NullPointerException if path is null
    public static CheckpointState load(Path path, boolean verifyChecksum) throws IOException, CheckpointException {
        Objects.requireNonNull(path, "path cannot be null");

        if (!Files.exists(path)) {
            throw new CheckpointException("Checkpoint file not found: " + path);
        }

        String json;
        try (Reader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            StringBuilder sb = new StringBuilder();
            char[] buffer = new char[8192];
            int read;
            while ((read = reader.read(buffer)) != -1) {
                sb.append(buffer, 0, read);
            }
            json = sb.toString();
        }

        CheckpointState state;
        try {
            Gson gson = VshapesGsonConfig.gson();
            state = gson.fromJson(json, CheckpointState.class);
        } catch (JsonSyntaxException e) {
            throw new CheckpointException("Invalid checkpoint JSON: " + e.getMessage(), e);
        }

        if (state == null) {
            throw new CheckpointException("Checkpoint file is empty or null");
        }

        // Validate version
        if (state.version() != CheckpointState.CURRENT_VERSION) {
            throw new CheckpointException(
                "Unsupported checkpoint version: " + state.version() +
                " (expected: " + CheckpointState.CURRENT_VERSION + ")");
        }

        // Verify checksum if requested
        if (verifyChecksum && state.checksum() != null) {
            // Recompute checksum without the checksum field
            CheckpointState stateWithoutChecksum = new CheckpointState.Builder()
                .sourcePath(state.sourcePath())
                .totalDimensions(state.totalDimensions())
                .completedDimensions(state.completedDimensions())
                .timestamp(state.timestamp())
                .checksum(null)
                .dimensionStatistics(state.dimensionStatistics())
                .scalarModels(state.scalarModels())
                .build();

            String expectedChecksum = computeChecksum(VshapesGsonConfig.gson().toJson(stateWithoutChecksum));
            if (!expectedChecksum.equals(state.checksum())) {
                throw new CheckpointException(
                    "Checkpoint checksum mismatch: expected " + expectedChecksum +
                    " but found " + state.checksum());
            }
        }

        return state;
    }

    /// Finds the latest checkpoint in a directory.
    ///
    /// Searches for files matching the pattern `checkpoint-*.json` and returns
    /// the one with the highest completed dimension count.
    ///
    /// @param directory the directory to search
    /// @return the path to the latest checkpoint, or null if none found
    /// @throws IOException if listing the directory fails
    public static Path findLatestCheckpoint(Path directory) throws IOException {
        Objects.requireNonNull(directory, "directory cannot be null");

        if (!Files.isDirectory(directory)) {
            return null;
        }

        return Files.list(directory)
            .filter(p -> p.getFileName().toString().startsWith("checkpoint-"))
            .filter(p -> p.getFileName().toString().endsWith(".json"))
            .max((p1, p2) -> {
                try {
                    CheckpointState s1 = load(p1, false);
                    CheckpointState s2 = load(p2, false);
                    return Integer.compare(s1.completedDimensions(), s2.completedDimensions());
                } catch (Exception e) {
                    return 0;
                }
            })
            .orElse(null);
    }

    /// Generates a checkpoint filename based on the current progress.
    ///
    /// @param state the checkpoint state
    /// @return a filename like "checkpoint-0150.json" for 150 completed dimensions
    public static String generateFilename(CheckpointState state) {
        return String.format("checkpoint-%04d.json", state.completedDimensions());
    }

    /// Computes a SHA-256 checksum of the given content.
    private static String computeChecksum(String content) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(content.getBytes(StandardCharsets.UTF_8));
            return CHECKSUM_PREFIX + HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is always available
            throw new RuntimeException("SHA-256 not available", e);
        }
    }

    /// Exception thrown when checkpoint loading or validation fails.
    public static class CheckpointException extends Exception {
        public CheckpointException(String message) {
            super(message);
        }

        public CheckpointException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
