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

import com.google.gson.annotations.SerializedName;
import io.nosqlbench.vshapes.extract.DimensionStatistics;
import io.nosqlbench.vshapes.model.ScalarModel;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/// Container for checkpoint state during long-running vector analysis.
///
/// ## Purpose
///
/// Enables long-running analyses to be saved to disk and resumed later.
/// Captures all necessary state to continue processing from where it left off.
///
/// ## JSON Schema
///
/// ```json
/// {
///   "version": 1,
///   "source_path": "/path/to/vectors.fvec",
///   "total_dimensions": 384,
///   "completed_dimensions": 150,
///   "timestamp": "2025-01-07T10:30:00Z",
///   "checksum": "sha256:abc123...",
///   "dimension_statistics": [...],
///   "scalar_models": [...]
/// }
/// ```
///
/// ## Usage
///
/// ```java
/// // Create checkpoint during processing
/// CheckpointState state = new CheckpointState.Builder()
///     .sourcePath(inputPath)
///     .totalDimensions(384)
///     .completedDimensions(150)
///     .dimensionStatistics(stats)
///     .scalarModels(models)
///     .build();
///
/// // Save to disk
/// CheckpointManager.save(checkpointPath, state);
///
/// // Later, resume from checkpoint
/// CheckpointState restored = CheckpointManager.load(checkpointPath);
/// int resumeFrom = restored.completedDimensions();
/// ```
///
/// @see CheckpointManager
public final class CheckpointState {

    /// Current checkpoint format version.
    public static final int CURRENT_VERSION = 1;

    @SerializedName("version")
    private final int version;

    @SerializedName("source_path")
    private final String sourcePath;

    @SerializedName("total_dimensions")
    private final int totalDimensions;

    @SerializedName("completed_dimensions")
    private final int completedDimensions;

    @SerializedName("timestamp")
    private final String timestamp;

    @SerializedName("checksum")
    private final String checksum;

    @SerializedName("dimension_statistics")
    private final List<DimensionStatistics> dimensionStatistics;

    @SerializedName("scalar_models")
    private final List<ScalarModel> scalarModels;

    /// Private constructor used by Builder.
    private CheckpointState(Builder builder) {
        this.version = CURRENT_VERSION;
        this.sourcePath = builder.sourcePath;
        this.totalDimensions = builder.totalDimensions;
        this.completedDimensions = builder.completedDimensions;
        this.timestamp = builder.timestamp != null ? builder.timestamp : Instant.now().toString();
        this.checksum = builder.checksum;
        this.dimensionStatistics = builder.dimensionStatistics;
        this.scalarModels = builder.scalarModels;
    }

    /// Returns the checkpoint format version.
    public int version() {
        return version;
    }

    /// Returns the source file path being analyzed.
    public String sourcePath() {
        return sourcePath;
    }

    /// Returns the total number of dimensions to process.
    public int totalDimensions() {
        return totalDimensions;
    }

    /// Returns the number of dimensions that have been completed.
    public int completedDimensions() {
        return completedDimensions;
    }

    /// Returns the timestamp when this checkpoint was created.
    public String timestamp() {
        return timestamp;
    }

    /// Returns the checksum for verification.
    public String checksum() {
        return checksum;
    }

    /// Returns the list of computed dimension statistics.
    /// The list contains statistics for dimensions 0 through completedDimensions-1.
    public List<DimensionStatistics> dimensionStatistics() {
        return dimensionStatistics;
    }

    /// Returns the list of fitted scalar models.
    /// The list contains models for dimensions 0 through completedDimensions-1.
    public List<ScalarModel> scalarModels() {
        return scalarModels;
    }

    /// Returns the progress as a percentage (0-100).
    public double progressPercent() {
        if (totalDimensions == 0) {
            return 0.0;
        }
        return 100.0 * completedDimensions / totalDimensions;
    }

    /// Returns true if all dimensions have been processed.
    public boolean isComplete() {
        return completedDimensions >= totalDimensions;
    }

    @Override
    public String toString() {
        return String.format(
            "CheckpointState[v%d, source=%s, progress=%d/%d (%.1f%%)]",
            version, sourcePath, completedDimensions, totalDimensions, progressPercent());
    }

    /// Builder for creating CheckpointState instances.
    public static final class Builder {
        private String sourcePath;
        private int totalDimensions;
        private int completedDimensions;
        private String timestamp;
        private String checksum;
        private List<DimensionStatistics> dimensionStatistics;
        private List<ScalarModel> scalarModels;

        /// Sets the source file path.
        public Builder sourcePath(String sourcePath) {
            this.sourcePath = Objects.requireNonNull(sourcePath);
            return this;
        }

        /// Sets the total number of dimensions.
        public Builder totalDimensions(int totalDimensions) {
            if (totalDimensions < 0) {
                throw new IllegalArgumentException("totalDimensions must be non-negative");
            }
            this.totalDimensions = totalDimensions;
            return this;
        }

        /// Sets the number of completed dimensions.
        public Builder completedDimensions(int completedDimensions) {
            if (completedDimensions < 0) {
                throw new IllegalArgumentException("completedDimensions must be non-negative");
            }
            this.completedDimensions = completedDimensions;
            return this;
        }

        /// Sets the timestamp (optional, defaults to current time).
        public Builder timestamp(String timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        /// Sets the checksum (optional).
        public Builder checksum(String checksum) {
            this.checksum = checksum;
            return this;
        }

        /// Sets the dimension statistics.
        public Builder dimensionStatistics(List<DimensionStatistics> dimensionStatistics) {
            this.dimensionStatistics = dimensionStatistics;
            return this;
        }

        /// Sets the scalar models.
        public Builder scalarModels(List<ScalarModel> scalarModels) {
            this.scalarModels = scalarModels;
            return this;
        }

        /// Builds the CheckpointState.
        public CheckpointState build() {
            Objects.requireNonNull(sourcePath, "sourcePath is required");
            if (completedDimensions > totalDimensions) {
                throw new IllegalStateException(
                    "completedDimensions (" + completedDimensions + ") cannot exceed totalDimensions (" + totalDimensions + ")");
            }
            return new CheckpointState(this);
        }
    }
}
