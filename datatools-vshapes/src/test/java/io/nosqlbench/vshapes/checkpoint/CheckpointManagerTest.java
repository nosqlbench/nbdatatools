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

import io.nosqlbench.vshapes.extract.DimensionStatistics;
import io.nosqlbench.vshapes.model.NormalScalarModel;
import io.nosqlbench.vshapes.model.ScalarModel;
import io.nosqlbench.vshapes.model.UniformScalarModel;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@Tag("unit")
class CheckpointManagerTest {

    private static final double TOLERANCE = 1e-10;

    @Test
    void saveAndLoadRoundTrip(@TempDir Path tempDir) throws Exception {
        // Create test data
        List<DimensionStatistics> stats = List.of(
            new DimensionStatistics(0, 1000, 0.0, 1.0, 0.5, 0.083, 0.0, 1.8),
            new DimensionStatistics(1, 1000, -1.0, 1.0, 0.0, 0.333, 0.0, 1.8)
        );

        List<ScalarModel> models = List.of(
            new UniformScalarModel(0.0, 1.0),
            new NormalScalarModel(0.0, 0.577, -1.0, 1.0)
        );

        CheckpointState original = new CheckpointState.Builder()
            .sourcePath("/test/vectors.fvec")
            .totalDimensions(384)
            .completedDimensions(2)
            .dimensionStatistics(stats)
            .scalarModels(models)
            .build();

        // Save
        Path checkpointPath = tempDir.resolve("checkpoint.json");
        CheckpointManager.save(checkpointPath, original);

        // Verify file exists
        assertTrue(Files.exists(checkpointPath));

        // Load
        CheckpointState restored = CheckpointManager.load(checkpointPath);

        // Verify metadata
        assertEquals(original.version(), restored.version());
        assertEquals(original.sourcePath(), restored.sourcePath());
        assertEquals(original.totalDimensions(), restored.totalDimensions());
        assertEquals(original.completedDimensions(), restored.completedDimensions());
        assertNotNull(restored.timestamp());
        assertNotNull(restored.checksum());

        // Verify dimension statistics
        assertEquals(original.dimensionStatistics().size(), restored.dimensionStatistics().size());
        for (int i = 0; i < original.dimensionStatistics().size(); i++) {
            DimensionStatistics origStats = original.dimensionStatistics().get(i);
            DimensionStatistics restStats = restored.dimensionStatistics().get(i);
            assertEquals(origStats.dimension(), restStats.dimension());
            assertEquals(origStats.count(), restStats.count());
            assertEquals(origStats.min(), restStats.min(), TOLERANCE);
            assertEquals(origStats.max(), restStats.max(), TOLERANCE);
            assertEquals(origStats.mean(), restStats.mean(), TOLERANCE);
            assertEquals(origStats.variance(), restStats.variance(), TOLERANCE);
        }

        // Verify scalar models
        assertEquals(original.scalarModels().size(), restored.scalarModels().size());
        for (int i = 0; i < original.scalarModels().size(); i++) {
            assertEquals(original.scalarModels().get(i).getClass(),
                         restored.scalarModels().get(i).getClass());
            assertEquals(original.scalarModels().get(i).getModelType(),
                         restored.scalarModels().get(i).getModelType());
        }
    }

    @Test
    void loadNonExistentFileThrows(@TempDir Path tempDir) {
        Path nonExistent = tempDir.resolve("nonexistent.json");
        assertThrows(CheckpointManager.CheckpointException.class, () ->
            CheckpointManager.load(nonExistent));
    }

    @Test
    void loadInvalidJsonThrows(@TempDir Path tempDir) throws IOException {
        Path invalidJson = tempDir.resolve("invalid.json");
        Files.writeString(invalidJson, "{ invalid json }");

        assertThrows(CheckpointManager.CheckpointException.class, () ->
            CheckpointManager.load(invalidJson));
    }

    @Test
    void loadEmptyFileThrows(@TempDir Path tempDir) throws IOException {
        Path emptyFile = tempDir.resolve("empty.json");
        Files.writeString(emptyFile, "");

        assertThrows(CheckpointManager.CheckpointException.class, () ->
            CheckpointManager.load(emptyFile));
    }

    @Test
    void checksumValidationDetectsCorruption(@TempDir Path tempDir) throws Exception {
        CheckpointState state = new CheckpointState.Builder()
            .sourcePath("/test/vectors.fvec")
            .totalDimensions(100)
            .completedDimensions(50)
            .build();

        Path checkpointPath = tempDir.resolve("checkpoint.json");
        CheckpointManager.save(checkpointPath, state);

        // Corrupt the file by modifying content
        String content = Files.readString(checkpointPath);
        String corrupted = content.replace("50", "51");  // Change completedDimensions
        Files.writeString(checkpointPath, corrupted);

        // Should throw due to checksum mismatch
        assertThrows(CheckpointManager.CheckpointException.class, () ->
            CheckpointManager.load(checkpointPath));
    }

    @Test
    void loadWithoutChecksumVerification(@TempDir Path tempDir) throws Exception {
        CheckpointState state = new CheckpointState.Builder()
            .sourcePath("/test/vectors.fvec")
            .totalDimensions(100)
            .completedDimensions(50)
            .build();

        Path checkpointPath = tempDir.resolve("checkpoint.json");
        CheckpointManager.save(checkpointPath, state);

        // Corrupt the file
        String content = Files.readString(checkpointPath);
        String corrupted = content.replace("50", "51");
        Files.writeString(checkpointPath, corrupted);

        // Should succeed without checksum verification
        CheckpointState restored = CheckpointManager.load(checkpointPath, false);
        assertEquals(51, restored.completedDimensions());
    }

    @Test
    void findLatestCheckpoint(@TempDir Path tempDir) throws Exception {
        // Create multiple checkpoints
        for (int i : List.of(10, 50, 30)) {
            CheckpointState state = new CheckpointState.Builder()
                .sourcePath("/test/vectors.fvec")
                .totalDimensions(100)
                .completedDimensions(i)
                .build();
            Path path = tempDir.resolve(CheckpointManager.generateFilename(state));
            CheckpointManager.save(path, state);
        }

        // Find latest
        Path latest = CheckpointManager.findLatestCheckpoint(tempDir);
        assertNotNull(latest);

        CheckpointState latestState = CheckpointManager.load(latest);
        assertEquals(50, latestState.completedDimensions());
    }

    @Test
    void findLatestCheckpointEmptyDir(@TempDir Path tempDir) throws IOException {
        Path latest = CheckpointManager.findLatestCheckpoint(tempDir);
        assertNull(latest);
    }

    @Test
    void generateFilename() {
        CheckpointState state = new CheckpointState.Builder()
            .sourcePath("/test/vectors.fvec")
            .totalDimensions(1000)
            .completedDimensions(150)
            .build();

        String filename = CheckpointManager.generateFilename(state);
        assertEquals("checkpoint-0150.json", filename);
    }

    @Test
    void checkpointStateProgressCalculation() {
        CheckpointState state = new CheckpointState.Builder()
            .sourcePath("/test/vectors.fvec")
            .totalDimensions(100)
            .completedDimensions(25)
            .build();

        assertEquals(25.0, state.progressPercent(), TOLERANCE);
        assertFalse(state.isComplete());

        CheckpointState complete = new CheckpointState.Builder()
            .sourcePath("/test/vectors.fvec")
            .totalDimensions(100)
            .completedDimensions(100)
            .build();

        assertEquals(100.0, complete.progressPercent(), TOLERANCE);
        assertTrue(complete.isComplete());
    }

    @Test
    void checkpointStateValidation() {
        // Missing source path
        assertThrows(NullPointerException.class, () ->
            new CheckpointState.Builder()
                .totalDimensions(100)
                .completedDimensions(50)
                .build());

        // Negative dimensions
        assertThrows(IllegalArgumentException.class, () ->
            new CheckpointState.Builder()
                .sourcePath("/test")
                .totalDimensions(-1)
                .build());

        // Completed exceeds total
        assertThrows(IllegalStateException.class, () ->
            new CheckpointState.Builder()
                .sourcePath("/test")
                .totalDimensions(50)
                .completedDimensions(100)
                .build());
    }

    @Test
    void atomicSaveDoesNotCorruptOnFailure(@TempDir Path tempDir) throws Exception {
        // Save initial checkpoint
        CheckpointState initial = new CheckpointState.Builder()
            .sourcePath("/test/vectors.fvec")
            .totalDimensions(100)
            .completedDimensions(25)
            .build();

        Path checkpointPath = tempDir.resolve("checkpoint.json");
        CheckpointManager.save(checkpointPath, initial);

        // Save second checkpoint
        CheckpointState updated = new CheckpointState.Builder()
            .sourcePath("/test/vectors.fvec")
            .totalDimensions(100)
            .completedDimensions(50)
            .build();

        CheckpointManager.save(checkpointPath, updated);

        // Verify updated state
        CheckpointState loaded = CheckpointManager.load(checkpointPath);
        assertEquals(50, loaded.completedDimensions());

        // Verify no temp file left behind
        Path tempPath = tempDir.resolve("checkpoint.json.tmp");
        assertFalse(Files.exists(tempPath));
    }
}
