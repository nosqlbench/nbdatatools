package io.nosqlbench.vshapes.trace;

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

import io.nosqlbench.vshapes.model.ScalarModel;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;

/// StateObserver that writes NDJSON (newline-delimited JSON) trace files.
///
/// ## Output Format
///
/// Each line is a JSON object representing an event:
///
/// ```json
/// {"event":"dimension_start","dimension":0,"timestamp":1234567890}
/// {"event":"accumulator_update","dimension":0,"state":{...},"timestamp":1234567891}
/// {"event":"dimension_complete","dimension":0,"model":{...},"timestamp":1234567892}
/// ```
///
/// ## Usage
///
/// ```java
/// try (NdjsonTraceObserver observer = new NdjsonTraceObserver(Path.of("trace.ndjson"))) {
///     extractor.setObserver(observer);
///     VectorSpaceModel model = extractor.extractVectorModel(data);
/// }
/// ```
///
/// ## Thread Safety
///
/// This implementation synchronizes writes to ensure thread-safe output.
///
/// @see StateObserver
public final class NdjsonTraceObserver implements StateObserver, Closeable {

    private final BufferedWriter writer;
    private final Object writeLock = new Object();

    /// Creates an NDJSON trace observer that writes to a file.
    ///
    /// @param outputPath path to write trace output
    /// @throws IOException if the file cannot be opened for writing
    public NdjsonTraceObserver(Path outputPath) throws IOException {
        this.writer = Files.newBufferedWriter(outputPath,
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING,
            StandardOpenOption.WRITE);
    }

    /// Creates an NDJSON trace observer that writes to a Writer.
    ///
    /// @param writer the writer to use (caller retains ownership)
    public NdjsonTraceObserver(Writer writer) {
        this.writer = (writer instanceof BufferedWriter bw)
            ? bw
            : new BufferedWriter(writer);
    }

    @Override
    public void onDimensionStart(int dimension) {
        writeEvent(Map.of(
            "event", "dimension_start",
            "dimension", dimension,
            "timestamp", System.currentTimeMillis()
        ));
    }

    @Override
    public void onAccumulatorUpdate(int dimension, Object accumulatorState) {
        writeEvent(Map.of(
            "event", "accumulator_update",
            "dimension", dimension,
            "state", accumulatorState,
            "timestamp", System.currentTimeMillis()
        ));
    }

    @Override
    public void onDimensionComplete(int dimension, ScalarModel result) {
        writeEvent(Map.of(
            "event", "dimension_complete",
            "dimension", dimension,
            "model", result,
            "timestamp", System.currentTimeMillis()
        ));
    }

    private void writeEvent(Map<String, Object> event) {
        synchronized (writeLock) {
            try {
                writer.write(StateObserver.toCompactJson(event));
                writer.newLine();
                writer.flush();
            } catch (IOException e) {
                throw new RuntimeException("Failed to write trace event", e);
            }
        }
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }
}
