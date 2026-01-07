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

import io.nosqlbench.vshapes.checkpoint.VshapesGsonConfig;
import io.nosqlbench.vshapes.model.ScalarModel;

/// Observer interface for monitoring dimension-by-dimension model extraction progress.
///
/// ## Purpose
///
/// Enables visibility into long-running extraction operations by providing callbacks
/// at key points during processing:
///
/// ```text
/// ┌─────────────────────────────────────────────────────────────────┐
/// │                    EXTRACTION LIFECYCLE                         │
/// └─────────────────────────────────────────────────────────────────┘
///
///   For each dimension 0..N-1:
///
///   ┌──────────────────┐
///   │ onDimensionStart │ ──► Called when processing begins for dim
///   └──────────────────┘
///           │
///           ▼
///   ┌───────────────────────┐
///   │ onAccumulatorUpdate   │ ──► Called periodically as data accumulates
///   │ (multiple times)      │     (frequency depends on implementation)
///   └───────────────────────┘
///           │
///           ▼
///   ┌─────────────────────┐
///   │ onDimensionComplete │ ──► Called when model is fitted for dim
///   └─────────────────────┘
/// ```
///
/// ## Usage
///
/// ```java
/// // Create a tracing observer that writes NDJSON
/// StateObserver observer = new NdjsonTraceObserver(outputPath);
///
/// // Attach to extractor
/// extractor.setObserver(observer);
///
/// // Run extraction (callbacks fire automatically)
/// VectorSpaceModel model = extractor.extract(source);
/// ```
///
/// ## Thread Safety
///
/// Implementations should be thread-safe if the extractor processes dimensions
/// concurrently. The default NOOP observer is inherently thread-safe.
///
/// @see io.nosqlbench.vshapes.extract.ModelExtractor
public interface StateObserver {

    /// No-op observer that does nothing.
    ///
    /// Use this as a default when no observation is needed.
    StateObserver NOOP = new StateObserver() {
        @Override
        public void onDimensionStart(int dimension) {
            // No-op
        }

        @Override
        public void onAccumulatorUpdate(int dimension, Object accumulatorState) {
            // No-op
        }

        @Override
        public void onDimensionComplete(int dimension, ScalarModel result) {
            // No-op
        }
    };

    /// Called when processing begins for a dimension.
    ///
    /// @param dimension the zero-based dimension index
    void onDimensionStart(int dimension);

    /// Called periodically as data accumulates for a dimension.
    ///
    /// The frequency of these calls depends on the implementation. Some extractors
    /// may call once per chunk, others once per batch.
    ///
    /// @param dimension the zero-based dimension index
    /// @param accumulatorState the current accumulator state (may be any type)
    void onAccumulatorUpdate(int dimension, Object accumulatorState);

    /// Called when a dimension's model has been fitted.
    ///
    /// @param dimension the zero-based dimension index
    /// @param result the fitted scalar model for this dimension
    void onDimensionComplete(int dimension, ScalarModel result);

    /// Formats an object as a JSON string using the vshapes Gson configuration.
    ///
    /// Utility method for observers that need to serialize state snapshots.
    ///
    /// @param state the object to format
    /// @return JSON string representation
    static String toJson(Object state) {
        return VshapesGsonConfig.gson().toJson(state);
    }

    /// Formats an object as a compact (non-pretty-printed) JSON string.
    ///
    /// Useful for NDJSON output where each record should be on a single line.
    ///
    /// @param state the object to format
    /// @return compact JSON string representation
    static String toCompactJson(Object state) {
        return VshapesGsonConfig.compactGson().toJson(state);
    }
}
