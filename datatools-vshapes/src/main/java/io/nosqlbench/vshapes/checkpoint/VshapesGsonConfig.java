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
import com.google.gson.GsonBuilder;
import io.nosqlbench.vshapes.model.ScalarModelTypeAdapterFactory;

/// Centralized Gson configuration for vshapes checkpoint serialization.
///
/// ## Purpose
///
/// Provides a properly configured [Gson] instance for serializing and
/// deserializing vshapes state objects, including:
///
/// - [io.nosqlbench.vshapes.model.ScalarModel] implementations (polymorphic)
/// - [io.nosqlbench.vshapes.extract.DimensionStatistics]
/// - Accumulator state snapshots
/// - Checkpoint state containers
///
/// ## Usage
///
/// ```java
/// Gson gson = VshapesGsonConfig.gson();
///
/// // Serialize a ScalarModel
/// ScalarModel model = new NormalScalarModel(0.0, 1.0);
/// String json = gson.toJson(model, ScalarModel.class);
/// // {"type":"normal","mean":0.0,"std_dev":1.0}
///
/// // Deserialize polymorphically
/// ScalarModel restored = gson.fromJson(json, ScalarModel.class);
/// assert restored instanceof NormalScalarModel;
/// ```
///
/// ## Configuration
///
/// The Gson instance is configured with:
///
/// | Feature | Setting | Purpose |
/// |---------|---------|---------|
/// | Pretty printing | Enabled | Human-readable checkpoints |
/// | Serialize nulls | Disabled | Compact output |
/// | HTML escaping | Disabled | Cleaner numeric output |
/// | ScalarModel adapter | Registered | Polymorphic model support |
///
/// ## Thread Safety
///
/// The [Gson] instance is thread-safe and can be shared across threads.
/// This class uses a singleton pattern for efficiency.
///
/// @see ScalarModelTypeAdapterFactory
/// @see CheckpointManager
public final class VshapesGsonConfig {

    private static final Gson INSTANCE;

    static {
        INSTANCE = new GsonBuilder()
            .setPrettyPrinting()
            .disableHtmlEscaping()
            .serializeSpecialFloatingPointValues()  // Handle NaN, Infinity, -Infinity
            .registerTypeAdapterFactory(ScalarModelTypeAdapterFactory.create())
            .create();
    }

    private VshapesGsonConfig() {
        // Utility class
    }

    /// Returns the configured Gson instance.
    ///
    /// This instance is:
    /// - Thread-safe
    /// - Pre-configured with all vshapes type adapters
    /// - Suitable for checkpoint serialization
    ///
    /// @return the shared Gson instance
    public static Gson gson() {
        return INSTANCE;
    }

    /// Creates a new GsonBuilder with vshapes defaults.
    ///
    /// Use this when you need to customize the Gson configuration
    /// beyond the defaults while keeping vshapes type adapters.
    ///
    /// @return a new GsonBuilder with vshapes configuration
    public static GsonBuilder builder() {
        return new GsonBuilder()
            .setPrettyPrinting()
            .disableHtmlEscaping()
            .serializeSpecialFloatingPointValues()
            .registerTypeAdapterFactory(ScalarModelTypeAdapterFactory.create());
    }

    /// Creates a compact (non-pretty-printed) Gson instance.
    ///
    /// Useful for NDJSON (newline-delimited JSON) output where
    /// each record should be on a single line.
    ///
    /// @return a compact Gson instance
    public static Gson compactGson() {
        return new GsonBuilder()
            .disableHtmlEscaping()
            .serializeSpecialFloatingPointValues()
            .registerTypeAdapterFactory(ScalarModelTypeAdapterFactory.create())
            .create();
    }
}
