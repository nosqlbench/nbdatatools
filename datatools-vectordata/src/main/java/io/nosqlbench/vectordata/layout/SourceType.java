package io.nosqlbench.vectordata.layout;

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

/// Defines the type of data source for vector datasets.
///
/// ## Source Types
///
/// - {@link #XVEC} - File-backed sources using xvec format (.fvec, .ivec files)
/// - {@link #VIRTDATA} - Generator-backed sources using a VectorSpaceModel JSON file
///
/// ## Type Inference
///
/// When parsing source specifications, the type can be:
/// - Explicitly set via `type: virtdata` or `type: xvec`
/// - Inferred from the file extension: `.json` implies {@link #VIRTDATA}
/// - Defaulted to {@link #XVEC} for all other paths
///
/// ## Usage
///
/// ```yaml
/// # Explicit virtdata source
/// base_vectors:
///   type: virtdata
///   source: model.json
///   window: 0..1000000
///
/// # Inferred virtdata (from .json extension)
/// base_vectors: model.json[0..1000000]
///
/// # Traditional xvec source (default)
/// base_vectors: vectors.fvec
/// ```
///
/// @see FSource
/// @see io.nosqlbench.vectordata.layoutv2.DSSource
public enum SourceType {

    /// File-backed xvec format (default).
    ///
    /// Sources of this type read vectors from `.fvec` or `.ivec` files
    /// using memory-mapped I/O for efficient access.
    XVEC,

    /// Generator-backed virtdata source.
    ///
    /// Sources of this type generate vectors on-the-fly using a
    /// {@link io.nosqlbench.vshapes.model.VectorSpaceModel} loaded from
    /// a JSON configuration file. Vectors are computed deterministically
    /// based on their ordinal index.
    ///
    /// When using virtdata sources:
    /// - The source path points to a model JSON file
    /// - Generator selection is implicit based on model contents
    /// - Dimensions are derived from the model
    /// - Cardinality requires a window specification (otherwise unbounded)
    VIRTDATA;

    /// Infers the source type from a file path.
    ///
    /// If the path ends with `.json`, returns {@link #VIRTDATA}.
    /// Otherwise returns {@link #XVEC}.
    ///
    /// @param path the source file path
    /// @return the inferred source type
    public static SourceType inferFromPath(String path) {
        if (path != null && path.toLowerCase().endsWith(".json")) {
            return VIRTDATA;
        }
        return XVEC;
    }

    /// Parses a source type from a string value.
    ///
    /// Accepts case-insensitive values: "xvec", "virtdata", "virtual", "generator".
    /// Returns null for unrecognized values.
    ///
    /// @param value the string value to parse
    /// @return the parsed source type, or null if not recognized
    public static SourceType fromString(String value) {
        if (value == null) {
            return null;
        }
        String lower = value.toLowerCase();
        if ("xvec".equals(lower) || "file".equals(lower)) {
            return XVEC;
        } else if ("virtdata".equals(lower) || "virtual".equals(lower)
                   || "generator".equals(lower) || "model".equals(lower)) {
            return VIRTDATA;
        }
        return null;
    }

    /// Returns the canonical string representation for serialization.
    ///
    /// @return "xvec" or "virtdata"
    public String toValue() {
        return name().toLowerCase();
    }
}
