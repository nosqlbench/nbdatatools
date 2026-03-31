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
/// - {@link #SLAB} - Slabtastic format for predicate data (.slab files)
/// - {@link #SQLITE} - SQLite database for predicate data (.db, .sqlite files)
///
/// ## Type Inference
///
/// When parsing source specifications, the type can be:
/// - Explicitly set via `type: xvec`
/// - Inferred from the file extension:
///   `.slab` implies {@link #SLAB}, `.db`/`.sqlite` implies {@link #SQLITE}
/// - Defaulted to {@link #XVEC} for all other paths
///
/// ## Usage
///
/// ```yaml
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

    /// Slabtastic format source.
    ///
    /// Sources of this type read from `.slab` files using the slabtastic
    /// page-aligned, footer-indexed data format. Supports namespaced
    /// storage of predicates, result indices, metadata layout, and
    /// metadata content.
    SLAB,

    /// SQLite database source.
    ///
    /// Sources of this type read from `.db` or `.sqlite` files using
    /// SQLite JDBC. Predicate data is stored in tables keyed by ordinal.
    SQLITE;

    /// Infers the source type from a file path.
    ///
    /// @param path the source file path
    /// @return the inferred source type
    public static SourceType inferFromPath(String path) {
        if (path == null) {
            return XVEC;
        }
        String lower = path.toLowerCase();
        if (lower.endsWith(".slab")) {
            return SLAB;
        } else if (lower.endsWith(".db") || lower.endsWith(".sqlite")) {
            return SQLITE;
        }
        return XVEC;
    }

    /// Parses a source type from a string value.
    ///
    /// Accepts case-insensitive values: "xvec", "file", "slab", "sqlite", "db".
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
        } else if ("slab".equals(lower) || "slabtastic".equals(lower)) {
            return SLAB;
        } else if ("sqlite".equals(lower) || "db".equals(lower)) {
            return SQLITE;
        }
        return null;
    }

    /// Returns the canonical string representation for serialization.
    ///
    /// @return the lowercase name of this source type
    public String toValue() {
        return name().toLowerCase();
    }
}
