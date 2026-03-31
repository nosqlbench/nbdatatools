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

package io.nosqlbench.slabtastic;

/// Constants for the slabtastic binary file format.
///
/// Slabtastic is a large file format supporting files of up to 2^63 bytes.
/// All multi-byte values are stored little-endian. All offset pointers
/// targeting the whole file range are twos-complement signed 8-byte integers
/// for easy interop with conventional stacks. The magic bytes spell "SLAB"
/// in UTF-8 when written in file order.
///
/// Page sizes are governed by:
/// - Minimum page size: 512 bytes (2^9)
/// - Maximum page size: 2^32 bytes
/// - All pages must be a multiple of 512 bytes
///
/// When a single record exceeds the limits of a page, it is an error in v1.
///
/// The v1 page footer is 16 bytes and supports up to 2^40 in ordinal
/// magnitude and 2^24 in record count per page. The footer format is
/// page-specific, since files may later contain pages with an updated
/// format. Version 0 is invalid; readers must verify that the page version
/// is recognized before reading.
public interface SlabConstants {

    /// Magic int: bytes `S`, `L`, `A`, `B` stored little-endian → `0x42414C53`.
    int MAGIC = 0x42414C53;

    /// Page header size in bytes: 4 (magic) + 4 (page size).
    int HEADER_SIZE = 8;

    /// Version 1 footer size in bytes.
    int FOOTER_V1_SIZE = 16;

    /// Invalid page type.
    byte PAGE_TYPE_INVALID = 0;

    /// Pages-page type — the index page at the end of the file.
    byte PAGE_TYPE_PAGES_PAGE = 1;

    /// Data page type — holds user records.
    byte PAGE_TYPE_DATA = 2;

    /// Namespaces page type — the namespace index page at the end of a
    /// multi-namespace file.
    byte PAGE_TYPE_NAMESPACES_PAGE = 3;

    /// Invalid namespace index. Byte 13 of the footer was formerly called
    /// "version"; namespace index 0 is reserved and invalid, matching the
    /// old version-0-is-invalid semantics.
    byte NAMESPACE_INVALID = 0;

    /// Default namespace index. Existing single-namespace files have
    /// version=1 in byte 13, which reads as namespace_index=1 = default.
    byte NAMESPACE_DEFAULT = 1;

    /// Maximum length in bytes for a namespace name.
    int NAMESPACE_MAX_NAME_LENGTH = 128;

    /// Invalid version.
    /// @deprecated Use {@link #NAMESPACE_INVALID} instead. The version byte
    ///             has been repurposed as namespace index.
    @Deprecated
    byte VERSION_INVALID = NAMESPACE_INVALID;

    /// Format version 1.
    /// @deprecated Use {@link #NAMESPACE_DEFAULT} instead. The version byte
    ///             has been repurposed as namespace index.
    @Deprecated
    byte VERSION_1 = NAMESPACE_DEFAULT;

    /// Minimum page size in bytes (2^9).
    int MIN_PAGE_SIZE = 512;

    /// Maximum page size in bytes (2^32 − 1, expressed as a long since
    /// Java ints are signed).
    long MAX_PAGE_SIZE = 0xFFFFFFFFL;

    /// Maximum ordinal value for a 5-byte signed integer (2^39 − 1).
    long MAX_ORDINAL = (1L << 39) - 1;

    /// Minimum ordinal value for a 5-byte signed integer (−2^39).
    long MIN_ORDINAL = -(1L << 39);

    /// Maximum record count per page for a 3-byte unsigned integer (2^24 − 1).
    int MAX_RECORD_COUNT = (1 << 24) - 1;

    /// Size of each offset entry in bytes.
    int OFFSET_ENTRY_SIZE = 4;

    /// Size of each pages-page record in bytes: 8 (ordinal) + 8 (file offset).
    int PAGES_PAGE_RECORD_SIZE = 16;

    /// Page size alignment granularity in bytes.
    int PAGE_ALIGNMENT = 512;
}
