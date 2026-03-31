/// Slabtastic is a page-aligned, footer-indexed, appendable on-disk data layout
/// designed for streaming writes and random-access reads. It is a large file
/// format supporting files of up to 2^63 bytes. All multi-byte offset pointers
/// targeting the whole file range are twos-complement signed 8-byte integers,
/// little-endian, for easy interop with conventional stacks.
///
/// ## Key properties
///
/// - **Page-aligned**: Data is organized into pages sized as multiples of 512
///   bytes (minimum 512, maximum 2^32 bytes), enabling optimal chunking from
///   block stores. Page alignment to the minimum page size is enforced by the
///   requirement that all pages be a multiple of 512 bytes.
/// - **Footer-indexed**: Each page carries a 16-byte footer containing the starting
///   ordinal (5-byte signed), record count (3-byte unsigned), page size, page type,
///   namespace index, and footer length. You can always read the last 16 bytes of
///   any page to locate the footer start, offset array, and page start. A
///   fence-post offset array (N+1 entries for N records) immediately before the
///   footer provides O(1) record access within a page.
/// - **Appendable**: New pages can be appended without rewriting existing data.
///   A special "pages page" at the end of the file serves as the page map. In
///   append-only mode, a new pages page is written after the old one (which
///   becomes dead data), referencing both old and new data pages. The last pages
///   page in a slabtastic file is always the authoritative pages page.
/// - **Random-accessible**: Given the pages page and per-page offset arrays, any
///   record can be located by global ordinal via O(log2 n) binary search over the
///   page index followed by constant-time local lookup.
/// - **Multi-batch readable**: The {@link io.nosqlbench.slabtastic.SlabReader#getAll}
///   API reads multiple records in a single call, coalescing ordinals that share the
///   same page into a single I/O operation and dispatching all page reads
///   asynchronously. Results are returned in submission order with partial success
///   support via {@link io.nosqlbench.slabtastic.BatchResult}.
/// - **Sparse**: Ordinal values may not be fully contiguous between the minimum and
///   maximum ordinals in a file. APIs that read ordinals must be able to signal
///   that a requested ordinal is not present (via {@link java.util.Optional#empty()}).
///
/// ## Page layout (all values little-endian)
///
/// ```
/// [magic:4][pageSize:4][records...][gap][offsets:(N+1)*4][footer:16]
/// ```
///
/// Records are simply packed data with no known structure. The offsets fully
/// define the beginning and end of each record — there is one more offset than
/// records to make indexing math simple. All record offsets are encoded as
/// array-structured int offsets from the beginning of the page.
///
/// ## Page types
///
/// - **Type 1 (pages page)**: Index page at the end of a single-namespace file.
///   Stores `[start_ordinal:8][offset:8]` tuples sorted by ordinal.
/// - **Type 2 (data page)**: Holds user records.
/// - **Type 3 (namespaces page)**: Index page at the end of a multi-namespace file.
///   Stores {@link io.nosqlbench.slabtastic.NamespacesPageEntry} records mapping
///   namespace names to their respective pages pages.
///
/// ## Namespaces
///
/// Namespaces organize data into independent ordinal spaces within a single file.
/// Each namespace has its own pages page and set of data pages. The namespace index
/// (byte 13 of the footer, formerly the version byte) identifies which namespace
/// a page belongs to. Single-namespace files end with a pages page (type 1) for
/// backward compatibility. Multi-namespace files end with a namespaces page (type 3).
///
/// ## Pages page
///
/// In a single-namespace file, the last page is a pages page (type 1). In a
/// multi-namespace file, each namespace has its own pages page, all referenced
/// from the namespaces page (type 3) at EOF. Pages not referenced in any pages
/// page are considered logically deleted. A slabtastic file that does not end in
/// a pages page or a namespaces page is invalid.
///
/// ## File naming
///
/// The filename extension for slab files is ".slab".
///
/// ## Entry points
///
/// - {@link io.nosqlbench.slabtastic.SlabWriter} — sequential/append file writer
/// - {@link io.nosqlbench.slabtastic.SlabReader} — random-access and multi-batch file reader
/// - {@link io.nosqlbench.slabtastic.BatchRequest} — single request within a multi-batch read
/// - {@link io.nosqlbench.slabtastic.BatchResult} — ordered results from a multi-batch read
/// - {@link io.nosqlbench.slabtastic.SlabConstants} — format constants
/// - {@link io.nosqlbench.slabtastic.cli.CMD_slab} — CLI maintenance tool
///
/// The reader uses {@link java.nio.channels.AsynchronousFileChannel} for I/O.
package io.nosqlbench.slabtastic;

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
