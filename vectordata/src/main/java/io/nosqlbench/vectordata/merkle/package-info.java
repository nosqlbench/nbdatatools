/// This package contains a merkle-tree implementation of a file transfer client.
///
/// # Merkle Tree Structure
///
/// A Merkle tree is a binary tree where each leaf node contains the hash of a chunk of data,
/// and each internal node contains the hash of its two children. This structure allows for
/// efficient verification of large data sets.
///
/// ```
///                    ┌───────────────┐
///                    │     Root      │  Array Index: 0
///                    │    Hash H     │
///                    └───────┬───────┘
///                            │
///            ┌───────────────┴───────────────┐
///            │                               │
///     ┌──────▼──────┐               ┌────────▼────────┐
///     │  Internal   │  Index: 1     │    Internal     │  Index: 2
///     │   Hash H1   │               │     Hash H2     │
///     └──────┬──────┘               └────────┬────────┘
///            │                               │
///     ┌──────┴──────┐               ┌────────┴────────┐
///     │              │              │                 │
/// ┌───▼───┐      ┌───▼───┐      ┌───▼───┐         ┌───▼───┐
/// │ Leaf  │      │ Leaf  │      │ Leaf  │         │ Leaf  │
/// │Hash A │      │Hash B │      │Hash C │         │Hash D │
/// │Idx: 3 │      │Idx: 4 │      │Idx: 5 │         │Idx: 6 │
/// └───┬───┘      └───┬───┘      └───┬───┘         └───┬───┘
///     │              │              │                 │
/// ┌───▼───┐      ┌───▼───┐      ┌───▼───┐         ┌───▼───┐
/// │Chunk 1│      │Chunk 2│      │Chunk 3│         │Chunk 4│
/// │  Data │      │  Data │      │  Data │         │  Data │
/// └───────┘      └───────┘      └───────┘         └───────┘
///```
///
/// ## Heap-Structured Arrangement
///
/// The merkle tree is implemented using a heap-structured arrangement in a flat array:
/// - The tree is stored in a flat array with root-first ordering
/// - Index 0 is used for the root node of the tree
/// - For a node at index i, its left child is at index 2*i + 1 and right child at 2*i + 2
/// - The parent of a node at index i is at index (i-1)/2
/// - Leaf nodes start at the offset index (calculated as capLeaf - 1)
///
/// ```
/// Array indices:  [0][1][2][3][4][5][6]
/// Tree nodes:     Root    H1     H2     A      B      C      D
///```
///
/// In this implementation:
/// - Leaf nodes (Hash A, B, C, D) contain SHA-256 hashes of fixed-size chunks of the file
/// - Internal nodes (H1, H2) contain SHA-256 hashes of their two child nodes
/// - The root node (H) at index 0 contains the hash of the entire file
/// - A BitSet tracks which nodes have valid hashes (1 = valid, 0 = invalid)
/// - When a chunk is modified, its hash and all parent hashes up to the root are invalidated
/// - Hashes are computed lazily when accessed, if they are marked as invalid
///
/// # Update Operations
///
/// ## Leaf Node Update Process
///
/// When a leaf node is updated, all parent nodes up to the root are invalidated to maintain consistency.
/// The diagram below shows the process of updating a leaf node (Hash B):
///
/// ```
///                    ┌───────────────┐
///                    │     Root      │  Step 3: Root invalidated
///                    │    Hash H     │  (set to zeroes, bit=0)
///                    └───────┬───────┘
///                            │
///            ┌───────────────┴───────────────┐
///            │                               │
///     ┌──────▼──────┐               ┌────────▼────────┐
///     │  Internal   │  Step 2: Parent invalidated     │
///     │   Hash H1   │  (set to zeroes, bit=0)         │
///     └──────┬──────┘               └────────┬────────┘
///            │                               │
///     ┌──────┴──────┐               ┌────────┴────────┐
///     │              │              │                 │
/// ┌───▼───┐      ┌───▼───┐      ┌───▼───┐         ┌───▼───┐
/// │ Leaf  │      │ Leaf  │      │ Leaf  │         │ Leaf  │
/// │Hash A │      │Hash B │      │Hash C │         │Hash D │
/// │       │      │       │      │       │         │       │
/// └───────┘      └───┬───┘      └───────┘         └───────┘
///                    │
///                    ▼
///               Step 1: Leaf updated
///(new hash computed, bit=1)
///```
///
/// ## Lazy Hash Computation
///
/// Hashes are computed lazily when accessed. When a node's hash is requested:
/// 1. If the node is valid (bit=1), return the stored hash
/// 2. If the node is invalid (bit=0), compute the hash from its children recursively, only
/// recomputing the hash for each node if it is marked as invalid.
/// 3. After computing, mark the node as having a valid hash (bit=1)
///
/// ```
///                    ┌───────────────┐
///                    │     Root      │  Step 3: Root hash computed
///                    │    Hash H     │  and marked valid (bit=1)
///                    └───────┬───────┘
///                            │
///            ┌───────────────┴───────────────┐
///            │                               │
///     ┌──────▼──────┐               ┌────────▼────────┐
///     │  Internal   │  Step 2: H1 computed from       │
///     │   Hash H1   │  children and marked valid      │
///     └──────┬──────┘               └────────┬────────┘
///            │                               │
///     ┌──────┴──────┐               ┌────────┴────────┐
///     │              │              │                 │
/// ┌───▼───┐      ┌───▼───┐      ┌───▼───┐         ┌───▼───┐
/// │ Leaf  │      │ Leaf  │      │ Leaf  │         │ Leaf  │
/// │Hash A │      │Hash B │      │Hash C │         │Hash D │
/// │ bit=1 │      │ bit=1 │      │ bit=1 │         │ bit=1 │
/// └───────┘      └───────┘      └───────┘         └───────┘
///
/// Step 1: Leaf hashes are already valid (bit=1)
///```
///
/// ## Shutdown Sequence - Breadth-First Hash Computation
///
/// During shutdown, calculable hashes are computed in a breadth-first manner:
///
/// ```
///                    ┌───────────────┐
///                    │     Root      │  Level 0: Computed last
///                    │    Hash H     │
///                    └───────┬───────┘
///                            │
///            ┌───────────────┴───────────────┐
///            │                               │
///     ┌──────▼──────┐               ┌────────▼────────┐
///     │  Internal   │  Level 1: Computed second       │
///     │   Hash H1   │               │     Hash H2     │
///     └──────┬──────┘               └────────┬────────┘
///            │                               │
///     ┌──────┴──────┐               ┌────────┴────────┐
///     │              │              │                 │
/// ┌───▼───┐      ┌───▼───┐      ┌───▼───┐         ┌───▼───┐
/// │ Leaf  │      │ Leaf  │      │ Leaf  │         │ Leaf  │
/// │Hash A │      │Hash B │      │Hash C │         │Hash D │
/// │       │      │       │      │       │         │       │
/// └───────┘      └───────┘      └───────┘         └───────┘
///
/// Level 2: Leaf nodes (already computed)
///
/// Computation rule: A parent node is computed only if:
/// 1. The parent is invalid (bit=0)
/// 2. Both children are valid (bit=1)
///```
///
/// # REQUIREMENTS
///
/// ## Merkle Tree
///
/// * Since merkle tree data will always be smaller than the Java memory-mapped size limit, this
/// should be how all merkle tree changes are made. Specifically, merkle tree data which is
/// backed on disk should always be represented, updated, and accessed via a memory mapped file.
/// * There should be two tracking structures for merkle tree content:
///   1) A persisted merkle tree which is stored directly in a byte array (A byte buffer) so that it can
///      support write-through semantics with its underlying file store
///   2) a persisted bit set which is used to quickly track which chunks of the file (the leaf
/// layer of the merkle tree) have been validated.
/// * When a merkle tree is created without being built from a content file, then it should be
/// considered unpopulated with all hashes invalid and the bitset set thusly.
/// * When a merkle tree is created with a content file, then all hashes including parent hashes
/// should be computed based on that content and then all bits set to 1 in the bitset.
/// * When a merkle tree is loaded  then the bitset should be
/// initialized from this, indicating that specific nodes are valid and others are not.
/// * At no time should a bitset entry indicate that a chunk is valid when it isn't.
/// * At no time should a merkle tree hash indicate that a chunk is valid when it isn't.
/// * When a hash node is updated, as long as the sibling hash is valid, then the parent hash
///   should be computed and then the process should be repeated until a parent sibling is also
///   invalid, making computing a valid parent hash impossible.
/// * Since all new merkle trees start with no valid hashes, it is not necessary to invalidate
/// any merkle node. All merkle nodes go from a state of invalid to valid and no other changes
/// are allowed for any single merkle bitset value.
/// * There is no need to proactively mark a merkle node as invalid since this should be its
///   default state. It can only transition to valid after being presented with content from
/// which to update the hash value (for leaf nodes) or being computed from the two children
/// hashes (for non-leaf nodes). In both of these cases, a hash value should be computed and the
/// valid bit enabled for that node.
/// * The backing mmap for the merkle tree file should be sized to hold all of the hashes for
/// leaf nodes and other nodes, the bitset, and the footer.
/// * Like the hash data, the bitset data should use the mmap as its backing store, so changes to
///  the bitset get persisted transparently to disk. This means that a long[] array should be
/// made as the backing store for the bitset by calling [java.nio.LongBuffer#array] after a long
/// buffer slice is made from the bitset region of the mmap file.
/// * The mmap should be sized to hold the known size of the hashes, bitset, and footer by
/// default.
///
/// ## MerklePainter
///
/// * MerklePainter should honor the minimum and maximum download sizes, selecting contiguous
/// chunks of data as needed to meet the minimum download size.
/// * MerklePainter should track the most recent request by the chunk index. Each time a request is
/// made which is logically contiguous from the last one, a counter for contiguous requests
/// should be increased. If this counter exceeds a configurable AUTOBUFFER_THRESHOLD value of 10,
/// then MerklePainter should go into a mode where it attempts to use the maximum download size
/// and keeps at least 4 additional requests running in read-ahead mode from the chunk index of
/// the most recent paint request.
/// * MerklePainter should check the validity of the request range against the MerkleTree
/// validity bitset before scheduling paint operations. Paint operations should only schedule
/// downloads for chunks which are not already valid.
/// * A separate method to check whether a range of requested chunks is already valid should be
/// created, and used as an early-exit check for both synchronous and asynchronous paint methods.
///
/// ## Merkle File Setup
///
/// * The merklev2 package provides the current implementation for merkle tree functionality.
/// * Remote content access should be handled through the MAFileChannel implementation.
/// * If the protocol scheme for the remote content URL is "file", local file access should be
/// optimized to avoid duplication of data on disk.
///
/// ## Auto-Buffering Behavior
///
/// The MerklePainter implements an auto-buffering feature that optimizes downloads based on access patterns:
///
/// ```
///                                                 ┌───────────────────────┐
///                                                 │ User Request Pattern  │
///                                                 └──────────┬────────────┘
///                                                            │
///                                                            ▼
///                                ┌───────────────────────────────────────────────┐
///                                │ Is request contiguous with previous request?  │
///                                └───────────────────┬───────────────────────────┘
///                                                    │
///                      ┌────────────────────────────┐│┌─────────────────────────────┐
///                      │                            ││                              │
///                      ▼                            ││                              ▼
///          ┌─────────────────────────┐             ││            ┌─────────────────────────────┐
///          │ Non-contiguous Request  │             ││            │    Contiguous Request       │
///          └────────────┬────────────┘             ││            └───────────────┬─────────────┘
///                       │                           ││                            │
///                       ▼                           ││                            ▼
///          ┌─────────────────────────┐             ││            ┌─────────────────────────────┐
///          │ Reset Counter           │             ││            │ Increment Counter           │
///          │ Disable Auto-buffer     │             ││            │                             │
///          └────────────┬────────────┘             ││            └───────────────┬─────────────┘
///                       │                           ││                            │
///                       │                           ││                            ▼
///                       │                           ││            ┌─────────────────────────────┐
///                       │                           ││            │ Counter >= THRESHOLD?       │
///                       │                           ││            └───────────────┬─────────────┘
///                       │                           ││                            │
///                       │                           ││              ┌─────────────┴─────────────┐
///                       │                           ││              │                           │
///                       │                           ││              ▼                           ▼
///                       │                           ││    ┌─────────────────┐        ┌──────────────────┐
///                       │                           ││    │      No         │        │       Yes        │
///                       │                           ││    └────────┬────────┘        └──────────┬───────┘
///                       │                           ││             │                            │
///                       │                           ││             │                            ▼
///                       │                           ││             │                 ┌──────────────────┐
///                       │                           ││             │                 │ Enable Auto-     │
///                       │                           ││             │                 │ buffer Mode      │
///                       │                           ││             │                 └──────────┬───────┘
///                       │                           ││             │                            │
///                       ▼                           ▼│             ▼                            ▼
///                       └───────────────────────────┐│┌────────────────────────────────────────┘
///                                                   ││
///                                                   ▼▼
///                                    ┌───────────────────────────┐
///                                    │ Process User Request      │
///                                    └───────────────┬───────────┘
///                                                    │
///                                                    ▼
///                                    ┌───────────────────────────┐
///                                    │ Auto-buffer Mode Active?  │
///                                    └───────────────┬───────────┘
///                                                    │
///                                    ┌───────────────┴───────────┐
///                                    │                           │
///                                    ▼                           ▼
///                         ┌────────────────────┐      ┌────────────────────────┐
///                         │        No          │      │          Yes           │
///                         └────────┬───────────┘      └────────────┬───────────┘
///                                  │                               │
///                                  │                               ▼
///                                  │                  ┌────────────────────────┐
///                                  │                  │ Schedule Read-ahead    │
///                                  │                  │ Downloads (4 requests) │
///                                  │                  └────────────┬───────────┘
///                                  │                               │
///                                  ▼                               ▼
///                                  └───────────────────────────────┘
///```
///
/// When auto-buffer mode is enabled:
/// 1. The MerklePainter uses the maximum download size for better throughput
/// 2. It keeps at least 4 additional requests running in read-ahead mode
/// 3. Read-ahead requests start from where the user request ended
/// 4. This significantly improves performance for sequential access patterns
///
/// * When the JVM is shutdown, an active merkle painter will do the following:
///   1. Stop or abandon any pending transfers.
///   2. Compute any merkle tree hashes which are calculable, meaning that there are two valid
/// merkle tree siblings and an invalid merkle tree parent. The bit set tracking should be used
/// to determine this. The computation should be breadth first.
///   3. The merkle tree data will be flushed to disk, delaying by a millisecond if needed to
/// ensure that the merkle tree file mtime is at least one millisecond newer than that of the
/// content described by it.
///
/// * At no time should there be concurrent requests for an overlapping region of a remote file.
/// * If a caller needs a block that is being requested already, it should block on the same
/// future from the original request to download that chunk.
/// * Scheduling of downloads should be atomically consistent, based on effective mutex or other
/// method synchronization where needed to ensure this.
///
/// * When a range of data is downloaded, each of the chunks in it needs to be verified against
/// the reference merkle tree before it is considered valid. This occurs in two steps:
/// 1) A hash for the downloaded content is computed
/// 2) The has is compared to the reference hash for the same chunk index
/// If the hashes match, then the content is persisted and then the merkle tree is updated.
/// if the hashes do not match, then the download for that chunk is considered failed and
/// reattempted up to a number of retries before another error is thrown to indicate a failed
/// download.
///
/// ## Merkle Painter Events
/// The events for merkle painter should be captured in MerklePainterEvent as enumerations which
/// support the {@link io.nosqlbench.vectordata.status.EventType} base type. They should be
/// justified to no more than 15 characters wide and include all uppercase symbolic names.
///
/// Since a merkle painter contains two different merkle trees, the reference tree and the active
///  tree for the downloading content, the bimg files need to be distinct. For this reason, the
/// bimg file names must be based on the full merkle tree file name, adding the bimg extension to
///  any extensions already present on the file.
package io.nosqlbench.vectordata.merkle;

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
