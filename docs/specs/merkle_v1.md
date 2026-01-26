# Merkle File Format Specification v1

This document specifies the binary format and logic for the Merkle Tree system used to verify vector data integrity. This specification covers the persistent file format (`.mrkl` / `.mref`), tree construction algorithm, chunking logic, and hash computation.

## 1. Tree Geometry & Algorithms

The system uses a **binary Merkle Tree** constructed bottom-up from data chunks.

> **Context:** These files are used to verify the integrity of vector data files defined in the [Dataset Specification](dataset_yaml_v1.md). The [Data Access API](data_access_v1.md) and [Streaming & Caching System](streaming_and_caching.md) use them to manage state during downloads.

### 1.1 Chunking Logic (Automatic Sizing)

The content is divided into chunks based on a `chunkSize` calculated from the `totalContentSize`.

**Algorithm:**
1.  **Minimum Chunk Size:** 1MB (`1048576` bytes).
2.  **Maximum Chunk Size:** 64MB (`67108864` bytes).
3.  **Target Chunk Count:** Max 4096.

**Logic:**
```pseudo
if contentSize < 1024 bytes:
    chunkSize = 64 bytes
elif contentSize < 1MB:
    chunkSize = next_power_of_2(>= contentSize) (starts at 1KB)
else:
    chunkSize = 1MB
    while (ceil(contentSize / chunkSize) > 4096) AND (chunkSize < 64MB):
        chunkSize *= 2
```

**Chunk Boundaries:**
*   `chunkCount` = `ceil(totalContentSize / chunkSize)`
*   Chunk `i` starts at: `i * chunkSize`
*   Chunk `i` ends at: `min((i + 1) * chunkSize, totalContentSize)`
*   The last chunk may be smaller than `chunkSize`.

### 1.2 Tree Construction

The tree is a complete binary tree, padded to a power-of-2 leaf capacity.

**Dimensions:**
*   `totalChunks` (or `leafCount`): Actual number of data chunks.
*   `capLeaf`: Next power of 2 >= `totalChunks`.
*   `nodeCount`: Total nodes in the array = `2 * capLeaf - 1`.
*   `offset`: Index where leaf nodes begin = `capLeaf - 1`.
*   `internalNodeCount`: Number of internal nodes = `offset`.

**Node Indexing (Flat Array):**
The tree is stored as a flat array of SHA-256 hashes (32 bytes each).
*   **Indices `0` to `offset - 1`**: Internal Nodes.
*   **Indices `offset` to `offset + capLeaf - 1`**: Leaf Nodes.
    *   `offset + 0`: Hash of Chunk 0.
    *   `offset + totalChunks - 1`: Hash of Last Chunk.
    *   `offset + totalChunks` to `nodeCount - 1`: Padding (Zero Hashes).

**Relationships:**
For a node at index `i`:
*   **Left Child:** `2 * i + 1`
*   **Right Child:** `2 * i + 2`
*   **Parent:** `(i - 1) / 2` (integer division)

### 1.3 Hash Computation

**Algorithm:** SHA-256

**Leaf Nodes (Indices `offset` to `offset + totalChunks - 1`):**
*   `Hash(i) = SHA-256(Data of Chunk(i - offset))`
*   *Note:* Only the actual bytes of the chunk are hashed.

**Padded Leaf Nodes (Indices `offset + totalChunks` to `nodeCount - 1`):**
*   `Hash(i) = SHA-256(Zero Bytes)` (Usually 32 bytes of zeros, or implementation specific zero hash).
*   *Correction:* In `MerkleDataImpl.java`, padding leaves are initialized with a zero-filled byte array (32 bytes).

**Internal Nodes (Indices `offset - 1` down to `0`):**
*   Iterate `i` from `offset - 1` down to `0`.
*   `LeftHash = Hash(2 * i + 1)`
*   `RightHash = Hash(2 * i + 2)`
    *   If `RightChildIndex >= nodeCount` (should not happen in this padded layout), use `LeftHash`.
*   `Hash(i) = SHA-256(LeftHash || RightHash)` (Concatenation of 32+32 bytes).

---

## 2. File Format Layout

The file consists of three contiguous regions. All multi-byte integers are **Big Endian**.

### 2.1 Region 1: Hash Data
Contains the complete tree array.

*   **Offset:** `0`
*   **Length:** `nodeCount * 32` bytes.
*   **Content:** Sequence of 32-byte SHA-256 hashes, ordered by node index `0` to `nodeCount - 1`.

### 2.2 Region 2: Validity BitSet
Tracks which chunks (leaves) have been verified.

*   **Offset:** `nodeCount * 32`
*   **Length:** `ceil(leafCount / 8)` bytes.
*   **Content:** A bitmask where bit `k` corresponds to Chunk `k`.
*   **Bit Ordering:** Standard byte array representation of a BitSet.
    *   Byte 0 contains bits 0-7.
    *   Bit 0 is the *least* significant bit of Byte 0?
    *   *Java `BitSet.toByteArray()`:* Little-endian byte order for words, but bytes are emitted ls-byte first. Inside a byte, bit 0 is LSB.
    *   **Verification:** `BitSet.valueOf(bytes).get(index)`.

### 2.3 Region 3: Footer (Metadata)
Fixed-size 53-byte footer.

*   **Offset:** `nodeCount * 32 + bitSetSize`
*   **Length:** 53 bytes.

**Structure:**

| Offset (Local) | Field | Type | Size | Description |
| :--- | :--- | :--- | :--- | :--- |
| 0 | `chunkSize` | `long` | 8 | Chunk size in bytes. |
| 8 | `totalContentSize` | `long` | 8 | Total content size in bytes. |
| 16 | `totalChunks` | `int` | 4 | Actual chunk count. |
| 20 | `leafCount` | `int` | 4 | Same as `totalChunks`. |
| 24 | `capLeaf` | `int` | 4 | Leaf capacity (power of 2). |
| 28 | `nodeCount` | `int` | 4 | Total nodes (`2*capLeaf - 1`). |
| 32 | `offset` | `int` | 4 | Index of first leaf node. |
| 36 | `internalNodeCount` | `int` | 4 | Number of internal nodes. |
| 40 | `bitSetSize` | `int` | 4 | Size of BitSet region in bytes. |
| 44 | `footerLength` | `byte` | 1 | Fixed value: 53. |

## 3. Reference vs. State Files (.mref vs .mrkl)

This specification defines a single file structure that serves two complementary purposes. This duality is central to the design: it unifies the concept of "Data Identity" (what the data *is*) with "Data Presence" (what data *I have*).

### 3.1 The Concept of Duality

In many systems, checksums (like MD5 or SHA-256 files) are separate from download state trackers (like `.part` files). The Merkle File Format unifies these.

*   **Identity (Reference):** The tree structure and hashes define the identity of the dataset. If you have the full tree, you know exactly what the data *should* be.
*   **Presence (State):** The BitSet defines the presence of the data. If bit `i` is set, it asserts "I have chunk `i` and it matches the identity defined in the tree."

This means a fully downloaded and verified state file (`.mrkl`) is structurally identical to a reference file (`.mref`), except potentially for the filename.

### 3.2 File Roles

#### The Reference File (`.mref`)
*   **Role:** The "Platonic Ideal" of the dataset.
*   **Source:** Generated by the data publisher.
*   **BitSet Semantics:** The BitSet in a reference file is effectively "all 1s". It asserts that the publisher possesses the valid data.
*   **Immutability:** This file is read-only. It is the source of truth.

#### The State File (`.mrkl`)
*   **Role:** The "Local Reality" of the dataset.
*   **Source:** Created by the client at the start of a download.
*   **BitSet Semantics:** The BitSet starts as "all 0s". It tracks the incremental acquisition of valid chunks.
*   **Mutability:** This file is read-write. As chunks are downloaded and verified against the hashes (copied from the `.mref`), bits are flipped to 1.

### 3.3 Implementation Strategy for Developers

Implementors can leverage this duality to simplify their code. You do not need separate classes for "Verifier" and "Tracker".

**Unified Data Structure:**
Define a single `MerkleTree` class that maps the file.
```java
class MerkleTree {
    Hash[] hashes;
    BitSet validBits;
    Footer metadata;
}
```

**The "Download & Verify" Workflow:**
1.  **Acquire Reference:** Download the `.mref` file (small) from the source.
2.  **Initialize State:** Copy the `.mref` file to a local `.mrkl` file.
    *   *Critical Step:* Zero out the BitSet region in the `.mrkl` file.
    *   Now you have a local tree with all the correct hashes, but "knowing" you have no data yet.
3.  **Download Loop:**
    *   Identify a missing chunk (find a `0` bit in `.mrkl`).
    *   Download the chunk data.
    *   **Verify:** Hash the downloaded data and compare it to the hash at `offset + i` in your `.mrkl` file.
    *   **Update:** If they match, set bit `i` to `1` and flush to disk.
4.  **Resume:** If the process dies, simply reload the `.mrkl` file. The BitSet tells you exactly what is left to do.

**Advantages:**
*   **Zero-Cost Verification lookup:** The expected hash is already in your state file; no need to look up a separate reference.
*   **Crash Resilience:** The state is persisted atomically (or near-atomically).
*   **Verification is local:** You verify against your *copy* of the truth.

### 3.4 Summary Table

| Feature | Reference (`.mref`) | State (`.mrkl`) |
| :--- | :--- | :--- |
| **Primary Actor** | Publisher | Consumer |
| **Write Mode** | Write-Once (Generation) | Random Access (Update) |
| **BitSet Initial State**| All 1s (Valid) | All 0s (Empty) |
| **Hashes** | Generated from Data | Copied from Reference |
| **Logic** | "This is the data." | "I have verified this part." |

---

## 4. Concrete Example

**Scenario:** Content Size = 100 bytes.
*   **chunkSize:** 64 bytes (Minimum for < 1KB is 64).
*   **totalChunks:** ceil(100/64) = 2.
    *   Chunk 0: 0-64 (64 bytes)
    *   Chunk 1: 64-100 (36 bytes)
*   **capLeaf:** Next power of 2 >= 2 is 2.
*   **nodeCount:** 2 * 2 - 1 = 3.
*   **offset:** 2 - 1 = 1.

**Tree Indices:**
*   Index 0: Root (Internal)
*   Index 1: Leaf 0 (Chunk 0)
*   Index 2: Leaf 1 (Chunk 1)

**File Layout:**
*   **0-31**: Hash(Root)
*   **32-63**: Hash(Chunk 0)
*   **64-95**: Hash(Chunk 1)
*   **96**: BitSet (1 byte, need bits 0 and 1).
    *   If both valid: `00000011` (binary) -> `0x03`.
*   **97-149**: Footer (53 bytes).

## 4. Usage

### 4.1 Verification (Read-Only `.mref`)
1.  Map the file.
2.  Read footer to get geometry.
3.  To verify Chunk `i`:
    *   Hash the data from source: `H = SHA-256(data)`.
    *   Read expected hash from file offset: `(offset + i) * 32`.
    *   Compare.

### 4.2 State Tracking (`.mrkl`)
1.  Open/Map the file.
2.  If creating from scratch, initialize hash region and footer from `.mref`, clear BitSet region.
3.  When Chunk `i` is validated:
    *   Set bit `i` in the BitSet region.
    *   `ByteIndex = i / 8`
    *   `BitIndex = i % 8`
    *   Read byte, OR with `(1 << BitIndex)`, Write byte.
    *   Sync/Force changes to disk.