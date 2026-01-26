# Data Access API Specification v1

This document specifies the logical API for accessing vector data. It bridges the gap between the persistent file formats (`.fvec`, `.mrkl`) and the application-level consumption of vectors.

## 1. Overview

The Data Access API abstracts away the complexity of file formats, memory mapping, and network IO. It provides a uniform, high-level interface where **"everything is a vector"**.

> **See Also:**
> *   [Dataset Specification](dataset_yaml_v1.md): Defines the physical layout and configuration of the data.
> *   [Catalog Specification](catalog.md): Defines how datasets are discovered and resolved.
> *   [Merkle File Format](merkle_v1.md): Defines the integrity verification mechanism used during pre-buffering.

### 1.1 Core Philosophy
*   **Uniformity:** Whether reading `float[]` embeddings or `int[]` neighbor indices, the access pattern is identical: `get(index)`.
*   **Profile-Centric:** Data is always accessed through a **Profile**, which groups related datasets (e.g., "base vectors", "queries") into a coherent view.
*   **Lazy & Efficient:** Resources are opened only when requested. Random access is optimized via memory mapping. Bulk access is optimized via chunked reads.

## 2. API Hierarchy

The API is structured hierarchically to guide the user from a broad collection of data down to specific vector elements.

```text
TestDataGroup (Dataset)
  │
  └── profile("name") ──▶ TestDataView (Profile)
                            │
                            ├── getBaseVectors()      ──▶ DatasetView<float[]>
                            ├── getQueryVectors()     ──▶ DatasetView<float[]>
                            ├── getNeighborIndices()  ──▶ DatasetView<int[]>
                            └── getNeighborDistances() ──▶ DatasetView<float[]>
                                                          │
                                                          └── get(i) ──▶ Vector
```

### 2.1 `TestDataGroup` (The Dataset)
Represents the root of a dataset (e.g., `sift-128`).
*   **Role:** Resolves the `dataset.yaml` and manages the lifecycle of profiles.
*   **Key Operation:** `profile(String name)` -> returns a `TestDataView`.

### 2.2 `TestDataView` (The Profile)
Represents a specific "view" or "profile" of the dataset (e.g., "default", "small").
*   **Role:** Provides accessors for the semantic facets of the dataset.
*   **Key Operations:**
    *   `getBaseVectors()` -> `Optional<DatasetView<float[]>>`
    *   `getQueryVectors()` -> `Optional<DatasetView<float[]>>`
    *   `getNeighborIndices()` -> `Optional<DatasetView<int[]>>`
    *   `getNeighborDistances()` -> `Optional<DatasetView<float[]>>`
*   **Metadata:** Access to `getDimension()`, `getDistanceFunction()`, `getLicense()`.

### 2.3 `DatasetView<T>` (The Data)
Represents a sequence of vectors of a specific type `T` (e.g., `float[]`, `int[]`).
*   **Role:** The primary data access interface. This is implemented by `CoreXVecDatasetViewMethods`.

---

## 3. DatasetView Specification (`CoreXVecDatasetViewMethods`)

This section details the requirements for the `DatasetView` interface, specifically as implemented for xvec formats.

### 3.1 Type System ("Everything is a Vector")
The API treats all data as vectors. The generic type `T` defines the component structure.
*   **.fvec**: `T = float[]`
*   **.ivec**: `T = int[]`
*   **.bvec**: `T = byte[]`

**Requirement:** Implementations must determine the runtime type `T` from the source (file extension) and expose it via `getDataType()`.

### 3.2 Random Access
*   **Method:** `T get(long index)`
*   **Behavior:** Returns the vector at the specified 0-based ordinal.
*   **Performance:** Must support O(1) random access. Implementation should calculate the byte offset (`header + index * stride`) and read directly.
*   **Safety:** Must throw bounds exceptions if `index` is out of range or if the file is truncated.

### 3.3 Chunked Access (Bulk IO)
*   **Method:** `T[] getRange(long startInclusive, long endExclusive)`
*   **Behavior:** Returns an array of vectors.
*   **Motivation:** Reducing IO syscall overhead when processing batches of vectors (e.g., for bulk indexing or throughput testing).
*   **Implementation:** Should utilize bulk read capabilities of the underlying channel (e.g., reading 1MB chunks into a buffer) rather than iterating `get()` calls.

### 3.4 Asynchronous Access
*   **Methods:** `getAsync(index)`, `getRangeAsync(start, end)`
*   **Return:** `Future<T>` / `CompletableFuture<T>`
*   **Ergonomics:** Allows UI threads or event loops to request data without blocking.
*   **Implementation:** Can be a simple wrapper around synchronous calls if the underlying IO is fast (mmap), or a true async dispatcher.

### 3.5 Pre-buffering
*   **Method:** `prebuffer()`
*   **Behavior:** Triggers an asynchronous download/cache-fill of the underlying data.
*   **Mechanism:** Leverages the [Streaming & Caching System](streaming_and_caching.md) to download missing chunks and verify them against the Merkle state.
*   **Usage:** Users call this to ensure high-performance access during a benchmark.
*   **Contract:** The returned `Future` completes only when the data is locally available and valid.

---

## 4. IO Requirements

### 4.1 Memory Mapping
Implementations should leverage memory-mapped IO (e.g., Java's `FileChannel.map` or Rust's `mmap`) for local files.
*   **Why:** Allows the OS to manage page caching.
*   **Benefit:** "Zero-copy" access patterns where the vector object wraps raw memory bytes.

### 4.2 Thread Safety
*   **Requirement:** All `get` and `getRange` methods must be thread-safe.
*   **Concurrency:** Multiple threads must be able to read different (or same) vectors simultaneously without locking contention. Absolute file positioning (`pread`) is preferred over seek-then-read.

### 4.3 Validation
*   **Dimension Check:** On open, the implementation must read the dimension header from the first record.
*   **Consistency:** For every read operation, the dimension header of the target record *must* be verified against the expected file dimension to detect corruption or file misalignment.

---

## 5. Usage Example (Conceptual)

```java
// 1. Select Dataset and Profile
TestDataGroup group = TestDataGroup.load("sift-128");
TestDataView profile = group.profile("default");

// 2. Get a view of the base vectors
DatasetView<float[]> base = profile.getBaseVectors().orElseThrow();

// 3. Random Access
float[] vector = base.get(42);

// 4. Chunked Access
float[][] batch = base.getRange(0, 100);

// 5. Metadata
int dim = base.getVectorDimensions(); // 128
```
