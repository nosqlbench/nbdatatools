# Dataset Specification v1

This document specifies the layout, configuration, and data formats for vector datasets used in this project. It is intended to be a definitive guide for implementing readers and writers in any programming language.

## 1. Directory Layout

A dataset is a collection of files, usually contained within a single directory or accessible via a common base URL.

The entry point for a dataset is a file named `dataset.yaml` (or `dataset.yml`). This file contains metadata and configuration for accessing the vector data.

> **See Also:**
> *   [Data Access API](data_access_v1.md): Specification for the logical API to consume these datasets.
> *   [Merkle File Format](merkle_v1.md): Specification for integrity verification files (`.mref`, `.mrkl`).

**Example Structure:**
```
my-dataset/
├── dataset.yaml
├── base.fvec
├── query.fvec
├── indices.ivec
└── distances.fvec
```

## 2. Configuration (`dataset.yaml`)

The `dataset.yaml` file uses the YAML format. It has two top-level sections: `attributes` and `profiles`.

### 2.1 Attributes

The `attributes` section is a key-value map containing metadata about the dataset.

*   **`distance_function`** (Required): The distance metric used for ground truth. Common values: `COSINE`, `EUCLIDEAN`, `DOT_PRODUCT`.
*   **`url`**: Canonical URL for the dataset.
*   **`license`**: License information.
*   **`vendor`**: Dataset provider.
*   **`model`**: Name of the model used to generate embeddings.
*   *Other arbitrary keys are permitted.*

### 2.2 Profiles

The `profiles` section defines one or more views of the dataset. A profile maps logical data facets (like "base vectors" or "ground truth") to physical sources and configurations.

*   **`default`** (Recommended): The standard profile used when no specific profile is requested. Other profiles inherit configuration from `default`.

#### Profile Facets

A profile can define the following facets:

1.  **`base_vectors`**: The large set of vectors to be indexed/searched.
2.  **`query_vectors`**: The set of vectors used to query the index.
3.  **`neighbor_indices`**: Ground truth integer indices of nearest neighbors.
4.  **`neighbor_distances`**: Ground truth float distances of nearest neighbors.

#### Facet Configuration

A facet can be configured in several formats, ranging from simple strings to detailed objects.

**1. Simple Filename String**
Specifies the filename relative to the `dataset.yaml` location.
```yaml
base_vectors: base.fvec
```

**2. Sugared String (with Window/Type)**
Specifies the source and an optional window or type suffix using bracket or colon notation.
*   **Window suffixes**: `[start..end]`, `(start..end)`, `[count]`, `:count`.
*   **Examples**:
    *   `base.fvec[0..1000]` - Slices the first 1000 vectors.
    *   `model.json[1000000]` - Generates 1 million vectors from a model.

**3. Detailed Object**
Specifies the source file and explicit parameters.
```yaml
base_vectors:
  source: base.fvec
  window: 0..1000  # Limits the view to a specific range [start..end]
  type: xvec       # Explicit source type (xvec or virtdata)
```

**Inheritance:**
If a specific profile omits a facet, it inherits the configuration from the `default` profile.

### 2.3 Source Types and Inference

There are two primary types of data sources:

1.  **`xvec`** (Default): Reads from binary `.fvec` or `.ivec` files.
2.  **`virtdata`**: Generates vectors on-the-fly using a model JSON file.

**Type Inference Rules:**
If the type is not explicitly provided in the detailed object:
*   Paths ending in `.json` are inferred as `virtdata`.
*   All other paths default to `xvec`.

## 3. Data Formats

### 3.1 The `xvec` Family of Formats

The project adopts the `xvec` family of binary formats (derived from formats common in FAISS and Texmex) for storing dense vector data.

**Key Characteristics:**
1.  **Dense Binary:** Data is packed tightly without compression or encoding overhead.
2.  **Self-Describing Records:** Each record typically starts with a header (dimension/length) followed by the data.
3.  **Little Endian:** All multi-byte values (integers, floats) are stored in **Little Endian** byte order.
4.  **Uniform Dimensionality:** While the generic format allows varying dimensions per record, this project enforces a **Strict Uniformity Constraint**.

#### Uniform Dimensionality Constraint
All records in a single file **must** have the same dimension (or length). This allows for O(1) random access by calculating offsets.

*   **Record Size Calculation:** `RecordSize = HeaderSize + (Dimension * ComponentSize)`
*   **Total Records:** `FileSize / RecordSize`
*   **Offset for Index `i`:** `i * RecordSize`

#### Supported Formats

| Extension | Component Type | Component Size | Header | Description |
| :--- | :--- | :--- | :--- | :--- |
| `.fvec` / `.fvecs` | `float32` | 4 bytes | `int32` (Dimension) | Standard float vectors. |
| `.ivec` / `.ivecs` | `int32` | 4 bytes | `int32` (Length) | Integer arrays (e.g., indices). |
| `.bvec` / `.bvecs` | `uint8` | 1 byte | `int32` (Dimension) | Byte/Binary vectors. |

#### Detailed Record Layouts

**1. Float Vectors (`.fvec`)**
Used for base vectors, query vectors, and neighbor distances.
*   **Header:** `dimension` (int32, 4 bytes)
*   **Payload:** `N` components of `float32` (4 bytes each)
*   **Stride:** `4 + (N * 4)` bytes

**2. Integer Vectors (`.ivec`)**
Used for ground truth neighbor indices.
*   **Header:** `length` (int32, 4 bytes)
*   **Payload:** `K` components of `int32` (4 bytes each)
*   **Stride:** `4 + (K * 4)` bytes

**3. Byte Vectors (`.bvec`)**
Used for quantized or binary vectors.
*   **Header:** `dimension` (int32, 4 bytes)
*   **Payload:** `N` components of `uint8` (1 byte each)
*   **Stride:** `4 + (N * 1)` bytes

### 3.2 `virtdata` Model Format
Uses a JSON configuration file describing a vector space model (e.g., distribution parameters per dimension). Implementation of the generator itself is language-specific but must be deterministic based on the vector index.

## 4. Resource Resolution

The resolution of relative paths depends on the source context. The [Streaming & Caching System](streaming_and_caching.md) handles the complexity of remote access transparently.

1.  **Local Filesystem:** Relative paths are resolved against the directory containing `dataset.yaml`.
2.  **Remote (HTTP/HTTPS):** Relative paths are resolved against the base URL of the `dataset.yaml`. Readers must use HTTP Range requests for `xvec` formats to support random access without full downloads.

## 5. Implementation Recommendations

*   **Memory Mapping:** Use `mmap` for local files.
*   **Lazy Loading:** Open facet files only when accessed.
*   **Validation:** Verify `dimension`/`length` consistency across records.