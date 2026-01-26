# Streaming and Caching Specification v1

This document specifies the behavior of the streaming and caching system for remote vector datasets. This system enables transparent, high-performance access to remote data by leveraging local sparse caching and Merkle-based integrity verification.

> **Context:**
> *   **Data Source:** Mirrors datasets defined by the [Dataset Specification](dataset_yaml_v1.md).
> *   **Integrity:** Uses the [Merkle File Format](merkle_v1.md) for verification and state tracking.
> *   **Discovery:** Bootstraps from the [Catalog System](catalog.md).
> *   **Consumption:** Provides the backing implementation for the [Data Access API](data_access_v1.md).

## 1. Overview

The streaming system allows applications to treat remote datasets as if they were local. When a remote dataset is accessed, the system creates a local "mirror" that is populated lazily (on-demand) as the application reads data.

### 1.1 Sparse Mirroring
A cached dataset consists of three components on the local filesystem:
1.  **Sparse Data File**: A file with the same nominal size as the remote resource, but containing only the chunks that have been downloaded.
2.  **State File (`.mrkl`)**: A tracker that records which chunks have been successfully downloaded and verified.
3.  **Reference File (`.mref`)**: (Transient) The authoritative Merkle tree used to initialize the state file.

## 2. On-Demand Read-Through

The `MAFileChannel` (Merkle-Aware File Channel) is the core component responsible for the read-through logic.

### 2.1 The Read Protocol
When a read request is made for a byte range `[pos, pos + len)`:
1.  **Identify Chunks**: The system maps the byte range to one or more fixed-size chunks (see [Merkle Specification](merkle_v1.md#11-chunking-logic-automatic-sizing)).
2.  **Check Validity**: For each chunk, the system checks the local `.mrkl` file's validity BitSet.
3.  **Download & Verify**: If a chunk is missing (bit is `0`):
    *   The chunk is downloaded from the remote source (using HTTP Range requests).
    *   The downloaded data is hashed (SHA-256).
    *   The hash is compared against the authoritative hash stored in the `.mrkl` file.
    *   If valid, the data is written to the sparse data file, and the BitSet is updated to `1`.
4.  **Local Read**: Once all required chunks are valid locally, the read operation is satisfied from the local sparse data file.

### 2.2 Pre-buffering
Applications can proactively trigger downloads for specific ranges or the entire dataset using the `prebuffer()` API. This is recommended before benchmarks to eliminate network latency from the critical path.

## 3. Configuration

### 3.1 Cache Directory
The cache directory is where all downloaded dataset components are stored.

*   **Default Location**: `~/.cache/vectordata/`
*   **Structure**: Datasets are stored in subdirectories named after their resolved identity or a hash of their URL.
*   **Override**: The cache directory can be explicitly configured via the `setCacheDir()` method on a `ProfileSelector` or passed as an argument to `DatasetLoader.load()`.

### 3.2 Configuration Files
The system looks for configuration in the user's home directory:

*   **Config Directory**: `~/.config/vectordata/`
*   **`catalogs.yaml`**: (See [Catalog Specification](catalog.md)) Bootstraps the discovery of dataset sources.

## 4. Filesystem Headroom Checks

To prevent system instability, the system performs mandatory disk space checks before initiating any download.

### 4.1 Check Logic
Before a dataset mirror is initialized, the `FilesystemSpaceChecker` evaluates the target volume.

*   **Requirement**: The available space on the filesystem must be greater than the `totalContentSize` of the dataset plus a **safety margin**.
*   **Safety Margin**: The default margin is **20%** (`0.20`).
*   **Behavior**: If the required space (`size * 1.2`) is not available, the system throws an `InsufficientSpaceException` and refuses to initialize the mirror.

## 5. Implementation Requirements

1.  **Sparse File Support**: Implementations should use sparse files where supported by the OS to avoid allocating physical disk blocks for unread data.
2.  **Atomic State Updates**: Updates to the `.mrkl` BitSet should be persisted periodically or upon successful verification of a chunk to ensure crash resilience.
3.  **Concurrent Downloads**: The system should support multiple concurrent chunk downloads to maximize bandwidth utilization.
