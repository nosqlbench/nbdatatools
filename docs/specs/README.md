# Vector Data Specifications

This directory contains the definitive specifications for the vector data ecosystem. These documents are intended to guide the implementation of compatible tools, libraries, and datasets across different programming languages and platforms.

## Core Specifications

The ecosystem is defined by four interacting specifications:

*   **[Catalog Specification](catalog.md)**: Defines how to discover and resolve named datasets from local and remote sources.
    *   *Key concepts:* `catalogs.yaml`, `catalog.json`, Discovery Logic.
*   **[Dataset Specification](dataset_yaml_v1.md)**: Defines the structure, configuration, and file layout of a vector dataset.
    *   *Key concepts:* `dataset.yaml`, Attributes, Profiles, Facets, Sugared Sources.
*   **[Merkle File Format](merkle_v1.md)**: Defines the binary format used for data integrity verification and incremental download tracking.
    *   *Key concepts:* `.mref` vs `.mrkl`, Hash Tree, BitSet state, Chunking.
*   **[Streaming & Caching](streaming_and_caching.md)**: Defines how remote data is lazily mirrored, verified, and cached locally.
    *   *Key concepts:* Sparse mirroring, On-demand read-through, Headroom checks, Cache configuration.
*   **[Data Access API](data_access_v1.md)**: Defines the logical API for consuming vector data in applications.
    *   *Key concepts:* `TestDataGroup`, `TestDataView`, `DatasetView<T>`, "Everything is a vector".

## Architecture Overview

```text
Discovery              Definition             Integrity              Consumption
[Catalog] ──────────▶ [Dataset] ───────────▶ [Merkle] ───────────▶ [Data Access API]
   │                      │                      │                        │
   ▼                      ▼                      ▼                        ▼
Finds "sift-128"       Parses attributes      Verifies chunks        Returns float[]
Resolves URL           & profiles             Tracks download        Handles caching
```

## Implementation Guide

To implement a client for this ecosystem:

1.  Start with the **[Catalog Specification](catalog.md)** to understand how to find data.
2.  Implement the **[Dataset Specification](dataset_yaml_v1.md)** parser to read configurations.
3.  Implement the **[Merkle File Format](merkle_v1.md)** reader to ensure robust data transfer.
4.  Expose the data via the **[Data Access API](data_access_v1.md)** patterns for a consistent user experience.
