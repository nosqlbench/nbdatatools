# Catalog Specification v1

This document specifies the design and implementation of the dataset catalog system. The catalog system provides a mechanism for discovering, resolving, and accessing named vector datasets from multiple local and remote sources.

## 1. Overview

The catalog system aggregates dataset definitions from multiple sources into a single searchable index. Users can reference datasets by simple names (e.g., `sift-128`), and the system resolves these names to specific URLs or file paths, including their associated metadata and profiles.

> **See Also:**
> *   [Data Access API](data_access_v1.md): The catalog serves as the primary entry point for the Data Access API.
> *   [Dataset Specification](dataset_yaml_v1.md): Defines the structure of the datasets discovered by the catalog.

## 2. Catalog Discovery

The system discovers catalogs through a hierarchical configuration approach involving specific file names and directory structures.

### 2.1 Configuration Files (`catalogs.*` vs `catalog.*`)

The system distinguishes between a **list of catalogs** and a **catalog definition** based on the filename.

1.  **List of Catalogs** (`catalogs.yaml`, `catalogs.json`):
    *   Contains a list of strings (zero or more).
    *   Each string is a reference to a catalog location (URL or path).
    *   This is used to bootstrap the discovery process or aggregate multiple sources.

2.  **Catalog Definition** (`catalog.yaml`, `catalog.json`):
    *   Contains the actual catalog definition (a list of dataset entries).
    *   Describes datasets within its path hierarchy.

### 2.2 Directory Resolution

When a raw directory path is provided as a source, the system inspects its contents to determine its role:

1.  **Catalog Base Path**:
    *   If the directory contains `catalog.json` (or `catalog.yaml`), it is treated as a catalog source.
    *   The system loads the catalog definition from that file.

2.  **Dataset Base Path**:
    *   If the directory contains `dataset.yaml`, it is treated as a single dataset.
    *   (Future/Implicit support: The system may implicitly wrap this into a single-entry catalog).

3.  **Configuration Base Path**:
    *   If the directory contains `catalogs.yaml` (or `catalogs.json`), it is treated as a configuration source.
    *   The system reads the list of catalogs from this file and resolves them.

## 3. Catalog Data Format (`catalog.json`)

The `catalog.json` file is the primary data structure for dataset definitions. It consists of a JSON array of objects, where each object represents a dataset entry.

### 3.1 Dataset Entry Structure

A dataset entry can take two forms: a direct entry or a layout-embedded entry.

#### 3.1.1 Direct Entry
A direct entry contains all the fields necessary to define a `DatasetEntry`.

*   **`name`** (String): Unique identifier for the dataset.
*   **`url`** (String/URL): Base URL or path where the dataset files are located.
*   **`attributes`** (Map<String, String>): Metadata (e.g., `distance_function`, `model`).
*   **`profiles`** (Map<String, Object>): Profile definitions. The structure of each profile must conform to the [Dataset Specification](dataset_yaml_v1.md#22-profiles).
*   **`tags`** (Map<String, String>): Optional categorization tags.

#### 3.1.2 Layout-embedded Entry
This form allows embedding the content of a `dataset.yaml` file directly within the catalog.

*   **`name`** (String): Dataset name.
*   **`path`** (String): Relative path from the catalog location to the `dataset.yaml` or dataset directory.
*   **`layout`** (Object): An embedded dataset configuration object. The structure of this object must conform to the [Dataset Specification](dataset_yaml_v1.md#2-configuration-datasetyaml).
    *   `attributes`
    *   `profiles`
    *   `tags`

### 3.2 Example `catalog.json`
```json
[
  {
    "name": "sift-128",
    "url": "https://example.com/data/sift-128/",
    "attributes": {
      "distance_function": "L2",
      "dimensions": "128"
    },
    "profiles": {
      "default": {
        "base_vectors": "base.fvec",
        "query_vectors": "query.fvec"
      }
    }
  },
  {
    "name": "test-layout",
    "path": "testxvec/dataset.yaml",
    "layout": {
      "attributes": { "distance_function": "COSINE" },
      "profiles": {
        "default": { "base": "base.fvec" }
      }
    }
  }
]
```

## 4. Resolution Logic

When a user requests a dataset by name (e.g., `mvn datasets get sift-128`):

1.  **Aggregation**: The system loads all `catalog.json` files from all configured locations.
2.  **Indexing**: All entries are flattened into a single list.
3.  **Matching**:
    *   **Exact Match**: Case-insensitive comparison of the name.
    *   **Glob Match**: Supporting standard filesystem globbing (e.g., `sift-*`).
    *   **Regex Match**: Supporting regular expression patterns.
4.  **Conflict Resolution**: If multiple entries have the same name, the system should either prioritize based on source order or throw an error if ambiguous.

## 5. Implementation Notes

*   **HTTP Client**: Implementations should use a robust HTTP client supporting timeouts and standard headers.
*   **Caching**: To improve performance, implementations may cache downloaded `catalog.json` files or their aggregated index.
*   **Relative Paths**: Paths within a `catalog.json` (like the `url` or `path` fields) should be resolved relative to the location of the `catalog.json` file itself.
*   **Lazy Loading**: Catalogs should be loaded only when needed for discovery or resolution.
