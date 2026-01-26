# Catalog Specification v1

This document specifies the design and implementation of the dataset catalog system. The catalog system provides a mechanism for discovering, resolving, and accessing named vector datasets from multiple local and remote sources.

## 1. Overview

The catalog system aggregates dataset definitions from multiple sources into a single searchable index. Users can reference datasets by simple names (e.g., `sift-128`), and the system resolves these names to specific URLs or file paths, including their associated metadata and profiles.

## 2. Catalog Discovery

The system discovers catalogs through a hierarchical configuration approach.

### 2.1 Configuration File (`catalogs.yaml`)

The primary way to configure catalog sources is through a `catalogs.yaml` file. This file contains a YAML list of strings, where each string represents a catalog location.

**Search Locations:**
1.  **Default Config Directory**: `~/.config/vectordata/catalogs.yaml`
2.  **Explicit Config Directory**: Provided via API or command-line arguments.

**Example `catalogs.yaml`:**
```yaml
- https://example.com/datasets/catalog.json
- ~/my-datasets/
- /opt/data/shared-catalog.yaml
```

### 2.2 Location Resolution

A catalog location string can be resolved in several ways:

1.  **Remote URL**: If it starts with `http://` or `https://`, it is treated as a remote source.
2.  **Local Directory**: If it is a directory path:
    *   If it contains `catalogs.yaml`, it is treated as a config directory (recursive discovery).
    *   If it contains `catalog.json`, it is treated as a catalog source.
3.  **Local File**:
    *   If it is a `.yaml` file, it is parsed as a list of further catalog locations.
    *   If it is a `.json` file, it is parsed as a `catalog.json`.

## 3. Catalog Data Format (`catalog.json`)

The `catalog.json` file is the primary data structure for dataset definitions. It consists of a JSON array of objects, where each object represents a dataset entry.

### 3.1 Dataset Entry Structure

A dataset entry can take two forms: a direct entry or a layout-embedded entry.

#### 3.1.1 Direct Entry
A direct entry contains all the fields necessary to define a `DatasetEntry`.

*   **`name`** (String): Unique identifier for the dataset.
*   **`url`** (String/URL): Base URL or path where the dataset files are located.
*   **`attributes`** (Map<String, String>): Metadata (e.g., `distance_function`, `model`).
*   **`profiles`** (Map<String, Object>): Profile definitions (matches the `profiles` section in `dataset.yaml`).
*   **`tags`** (Map<String, String>): Optional categorization tags.

#### 3.1.2 Layout-embedded Entry
This form allows embedding the content of a `dataset.yaml` file directly within the catalog.

*   **`name`** (String): Dataset name.
*   **`path`** (String): Relative path from the catalog location to the `dataset.yaml` or dataset directory.
*   **`layout`** (Object): Contains the standard `dataset.yaml` structure:
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
