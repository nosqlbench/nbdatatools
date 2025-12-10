# Dataset Layout Specification v1

This specification defines the canonical NBDataTools dataset layout. Each dataset is a directory containing a `dataset.yaml` manifest plus facet files (e.g., `.fvec`, `.ivec`, `.bvec`, `.parquet`).

## Directory Structure

```
dataset/
├── dataset.yaml
├── base.fvec
├── query.fvec
├── neighbors.ivec
└── distances.fvec
```

## Manifest (`dataset.yaml`)

### attributes section (required)
- `distance_function` – COSINE, EUCLIDEAN, etc.
- `license`, `vendor`, `model`, `url`, `notes` – descriptive metadata

### profiles section (required)
Each profile (`default`, `small`, etc.) may define:
- `base_vectors`, `query_vectors`, `neighbor_indices`, `neighbor_distances`
- Each facet entry contains:
  - `source` – relative path to the facet file (e.g., `base.fvec`)
  - `window` – optional range `[start, end)`
  - Additional fields such as `chunk_size`, `dimensions` if overrides are needed

### Example

```yaml
attributes:
  distance_function: COSINE
  license: Apache-2.0
  vendor: nosqlbench

profiles:
  default:
    base_vectors:
      source: base.fvec
      window: [0, 1_000_000)
    query_vectors:
      source: query.fvec
      window: [0, 10_000)
    neighbor_indices:
      source: neighbors.ivec
    neighbor_distances:
      source: distances.fvec
```

## Facet Files

- `.fvec` – float vectors (first int32 dimension followed by data)
- `.ivec` – integer vectors
- `.bvec` – byte vectors
- `.parquet` – columnar datasets

Writers must ensure all referenced files exist, share consistent dimensions, and are encoded little-endian.
