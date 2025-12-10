# Dataset Reader Contract v1

Readers MUST:
- Locate `dataset.yaml` in the supplied directory
- Parse the `attributes` section for metadata (distance, license, etc.)
- Support the `profiles` map, defaulting to `default`
- Interpret facet entries as relative paths to data files and apply optional `window` ranges
- Validate that referenced files exist and dimensions align

Readers SHOULD:
- Respect optional profile overrides (chunk sizes, alternative facets)
- Support both local paths and remote URLs (HTTP range reads)
- Expose type-safe views such as base vectors, query vectors, neighbor indices/distances

Readers MAY:
- Implement caching/prebuffering
- Perform integrity checks (Merkle references, checksums) if available
