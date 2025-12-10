# Data Formats

NBDataTools supports multiple vector data formats for input and output. This chapter details each format, its characteristics, and when to use it.

## Format Overview

| Format | Extension | Type | Metadata | Random Access | Compression | Use Case |
|--------|-----------|------|----------|---------------|-------------|----------|
| **Dataset dir** | dataset.yaml + facets | Mixed | Yes | Yes | With compression | Canonical layout |
| **fvec** | .fvec | Binary | No | No | No | Float vectors |
| **ivec** | .ivec | Binary | No | No | No | Integer vectors |
| **bvec** | .bvec | Binary | No | No | No | Byte vectors |
| **Parquet** | .parquet | Binary | Yes | Yes | Yes | Columnar data |
| **JSON** | .json | Text | Yes | Yes | No | Configuration |

## Dataset Directory Layout (Recommended)

### Overview

NBDataTools now treats dataset directories as the canonical layout:
- Metadata (attributes, profiles, windows) in `dataset.yaml`
- Facets stored as standard vector files (`.fvec/.ivec/.bvec/.parquet`)
- Works across local filesystems and remote HTTP range requests

### Structure

```
dataset/
├── dataset.yaml              # Attributes, profiles, windows
├── base.fvec                 # Base vectors (n × d)
├── query.fvec                # Query vectors (m × d)
├── neighbors.ivec            # Ground-truth indices (m × k)
└── distances.fvec            # Ground-truth distances (m × k)
```

### dataset.yaml Example

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

### Working with Dataset Directories

Inspect structure and metadata:
```bash
java -jar nbvectors.jar analyze describe datasets/mteb-lite
```

List views/profiles:
```bash
java -jar nbvectors.jar vectordata views datasets/mteb-lite
```

Prebuffer a profile for streaming access:
```bash
java -jar nbvectors.jar datasets prebuffer datasets/mteb-lite --profile default
```

```## Binary Vector Formats

### fvec Format (Float Vectors)

**Structure**: Simple binary format for 32-bit float vectors.

```
[4 bytes: dimensions as int32]
[d * 4 bytes: vector 1 as float32 array]
[d * 4 bytes: vector 2 as float32 array]
...
```

**Reading example**:
```python
import numpy as np

def read_fvec(filename):
    return np.fromfile(filename, dtype=np.float32).reshape(-1, d+1)[:, 1:]
```

**Use cases**:
- Dense float embeddings
- Neural network outputs
- Scientific data

### ivec Format (Integer Vectors)

**Structure**: Binary format for 32-bit integer vectors.

```
[4 bytes: dimensions as int32]
[d * 4 bytes: vector 1 as int32 array]
[d * 4 bytes: vector 2 as int32 array]
...
```

**Common uses**:
- Neighbor indices (ground truth)
- Categorical features
- Discrete data

### bvec Format (Byte Vectors)

**Structure**: Binary format for 8-bit unsigned integers.

```
[4 bytes: dimensions as int32]
[d bytes: vector 1 as uint8 array]
[d bytes: vector 2 as uint8 array]
...
```

**Use cases**:
- Compact binary features
- Quantized vectors
- Memory-efficient storage

### Converting Binary Formats

Convert between binary formats:
```bash
# Float vectors to CSV
java -jar nbvectors.jar convert file \
  --input vectors.fvec \
  --output vectors.csv

# Integer vectors to JSON
java -jar nbvectors.jar convert file \
  --input indices.ivec \
  --output indices.json

# Byte vectors to float vectors
java -jar nbvectors.jar convert file \
  --input features.bvec \
  --output features.fvec
```

## Parquet Format

### Overview

Apache Parquet is a columnar storage format that provides:
- Efficient compression
- Schema evolution
- Nested data structures
- Wide tool support

### Structure

Typical schema for vector data:
```
root
 |-- id: integer (nullable = false)
 |-- vector: array (nullable = false)
 |    |-- element: float (containsNull = false)
 |-- metadata: struct (nullable = true)
 |    |-- source: string
 |    |-- timestamp: timestamp
```

### Extracting Columns

```bash
java -jar nbvectors.jar convert file \
  --input vectors.parquet \
  --output vectors.fvec \
  --parquet-column vector \
  --parquet-id-column id
```

### Advantages

- **Compression**: Often 50-80% smaller than raw formats
- **Compatibility**: Works with Spark, Pandas, Arrow
- **Partitioning**: Natural data organization
- **Schema**: Self-documenting structure

## JSON Format

### Overview

JSON is used for:
- Configuration files
- Metadata export
- Small datasets
- Human-readable summaries

### Dataset Specification Format

```json
{
  "version": "1.0",
  "name": "my_dataset",
  "distance": "euclidean",
  "base": {
    "source": "s3://bucket/base_vectors.fvec",
    "count": 1000000,
    "dimensions": 128
  },
  "query": {
    "source": "s3://bucket/query_vectors.fvec",
    "count": 10000,
    "dimensions": 128
  },
  "ground_truth": {
    "neighbors": "s3://bucket/neighbors.ivec",
    "distances": "s3://bucket/distances.fvec",
    "k": 100
  },
  "metadata": {
    "created": "2024-01-15T10:30:00Z",
    "license": "CC-BY-4.0",
    "vendor": "Example Corp"
  }
}
```

### Editing Metadata

Adjust dataset attributes or profiles directly in `dataset.yaml`. Common updates include:
- Adding/removing profiles for different corpus sizes
- Changing `distance_function` or annotations such as `license`
- Updating facet paths when moving data between directories

After editing, re-run `analyze describe <dataset-dir>` to confirm the manifest is still valid.

### Exporting Summaries

You can capture lightweight metadata snapshots in JSON by running:
```bash
java -jar nbvectors.jar analyze describe datasets/mteb-lite --format json > summary.json
```

Use this when sharing dataset characteristics without distributing the full vectors.

### Programmatic Updates

Automate manifest changes with your language of choice—for example, in Python:
```python
import yaml
from pathlib import Path

data = yaml.safe_load(Path('dataset/dataset.yaml').read_text())
data['attributes']['notes'] = 'Verified 2024-03-10'
Path('dataset/dataset.yaml').write_text(yaml.safe_dump(data))
```

## Format Selection Guide

### When to Use Each Format

**Use dataset directories when you need:**
- ✅ Complete test datasets with metadata
- ✅ Random/random access via profiles and windows
- ✅ HTTP-friendly range reads
- ✅ Easy manual inspection/version control

**Use fvec/ivec/bvec when:**
- ✅ Working with existing tools that expect these formats
- ✅ Simple sequential processing
- ✅ Minimal overhead is critical
- ✅ Converting from legacy systems

**Use Parquet when:**
- ✅ Integrating with big data tools (Spark, etc.)
- ✅ Need columnar compression
- ✅ Working with structured/nested data
- ✅ Schema evolution is important

**Use JSON when:**
- ✅ Human readability is priority
- ✅ Configuration or metadata only
- ✅ Small datasets (< 1MB)
- ✅ Web API integration

## Format Conversion Matrix

| From ↓ To → | Dataset dir | fvec | ivec | bvec | Parquet | JSON |
|-------------|-------------|------|------|------|---------|------|
| **Dataset dir** | - | ✅ (facets) | ✅ (facets) | ✅ (facets) | ✅ | ✅ |
| **fvec** | ✅ | - | ❌ | ❌ | ✅ | ✅* |
| **ivec** | ✅ | ❌ | - | ❌ | ✅ | ✅* |
| **bvec** | ✅ | ❌ | ❌ | - | ✅ | ✅* |
| **Parquet** | ✅ | ✅ | ✅ | ✅ | - | ✅ |
| **JSON** | ✅** | ✅** | ✅** | ✅** | ✅** | - |

*Small datasets only  
**Metadata summaries only

## Performance Characteristics

### Read Performance

| Format | Sequential Read | Random Read | Memory Usage |
|--------|----------------|-------------|--------------|
| Dataset dir | Fast | Fast | Medium |
| fvec | Very Fast | Slow | Low |
| ivec | Very Fast | Slow | Low |
| bvec | Very Fast | Slow | Very Low |
| Parquet | Medium | Medium | Medium |
| JSON | Slow | Slow | High |

### Write Performance

| Format | Write Speed | Compression | Append Support |
|--------|-------------|-------------|----------------|
| Dataset dir | Fast (per facet) | Yes (per file) | Yes |
| fvec | Very Fast | No | Yes |
| ivec | Very Fast | No | Yes |
| bvec | Very Fast | No | Yes |
| Parquet | Medium | Yes | No |
| JSON | Slow | No | No |

## Working with Large Files

### Chunked Processing

For files larger than memory:

Use prebuffering when datasets need to be in place before a performance oriented test. Dynamic buffering from a remote source should be avoided for performance testing.
```bash
java -jar nbvectors.jar datasets prebuffer datasets/mteb-lite --profile default
```

### Streaming Conversion

Convert large files without loading fully:

```bash
java -jar nbvectors.jar convert file \
  --input huge_vectors.fvec \
  --output huge_vectors.csv \
  --parallel 8
```

## Format Validation

### Verify File Integrity

Check format consistency:
```bash
# Verify dataset.yaml manifest
java -jar nbvectors.jar analyze describe datasets/mteb-lite

# Check binary format dimensions
java -jar nbvectors.jar analyze count_zeros --file vectors.fvec
```

### Validate Against Schema

For dataset directories, validate by running:
```bash
java -jar nbvectors.jar vectordata views datasets/mteb-lite
```

## Best Practices

### 1. Choose the Right Format

- **Development**: Use dataset directories for full-fidelity manifests
- **Production**: Use the format your serving stack expects (fvec/ivec/bvec/parquet)
- **Archival**: Keep dataset directories plus compressed facets
- **Transfer**: Consider Parquet for compact long-term storage

### 2. Include Metadata

Always include:
- Distance function
- Creation date
- Data source
- License information

### 3. Verify Conversions

After converting:
1. Check dimensions match
2. Verify vector count
3. Sample and compare values
4. Test with your application

### 4. Handle Errors Gracefully

Common issues:
- Dimension mismatches
- Corrupted files
- Insufficient memory
- Type conversions

## Summary

NBDataTools supports a variety of formats to meet different needs:

- **Dataset directories** are the canonical shared format
- **Binary formats** (fvec, ivec, bvec) for compatibility
- **Parquet** for big data integration
- **JSON** for configuration and small datasets

Choose based on your specific requirements for metadata, performance, compatibility, and tooling.

Next: Learn about [Working with Datasets](05-working-with-datasets.md) for practical workflows.
