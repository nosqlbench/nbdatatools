# Data Formats

NBDataTools supports multiple vector data formats for input and output. This chapter details each format, its characteristics, and when to use it.

## Format Overview

| Format | Extension | Type | Metadata | Random Access | Compression | Use Case |
|--------|-----------|------|----------|---------------|-------------|----------|
| **HDF5** | .hdf5, .h5 | Binary | Yes | Yes | Yes | Standard format |
| **fvec** | .fvec | Binary | No | No | No | Float vectors |
| **ivec** | .ivec | Binary | No | No | No | Integer vectors |
| **bvec** | .bvec | Binary | No | No | No | Byte vectors |
| **Parquet** | .parquet | Binary | Yes | Yes | Yes | Columnar data |
| **JSON** | .json | Text | Yes | Yes | No | Configuration |

## HDF5 Format (Recommended)

### Overview

HDF5 (Hierarchical Data Format 5) is the standard format for NBDataTools. It provides:
- Hierarchical organization
- Rich metadata support
- Efficient storage with compression
- Partial I/O capabilities
- Cross-platform compatibility

### Structure

```
dataset.hdf5
├── /                           # Root group
│   ├── @version               # Format version attribute
│   ├── @distance              # Distance function
│   └── @created               # Creation timestamp
├── /base                      # Base vectors group
│   ├── data                   # Vector data (n × d array)
│   ├── @count                 # Number of vectors
│   └── @dimensions            # Vector dimensions
├── /query                     # Query vectors group
│   ├── data                   # Query data (m × d array)
│   ├── @count                 # Number of queries
│   └── @dimensions            # Query dimensions
├── /neighbors                 # Ground truth indices
│   ├── data                   # Indices (m × k array)
│   └── @k                     # Number of neighbors
└── /distances                 # Ground truth distances
    ├── data                   # Distances (m × k array)
    └── @k                     # Number of neighbors
```

### Creating HDF5 Files

From vector files:
```bash
java -jar nbvectors.jar export_hdf5 \
  --input base.fvec \
  --output dataset.hdf5 \
  --dataset-name "my_dataset"
```

With complete test data:
```bash
java -jar nbvectors.jar export_hdf5 \
  --input base.fvec \
  --queries queries.fvec \
  --neighbors neighbors.ivec \
  --distances distances.fvec \
  --output complete.hdf5 \
  --distance euclidean
```

### Reading HDF5 Files

Inspect structure:
```bash
java -jar nbvectors.jar show_hdf5 --file dataset.hdf5 --tree
```

View attributes:
```bash
java -jar nbvectors.jar show_hdf5 --file dataset.hdf5 --attributes
```

### HDF5 Attributes

Standard attributes include:

| Attribute | Location | Description |
|-----------|----------|-------------|
| `version` | `/` | Format version (e.g., "1.0") |
| `distance` | `/` | Distance function name |
| `created` | `/` | ISO 8601 timestamp |
| `license` | `/` | Dataset license |
| `vendor` | `/` | Dataset provider |
| `model` | `/` | Model/algorithm used |
| `count` | `/base`, `/query` | Number of vectors |
| `dimensions` | `/base`, `/query` | Vector dimensionality |
| `k` | `/neighbors`, `/distances` | Number of neighbors |

## Binary Vector Formats

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

Convert to HDF5:
```bash
# Float vectors
java -jar nbvectors.jar export_hdf5 \
  --input vectors.fvec \
  --output vectors.hdf5

# Integer vectors
java -jar nbvectors.jar export_hdf5 \
  --input indices.ivec \
  --output indices.hdf5

# Byte vectors
java -jar nbvectors.jar export_hdf5 \
  --input features.bvec \
  --output features.hdf5
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

### Converting from Parquet

```bash
java -jar nbvectors.jar export_hdf5 \
  --input vectors.parquet \
  --output vectors.hdf5 \
  --parquet-column "vector" \
  --parquet-id-column "id"
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

### Building from JSON

```bash
java -jar nbvectors.jar build_hdf5 \
  --spec dataset_spec.json \
  --output dataset.hdf5
```

### Exporting to JSON

Export metadata only:
```bash
java -jar nbvectors.jar export_json \
  --input dataset.hdf5 \
  --output metadata.json
```

Export with sample data:
```bash
java -jar nbvectors.jar export_json \
  --input dataset.hdf5 \
  --output summary.json \
  --sample-size 100
```

## Format Selection Guide

### When to Use Each Format

**Use HDF5 when you need:**
- ✅ Complete test datasets with metadata
- ✅ Random access to vectors
- ✅ Compression and efficiency
- ✅ Cross-platform compatibility
- ✅ Rich attribute support

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

| From ↓ To → | HDF5 | fvec | ivec | bvec | Parquet | JSON |
|-------------|------|------|------|------|---------|------|
| **HDF5** | - | ✅ | ✅ | ✅ | ✅ | ✅ |
| **fvec** | ✅ | - | ❌ | ❌ | ✅ | ✅* |
| **ivec** | ✅ | ❌ | - | ❌ | ✅ | ✅* |
| **bvec** | ✅ | ❌ | ❌ | - | ✅ | ✅* |
| **Parquet** | ✅ | ✅ | ✅ | ✅ | - | ✅ |
| **JSON** | ✅** | ✅** | ✅** | ✅** | ✅** | - |

*Small datasets only  
**Via build_hdf5 command

## Performance Characteristics

### Read Performance

| Format | Sequential Read | Random Read | Memory Usage |
|--------|----------------|-------------|--------------|
| HDF5 | Fast | Fast | Medium |
| fvec | Very Fast | Slow | Low |
| ivec | Very Fast | Slow | Low |
| bvec | Very Fast | Slow | Very Low |
| Parquet | Medium | Medium | Medium |
| JSON | Slow | Slow | High |

### Write Performance

| Format | Write Speed | Compression | Append Support |
|--------|-------------|-------------|----------------|
| HDF5 | Fast | Yes | Yes |
| fvec | Very Fast | No | Yes |
| ivec | Very Fast | No | Yes |
| bvec | Very Fast | No | Yes |
| Parquet | Medium | Yes | No |
| JSON | Slow | No | No |

## Working with Large Files

### Chunked Processing

For files larger than memory:

```bash
# HDF5 supports partial reads natively
java -jar nbvectors.jar analyze describe \
  --file large_dataset.hdf5 \
  --sample-size 1000
```

### Streaming Conversion

Convert large files without loading fully:

```bash
java -jar nbvectors.jar export_hdf5 \
  --input huge_vectors.fvec \
  --output huge_vectors.hdf5 \
  --streaming \
  --chunk-size 1000000
```

## Format Validation

### Verify File Integrity

Check format consistency:
```bash
# Verify HDF5 structure
java -jar nbvectors.jar analyze describe --file dataset.hdf5

# Check binary format dimensions
java -jar nbvectors.jar analyze count_zeros --file vectors.fvec
```

### Validate Against Schema

For HDF5 files:
```bash
java -jar nbvectors.jar validate \
  --file dataset.hdf5 \
  --schema vectordata-v1
```

## Best Practices

### 1. Choose the Right Format

- **Development**: Use HDF5 for flexibility
- **Production**: Use format matching your tools
- **Archival**: Use HDF5 with compression
- **Transfer**: Consider Parquet for size

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

- **HDF5** is the recommended format for most use cases
- **Binary formats** (fvec, ivec, bvec) for compatibility
- **Parquet** for big data integration
- **JSON** for configuration and small datasets

Choose based on your specific requirements for metadata, performance, compatibility, and tooling.

Next: Learn about [Working with Datasets](05-working-with-datasets.md) for practical workflows.