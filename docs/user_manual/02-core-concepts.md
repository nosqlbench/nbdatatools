# Core Concepts

Understanding these core concepts will help you work effectively with NBDataTools.

## Vector Data Fundamentals

### What are Vector Datasets?

Vector datasets consist of high-dimensional numerical arrays, commonly used in:
- Machine Learning (embeddings)
- Information Retrieval (document vectors)
- Computer Vision (feature vectors)
- Recommendation Systems (item/user vectors)

### Components of a Test Dataset

A complete test dataset for ANN benchmarking includes:

```
┌─────────────────────────────────────┐
│         Test Dataset                │
├─────────────────────────────────────┤
│ • Base Vectors (corpus)             │
│ • Query Vectors (test queries)      │
│ • Ground Truth (correct answers)    │
│   - Neighbor Indices                │
│   - Neighbor Distances              │
│ • Metadata (distance function, etc) │
└─────────────────────────────────────┘
```

## Standard Dataset Layout

### Why dataset.yaml + facet files?

NBDataTools represents datasets as directories because:
- **Transparent**: Each facet (base/query/indices/etc.) lives in a plain file (fvec/ivec/bvec/parquet)
- **Composable**: Profiles describe which slices of each file participate in a given test
- **Versionable**: Git-friendly YAML plus binary files, no monolithic container
- **Portable**: Works across local disks, object stores, and HTTP range requests
- **Efficient**: Supports partial I/O through MAFileChannel and dataset windows

### Directory Structure

```
dataset/
├── dataset.yaml              # Declares attributes, profiles, windows
├── base.fvec                 # Base vectors
├── query.fvec                # Query vectors
├── neighbors.ivec            # Neighbor indices (ground truth)
└── distances.fvec            # Neighbor distances
```

`dataset.yaml` describes:
- **Attributes**: metadata such as distance function, license, vendor
- **Profiles**: named combinations of base/query/windows (e.g., `default`, `1m`, `10m`)
- **Facets**: paths to the actual vector/index files and their ranges

## Data Access Patterns

### 1. Streaming Access

For processing large datasets sequentially:

```java
// Pseudocode
for (vector : dataset) {
    process(vector)
}
```

**Characteristics**:
- Memory efficient
- Forward-only iteration
- Good for batch processing

### 2. Random Access

For accessing specific vectors by index:

```java
// Pseudocode
vector = dataset.get(index)
```

**Characteristics**:
- Direct access by position
- Higher memory overhead
- Good for sampling or specific lookups

### 3. Range Access

For processing chunks of data:

```java
// Pseudocode
vectors = dataset.getRange(start, count)
```

**Characteristics**:
- Balanced memory usage
- Good for parallel processing
- Efficient for windowed operations

## Merkle Tree Verification

### What is Merkle Verification?

NBDataTools uses Merkle trees to ensure data integrity:

```
                 Root Hash
                /          \
           Hash AB          Hash CD
          /      \        /        \
      Hash A   Hash B   Hash C   Hash D
        |        |        |        |
     Chunk 1  Chunk 2  Chunk 3  Chunk 4
```

### Benefits

- **Integrity**: Detect any data corruption
- **Efficiency**: Verify chunks independently
- **Resumability**: Know which parts are valid
- **Trust**: Cryptographic proof of correctness

### How It Works

1. Data is divided into fixed-size chunks
2. Each chunk has a hash
3. Hashes combine to form a tree
4. Root hash verifies entire dataset
5. Any chunk can be verified independently

## Distance Functions

### Supported Distance Metrics

NBDataTools supports common distance functions:

| Metric | Formula | Use Case |
|--------|---------|----------|
| **Euclidean** | √Σ(x-y)² | General purpose |
| **Cosine** | 1 - (x·y)/(‖x‖‖y‖) | Text, high-dimensional |
| **Inner Product** | -Σ(x·y) | Recommendation systems |
| **Angular** | arccos(cosine_sim) | Normalized vectors |

### Importance in Testing

The distance function determines:
- How similarity is calculated
- What constitutes "nearest" neighbors
- Ground truth computation
- Algorithm selection

## Data Windows and Views

### What are Data Windows?

Windows allow you to work with subsets of data:

```
Full Dataset: [==========================================]
Window 1:     [=====]
Window 2:            [=====]
Window 3:                   [=====]
```

### Use Cases

- **Training/Test Split**: Separate data for evaluation
- **Cross-validation**: Multiple folds for robust testing
- **Sampling**: Work with representative subsets
- **Parallel Processing**: Divide work across workers

### Window Specification

Windows are defined by intervals:

```json
{
  "window": {
    "intervals": [
      {"start": 0, "end": 1000},
      {"start": 5000, "end": 6000}
    ]
  }
}
```

## Async Operations

### Why Async?

NBDataTools uses asynchronous operations for:
- **Performance**: Non-blocking I/O
- **Scalability**: Handle multiple requests
- **Responsiveness**: UI doesn't freeze
- **Resource Efficiency**: Better CPU utilization

### Prebuffering

Prebuffering loads data before it's needed:

```java
// Start loading data
CompletableFuture<Void> future = dataset.prebuffer(1000, 100);

// Do other work...

// Wait for data when needed
future.join();
vectors = dataset.getRange(1000, 100); // Fast, already loaded
```

## File Format Support

### Input Formats

| Format | Extension | Description | Type |
|--------|-----------|-------------|------|
| **fvec** | .fvec | Float vectors | Binary |
| **ivec** | .ivec | Integer vectors | Binary |
| **bvec** | .bvec | Byte vectors | Binary |
| **Parquet** | .parquet | Columnar format | Binary |
| **Dataset dir** | dataset.yaml + files | Profile-based layout | Mixed |
| **JSON** | .json | Text format | Text |

### Format Characteristics

**Binary Formats** (fvec, ivec, bvec):
- Simple, efficient
- No metadata
- Sequential access only

**Parquet**:
- Columnar storage
- Good compression
- Schema support

**Dataset directories**:
- Attributes + profiles in YAML
- Facets stored as standard vector files
- Works locally or via HTTP range requests

## Service Architecture

### Plugin System

NBDataTools uses Service Provider Interfaces (SPIs):

```
┌─────────────────┐
│   Application   │
├─────────────────┤
│   Core APIs     │
├─────────────────┤
│   SPI Layer     │
├─────┬─────┬─────┤
│Impl1│Impl2│Impl3│ <- Pluggable implementations
└─────┴─────┴─────┘
```

### Benefits

- **Extensibility**: Add new formats without changing core
- **Modularity**: Separate concerns
- **Flexibility**: Choose implementations at runtime

## Performance Considerations

### Memory Management

Different access patterns have different memory profiles:

| Pattern | Memory Usage | Speed | Use When |
|---------|--------------|-------|----------|
| Streaming | Low | Medium | Processing entire dataset |
| Random | High | Fast | Need specific vectors |
| Windowed | Medium | Fast | Batch processing |

### Chunking Strategy

Chunk size affects performance:
- **Large chunks**: Better throughput, more memory
- **Small chunks**: Less memory, more overhead
- **Default**: 1MB chunks balance both concerns

## Best Practices

### 1. Choose the Right Access Pattern

- Use **streaming** for full dataset processing
- Use **random access** for sampling
- Use **range access** for batch operations

### 2. Leverage Prebuffering

```java
// Good: Prebuffer before intensive operations
dataset.prebuffer(start, count).join();
for (int i = start; i < start + count; i++) {
    process(dataset.get(i));
}
```

### 3. Understand Your Data

- Know the vector dimensions
- Understand the distance metric
- Check the data distribution

### 4. Use Appropriate Tools

- `analyze` commands for understanding data
- `convert ...` for format conversion
- `datasets` for standard test data

## Summary

These core concepts form the foundation of NBDataTools:

- **Vector datasets** contain high-dimensional data for testing
- **Dataset directories** provide a portable canonical format
- **Multiple access patterns** suit different use cases
- **Merkle verification** ensures data integrity
- **Async operations** improve performance
- **Plugin architecture** enables extensibility

Understanding these concepts will help you use NBDataTools effectively for your vector data management needs.

Next: Learn about the [Command Line Interface](03-cli-reference.md) for practical usage.
