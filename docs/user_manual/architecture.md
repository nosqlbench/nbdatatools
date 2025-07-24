# Architecture Overview

This document provides a high-level overview of NBDataTools architecture and design principles.

## System Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    NBDataTools System                       │
├─────────────────────────────────────────────────────────────┤
│  Command Line Interface (nbvectors.jar)                    │
├─────────────────────┬───────────────────┬───────────────────┤
│    Analysis Tools   │  Conversion Tools │ Dataset Management│
│  • analyze          │  • export_hdf5    │  • datasets       │
│  • verify_knn       │  • export_json    │  • catalog_hdf5   │
│  • count_zeros      │  • build_hdf5     │  • show_hdf5      │
│  • describe         │                   │  • tag_hdf5       │
├─────────────────────┴───────────────────┴───────────────────┤
│                      Core APIs                              │
├─────────────────────────────────────────────────────────────┤
│  Dataset View Layer                                         │
│  • TestDataView     • DatasetView<T>    • Specialized Views│
│  • FloatVectors     • IntVectors        • NeighborIndices  │
├─────────────────────────────────────────────────────────────┤
│  I/O Abstraction Layer                                     │
│  • VectorFileStream    • VectorFileArray  • StreamStore    │
│  • BoundedStream       • RandomAccess     • FileArray      │
├─────────────────────────────────────────────────────────────┤
│  Transport Layer                                            │
│  • ChunkedTransportClient    • HTTP Provider               │
│  • File Provider             • Custom Providers            │
├─────────────────────────────────────────────────────────────┤
│  Data Integrity Layer                                      │
│  • MerkleShape      • MerkleState       • ChunkScheduler   │
│  • Verification     • Partial Downloads • Integrity Proofs │
├─────────────────────────────────────────────────────────────┤
│  Storage Backends                                           │
│  • HDF5 Files       • Binary Formats    • Remote Storage   │
│  • Local Cache      • Parquet Files     • Cloud Storage    │
└─────────────────────────────────────────────────────────────┘
```

## Core Components

### 1. Command Line Interface

The CLI provides unified access to all NBDataTools functionality:

- **Single Entry Point**: `nbvectors.jar` with subcommands
- **Auto-Discovery**: Commands discovered via Service Provider Interface
- **Consistent Interface**: Common patterns across all commands
- **Help System**: Built-in help for all commands and options

### 2. Dataset View Layer

High-level abstractions for working with vector datasets:

```java
// Unified access to complete test datasets
TestDataView testData = TestDataView.open(path);

// Type-safe vector access
DatasetView<float[]> baseVectors = testData.getBaseVectors();
DatasetView<int[]> neighbors = testData.getNeighborIndices();

// Async operations
CompletableFuture<float[]> futureVector = baseVectors.getAsync(index);
```

**Key Features**:
- Type safety through generics
- Async-first API design
- Metadata integration
- Prebuffering support

### 3. I/O Abstraction Layer

Flexible I/O patterns for different use cases:

```java
// Streaming for large datasets
try (VectorFileStream<float[]> stream = VectorFileStream.open(path)) {
    for (float[] vector : stream) {
        process(vector);
    }
}

// Random access for sampling
VectorFileArray<float[]> array = VectorFileArray.open(path);
float[] vector = array.get(randomIndex);

// Writing data
try (VectorFileStreamStore<float[]> store = VectorFileStreamStore.open(path)) {
    store.writeBulk(vectors);
}
```

**Access Patterns**:
- **Sequential**: Memory-efficient streaming
- **Random**: Direct indexed access
- **Bounded**: Streaming with size information
- **Batch**: Efficient bulk operations

### 4. Transport Layer

Unified abstraction for data retrieval:

```java
ChunkedTransportClient client = ChunkedTransportIO.create(url);

// Range requests for efficient partial downloads
CompletableFuture<FetchResult<?>> future = client.fetchRange(start, length);

// Size queries for download planning
CompletableFuture<Long> sizeFuture = client.getSize();
```

**Supported Protocols**:
- HTTP/HTTPS with range request support
- Local file system access
- Extensible through SPI

### 5. Data Integrity Layer

Merkle tree-based verification system:

```
Data Flow with Integrity Verification:

1. Raw Data → Chunking → Hash Tree Creation
   ├─ Chunk 1 → Hash A
   ├─ Chunk 2 → Hash B     
   ├─ Chunk 3 → Hash C     } → Merkle Tree
   └─ Chunk 4 → Hash D

2. Download → Verification → Cache
   ├─ Download chunk
   ├─ Verify against hash
   └─ Cache if valid

3. Access → Integrity Check → Data
   ├─ Check chunk validity
   └─ Return verified data
```

**Benefits**:
- **Integrity**: Detect corruption or tampering
- **Efficiency**: Verify chunks independently
- **Resumability**: Resume from known-good state
- **Scalability**: Parallel verification

## Design Principles

### 1. Type Safety

Extensive use of generics prevents runtime type errors:

```java
// Compile-time type safety
DatasetView<float[]> floatVectors = ...;   // float arrays
DatasetView<int[]> intVectors = ...;       // int arrays
DatasetView<double[]> doubleVectors = ...; // double arrays

// Impossible to mix types
float[] vector = floatVectors.get(0);      // ✓ Safe
int[] vector = floatVectors.get(0);        // ✗ Compile error
```

### 2. Async-First Design

Non-blocking operations with CompletableFuture:

```java
// Non-blocking API
CompletableFuture<float[]> future = dataset.getAsync(index);

// Composable operations
future
    .thenApply(this::normalize)
    .thenAccept(this::process)
    .exceptionally(this::handleError);

// Parallel processing
List<CompletableFuture<float[]>> futures = IntStream.range(0, count)
    .mapToObj(dataset::getAsync)
    .collect(toList());
    
CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
```

### 3. Service Provider Interface (SPI)

Extensible plugin architecture:

```java
// Core defines interface
public interface ChunkedTransportProvider {
    ChunkedTransportClient getClient(URL url);
}

// Plugins implement interface
public class S3TransportProvider implements ChunkedTransportProvider {
    public ChunkedTransportClient getClient(URL url) {
        if ("s3".equals(url.getProtocol())) {
            return new S3TransportClient(url);
        }
        return null;
    }
}

// Runtime discovery via ServiceLoader
ServiceLoader<ChunkedTransportProvider> loader = 
    ServiceLoader.load(ChunkedTransportProvider.class);
```

### 4. Resource Management

Automatic resource cleanup with try-with-resources:

```java
// Automatic cleanup
try (TestDataView data = TestDataView.open(path)) {
    // Use data
} // Automatically closed

// Streaming with cleanup
try (VectorFileStream<float[]> stream = VectorFileStream.open(path)) {
    for (float[] vector : stream) {
        process(vector);
    }
} // Stream automatically closed
```

### 5. Separation of Concerns

Clear boundaries between different responsibilities:

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Data Access    │───→│   Transport     │───→│    Storage      │
│  (What to get)  │    │  (How to get)   │    │  (Where it is)  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
        │                       │                       │
        ▼                       ▼                       ▼
  Dataset Views          HTTP/File Clients         HDF5/Binary
  Type-safe APIs         Range Requests            Local/Remote
  Async Operations       Connection Pooling        Caching
```

## Data Flow

### Read Operation Flow

```
1. User Request
   └─ dataset.get(index)
   
2. Dataset View Layer
   ├─ Check bounds
   ├─ Calculate chunk requirements
   └─ Request data from I/O layer
   
3. I/O Abstraction Layer
   ├─ Check cache
   ├─ If miss: request from transport
   └─ Return data
   
4. Transport Layer
   ├─ Determine transport type (HTTP/File)
   ├─ Make range request
   └─ Return raw data
   
5. Data Integrity Layer
   ├─ Verify chunk integrity
   ├─ Update merkle state
   └─ Cache verified data
   
6. Return to User
   └─ Type-safe vector data
```

### Write Operation Flow

```
1. User Write Request
   └─ store.write(vector)
   
2. Stream Store Layer
   ├─ Buffer data
   ├─ Batch for efficiency
   └─ Write to storage backend
   
3. Storage Backend
   ├─ Serialize data (HDF5/Binary)
   ├─ Write to file/stream
   └─ Update metadata
   
4. Integrity Generation
   ├─ Calculate chunk hashes
   ├─ Build merkle tree
   └─ Store reference
```

## Performance Characteristics

### Memory Usage

| Access Pattern | Memory Usage | Description |
|---------------|--------------|-------------|
| Streaming | Low (MB) | Process one vector at a time |
| Random | Medium (10s MB) | Cache recently accessed chunks |
| Batch | High (100s MB) | Load multiple chunks/ranges |
| Prebuffer | Variable | User-controlled preloading |

### Scalability

```
Dataset Size vs Performance:

Small (< 1GB):     ████████████ Fast
Medium (1-10GB):   ████████░░░░ Good  
Large (10-100GB):  ██████░░░░░░ Fair (streaming recommended)
Huge (> 100GB):    ████░░░░░░░░ Slow (chunked access required)
```

### Concurrency

- **I/O Operations**: Configurable thread pools
- **CPU Tasks**: Parallel processing support
- **Network**: Connection pooling and multiplexing
- **Integrity**: Parallel chunk verification

## Extension Points

### 1. Custom Transport Providers

Add support for new protocols:

```java
public class RedisTransportProvider implements ChunkedTransportProvider {
    public ChunkedTransportClient getClient(URL url) {
        if ("redis".equals(url.getProtocol())) {
            return new RedisTransportClient(url);
        }
        return null;
    }
}
```

### 2. Custom File Formats

Support proprietary vector formats:

```java
public class CustomVectorStream implements VectorFileStream<float[]> {
    // Implement custom format reading
}
```

### 3. Custom Commands

Add new CLI commands:

```java
@CommandLine.Command(name = "my_command")
public class MyCommand implements BundledCommand {
    // Implement command logic
}
```

### 4. Custom Schedulers

Optimize chunk download strategies:

```java
public class OptimizedScheduler implements ChunkScheduler {
    public void scheduleDownloads(/*...*/) {
        // Custom scheduling logic
    }
}
```

## Quality Attributes

### Reliability
- Comprehensive error handling
- Graceful degradation
- Data integrity verification
- Automatic retry mechanisms

### Performance
- Async operations prevent blocking
- Configurable parallelism
- Efficient caching strategies
- Minimal memory footprint

### Scalability
- Streaming for large datasets
- Chunk-based processing
- Parallel operations
- Configurable resource usage

### Maintainability
- Clear separation of concerns
- Extensive documentation
- Type-safe interfaces
- Comprehensive testing

### Extensibility
- Service Provider Interface
- Plugin architecture
- Configuration system
- Custom implementations

## Security Considerations

### Data Integrity
- Cryptographic hash verification
- Merkle tree proofs
- Checksum validation
- Tamper detection

### Network Security
- HTTPS support
- Certificate verification
- Secure credential handling
- Connection encryption

### Access Control
- Configurable authentication
- Authorization hooks
- Audit logging
- Secure temporary files

## Future Architecture Evolution

### Planned Enhancements
- Distributed caching layer
- Advanced compression support
- Real-time streaming capabilities
- Enhanced monitoring and metrics

### Scalability Improvements
- Horizontal scaling support
- Cloud-native deployment
- Microservices architecture
- Event-driven processing

This architecture provides a solid foundation for vector data management while maintaining flexibility for future enhancements and use cases.