# Key Interfaces

This document describes the key interfaces that form the core APIs of the nbdatatools project. Interfaces are grouped by their logical relationships and purpose.

## Dataset View API

The Dataset View API provides a unified abstraction for accessing vector datasets, with support for asynchronous operations, prebuffering, and indexed access.

### Core Dataset Interface

#### DatasetView<T>
**Package**: `io.nosqlbench.vectordata.spec.datasets.types`  
**Purpose**: Core interface for accessing datasets with type-safe vector operations

The central abstraction for all dataset access. Key features:
- Asynchronous data retrieval with CompletableFuture
- Prebuffering support for performance optimization
- Indexed access to individual vectors
- Metadata methods for dataset dimensions and count

**Key Methods**:
- `get(int index)`: Get a single vector by index
- `getRange(int start, int count)`: Get a range of vectors
- `prebuffer(int start, int count)`: Preload data for faster access
- `getCount()`: Total number of vectors
- `getVectorDimensions()`: Dimensionality of vectors

### Specialized Vector Views

These interfaces extend `DatasetView<T>` for specific vector types:

#### FloatVectors, DoubleVectors, IntVectors
**Purpose**: Type-specific dataset views for primitive arrays
- `FloatVectors`: For `float[]` vectors (most common for embeddings)
- `DoubleVectors`: For `double[]` vectors (high precision)
- `IntVectors`: For `int[]` vectors (indices, discrete values)

#### BaseVectors and QueryVectors
**Purpose**: Semantic interfaces for different dataset roles
- `BaseVectors`: The corpus of vectors to search within
- `QueryVectors`: Vectors used as search queries
- Both extend `FloatVectors` to indicate their typical float representation

#### NeighborIndices and NeighborDistances
**Purpose**: Ground truth data for similarity search evaluation
- `NeighborIndices`: The indices of nearest neighbors for each query
- `NeighborDistances`: The distances to those nearest neighbors
- Used for evaluating search algorithm accuracy

### Test Dataset Access

#### TestDataView
**Package**: `io.nosqlbench.vectordata.discovery`  
**Purpose**: High-level interface for complete test datasets with metadata

Provides unified access to all components of a test dataset:
- Base vectors (the searchable corpus)
- Query vectors (test queries)
- Ground truth (neighbor indices and distances)
- Metadata (license, vendor, model information)

**Key Methods**:
- `getBaseVectors()`: Access the base vector dataset
- `getQueryVectors()`: Access query vectors
- `getNeighborIndices()`: Ground truth neighbor indices
- `getNeighborDistances()`: Ground truth distances
- `getLicense()`, `getVendor()`, `getModel()`: Dataset metadata

## Layout Configuration API

The layout API defines how datasets are configured and accessed, particularly for subsetting and windowing operations.

### DSView, DSWindow, DSInterval
**Package**: `io.nosqlbench.vectordata.layoutv2`  
**Purpose**: Configuration for dataset views and data windowing

These work together to define views over datasets:
- **DSView**: A named view with source and window configuration
- **DSWindow**: Specifies which portions of data to include (intervals)
- **DSInterval**: Defines a continuous range within the data

### DSSource, DSProfile, DSProfileGroup
**Purpose**: Dataset source and profile configuration
- **DSSource**: Identifies the data source
- **DSProfile**: Configuration profile for data access
- **DSProfileGroup**: Collection of related profiles

## Vector File I/O API

Interfaces for reading and writing vector data files with different access patterns.

### Stream-Based Access

#### VectorFileStream<T>
**Package**: `io.nosqlbench.nbdatatools.api.fileio`  
**Purpose**: Sequential, forward-only reading of vector files

Base interface for streaming vector data:
- Memory-efficient for large files
- Implements `Iterable<T>` for foreach loops
- Supports various file formats through implementations

#### BoundedVectorFileStream<T>
**Purpose**: Streaming with known size information
- Extends `VectorFileStream<T>` with `Sized` interface
- Useful for progress tracking and pre-allocation

#### VectorFileStreamStore
**Purpose**: Sequential writing of vector data to files
- Bulk write operations for efficiency
- Flush control for data persistence
- Creates files in various vector formats

### Random Access

#### VectorRandomAccessReader<T>
**Package**: `io.nosqlbench.nbdatatools.api.fileio`  
**Purpose**: Base interface for indexed vector access

#### VectorFileArray<T>
**Purpose**: Array-like random access to vector files
- Direct access by index
- Efficient for non-sequential access patterns
- No need to read entire file

## Transport Layer API

Abstractions for fetching data from various sources with range support.

### ChunkedTransportClient
**Package**: `io.nosqlbench.nbdatatools.api.transport`  
**Purpose**: Unified interface for range-based data fetching

Supports multiple transport protocols (HTTP, file, etc.):
- Range request support for efficient partial downloads
- Asynchronous operations with CompletableFuture
- Size queries for planning downloads

**Key Methods**:
- `fetchRange(long start, int length)`: Fetch a byte range
- `getSize()`: Get total size of resource
- `supportsRangeRequests()`: Check if ranges are supported

### ChunkedTransportProvider
**Purpose**: SPI for creating transport clients
- Factory pattern for different URL schemes
- Enables pluggable transport implementations

## Merkle Tree API

Interfaces for data integrity verification using Merkle trees.

### Core Merkle Interfaces

#### MerkleShape
**Package**: `io.nosqlbench.vectordata.merklev2`  
**Purpose**: Defines the structure and geometry of a Merkle tree

Central to understanding how data is chunked and verified:
- Chunk size and count calculations
- Tree structure navigation
- Conversion between different indexing schemes

**Key Concepts**:
- Chunks: Fixed-size data blocks
- Nodes: Tree nodes (internal and leaf)
- Ranges: Byte ranges for nodes

#### MerkleState
**Purpose**: Tracks verification state of data chunks

Maintains which chunks have been verified:
- Bit-based tracking of valid chunks
- Atomic state updates
- Persistence support

**Key Methods**:
- `isValid(int chunkIndex)`: Check if chunk is verified
- `saveIfValid(int chunkIndex, ByteBuffer data, Consumer<ByteBuffer> saver)`: Verify and save
- `getValidChunks()`: Get list of verified chunks

### Supporting Merkle Interfaces

#### ChunkScheduler
**Purpose**: Strategy interface for scheduling chunk downloads
- Different implementations for various access patterns
- Optimizes download order based on requirements

#### SchedulingTarget
**Purpose**: Target for chunk scheduling operations
- Manages futures for download coordination
- Queues tasks for execution

## Common Type Interfaces

Small interfaces that provide common capabilities across the codebase.

### Named
**Package**: `io.nosqlbench.nbdatatools.api.types`  
**Purpose**: For objects that have names
- Single method: `getName()`

### Sized
**Purpose**: For objects with known sizes
- Single method: `getSize()`

### Tagged
**Package**: `io.nosqlbench.vectordata.spec.tagging`  
**Purpose**: For objects with string key-value metadata
- Methods: `addTag()`, `getTag()`, `getTags()`

## Specialized Processing APIs

### Event System

#### EventSink
**Package**: `io.nosqlbench.vectordata.events`  
**Purpose**: Receiver for system events
- Used for monitoring and debugging
- Decouples event producers from consumers

## Design Patterns and Principles

The interfaces follow several key design patterns:

1. **Type Safety**: Extensive use of generics for compile-time type checking
2. **Async-First**: Core operations return CompletableFuture for non-blocking I/O
3. **Separation of Concerns**: Clear boundaries between data access, transport, and verification
4. **Strategy Pattern**: Multiple implementations for different use cases (e.g., ChunkScheduler)
5. **Visitor Pattern**: For traversing complex data structures (dataset.yaml, Parquet)
6. **Factory Pattern**: Service providers for extensibility
7. **Fluent Interfaces**: Many interfaces support method chaining

These interfaces form a comprehensive framework for efficient vector data management, with particular focus on:
- Large-scale vector dataset access
- Data integrity through Merkle verification
- Flexible transport mechanisms
- Support for multiple file formats
- Performance through streaming and chunking
