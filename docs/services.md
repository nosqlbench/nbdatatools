# Service Provider Interfaces

This document describes all Service Provider Interfaces (SPIs) defined in the nbdatatools project. These interfaces are declared in `META-INF/services` files and enable plugin-based architecture for extending functionality.

## Command Services

### BundledCommand
**Interface**: `io.nosqlbench.nbdatatools.api.services.BundledCommand`  
**Purpose**: Marker interface for auto-discovering picocli subcommands

This is a marker interface (no methods) used to automatically discover and register command-line interface commands. Any class that:
1. Implements this interface
2. Is annotated with `@picocli.CommandLine.Command`
3. Is listed in the corresponding `META-INF/services` file

will be automatically loaded as a subcommand in the CLI application. This enables a plugin architecture for adding new commands without modifying the main application code.

## Transport Services

### ChunkedTransportProvider
**Interface**: `io.nosqlbench.nbdatatools.api.transport.ChunkedTransportProvider`  
**Purpose**: Factory for creating transport clients based on URL schemes

This SPI enables pluggable transport implementations for fetching data from various sources. Each provider implementation:
- Handles specific URL schemes (e.g., http, https, file)
- Creates appropriate `ChunkedTransportClient` instances
- Supports chunked/ranged data retrieval

The transport abstraction allows uniform access to data regardless of whether it's stored locally (file://) or remotely (http://, https://).

## Vector File I/O Services

### VectorFileStream
**Interface**: `io.nosqlbench.nbdatatools.api.fileio.VectorFileStream`  
**Purpose**: Sequential streaming reader for vector data files

Base interface for reading vector data files in a streaming fashion. Key characteristics:
- Sequential access pattern (forward-only)
- Memory-efficient for large files
- Implements `Iterable<T>` for foreach loops
- Supports various vector data formats through different implementations

### BoundedVectorFileStream
**Interface**: `io.nosqlbench.nbdatatools.api.fileio.BoundedVectorFileStream`  
**Purpose**: Sized streaming reader that knows total element count

Extends `VectorFileStream` with size information, combining:
- All streaming capabilities of `VectorFileStream`
- Knowledge of total vector count before iteration begins
- Useful for progress tracking and pre-allocation scenarios

### VectorFileArray
**Interface**: `io.nosqlbench.nbdatatools.api.fileio.VectorFileArray`  
**Purpose**: Random access reader for vector data files

Provides array-like random access to vector data:
- Direct access by index without sequential reading
- Implements List-like access patterns
- Efficient for scenarios requiring non-sequential access
- Supports seeking to specific positions in large files

### VectorFileStreamStore
**Interface**: `io.nosqlbench.nbdatatools.api.fileio.VectorFileStreamStore`  
**Purpose**: Sequential writer for creating vector data files

Interface for writing vector data to files:
- Sequential write operations
- Bulk write support for efficiency
- Flush control for data persistence
- Supports creating files in various vector formats

## Design Principles

The service interfaces follow these key design principles:

1. **Separation of Concerns**: Clear distinction between reading (Stream/Array) and writing (Store) operations
2. **Access Patterns**: Different interfaces for sequential (Stream) vs random (Array) access
3. **Type Safety**: All vector interfaces use generics for type-safe operations
4. **Plugin Architecture**: SPI pattern enables adding new implementations without core changes
5. **Resource Management**: All I/O interfaces extend `AutoCloseable` for proper resource cleanup

## Usage Pattern

Services are typically discovered and instantiated using Java's `ServiceLoader`:

```java
ServiceLoader<VectorFileStream> loader = ServiceLoader.load(VectorFileStream.class);
for (VectorFileStream provider : loader) {
    // Use provider based on file format or other criteria
}
```

This allows the runtime to discover all available implementations and select the appropriate one based on file format, performance requirements, or other criteria.