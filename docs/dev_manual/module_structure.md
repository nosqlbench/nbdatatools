# Module Structure

This document explains the dependency matrix of the modules in the nbdatatools project, detailing what depends on what and why. The project is structured as a multi-module Maven reactor with clear separation of concerns and minimal circular dependencies.

## Overview

The nbdatatools project consists of 10 modules organized into logical layers. The precise dependency relationships are shown in the dependency matrix below.

## Module Details

### Core Foundation Layer

#### mvn-defaults
- **Purpose**: Provides common Maven configuration and build defaults
- **Dependencies**: None (parent POM)
- **Dependents**: All other modules inherit from this
- **Key Features**:
  - Common plugin configurations
  - Dependency version management
  - Build profiles and properties
  - License and deployment settings

#### testdata-apis
- **Purpose**: Core APIs and interfaces for test data handling
- **Dependencies**: 
  - External: SLF4J, Apache Commons, Jackson
- **Dependents**: vectordata, vshapes, commands
- **Key Features**:
  - VectorSpace interface definitions
  - File I/O abstractions
  - Progress indication interfaces
  - Service provider interfaces

### I/O and Transport Layer

#### io-xvec
- **Purpose**: Vector file format readers and writers
- **Dependencies**: testdata-apis
- **Dependents**: vectordata, vshapes, commands
- **Key Features**:
  - FVEC/IVEC/BVEC/DVEC/SVEC format support
  - Streaming and batch readers
  - CSV/JSON array processing
  - Memory-efficient vector I/O

#### io-transport
- **Purpose**: Abstract transport layer for data fetching
- **Dependencies**: testdata-apis
- **Dependents**: vectordata, commands
- **Key Features**:
  - HTTP byte-range fetching
  - File system access
  - Chunked streaming support
  - Transport provider SPI

#### parquet-reader
- **Purpose**: Apache Parquet format support for vectors
- **Dependencies**: testdata-apis, Apache Parquet
- **Dependents**: commands (optional)
- **Key Features**:
  - Parquet vector data reading
  - Schema inference
  - Columnar data access

### Core Engine Layer

#### vectordata
- **Purpose**: Core vector data processing and Merkle tree implementation
- **Dependencies**: testdata-apis, io-xvec, io-transport
- **Dependents**: commands
- **Key Features**:
  - Merkle tree-based vector indexing
  - Memory-mapped file access
  - Concurrent chunk processing
  - Data integrity verification
  - Performance monitoring
  - Dataset discovery and cataloging

### Application Layer

#### commands
- **Purpose**: Command-line interface and primary user entry point
- **Dependencies**: vectordata, io-xvec, io-transport, Jackson
- **Dependents**: nbvectors
- **Key Features**:
  - Complete CLI command set
  - Dataset.yaml/xvec processing
  - Data conversion utilities
  - Analysis and verification tools
  - Catalog management
  - Dataset generation

#### vshapes
- **Purpose**: Vector space analysis and visualization
- **Dependencies**: testdata-apis, io-xvec, Jackson
- **Optional Dependencies**: Jzy3D (for visualization)
- **Dependents**: None (can be used standalone)
- **Key Features**:
  - Local Intrinsic Dimensionality (LID) analysis
  - Nearest-neighbor margin analysis
  - Hubness detection and analysis
  - 3D visualization capabilities
  - Multiple output formats (CSV, JSON, reports)
  - Performance optimization and caching

### Distribution Layer

#### nbvectors
- **Purpose**: Executable distribution assembly
- **Dependencies**: commands, all other modules
- **Dependents**: None (final artifact)
- **Key Features**:
  - Command bundling
  - Executable JAR creation
  - Distribution packaging

### Testing Infrastructure

#### jetty-test-server
- **Purpose**: Embedded web server for integration tests
- **Dependencies**: Jetty, JUnit
- **Dependents**: Used by tests in multiple modules
- **Key Features**:
  - HTTP test server fixture
  - File serving capabilities
  - Test lifecycle management

## Dependency Matrix

```
Module              ┃ mvn-defaults ┃ testdata-apis ┃ io-xvec ┃ io-transport ┃ vectordata ┃ commands ┃ vshapes ┃ parquet ┃ jetty ┃ nbvectors
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
mvn-defaults        ┃      ☐       ┃       ☐       ┃    ☐    ┃      ☐       ┃     ☐      ┃     ☐    ┃    ☐    ┃    ☐    ┃   ☐   ┃     ☐
testdata-apis       ┃      ☑       ┃       ☐       ┃    ☐    ┃      ☐       ┃     ☐      ┃     ☐    ┃    ☐    ┃    ☐    ┃   ☐   ┃     ☐
io-xvec             ┃      ☑       ┃       ☑       ┃    ☐    ┃      ☐       ┃     ☐      ┃     ☐    ┃    ☐    ┃    ☐    ┃   ☐   ┃     ☐
io-transport        ┃      ☑       ┃       ☑       ┃    ☐    ┃      ☐       ┃     ☐      ┃     ☐    ┃    ☐    ┃    ☐    ┃   ☐   ┃     ☐
parquet-reader      ┃      ☑       ┃       ☑       ┃    ☐    ┃      ☐       ┃     ☐      ┃     ☐    ┃    ☐    ┃    ☐    ┃   ☐   ┃     ☐
vectordata          ┃      ☑       ┃       ☑       ┃    ☑    ┃      ☑       ┃     ☐      ┃     ☐    ┃    ☐    ┃    ☐    ┃   🧪   ┃     ☐
vshapes             ┃      ☑       ┃       ☑       ┃    ☑    ┃      ☐       ┃     ☐      ┃     ☐    ┃    ☐    ┃    ☐    ┃   ☐   ┃     ☐
jetty-test-server   ┃      ☑       ┃       ☐       ┃    ☐    ┃      ☐       ┃     ☐      ┃     ☐    ┃    ☐    ┃    ☐    ┃   ☐   ┃     ☐
commands            ┃      ☑       ┃       ☑       ┃    ☑    ┃      ☑       ┃     ☑      ┃     ☐    ┃    ☐    ┃    ☑    ┃   🧪   ┃     ☐
nbvectors           ┃      ☑       ┃       ☐       ┃    ☐    ┃      ☐       ┃     ☐      ┃     ☑    ┃    ☐    ┃    ☐    ┃   ☐   ┃     ☐
```

Legend: ☑ = Direct dependency, 🧪 = Test dependency only, ☐ = No dependency

## Architectural Principles

### Layered Architecture
The modules follow a strict layered architecture:
1. **Foundation**: Build configuration and core APIs
2. **I/O Layer**: Format-specific readers and transport
3. **Engine Layer**: Core processing logic
4. **Application Layer**: User-facing functionality
5. **Distribution**: Final packaging

### Separation of Concerns

#### testdata-apis
- Defines contracts without implementation
- Provides service provider interfaces
- Enables pluggable architectures

#### Specialized I/O Modules
- **io-xvec**: Binary vector formats
- **io-transport**: Network and file transport
- **parquet-reader**: Columnar data formats

#### Core Processing (vectordata)
- Implements complex algorithms (Merkle trees)
- Handles concurrency and performance
- Manages data integrity

#### Analysis (vshapes)
- Statistical analysis algorithms
- Visualization capabilities
- Independent of core data processing

### Dependency Management

#### Minimal Dependencies
- Each module only depends on what it absolutely needs
- External dependencies are carefully managed
- Optional dependencies (like Jzy3D) don't force inclusion

#### No Circular Dependencies
- Clean dependency graph with no cycles
- Upper layers can depend on lower layers only
- Shared functionality is factored into lower-level modules

#### Service Provider Interface (SPI)
- Plugin architecture allows runtime discovery
- New formats can be added without changing core code
- Enables extensibility without tight coupling

## Build Configuration

### Maven Profiles
The build uses profiles for optional modules:

```xml
<profiles>
  <profile>
    <id>vshapes</id>
    <activation>
      <activeByDefault>true</activeByDefault>
    </activation>
    <modules>
      <module>vshapes</module>
    </modules>
  </profile>
</profiles>
```

### Module Activation
- **vshapes**: Active by default, can be skipped with `-P!vshapes`
- All other modules: Always built
- Test modules: Can be skipped with `-DskipTests`

## Usage Patterns

### CLI Users
```
nbvectors → commands → vectordata → io-xvec/io-transport → testdata-apis
```

### Programmatic API Users
```
Application → vshapes → testdata-apis
           ↘ io-xvec
```

### Library Integration
```
Your Code → vectordata → io-xvec/io-transport → testdata-apis
```

## Extension Points

### Adding New Vector Formats
1. Implement readers in io-xvec or create new I/O module
2. Register via SPI in META-INF/services
3. No changes needed to upper layers

### Adding New Transport Methods
1. Implement transport provider in io-transport
2. Register via SPI
3. Available to all dependent modules

### Adding New Analysis Methods
1. Extend vshapes with new measures
2. Implement AnalysisMeasure interface
3. Register with VectorSpaceAnalyzer

### Adding New Commands
1. Add command classes to commands module
2. Register via BundledCommand SPI
3. Automatic discovery in CLI

## Testing Strategy

### Unit Tests
- Each module has comprehensive unit tests
- Mock implementations for external dependencies
- Fast feedback for development

### Integration Tests
- jetty-test-server provides HTTP testing
- Cross-module integration in vectordata
- Real file I/O testing

### Performance Tests
- Separate test suites for performance validation
- Large dataset testing (when available)
- Memory usage and throughput measurement

## Future Considerations

### Potential New Modules
- **vector-ml**: Machine learning integrations
- **vector-db**: Database connectivity
- **streaming-processors**: Real-time processing
- **gpu-acceleration**: CUDA/OpenCL support

### Architectural Evolution
- Consider reactive streams for large data
- Evaluate async processing patterns
- Cloud-native deployment options
- Microservice decomposition possibilities

This modular architecture provides flexibility for users who need different subsets of functionality while maintaining clear boundaries and enabling independent evolution of components.
