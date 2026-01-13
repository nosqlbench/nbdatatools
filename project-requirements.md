# NB Data Tools Requirements
The requirements in this file shall apply to all modules and code in this repository. Where any other requirements are in conflict, those in this file take precedence, and further, such conflicts should be considered a design consistency issue to be called out.

## Build Tools
This project should be built using Maven with the Java 25 compiler.

## Libraries
* Logging is provided by log4j2. No SLF4j bindings are required nor allowed.

## Runtime

### Java Versions

The core runtime is Java. Java 25 and Java 11 are both supported using the multi-version jar format. Some modules are specifically limited to Java 11 for downstream compatibility reasons. Modules are not required to individually support Java 11 and Java 25, since some of the support CLI mode and others are libraries. For modules which are not intended to be used as libraries, the Java 25 is the preferred runtime.
* datatools-vectordata: Java 11 only
* All other modules may be Java 11 or Java 25 or both

### Architecture Support

Although Java could support all JVMs across architectures, this project makes no promises nor attempts to test on any other than Linux x86_64.

However, this project has lots of number crunching, so when instruction set support is available for AVX variants, they should be supported. This means that panama needs to be enabled on systems which support it, and not enabling panama on those systems should throw an error to the user. All code which processes chunks of data should be written to be compatible with both panama and non-panama JVMs. 

## Documentation

Documentation is required to be complete to the extent that `mvn javadoc:jar` does not throw warnings or errors. Further, documentation should be written in Markdown format, using triple-slash lines, in a modern markdown style. It should include fenced code sections with diagrammatic representations of the data structures and algorithms, key concepts, data layouts, data flows, and computations.

A user guide should be maintained in docs/user-manual in a clean markdown format.

A developer guide should be maintained in docs/dev_manual in a clean markdown format.

Each module should have a markdown file in its root directory which describes the module and its purpose, with some explanation of the data structures and algorithms used.

## Correctness

Correctness of all numerical processing should be verified in unit tests. Numerical bounds on inputs and outputs should be checked with specific thresholds to ensure reliable results across a variety of reasonable inputs.

## Performance

All numerical processing should be benchmarked and optimized for performance using unit test tagged for such using jmh annotations.

All processing code should be written to be NUMA aware, using multi-threading by default. Thread counts should be auto-determined by system capabilities (e.g., leaving a small reserve for OS tasks) rather than hardcoded.

Algorithms should be written to be cache friendly, using data structures which are cache friendly. Any processing tasks which could benefit from SIMD instructions should be written to use them. Any processing tasks which could need more memory than might be available should be written to do incremental processing in a way that produces the same results as a single pass.

## Algebraic Design
Analyzers and processors must be designed algebraically. Intermediate results must be serializable and combinable. This ensures that:
1.  Multiple analyzers can run independently.
2.  Partial results from partitioned data can be merged to produce a mathematically consistent final result.
3.  Long-running processes can be paused/resumed via intermediate state serialization.

## Data Handling & IO
*   **Memory Mapping**: Large vector datasets and heavy IO operations must utilize memory mapping (`mmap`) patterns to ensure efficiency and reduce heap pressure.
*   **Zero-Copy**: Where possible, avoid copying data between buffers; use views and slices.

## Testing Standards
* **Framework**: All new tests must use JUnit 5 (Jupiter) and AssertJ for assertions.
* **Profiles**:
  * Unit tests must run quickly and be enabled by default.
  * Heavy performance tests must be guarded by the `performance` Maven profile.
  * Deep accuracy/verification tests must be guarded by the `accuracy` Maven profile.

## Coding Standards
* **License Headers**: All source files must include the standard Apache 2.0 license header. This is enforced by the `apache-rat-plugin`.
* Unnecessary use of runtime introspection is discouraged.
* Type-safe approaches are required whenever possible.
* Modular functionality should be the default, with SPI used to load available extensions.

## Module Naming
All module names must be lowercase, with hyphens for word separation, and start with `datatools-`. The part after the first hyphen is the module symbolic name. Packages within a module must be named in the format `io.nosqlbench.datatools.<module symbolic name>`.

## Dependency Management
* **Defaults Module**: All shared dependency versions and plugin configurations must be defined in `datatools-mvn-defaults`. Submodules should not override versions locally unless strictly necessary and documented. Otherwise, submodules can set their own versions for dependencies locally when they are the only module using them.

## CLI Development
* `picocli` is the required framework for building Command Line Interfaces.
* Common CLI options should be defined in package io.nosqlbench.command.common, using distinct semantic names for each option.
* **User Feedback**: Long-running operations (> 2 seconds) must provide visual status indicators (progress bars, ETA) to the user.

## Versioning
* This project uses a dynamic versioning scheme (`${revision}`). Modules must not hardcode their version or their parent's version; they must inherit or use the `${revision}` property.
