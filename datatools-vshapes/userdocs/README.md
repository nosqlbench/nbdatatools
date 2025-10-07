# Vector Space Analysis (vshapes) User Documentation

Welcome to the Vector Space Analysis module documentation. This module provides comprehensive analysis tools for high-dimensional vector datasets, focusing on intrinsic dimensionality, class separability, and hubness phenomena.

## Quick Start

```java
// Create or load your vector space
VectorSpace vectorSpace = /* your vector space implementation */;

// Analyze the vector space
VectorSpaceAnalyzer analyzer = new VectorSpaceAnalyzer();
VectorSpaceAnalyzer.AnalysisReport report = analyzer.analyzeVectorSpace(vectorSpace);

// Get a comprehensive summary
System.out.println(report.getSummary());
```

## Documentation Structure

- [**Getting Started**](getting-started.md) - Basic setup and your first analysis
- [**Core Concepts**](core-concepts.md) - Understanding the analysis measures
- [**API Reference**](api-reference.md) - Complete API documentation
- [**Analysis Measures**](analysis-measures.md) - Detailed explanation of LID, Margin, and Hubness
- [**Output Formats**](output-formats.md) - Working with different report formats
- [**Performance Guide**](performance-guide.md) - Optimization and caching strategies
- [**Examples**](examples/) - Practical usage examples
- [**Troubleshooting**](troubleshooting.md) - Common issues and solutions

## Key Features

### Analysis Measures
- **Local Intrinsic Dimensionality (LID)**: Estimates the intrinsic dimensionality around each data point
- **Nearest-Neighbor Margin**: Measures class separability for supervised datasets
- **Hubness Analysis**: Detects hub and anti-hub phenomena in high-dimensional spaces

### Output Options
- Text summaries with statistical insights
- CSV exports for data analysis
- JSON reports for programmatic processing
- Interpretation guides with actionable insights

### Performance Features
- Intelligent caching system using JSON artifacts
- Dependency-driven computation to avoid redundant work
- Configurable k-values for nearest-neighbor computations
- Memory-efficient implementations for large datasets

## Use Cases

- **Dataset Quality Assessment**: Evaluate intrinsic complexity and separability
- **Algorithm Selection**: Choose appropriate ML algorithms based on data characteristics
- **Preprocessing Guidance**: Identify when dimensionality reduction might help
- **Anomaly Detection**: Find unusual data patterns through hubness analysis
- **Research Analysis**: Understand high-dimensional data behavior

## Requirements

- Java 21+
- Jackson for JSON processing (included as dependency)
- JUnit 5 for testing (development only)

## License

Licensed under the Apache License, Version 2.0. See the LICENSE file for details.