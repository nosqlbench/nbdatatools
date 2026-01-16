# vshapes Module Requirements

This document outlines the specific architectural and functional requirements for the `datatools-vshapes` module.

> **Note**: This module strictly adheres to the global project requirements defined in `../project-requirements.md`. Those requirements (Testing, Concurrency, IO, CLI standards) apply here unless explicitly refined below.

## 1. Core Architecture

### 1.1 Tensor Model Hierarchy
The module must maintain a strict three-level tensor model hierarchy to represent vector spaces:
*   **ScalarModel (1st Order)**: Must represent a probability distribution for a single dimension.
*   **VectorModel (2nd Order)**: Must compose $M$ `ScalarModel` instances and define cardinality $N$.
*   **MatrixModel (3rd Order)**: Must compose $K$ `VectorModel` instances (clusters).

### 1.2 Type Hierarchy & Interfaces
*   **Separation of Concerns**: Functionality must be defined in interfaces (`ScalarModel`, `VectorModel`), while data storage must be handled by concrete implementations.
*   **Pearson Classification**: The module must implement the Pearson distribution system, categorizing continuous probability distributions based on skewness ($eta_1$) and kurtosis ($eta_2$).
*   **Extensibility**: The architecture must support adding new `ScalarModel` implementations (e.g., specific Pearson types) without modifying the core interfaces.

## 2. Implementation Mandates

### 2.1 Type-Specific Parameters
*   **No Generic Accessors**: `ScalarModel` implementations must expose *only* their natural parameters (e.g., `mean`/`stdDev` for Normal, `alpha`/`beta` for Beta). Generic accessors (like `getParam1()`) are strictly prohibited to ensure type safety and semantic clarity.
*   **Type Algebra**: The code must rely on pattern matching (e.g., `instanceof`) or the visitor pattern to handle specific model types, preserving the clean algebraic structure.

### 2.2 Optimization & Isomorphism
*   **Isomorphic Optimization**: The system must detect when all dimensions in a `VectorModel` share the same underlying distribution type and parameters.
*   **Marker Interface**: Such models must implement `IsomorphicVectorModel`.
*   **Vectorized Paths**: Samplers and processors must utilize this marker to switch from component-wise processing to SIMD-optimized (vectorized) processing paths.

### 2.3 Model Extraction Pipeline
*   **Pipeline Structure**: The extraction process must follow the `ModelExtractor` $	o$ `ComponentModelFitter` $	o$ `BestFitSelector` flow.
*   **Automatic Selection**: The system must be able to automatically select the best-fitting distribution for a dimension using `PearsonClassifier` based on observed moments.
*   **Configurable Strategies**: Users must be able to constrain the extractor (e.g., `normalOnly()`, `parametricOnly()`) or provide custom selectors.

### 2.4 Streaming Analysis
*   **Online Algorithms**: All analyzers must implement the `StreamingAnalyzer` interface, capable of processing data in a single pass ($O(N)$) without requiring the full dataset in memory.
*   **Specific Algebraic State**: While the global requirements mandate algebraic design, vshapes specifically requires that `AnalysisResults` from the `stream` package be mergeable.

## 3. Data Formats

### 3.1 Serialization
*   **JSON Format**: Models must be serializable to JSON with format optimization:
    *   **Compact**: Use a summarized format for isomorphic models (store params once).
    *   **Verbose**: Use a per-dimension array format only for heterogeneous models.