# Vector Space Analysis DAG

This document describes the directed acyclic graph (DAG) of computational dependencies for vector space analysis measures.

## Overview

The analysis system is designed as a DAG where:
- Each node represents a computational measure or intermediate result
- Edges represent dependencies between computations
- Results are cached as JSON artifacts to enable incremental processing
- New measures can be added by extending the DAG

## Current Measures

### Primary Input
- **VectorSpace**: Raw vector data input (float[][] or similar)

### Distance Computations  
- **DistanceMatrix**: Pairwise distances between all vectors
  - Dependencies: VectorSpace
  - Cache: `distances.json`
  - Note: Memory-intensive for large datasets

### Nearest Neighbor Analysis
- **kNN**: k-nearest neighbors for each vector
  - Dependencies: DistanceMatrix
  - Cache: `knn_k{k}.json`
  - Parameters: k (number of neighbors)

### Core Measures

#### LID (Local Intrinsic Dimensionality)
- **LID**: Estimates intrinsic dimensionality using MLE on nearest neighbors
  - Dependencies: kNN
  - Cache: `lid_k{k}.json` 
  - Formula: Based on maximum likelihood estimation of distances to k nearest neighbors
  - Output: Per-vector LID values + summary statistics

#### Margin (Nearest-Neighbor Margin)
- **Margin**: Distance-based separability measure
  - Dependencies: kNN
  - Cache: `margin_k{k}.json`
  - Formula: Ratio of distances to nearest different-class vs same-class neighbors  
  - Output: Per-vector margin values + summary statistics
  - Note: Requires class labels

#### Hubness (Reverse-kNN In-degree Skew)
- **Hubness**: Measures concentration of reverse k-nearest neighbor relationships
  - Dependencies: kNN
  - Cache: `hubness_k{k}.json`
  - Formula: Skewness of reverse-kNN in-degree distribution
  - Output: Hub scores, anti-hub scores, skewness metrics

### Summary Reports
- **VectorSpaceReport**: Comprehensive analysis combining all measures
  - Dependencies: LID, Margin, Hubness
  - Cache: `analysis_report.json`
  - Output: Combined statistical summary and recommendations

## DAG Visualization

```
VectorSpace
    |
    v
DistanceMatrix
    |
    v
kNN(k)
 |  |  |
 v  v  v
LID Margin Hubness
 |  |  |
 v  v  v
VectorSpaceReport
```

## Implementation Notes

1. **Incremental Processing**: Each computation checks for cached results before executing
2. **Parameter Variants**: Different parameter values (e.g., k) create separate cache entries
3. **Memory Management**: Large intermediate results (DistanceMatrix) may use disk caching
4. **Extensibility**: New measures add nodes to the DAG with declared dependencies

## Future Extensions

Potential future measures to add:
- **Clustering Coefficient**: Local connectivity measure
- **Fractal Dimension**: Alternative dimensionality estimate  
- **Concentration**: Distribution concentration metrics
- **Stability**: Perturbation analysis of neighbor relationships