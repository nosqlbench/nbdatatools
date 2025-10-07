# Core Concepts in Vector Space Analysis

This document explains the fundamental concepts and measures used in the vector space analysis module.

## Overview

The vshapes module analyzes high-dimensional vector datasets to understand their intrinsic properties, focusing on three key aspects:

1. **Intrinsic Dimensionality** - How many dimensions the data actually uses
2. **Class Separability** - How well different classes can be distinguished  
3. **Hubness Phenomena** - Whether some points dominate similarity searches

## Local Intrinsic Dimensionality (LID)

### What is LID?

Local Intrinsic Dimensionality estimates the number of dimensions needed to represent data locally around each point. While your data might live in a 1000-dimensional space, the LID might reveal it actually lies on a 3-dimensional manifold.

### How LID Works

LID uses Maximum Likelihood Estimation based on distances to k-nearest neighbors:

```
LID = -1 / (1/k * Σ(ln(r_i / r_k)))
```

Where:
- `r_i` are distances to the k nearest neighbors (sorted)
- `r_k` is the distance to the k-th nearest neighbor
- Higher LID values indicate higher intrinsic dimensionality

### Interpreting LID Results

- **Low LID (< 10% of ambient dimension)**: Data lies on a low-dimensional manifold
- **High LID (> 80% of ambient dimension)**: Data fills the ambient space uniformly
- **Moderate LID**: Data has structured but complex relationships
- **High variance in LID**: Non-uniform data density

### Example Output

```
Local Intrinsic Dimensionality (LID):
  Mean: 2.35 ± 0.82 (std dev)
  Range: 1.12 to 4.67
```

This suggests the data primarily lies on a ~2-3 dimensional manifold within the higher-dimensional space.

## Nearest-Neighbor Margin

### What is Margin?

The margin measures how well-separated different classes are by comparing distances to same-class vs. different-class nearest neighbors.

### How Margin Works

For each point, the margin is:

```
Margin = distance_to_nearest_different_class / distance_to_nearest_same_class
```

- **Margin > 1**: Point is closer to same-class neighbors (good separation)
- **Margin < 1**: Point is closer to different-class neighbors (poor separation)
- **Margin ≈ 1**: Ambiguous class boundary

### Interpreting Margin Results

- **High margin (> 2.0)**: Well-separated classes, classification should be easy
- **Low margin (< 1.2)**: Overlapping classes, classification will be challenging
- **Low valid fraction**: Many points lack clear class separation

### Example Output

```
Nearest-Neighbor Margin:
  Mean: 1.847 ± 0.623 (std dev)
  Range: 0.234 to 4.112
  Valid Margins: 847/1000 (84.7%)
```

This indicates reasonably good class separation, with most points having clear class membership.

## Hubness Analysis

### What is Hubness?

Hubness is a phenomenon where certain points (hubs) appear frequently in the k-nearest neighbor lists of other points, while others (anti-hubs) rarely appear. This becomes more pronounced in high dimensions.

### How Hubness Works

1. **Compute k-NN for all points**
2. **Count in-degrees**: How often each point appears in others' k-NN lists
3. **Standardize scores**: Convert to z-scores relative to mean/std
4. **Measure skewness**: Positive skewness indicates hubness

### Key Metrics

- **Skewness**: Distribution shape of in-degree counts
- **Hub count**: Points with standardized score > 2.0
- **Anti-hub count**: Points with standardized score < -2.0
- **In-degree statistics**: Mean and variation of neighbor list appearances

### Interpreting Hubness Results

- **Positive skewness (> 1.0)**: Strong hubness present
- **High hub fraction (> 10%)**: Many dominant points
- **High anti-hub fraction (> 10%)**: Many isolated regions
- **Low skewness**: Relatively uniform neighbor distribution

### Example Output

```
Hubness Analysis:
  Skewness: 1.234
  Hubs: 45 (4.5%)
  Anti-hubs: 23 (2.3%)
  In-degree mean: 10.0 ± 8.7
```

This shows moderate hubness that could affect similarity-based algorithms.

## Computational Dependencies

The analysis measures have the following dependencies:

```
LID ────┐
        ├─→ Final Report
Margin ─┤
        │
Hubness ─┘
```

All three measures can be computed independently, making the analysis efficient and allowing for partial results if one measure fails.

## Statistical Summaries

Each measure provides comprehensive statistics:

- **Mean**: Average value across all data points
- **Standard Deviation**: Measure of variability
- **Min/Max**: Range of values observed
- **Distribution shape**: Skewness and other moments where relevant

## Use Cases by Measure

### LID Applications
- **Dimensionality reduction**: Decide if/how much to reduce dimensions
- **Algorithm selection**: Choose methods appropriate for intrinsic dimensionality
- **Data understanding**: Identify manifold structure

### Margin Applications
- **Classification difficulty**: Predict how hard classification will be
- **Feature engineering**: Identify where better features are needed
- **Dataset quality**: Assess labeling consistency

### Hubness Applications
- **Similarity search**: Understand potential biases in nearest-neighbor methods
- **Clustering**: Identify natural cluster centers (hubs) and outliers (anti-hubs)
- **Recommendation systems**: Account for popularity biases

## Theoretical Background

### Curse of Dimensionality

As dimensions increase:
- Distances become more uniform
- Nearest neighbors become less meaningful
- Hub phenomena emerge
- Volume concentrates near surface of hypersphere

### Manifold Hypothesis

Many high-dimensional datasets lie on or near lower-dimensional manifolds. LID helps quantify this by measuring local dimensionality rather than global embedding dimension.

### Class Separability Theory

The margin measure is related to the margin in Support Vector Machines and provides insights into the fundamental separability of classes in the feature space.

## Configuration Options

### K-values
- **LID default k=20**: Balances local vs. global estimation
- **Hubness default k=10**: Standard for hubness analysis
- **Custom k-values**: Can be configured per measure

### Thresholds
- **Hub threshold**: ±2.0 standard deviations (configurable)
- **Margin validity**: Finite, positive distances required
- **Cache settings**: JSON artifacts for expensive computations

## Next Steps

- See [analysis measures](analysis-measures.md) for detailed mathematical formulations
- Check [performance guide](performance-guide.md) for optimization strategies  
- Browse [examples](examples/) for practical applications