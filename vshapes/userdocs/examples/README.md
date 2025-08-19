# Vector Space Analysis Examples

This directory contains practical examples demonstrating different use cases for the vshapes module.

## Available Examples

### BasicUsageExample.java
Demonstrates fundamental usage:
- Creating a simple vector space
- Performing analysis with default settings
- Accessing different output formats
- Working with analysis results

**To run:**
```bash
javac -cp "vshapes.jar:." BasicUsageExample.java
java -cp "vshapes.jar:." examples.BasicUsageExample
```

## Running the Examples

### Prerequisites
- Java 21+
- vshapes.jar and dependencies on classpath
- For visualization examples: Jzy3D dependencies

### Maven Setup
```xml
<dependency>
    <groupId>io.nosqlbench</groupId>
    <artifactId>vshapes</artifactId>
    <version>${nosqlbench.version}</version>
</dependency>
```

### Example Output

When you run BasicUsageExample, you should see output similar to:

```
=== BASIC VECTOR SPACE ANALYSIS ===

Analyzing 12 vectors...

Vector Space Analysis Report
===========================
Vector Space: example-clusters  
Vectors: 12, Dimensions: 3
Has Class Labels: true

Local Intrinsic Dimensionality (LID):
  Mean: 2.15 ± 0.34 (std dev)
  Range: 1.78 to 2.87

Nearest-Neighbor Margin:
  Mean: 15.23 ± 2.11 (std dev)  
  Range: 12.45 to 18.92
  Valid Margins: 12/12 (100.0%)

Hubness Analysis:
  Skewness: -0.12
  Hubs: 0 (0.0%)
  Anti-hubs: 0 (0.0%) 
  In-degree mean: 3.3 ± 1.8

=== OUTPUT FORMATS DEMONSTRATION ===

CSV Format:
----------
Metric,Value
VectorSpaceId,example-clusters
VectorCount,12
Dimension,3
HasClassLabels,true
LID_Mean,2.1534
...

JSON Format (excerpt):
---------------------
{
  "vectorSpaceId": "example-clusters",
  "vectorCount": 12,
  "dimension": 3,
  "hasClassLabels": true,
  "results": {
    "LID": {
      "mean": 2.1534,
...

Interpretation:
--------------
Vector Space Analysis Interpretation
===================================

Dataset: example-clusters (12 vectors, 3 dimensions)

Local Intrinsic Dimensionality (LID):
- Moderate intrinsic dimensionality suggests structured but complex data

Class Separability (Margin): 
- High margin values indicate well-separated classes

Hubness Analysis:
- Low skewness suggests relatively uniform neighbor distribution
```

## Creating Your Own Examples

### Template Structure
```java
package examples;

import io.nosqlbench.vshapes.*;
import java.util.Optional;

public class MyExample {
    public static void main(String[] args) {
        // 1. Create or load your vector space
        VectorSpace vectorSpace = createMyVectorSpace();
        
        // 2. Create analyzer
        VectorSpaceAnalyzer analyzer = new VectorSpaceAnalyzer();
        
        // 3. Perform analysis
        VectorSpaceAnalyzer.AnalysisReport report = analyzer.analyzeVectorSpace(vectorSpace);
        
        // 4. Use results
        System.out.println(report.getSummary());
    }
    
    private static VectorSpace createMyVectorSpace() {
        // Your data loading logic here
        return new MyVectorSpace(/* your data */);
    }
}
```

### VectorSpace Implementation Template
```java
public class MyVectorSpace implements VectorSpace {
    private final String id;
    private final float[][] vectors;
    private final Integer[] labels; // Optional
    
    public MyVectorSpace(String id, float[][] vectors, Integer[] labels) {
        this.id = id;
        this.vectors = vectors;
        this.labels = labels;
    }
    
    @Override
    public String getId() { return id; }
    
    @Override
    public int getVectorCount() { return vectors.length; }
    
    @Override
    public int getDimension() { 
        return vectors.length > 0 ? vectors[0].length : 0; 
    }
    
    @Override
    public float[] getVector(int index) { 
        return vectors[index].clone(); 
    }
    
    @Override
    public float[][] getAllVectors() { 
        return vectors.clone(); 
    }
    
    @Override
    public Optional<Integer> getClassLabel(int index) {
        return labels != null ? Optional.ofNullable(labels[index]) : Optional.empty();
    }
}
```

## Common Use Cases

### 1. Dataset Quality Assessment
```java
// Quick assessment of a new dataset
String summary = VectorSpaceAnalysisUtils.analyzeAndReport(myDataset);
System.out.println(summary);

// Check if data is suitable for machine learning
LIDMeasure.LIDResult lid = analyzer.getLIDResult();
if (lid.statistics.mean < myDataset.getDimension() * 0.1) {
    System.out.println("Dataset has low intrinsic dimensionality - good for ML");
}
```

### 2. Feature Engineering Guidance
```java
MarginMeasure.MarginResult margin = analyzer.getMarginResult();
if (margin.statistics.mean < 1.5) {
    System.out.println("Poor class separation - consider feature engineering");
}
```

### 3. Algorithm Selection
```java
HubnessMeasure.HubnessResult hubness = analyzer.getHubnessResult();
if (hubness.skewness > 1.0) {
    System.out.println("High hubness detected - be cautious with k-NN algorithms");
}
```

### 4. Batch Processing
```java
List<VectorSpace> datasets = loadMultipleDatasets();
for (VectorSpace dataset : datasets) {
    VectorSpaceAnalyzer.AnalysisReport report = analyzer.analyzeVectorSpace(dataset);
    
    // Save results
    VectorSpaceAnalysisUtils.analyzeAndSaveReport(
        dataset, 
        Paths.get("results", dataset.getId() + "_analysis.txt")
    );
}
```

## Best Practices

1. **Always validate your data first**
2. **Use consistent IDs for caching**
3. **Handle missing class labels gracefully**
4. **Monitor memory usage for large datasets**
5. **Save results in multiple formats**

## Extending the Examples

Feel free to modify these examples or create new ones. Consider contributing useful examples back to the project!

## Getting Help

- Check the main [documentation](../README.md)
- See [troubleshooting guide](../troubleshooting.md) for common issues
- Review [API reference](../api-reference.md) for detailed method documentation