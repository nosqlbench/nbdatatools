# Getting Started with Vector Space Analysis

This guide will walk you through setting up and performing your first vector space analysis using the vshapes module.

## Installation

Add the vshapes module to your Maven project:

```xml
<dependency>
    <groupId>io.nosqlbench</groupId>
    <artifactId>vshapes</artifactId>
    <version>${nosqlbench.version}</version>
</dependency>
```

## Basic Usage

### Step 1: Implement VectorSpace

First, you need to implement the `VectorSpace` interface to provide your data:

```java
import io.nosqlbench.vshapes.VectorSpace;
import java.util.Optional;

public class MyVectorSpace implements VectorSpace {
    private final float[][] vectors;
    private final int[] classLabels; // optional
    
    public MyVectorSpace(float[][] vectors, int[] classLabels) {
        this.vectors = vectors;
        this.classLabels = classLabels;
    }
    
    @Override
    public String getId() {
        return "my-dataset-v1";
    }
    
    @Override
    public int getVectorCount() {
        return vectors.length;
    }
    
    @Override
    public int getDimension() {
        return vectors[0].length;
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
        return classLabels != null ? Optional.of(classLabels[index]) : Optional.empty();
    }
}
```

### Step 2: Perform Analysis

```java
import io.nosqlbench.vshapes.VectorSpaceAnalyzer;

// Create your vector space
float[][] data = loadYourData(); // your data loading logic
int[] labels = loadYourLabels();   // optional class labels
VectorSpace vectorSpace = new MyVectorSpace(data, labels);

// Create analyzer and run analysis
VectorSpaceAnalyzer analyzer = new VectorSpaceAnalyzer();
VectorSpaceAnalyzer.AnalysisReport report = analyzer.analyzeVectorSpace(vectorSpace);

// Display results
System.out.println(report.getSummary());
```

### Step 3: Access Specific Results

```java
// Get LID analysis results
LIDMeasure.LIDResult lidResult = analyzer.getLIDResult();
if (lidResult != null) {
    System.out.printf("Average LID: %.2f ± %.2f\n", 
                     lidResult.statistics.mean, 
                     lidResult.statistics.stdDev);
}

// Get margin analysis results (requires class labels)
MarginMeasure.MarginResult marginResult = analyzer.getMarginResult();
if (marginResult != null) {
    System.out.printf("Class separability (margin): %.3f\n", 
                     marginResult.statistics.mean);
}

// Get hubness analysis results
HubnessMeasure.HubnessResult hubnessResult = analyzer.getHubnessResult();
if (hubnessResult != null) {
    System.out.printf("Hubness skewness: %.3f\n", hubnessResult.skewness);
    System.out.printf("Hub points: %d (%.1f%%)\n", 
                     hubnessResult.hubCount, 
                     hubnessResult.getHubFraction() * 100);
}
```

## Example: Complete Analysis

Here's a complete example analyzing a simple 2D dataset:

```java
import io.nosqlbench.vshapes.*;

public class SimpleExample {
    public static void main(String[] args) {
        // Create sample data: two clusters
        float[][] vectors = {
            {0.0f, 0.0f}, {0.1f, 0.1f}, {0.2f, 0.0f},  // Class 0
            {3.0f, 3.0f}, {3.1f, 3.2f}, {2.9f, 3.1f}   // Class 1
        };
        int[] labels = {0, 0, 0, 1, 1, 1};
        
        // Create vector space
        VectorSpace space = new MyVectorSpace(vectors, labels);
        
        // Analyze
        VectorSpaceAnalyzer analyzer = new VectorSpaceAnalyzer();
        VectorSpaceAnalyzer.AnalysisReport report = analyzer.analyzeVectorSpace(space);
        
        // Display summary
        System.out.println(report.getSummary());
        
        // Get interpretation
        String interpretation = VectorSpaceAnalysisUtils.interpretResults(report);
        System.out.println(interpretation);
    }
}
```

## Using Utility Methods

The `VectorSpaceAnalysisUtils` class provides convenient methods for common tasks:

```java
// Quick analysis with default settings
String summary = VectorSpaceAnalysisUtils.analyzeAndReport(vectorSpace);

// Save report to file
Path outputFile = Paths.get("analysis-report.txt");
VectorSpaceAnalysisUtils.analyzeAndSaveReport(vectorSpace, outputFile);

// Get CSV format
VectorSpaceAnalyzer.AnalysisReport report = /* ... */;
String csvReport = VectorSpaceAnalysisUtils.toCsvReport(report);

// Get JSON format
String jsonReport = VectorSpaceAnalysisUtils.toJsonReport(report);
```

## Next Steps

- Learn about the [core concepts](core-concepts.md) behind each analysis measure
- Explore different [output formats](output-formats.md) for your reports
- Check out the [performance guide](performance-guide.md) for optimizing large datasets
- Browse practical [examples](examples/) for specific use cases

## Common Issues

### ClassNotFoundException
Make sure all dependencies are properly included in your classpath.

### OutOfMemoryError
For large datasets, consider increasing JVM heap size or processing data in batches. See the [performance guide](performance-guide.md) for optimization strategies.

### IllegalArgumentException: "Margin measure requires class labels"
The Margin analysis requires class labels. Either provide labels in your VectorSpace implementation or remove the MarginMeasure from the analyzer.

For more troubleshooting help, see the [troubleshooting guide](troubleshooting.md).