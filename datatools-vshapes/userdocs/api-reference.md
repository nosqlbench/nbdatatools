# API Reference

Complete API documentation for the Vector Space Analysis module.

## Core Interfaces

### VectorSpace

The main interface for providing vector data to the analysis system.

```java
public interface VectorSpace {
    String getId();
    int getVectorCount();
    int getDimension();
    float[] getVector(int index);
    float[][] getAllVectors();
    Optional<Integer> getClassLabel(int index);
    default boolean hasClassLabels();
}
```

**Methods:**
- `getId()`: Unique identifier for caching purposes
- `getVectorCount()`: Number of vectors in the dataset
- `getDimension()`: Dimensionality of each vector
- `getVector(int)`: Get a specific vector by index
- `getAllVectors()`: Get all vectors as 2D array
- `getClassLabel(int)`: Get class label for a vector (if available)
- `hasClassLabels()`: Check if class labels are available

### AnalysisMeasure<T>

Base interface for all analysis measures.

```java
public interface AnalysisMeasure<T> {
    String getMnemonic();
    String[] getDependencies();
    T compute(VectorSpace vectorSpace, Path cacheDir, Map<String, Object> dependencyResults);
}
```

**Methods:**
- `getMnemonic()`: Short identifier for the measure
- `getDependencies()`: List of required dependency measures
- `compute()`: Perform the actual computation

## Main Classes

### VectorSpaceAnalyzer

Main orchestrator for vector space analysis.

```java
public class VectorSpaceAnalyzer {
    // Constructors
    public VectorSpaceAnalyzer()
    public VectorSpaceAnalyzer(Path cacheDir)
    
    // Analysis methods
    public AnalysisReport analyzeVectorSpace(VectorSpace vectorSpace)
    public void registerMeasure(AnalysisMeasure<?> measure)
    
    // Result accessors
    public <T> T getResult(String mnemonic, Class<T> resultClass)
    public LIDMeasure.LIDResult getLIDResult()
    public MarginMeasure.MarginResult getMarginResult()
    public HubnessMeasure.HubnessResult getHubnessResult()
    
    // Cache management
    public Path getCacheDirectory()
    public void clearCache()
}
```

### VectorSpaceAnalysisUtils

Utility methods for common analysis tasks.

```java
public final class VectorSpaceAnalysisUtils {
    // Quick analysis methods
    public static String analyzeAndReport(VectorSpace vectorSpace)
    public static void analyzeAndSaveReport(VectorSpace vectorSpace, Path outputPath)
    public static AnalysisReport analyzeWithCache(VectorSpace vectorSpace, Path cacheDir)
    
    // Report formatting
    public static String toCsvReport(AnalysisReport report)
    public static String toJsonReport(AnalysisReport report)
    public static String interpretResults(AnalysisReport report)
}
```

## Analysis Measures

### LIDMeasure

Local Intrinsic Dimensionality analysis using Maximum Likelihood Estimation.

```java
public class LIDMeasure extends AbstractAnalysisMeasure<LIDMeasure.LIDResult> {
    // Constructors
    public LIDMeasure()        // Uses k=20
    public LIDMeasure(int k)   // Custom k value
    
    // Result class
    public static class LIDResult {
        public final double[] lidValues;
        public final VectorUtils.Statistics statistics;
        public final int k;
        
        public double getLID(int index)
        public int getVectorCount()
        public VectorUtils.Statistics getStatistics()
    }
}
```

### MarginMeasure

Nearest-neighbor margin analysis for class separability.

```java
public class MarginMeasure extends AbstractAnalysisMeasure<MarginMeasure.MarginResult> {
    // Constructor
    public MarginMeasure()
    
    // Result class
    public static class MarginResult {
        public final double[] marginValues;
        public final VectorUtils.Statistics statistics;
        public final int validCount;
        
        public double getMargin(int index)
        public boolean isValidMargin(int index)
        public int getVectorCount()
        public int getValidCount()
        public double getValidFraction()
        public VectorUtils.Statistics getStatistics()
    }
}
```

### HubnessMeasure

Hubness analysis based on reverse k-nearest neighbor in-degree distribution.

```java
public class HubnessMeasure extends AbstractAnalysisMeasure<HubnessMeasure.HubnessResult> {
    // Constructors
    public HubnessMeasure()        // Uses k=10
    public HubnessMeasure(int k)   // Custom k value
    
    // Result class
    public static class HubnessResult {
        public final int[] inDegrees;
        public final double[] hubnessScores;
        public final VectorUtils.Statistics inDegreeStats;
        public final VectorUtils.Statistics hubnessStats;
        public final double skewness;
        public final int hubCount;
        public final int antiHubCount;
        public final int k;
        
        public int getInDegree(int index)
        public double getHubnessScore(int index)
        public boolean isHub(int index)
        public boolean isAntiHub(int index)
        public int getVectorCount()
        public double getSkewness()
        public double getHubFraction()
        public double getAntiHubFraction()
    }
}
```

## Utility Classes

### VectorUtils

Utility methods for vector computations.

```java
public final class VectorUtils {
    // Distance calculations
    public static double euclideanDistance(float[] a, float[] b)
    public static double squaredEuclideanDistance(float[] a, float[] b)
    
    // Nearest neighbor searches
    public static int[] findKNearestNeighbors(float[] queryVector, VectorSpace vectorSpace, int k, int excludeIndex)
    public static int[][] computeAllKNN(VectorSpace vectorSpace, int k)
    
    // Statistics
    public static Statistics computeStatistics(double[] values)
    
    // Statistics result class
    public static class Statistics {
        public final double mean;
        public final double stdDev;
        public final double min;
        public final double max;
    }
}
```

## Report Classes

### AnalysisReport

Container for analysis results with formatting capabilities.

```java
public static class AnalysisReport {
    public final VectorSpace vectorSpace;
    public final Map<String, Object> results;
    
    // Result accessors
    public <T> T getResult(String mnemonic, Class<T> resultClass)
    
    // Formatting
    public String getSummary()
    public String toString()  // Same as getSummary()
}
```

## Exception Handling

### Common Exceptions

- `IllegalArgumentException`: Invalid parameters or missing requirements
- `IndexOutOfBoundsException`: Invalid vector indices
- `IOException`: File system operations for caching

### Error Conditions

- **Missing class labels**: MarginMeasure requires class labels
- **Insufficient data**: Some measures need minimum data sizes
- **Cache errors**: Non-critical caching failures are logged but don't stop computation
- **Invalid vectors**: NaN or infinite values in input data

## Configuration

### Cache Settings

```java
// Default cache location
VectorSpaceAnalyzer analyzer = new VectorSpaceAnalyzer();  // Uses temp directory

// Custom cache location  
Path customCache = Paths.get("/path/to/cache");
VectorSpaceAnalyzer analyzer = new VectorSpaceAnalyzer(customCache);

// Clear cache
analyzer.clearCache();
```

### Custom K-values

```java
// Custom LID with k=50
LIDMeasure customLID = new LIDMeasure(50);
analyzer.registerMeasure(customLID);

// Custom Hubness with k=5
HubnessMeasure customHubness = new HubnessMeasure(5);
analyzer.registerMeasure(customHubness);
```

### Memory Management

For large datasets, consider:
- Batch processing of vectors
- Increased JVM heap size
- Custom VectorSpace implementations that load data on demand

## Thread Safety

- **VectorSpaceAnalyzer**: Not thread-safe, create separate instances per thread
- **Measure classes**: Stateless, safe for concurrent use
- **Result classes**: Immutable after creation, safe for sharing
- **Utility methods**: All static methods are thread-safe

## Performance Considerations

### Computational Complexity

- **LID**: O(n²) for distance computation, O(n log n) for k-NN
- **Margin**: O(n²) for pairwise distances
- **Hubness**: O(n²) for k-NN computation

### Memory Usage

- **Distance matrices**: O(n²) space if cached
- **k-NN results**: O(n × k) space
- **Vector storage**: O(n × d) space

### Optimization Tips

- Use caching for repeated analysis
- Consider dimensionality reduction for very high dimensions
- Process large datasets in batches if memory is limited

## Examples

See the [examples directory](examples/) for complete working examples of:
- Basic vector space implementation
- Custom analysis measures
- Batch processing
- Integration with machine learning workflows