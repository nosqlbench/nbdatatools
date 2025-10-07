# Troubleshooting Guide

Common issues and solutions when using the Vector Space Analysis module.

## Installation Issues

### ClassNotFoundException: Missing Dependencies

**Error:**
```
java.lang.ClassNotFoundException: com.fasterxml.jackson.databind.ObjectMapper
```

**Solution:**
Ensure all required dependencies are in your classpath:

```xml
<dependency>
    <groupId>io.nosqlbench</groupId>
    <artifactId>vshapes</artifactId>
    <version>${nosqlbench.version}</version>
</dependency>
```

Jackson is automatically included as a transitive dependency.

### Visualization Dependencies Missing

**Error:**
```
Warning: Jzy3D visualization dependencies not available
```

**Solution:**
Add Jzy3D dependencies for visualization features:

```xml
<dependency>
    <groupId>org.jzy3d</groupId>
    <artifactId>jzy3d-api</artifactId>
    <version>2.2.1</version>
</dependency>
<dependency>
    <groupId>org.jzy3d</groupId>
    <artifactId>jzy3d-native-jogl-awt</artifactId>
    <version>2.2.1</version>
</dependency>
```

## Runtime Errors

### IllegalArgumentException: "Margin measure requires class labels"

**Error:**
```
java.lang.IllegalArgumentException: Margin measure requires class labels
```

**Cause:** The MarginMeasure requires class labels, but your VectorSpace returns empty optionals.

**Solutions:**

1. **Provide class labels:**
```java
@Override
public Optional<Integer> getClassLabel(int index) {
    return Optional.of(classLabels[index]); // Return actual labels
}
```

2. **Skip margin analysis:**
```java
VectorSpaceAnalyzer analyzer = new VectorSpaceAnalyzer();
// Remove default measures and add only what you need
analyzer.clearCache(); // This doesn't remove measures, need custom approach

// Or catch the exception
try {
    VectorSpaceAnalyzer.AnalysisReport report = analyzer.analyzeVectorSpace(vectorSpace);
} catch (IllegalArgumentException e) {
    if (e.getMessage().contains("class labels")) {
        System.out.println("Skipping margin analysis - no class labels available");
    }
}
```

3. **Create analyzer without margin measure:**
```java
VectorSpaceAnalyzer analyzer = new VectorSpaceAnalyzer();
// Currently no direct way to remove default measures
// This is a limitation that could be addressed in future versions
```

### OutOfMemoryError

**Error:**
```
java.lang.OutOfMemoryError: Java heap space
```

**Causes & Solutions:**

1. **Increase heap size:**
```bash
java -Xmx8g MyAnalysisApp
```

2. **Use data sampling:**
```java
// Analyze a sample first
VectorSpace sample = createSample(largeDataset, 5000);
VectorSpaceAnalyzer.AnalysisReport report = analyzer.analyzeVectorSpace(sample);
```

3. **Implement streaming VectorSpace:**
```java
public class StreamingVectorSpace implements VectorSpace {
    @Override
    public float[] getVector(int index) {
        return loadVectorFromDisk(index); // Load on demand
    }
}
```

### IndexOutOfBoundsException

**Error:**
```
java.lang.IndexOutOfBoundsException: Index: 1500, Size: 1000
```

**Causes:**

1. **Inconsistent vector count:**
```java
@Override
public int getVectorCount() {
    return vectors.length; // Make sure this matches actual data
}
```

2. **Class labels array size mismatch:**
```java
@Override
public Optional<Integer> getClassLabel(int index) {
    if (index >= 0 && index < classLabels.length) {
        return Optional.of(classLabels[index]);
    }
    return Optional.empty(); // Safe fallback
}
```

## Data Quality Issues

### NaN or Infinite Values in Results

**Symptoms:**
- LID values showing as NaN
- Margin calculations producing infinite values
- Hubness scores are invalid

**Causes & Solutions:**

1. **Check input vectors:**
```java
public boolean validateVectorSpace(VectorSpace space) {
    for (int i = 0; i < space.getVectorCount(); i++) {
        float[] vector = space.getVector(i);
        for (float value : vector) {
            if (!Float.isFinite(value)) {
                System.err.printf("Invalid value at vector %d: %f\n", i, value);
                return false;
            }
        }
    }
    return true;
}
```

2. **Clean data before analysis:**
```java
public VectorSpace cleanVectorSpace(VectorSpace original) {
    List<float[]> cleanVectors = new ArrayList<>();
    List<Integer> cleanLabels = new ArrayList<>();
    
    for (int i = 0; i < original.getVectorCount(); i++) {
        float[] vector = original.getVector(i);
        boolean isValid = Arrays.stream(vector).allMatch(Float::isFinite);
        
        if (isValid) {
            cleanVectors.add(vector);
            original.getClassLabel(i).ifPresent(cleanLabels::add);
        }
    }
    
    return new MyVectorSpace(cleanVectors.toArray(new float[0][]), 
                           cleanLabels.stream().mapToInt(i -> i).toArray());
}
```

### All Vectors Are Identical

**Symptoms:**
- LID values are all 0 or NaN
- Distance-based measures fail
- Analysis produces degenerate results

**Solutions:**

1. **Check for constant vectors:**
```java
public boolean hasVariation(VectorSpace space) {
    float[] first = space.getVector(0);
    for (int i = 1; i < space.getVectorCount(); i++) {
        float[] current = space.getVector(i);
        if (!Arrays.equals(first, current)) {
            return true;
        }
    }
    return false;
}
```

2. **Check for adequate spacing:**
```java
public double computeMinimumDistance(VectorSpace space) {
    double minDist = Double.POSITIVE_INFINITY;
    int n = space.getVectorCount();
    
    for (int i = 0; i < n; i++) {
        for (int j = i + 1; j < n; j++) {
            double dist = VectorUtils.euclideanDistance(
                space.getVector(i), space.getVector(j));
            if (dist > 0 && dist < minDist) {
                minDist = dist;
            }
        }
    }
    
    return minDist;
}
```

## Performance Issues

### Analysis Takes Too Long

**For small datasets (<1000 vectors):**
- Should complete in seconds
- Check for infinite loops in VectorSpace implementation
- Verify vector dimensions are reasonable

**For large datasets:**
- See [Performance Guide](performance-guide.md)
- Use caching: `new VectorSpaceAnalyzer(cacheDir)`
- Consider sampling or dimensionality reduction

### Cache Issues

**Cache not working:**

1. **Check cache directory permissions:**
```java
Path cacheDir = Paths.get("/path/to/cache");
if (!Files.exists(cacheDir)) {
    try {
        Files.createDirectories(cacheDir);
        System.out.println("Created cache directory: " + cacheDir);
    } catch (IOException e) {
        System.err.println("Cannot create cache directory: " + e.getMessage());
    }
}
```

2. **Verify stable vector space IDs:**
```java
@Override
public String getId() {
    // Must be consistent across runs for caching to work
    return "dataset-v1-sha256"; // Use content hash or version
}
```

3. **Clear corrupted cache:**
```java
analyzer.clearCache(); // Clears memory cache and disk files
```

## Visualization Issues

### "Visualization not available" Warning

**Check dependencies:**
```java
if (VectorSpaceVisualization.isVisualizationAvailable()) {
    // Create visualizations
} else {
    System.out.println("Add Jzy3D dependencies for visualization");
}
```

### Blank or Corrupted Visualizations

1. **Check data range:**
```java
// Ensure vectors have reasonable coordinate ranges
float[] mins = new float[space.getDimension()];
float[] maxs = new float[space.getDimension()];
Arrays.fill(mins, Float.POSITIVE_INFINITY);
Arrays.fill(maxs, Float.NEGATIVE_INFINITY);

for (int i = 0; i < space.getVectorCount(); i++) {
    float[] vector = space.getVector(i);
    for (int d = 0; d < vector.length; d++) {
        mins[d] = Math.min(mins[d], vector[d]);
        maxs[d] = Math.max(maxs[d], vector[d]);
    }
}

System.out.println("Data ranges: " + Arrays.toString(mins) + " to " + Arrays.toString(maxs));
```

2. **Platform issues:**
```java
// On headless systems, save to file instead of displaying
Object chart = VectorSpaceVisualization.createLIDScatterPlot(...);
VectorSpaceVisualization.saveChartAsImage(chart, Paths.get("output.png"), 800, 600);
```

### OpenGL/Graphics Issues

**Linux systems:**
```bash
# Install required OpenGL libraries
sudo apt-get install libgl1-mesa-glx libglu1-mesa

# For remote systems, enable X11 forwarding
ssh -X user@hostname
```

**macOS systems:**
```bash
# Usually works out of the box, but if issues occur:
# Ensure Java has proper graphics acceleration
java -Djava.awt.headless=false MyApp
```

## Integration Issues

### Maven Dependency Conflicts

**Problem:** Version conflicts with Jackson or other dependencies

**Solution:**
```xml
<dependency>
    <groupId>io.nosqlbench</groupId>
    <artifactId>vshapes</artifactId>
    <version>${nosqlbench.version}</version>
    <exclusions>
        <exclusion>
            <groupId>com.fasterxml.jackson.core</groupId>
            <artifactId>jackson-databind</artifactId>
        </exclusion>
    </exclusions>
</dependency>

<!-- Use your preferred Jackson version -->
<dependency>
    <groupId>com.fasterxml.jackson.core</groupId>
    <artifactId>jackson-databind</artifactId>
    <version>2.15.2</version>
</dependency>
```

### Thread Safety Issues

**Problem:** Concurrent access to analyzer

**Solution:**
```java
// Create separate analyzers per thread
public class ThreadSafeAnalysis {
    private final ThreadLocal<VectorSpaceAnalyzer> analyzers = 
        ThreadLocal.withInitial(() -> new VectorSpaceAnalyzer(cacheDir));
    
    public AnalysisReport analyze(VectorSpace space) {
        return analyzers.get().analyzeVectorSpace(space);
    }
}
```

## Debugging Tips

### Enable Detailed Logging

```java
// Add logging to your VectorSpace implementation
public class DebuggingVectorSpace implements VectorSpace {
    @Override
    public float[] getVector(int index) {
        System.out.printf("Getting vector %d\n", index);
        return vectors[index];
    }
    
    @Override
    public Optional<Integer> getClassLabel(int index) {
        Optional<Integer> label = /* your logic */;
        System.out.printf("Vector %d label: %s\n", index, label);
        return label;
    }
}
```

### Validate Analysis Results

```java
public void validateResults(VectorSpaceAnalyzer.AnalysisReport report) {
    // Check LID results
    LIDMeasure.LIDResult lid = report.getResult("LID", LIDMeasure.LIDResult.class);
    if (lid != null) {
        System.out.printf("LID: mean=%.2f, std=%.2f, range=[%.2f, %.2f]\n",
                         lid.statistics.mean, lid.statistics.stdDev,
                         lid.statistics.min, lid.statistics.max);
        
        // Check for reasonable values
        if (lid.statistics.mean < 0) {
            System.err.println("Warning: Negative LID values detected");
        }
    }
    
    // Similar checks for other measures...
}
```

### Memory Usage Monitoring

```java
public void monitorMemory(String phase) {
    Runtime runtime = Runtime.getRuntime();
    long totalMemory = runtime.totalMemory();
    long freeMemory = runtime.freeMemory();
    long usedMemory = totalMemory - freeMemory;
    
    System.out.printf("%s: Used=%.1fMB, Free=%.1fMB, Total=%.1fMB\n",
                     phase,
                     usedMemory / 1024.0 / 1024.0,
                     freeMemory / 1024.0 / 1024.0,
                     totalMemory / 1024.0 / 1024.0);
}
```

## Getting Help

### Gathering Debug Information

When reporting issues, include:

1. **System information:**
```java
System.out.println("Java version: " + System.getProperty("java.version"));
System.out.println("OS: " + System.getProperty("os.name"));
System.out.println("Available memory: " + Runtime.getRuntime().maxMemory() / 1024 / 1024 + "MB");
```

2. **Dataset characteristics:**
```java
System.out.println("Vector count: " + vectorSpace.getVectorCount());
System.out.println("Dimensions: " + vectorSpace.getDimension());
System.out.println("Has class labels: " + vectorSpace.hasClassLabels());
```

3. **Error stacktrace and relevant code**

### Common Solutions Checklist

- [ ] All dependencies are included
- [ ] Vector data contains finite values only
- [ ] VectorSpace.getId() returns consistent values
- [ ] Sufficient memory allocated (-Xmx flag)
- [ ] Cache directory is writable
- [ ] Class labels provided if using MarginMeasure
- [ ] Graphics libraries available for visualization

### Performance Checklist

- [ ] Using caching for repeated analysis
- [ ] Appropriate k-values for dataset size
- [ ] Dimensionality reduction applied if needed
- [ ] Memory usage monitored and optimized
- [ ] Sample analysis performed for large datasets

If problems persist, check the [GitHub issues](https://github.com/nosqlbench/nbdatatools/issues) or create a new issue with the debug information above.