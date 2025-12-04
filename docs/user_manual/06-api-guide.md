# API Guide

This chapter covers programming with NBDataTools APIs for Java applications. The APIs provide type-safe, asynchronous access to vector datasets.

## Getting Started with APIs

### Maven Dependency

Add NBDataTools to your project:

```xml
<dependency>
    <groupId>io.nosqlbench</groupId>
    <artifactId>nbdatatools-vectordata</artifactId>
    <version>1.0.0</version>
</dependency>
```

### Gradle Dependency

```gradle
implementation 'io.nosqlbench:nbdatatools-vectordata:1.0.0'
```

## Core API Overview

### Main Interfaces

The API is built around these key interfaces:

```java
// High-level dataset access
TestDataView testData = ...;

// Type-specific vector access
DatasetView<float[]> baseVectors = testData.getBaseVectors();
DatasetView<float[]> queryVectors = testData.getQueryVectors();
DatasetView<int[]> neighbors = testData.getNeighborIndices();
DatasetView<float[]> distances = testData.getNeighborDistances();
```

### Type Safety

All vector operations are type-safe:

```java
DatasetView<float[]> floatVectors = ...;  // For embeddings
DatasetView<int[]> intVectors = ...;      // For indices  
DatasetView<double[]> doubleVectors = ...; // For high precision
```

## Loading Datasets

### From HDF5 Files

Load a complete test dataset:

```java
import io.nosqlbench.vectordata.discovery.TestDataView;
import java.nio.file.Paths;

// Load test dataset
TestDataView testData = TestDataView.open(Paths.get("dataset.hdf5"));

// Access components
DatasetView<float[]> baseVectors = testData.getBaseVectors();
DatasetView<float[]> queryVectors = testData.getQueryVectors();

System.out.printf("Loaded %d base vectors of %d dimensions%n", 
    baseVectors.getCount(), baseVectors.getVectorDimensions());
```

### From Individual Components

Load just the base vectors:

```java
import io.nosqlbench.vectordata.spec.datasets.types.FloatVectors;

// Load only base vectors
FloatVectors vectors = FloatVectors.open(Paths.get("vectors.hdf5"), "/base");

System.out.printf("Loaded %d vectors%n", vectors.getCount());
```

### Remote Datasets

Load from URLs:

```java
import java.net.URI;

// Load from HTTP/HTTPS
TestDataView remoteData = TestDataView.open(
    URI.create("https://example.com/dataset.hdf5"));

// The data is downloaded and cached automatically
```

## Unified Dataset Loading with DatasetLoader

The `DatasetLoader` class provides a single, unified API for loading datasets from any source without needing to know the underlying format or location. It automatically detects the dataset type and selects the appropriate implementation.

### Supported Dataset Sources

`DatasetLoader` supports three types of dataset sources:

1. **Remote URLs** - HTTP/HTTPS URLs to remote datasets
2. **Local HDF5 files** - Local .hdf5 or .h5 files
3. **Local filesystem datasets** - Directories containing dataset.yaml

### Basic Usage

```java
import io.nosqlbench.vectordata.discovery.DatasetLoader;
import io.nosqlbench.vectordata.discovery.ProfileSelector;
import io.nosqlbench.vectordata.discovery.TestDataView;

// Load from any source - automatic detection
try (ProfileSelector dataset = DatasetLoader.load("/path/to/dataset")) {
    TestDataView profile = dataset.profile("default");

    FloatVectors baseVectors = profile.getBaseVectors().orElseThrow();
    System.out.printf("Loaded %d vectors%n", baseVectors.getCount());
}
```

### Loading from Different Sources

#### Remote URLs

For remote datasets, `DatasetLoader` uses the `VirtualProfileSelector` infrastructure with on-demand downloading, Merkle tree verification, and local caching:

```java
// Load from remote URL - downloads on-demand with caching
try (ProfileSelector dataset = DatasetLoader.load("https://example.com/datasets/my-vectors/")) {
    TestDataView profile = dataset.profile("default");

    // Data is downloaded and verified automatically as needed
    FloatVectors baseVectors = profile.getBaseVectors().orElseThrow();
}

// With custom cache directory
try (ProfileSelector dataset = DatasetLoader.load(
        "https://example.com/datasets/my-vectors/",
        "/custom/cache/dir")) {
    TestDataView profile = dataset.profile("default");
}
```

**Remote Implementation:** Uses `VirtualProfileSelector` → `VirtualTestDataView` → `MAFileChannel`
- On-demand chunk downloading
- Merkle tree integrity verification
- Automatic local caching
- Efficient for large datasets (only downloads chunks as accessed)

#### Local HDF5 Files

For local HDF5 files, `DatasetLoader` uses the `TestDataGroup` implementation for direct file access:

```java
// Load from HDF5 file
try (ProfileSelector dataset = DatasetLoader.load("/path/to/dataset.hdf5")) {
    TestDataView profile = dataset.profile("default");

    // Direct access to HDF5 file
    FloatVectors baseVectors = profile.getBaseVectors().orElseThrow();
}

// Also works with .h5 extension
ProfileSelector dataset = DatasetLoader.load("/path/to/dataset.h5");

// Tilde expansion supported
ProfileSelector dataset = DatasetLoader.load("~/datasets/my-vectors.hdf5");
```

**HDF5 Implementation:** Uses `TestDataGroup` → `HDF5ProfileDataView`
- Direct HDF5 dataset access
- Supports all HDF5 dataset features
- Profile-based configuration from HDF5 attributes

#### Local Filesystem Datasets

For local directories containing `dataset.yaml`, `DatasetLoader` uses the `FilesystemTestDataGroup` implementation:

```java
// Load from directory with dataset.yaml
try (ProfileSelector dataset = DatasetLoader.load("/path/to/dataset/")) {
    TestDataView profile = dataset.profile("default");

    // Accesses individual vector files (fvec, ivec, etc.)
    FloatVectors baseVectors = profile.getBaseVectors().orElseThrow();
}

// Can also point directly to dataset.yaml
ProfileSelector dataset = DatasetLoader.load("/path/to/dataset/dataset.yaml");
```

**Filesystem Implementation:** Uses `FilesystemTestDataGroup` → `FilesystemTestDataView`
- Direct AsyncFileChannel access to vector files
- Supports xvec formats (fvec, ivec, bvec)
- Profile and windowing configuration via dataset.yaml

### Example dataset.yaml Structure

```yaml
attributes:
  distance_function: COSINE
  license: Apache-2.0
  url: https://github.com/nosqlbench/nbdatatools
  model: text-embedding-3-large
  vendor: OpenAI

profiles:
  default:
    base_vectors: base.fvec
    query_vectors: query.fvec
    neighbor_distances: distances.fvec
    neighbor_indices: indices.ivec

  small:
    base_vectors:
      source: base.fvec
      window: 0..10000
    query_vectors: query.fvec
    neighbor_indices: indices.ivec

  medium:
    base_vectors:
      source: base.fvec
      window: 0..100000
    query_vectors: query.fvec
    neighbor_indices: indices.ivec
```

### Automatic Format Detection

`DatasetLoader` automatically detects the dataset format using this logic:

```
Input is URL (starts with http:// or https://)?
  → Use VirtualProfileSelector (remote access with caching)

Input is directory?
  → Check for dataset.yaml → Use FilesystemTestDataGroup
  → Check for single .hdf5 file → Use TestDataGroup
  → Multiple HDF5 files → Error (ambiguous)
  → No recognizable format → Error

Input is file?
  → Named dataset.yaml → Use FilesystemTestDataGroup (parent dir)
  → Extension .hdf5 or .h5 → Use TestDataGroup
  → Try opening as HDF5 → Use TestDataGroup or Error
```

### Working with Multiple Profiles

All three implementations support multiple profiles:

```java
try (ProfileSelector dataset = DatasetLoader.load("/path/to/dataset")) {
    // Get available profiles
    Set<String> profileNames = dataset.getProfileNames();
    System.out.println("Available profiles: " + profileNames);

    // Load different profiles
    TestDataView smallProfile = dataset.profile("small");
    TestDataView mediumProfile = dataset.profile("medium");
    TestDataView largeProfile = dataset.profile("large");

    // Each profile can have different data ranges
    System.out.printf("Small: %d vectors%n",
        smallProfile.getBaseVectors().orElseThrow().getCount());
    System.out.printf("Medium: %d vectors%n",
        mediumProfile.getBaseVectors().orElseThrow().getCount());
}
```

### Advanced Usage

#### Custom Cache Directory for Remote Datasets

```java
// Specify custom cache directory for remote downloads
try (ProfileSelector dataset = DatasetLoader.load(
        "https://example.com/large-dataset/",
        "/mnt/fast-storage/cache")) {
    TestDataView profile = dataset.profile("default");
}
```

#### Loading from Different Input Types

```java
// From String path
ProfileSelector ds1 = DatasetLoader.load("/path/to/dataset");

// From Path object
Path datasetPath = Path.of("/path/to/dataset");
ProfileSelector ds2 = DatasetLoader.load(datasetPath);

// From URL object
URL datasetUrl = new URL("https://example.com/dataset/");
ProfileSelector ds3 = DatasetLoader.load(datasetUrl);
```

### Implementation Selection Table

| Source Type | Detection | Implementation | Access Method |
|-------------|-----------|----------------|---------------|
| Remote URL | Starts with http:// or https:// | VirtualProfileSelector | MAFileChannel with on-demand download |
| Local HDF5 file | Extension .hdf5/.h5 or HDF5 header | TestDataGroup | Direct HDF5 access |
| Filesystem dataset | dataset.yaml present | FilesystemTestDataGroup | AsyncFileChannel to xvec files |

### Performance Considerations

**Remote URLs (VirtualProfileSelector):**
- Downloads data chunks on-demand
- Best for: Large datasets where you only need portions
- Merkle verification ensures data integrity
- Caching improves subsequent access

**Local HDF5 (TestDataGroup):**
- Direct file access, no download overhead
- Best for: Complete datasets stored locally
- Fast random access to any vector

**Filesystem (FilesystemTestDataGroup):**
- Direct AsyncFileChannel to individual files
- Best for: Datasets with separate vector files
- Flexible windowing and range support

### Complete Example

```java
import io.nosqlbench.vectordata.discovery.DatasetLoader;
import io.nosqlbench.vectordata.discovery.ProfileSelector;
import io.nosqlbench.vectordata.discovery.TestDataView;
import io.nosqlbench.vectordata.spec.datasets.types.FloatVectors;

public class DatasetLoaderExample {
    public static void main(String[] args) {
        // Works with any dataset source
        String datasetSource = args.length > 0 ? args[0] : "~/datasets/my-vectors";

        try (ProfileSelector dataset = DatasetLoader.load(datasetSource)) {
            // List available profiles
            System.out.println("Available profiles: " + dataset.getProfileNames());

            // Load the default profile
            TestDataView profile = dataset.profile("default");

            // Access vectors
            FloatVectors baseVectors = (FloatVectors) profile.getBaseVectors().orElseThrow();
            FloatVectors queryVectors = (FloatVectors) profile.getQueryVectors().orElseThrow();

            System.out.printf("Dataset loaded: %d base vectors, %d query vectors%n",
                baseVectors.getCount(), queryVectors.getCount());

            // Process vectors
            for (int i = 0; i < Math.min(10, queryVectors.getCount()); i++) {
                float[] query = queryVectors.get(i);
                System.out.printf("Query %d: %d dimensions%n", i, query.length);
            }

        } catch (Exception e) {
            System.err.println("Failed to load dataset: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
```

This example works identically whether `datasetSource` is:
- A remote URL: `https://example.com/datasets/my-vectors/`
- A local HDF5 file: `/path/to/dataset.hdf5`
- A local filesystem dataset: `/path/to/dataset/`

## Basic Vector Operations

### Synchronous Access

Get individual vectors:

```java
// Get a single vector
float[] vector = baseVectors.get(0);
System.out.printf("Vector 0: %d dimensions%n", vector.length);

// Get multiple vectors
List<float[]> batch = baseVectors.getRange(100, 10);
System.out.printf("Got %d vectors%n", batch.size());
```

### Asynchronous Access

For better performance, use async methods:

```java
import java.util.concurrent.CompletableFuture;

// Async single vector
CompletableFuture<float[]> futureVector = baseVectors.getAsync(1000);
futureVector.thenAccept(vector -> {
    System.out.printf("Got vector with %d dimensions%n", vector.length);
});

// Async batch
CompletableFuture<List<float[]>> futureBatch = 
    baseVectors.getRangeAsync(1000, 100);
    
futureBatch.thenAccept(vectors -> {
    System.out.printf("Got batch of %d vectors%n", vectors.size());
});
```

### Error Handling

Handle potential errors:

```java
baseVectors.getAsync(1000)
    .thenAccept(vector -> {
        // Success
        System.out.println("Got vector successfully");
    })
    .exceptionally(throwable -> {
        // Error handling
        System.err.println("Failed to load vector: " + throwable.getMessage());
        return null;
    });
```

## Performance Optimization

### Prebuffering

Preload data for faster access:

```java
// Prebuffer a range of vectors
CompletableFuture<Void> prebufferFuture = 
    baseVectors.prebuffer(1000, 500);

// Wait for prebuffering to complete
prebufferFuture.join();

// Now access is fast
for (int i = 1000; i < 1500; i++) {
    float[] vector = baseVectors.get(i); // Fast - already in memory
    // Process vector...
}
```

### Batch Operations

Process data in batches for better performance:

```java
int batchSize = 1000;
int totalVectors = baseVectors.getCount();

for (int start = 0; start < totalVectors; start += batchSize) {
    int count = Math.min(batchSize, totalVectors - start);
    
    // Prebuffer the batch
    baseVectors.prebuffer(start, count).join();
    
    // Process the batch
    List<float[]> batch = baseVectors.getRange(start, count);
    processBatch(batch);
}
```

### Parallel Processing

Process multiple ranges in parallel:

```java
import java.util.concurrent.ForkJoinPool;
import java.util.stream.IntStream;

int numThreads = Runtime.getRuntime().availableProcessors();
ForkJoinPool customPool = new ForkJoinPool(numThreads);

try {
    customPool.submit(() -> {
        IntStream.range(0, totalVectors / batchSize)
            .parallel()
            .forEach(batchIndex -> {
                int start = batchIndex * batchSize;
                int count = Math.min(batchSize, totalVectors - start);
                
                // Each thread processes its batch
                List<float[]> batch = baseVectors.getRange(start, count);
                processBatch(batch);
            });
    }).get();
} finally {
    customPool.shutdown();
}
```

## Working with Different Vector Types

### Float Vectors (Most Common)

```java
DatasetView<float[]> floatVectors = testData.getBaseVectors();

float[] vector = floatVectors.get(0);
// Process as needed
```

### Integer Vectors (for Indices)

```java
DatasetView<int[]> neighbors = testData.getNeighborIndices();

int[] neighborIndices = neighbors.get(0); // Neighbors for query 0
System.out.printf("Query 0 has %d neighbors%n", neighborIndices.length);
```

### Working with Ground Truth

```java
// Get k-NN ground truth for a query
int queryIndex = 42;
int[] groundTruthIndices = testData.getNeighborIndices().get(queryIndex);
float[] groundTruthDistances = testData.getNeighborDistances().get(queryIndex);

System.out.printf("Query %d ground truth:%n", queryIndex);
for (int i = 0; i < groundTruthIndices.length; i++) {
    System.out.printf("  Neighbor %d: index=%d, distance=%.4f%n", 
        i, groundTruthIndices[i], groundTruthDistances[i]);
}
```

## Streaming Access

### Stream-Based Processing

For memory-efficient processing of large datasets:

```java
import io.nosqlbench.nbdatatools.api.fileio.VectorFileStream;

// Open as stream
try (VectorFileStream<float[]> stream = 
     VectorFileStream.open(Paths.get("large_vectors.hdf5"))) {
    
    // Process each vector
    for (float[] vector : stream) {
        processVector(vector);
    }
}
```

### Bounded Streams

When you know the size:

```java
import io.nosqlbench.nbdatatools.api.fileio.BoundedVectorFileStream;

try (BoundedVectorFileStream<float[]> stream = 
     BoundedVectorFileStream.open(Paths.get("vectors.hdf5"))) {
    
    long totalVectors = stream.getSize();
    System.out.printf("Processing %d vectors...%n", totalVectors);
    
    long processed = 0;
    for (float[] vector : stream) {
        processVector(vector);
        processed++;
        
        // Progress reporting
        if (processed % 10000 == 0) {
            System.out.printf("Progress: %.1f%%\n", 
                100.0 * processed / totalVectors);
        }
    }
}
```

## Metadata Access

### Dataset Metadata

Access dataset attributes:

```java
TestDataView testData = TestDataView.open(Paths.get("dataset.hdf5"));

// Basic metadata
String distanceFunction = testData.getDistance();
String license = testData.getLicense();
String vendor = testData.getVendor();

System.out.printf("Dataset: %s, Distance: %s, License: %s%n", 
    vendor, distanceFunction, license);
```

### Custom Attributes

Read custom attributes from HDF5:

```java
import io.nosqlbench.vectordata.spec.tagging.Tagged;

// If your dataset implements Tagged interface
if (testData instanceof Tagged) {
    Tagged tagged = (Tagged) testData;
    
    String customAttribute = tagged.getTag("my_custom_field");
    System.out.println("Custom field: " + customAttribute);
}
```

## Distance Calculations

### Built-in Distance Functions

Calculate distances using the dataset's distance function:

```java
// Get vectors
float[] query = queryVectors.get(0);
float[] candidate = baseVectors.get(1000);

// Calculate distance based on dataset's distance function
String distanceFunc = testData.getDistance();
double distance;

switch (distanceFunc) {
    case "euclidean":
        distance = euclideanDistance(query, candidate);
        break;
    case "cosine":
        distance = cosineDistance(query, candidate);
        break;
    case "inner_product":
        distance = innerProductDistance(query, candidate);
        break;
    default:
        throw new IllegalArgumentException("Unknown distance: " + distanceFunc);
}

System.out.printf("Distance: %.4f%n", distance);
```

### Distance Function Implementations

```java
public static double euclideanDistance(float[] a, float[] b) {
    double sum = 0.0;
    for (int i = 0; i < a.length; i++) {
        double diff = a[i] - b[i];
        sum += diff * diff;
    }
    return Math.sqrt(sum);
}

public static double cosineDistance(float[] a, float[] b) {
    double dotProduct = 0.0;
    double normA = 0.0;
    double normB = 0.0;
    
    for (int i = 0; i < a.length; i++) {
        dotProduct += a[i] * b[i];
        normA += a[i] * a[i];
        normB += b[i] * b[i];
    }
    
    double cosine = dotProduct / (Math.sqrt(normA) * Math.sqrt(normB));
    return 1.0 - cosine; // Cosine distance
}

public static double innerProductDistance(float[] a, float[] b) {
    double dotProduct = 0.0;
    for (int i = 0; i < a.length; i++) {
        dotProduct += a[i] * b[i];
    }
    return -dotProduct; // Negative for distance
}
```

## Creating Datasets Programmatically

### Writing Vector Data

Create new HDF5 datasets:

```java
import io.nosqlbench.nbdatatools.api.fileio.VectorFileStreamStore;

// Create vectors
List<float[]> vectors = generateVectors(10000, 128);

// Write to HDF5
try (VectorFileStreamStore<float[]> store = 
     VectorFileStreamStore.open(Paths.get("new_dataset.hdf5"))) {
    
    // Write vectors in batches
    store.writeBulk(vectors);
    
    // Add metadata
    store.setAttribute("distance", "euclidean");
    store.setAttribute("created", Instant.now().toString());
    
    store.flush();
}
```

### Building Complete Test Datasets

Create a complete test dataset:

```java
// Generate test data
List<float[]> baseVectors = generateVectors(100000, 128);
List<float[]> queryVectors = generateVectors(1000, 128);

// Compute ground truth (simplified example)
List<int[]> groundTruthIndices = new ArrayList<>();
List<float[]> groundTruthDistances = new ArrayList<>();

for (float[] query : queryVectors) {
    // Find k nearest neighbors (simplified brute force)
    int k = 100;
    PriorityQueue<Neighbor> neighbors = findKNearestNeighbors(query, baseVectors, k);
    
    int[] indices = new int[k];
    float[] distances = new float[k];
    
    for (int i = 0; i < k; i++) {
        Neighbor neighbor = neighbors.poll();
        indices[i] = neighbor.index;
        distances[i] = (float) neighbor.distance;
    }
    
    groundTruthIndices.add(indices);
    groundTruthDistances.add(distances);
}

// Write complete dataset
writeCompleteDataset(
    Paths.get("complete_testset.hdf5"),
    baseVectors, queryVectors, 
    groundTruthIndices, groundTruthDistances,
    "euclidean"
);
```

## Configuration and Windows

### Dataset Windows

Access subsets of data using windows:

```java
import io.nosqlbench.vectordata.layoutv2.DSView;
import io.nosqlbench.vectordata.layoutv2.DSWindow;
import io.nosqlbench.vectordata.layoutv2.DSInterval;

// Define a window (first 10000 vectors)
DSInterval interval = DSInterval.of(0, 10000);
DSWindow window = DSWindow.of(List.of(interval));
DSView view = DSView.of("subset", window);

// Apply window to dataset view
DatasetView<float[]> subset = baseVectors.withView(view);

System.out.printf("Subset has %d vectors%n", subset.getCount());
```

### Custom Profiles

Load datasets with custom profiles:

```java
import io.nosqlbench.vectordata.layoutv2.DSProfile;

// Load with custom configuration
DSProfile profile = DSProfile.builder()
    .name("my_profile")
    .cacheSize(1000000) // 1M vectors in cache
    .prebufferSize(10000) // Prebuffer 10K vectors
    .build();

TestDataView customData = TestDataView.open(
    Paths.get("dataset.hdf5"), 
    profile
);
```

## Error Handling and Validation

### Comprehensive Error Handling

```java
try {
    TestDataView testData = TestDataView.open(Paths.get("dataset.hdf5"));
    
    // Validate dataset structure
    if (testData.getBaseVectors().getCount() == 0) {
        throw new IllegalStateException("Dataset has no base vectors");
    }
    
    if (testData.getQueryVectors() != null && 
        testData.getNeighborIndices() == null) {
        System.err.println("Warning: Dataset has queries but no ground truth");
    }
    
    // Use dataset...
    
} catch (IOException e) {
    System.err.println("Failed to load dataset: " + e.getMessage());
} catch (IllegalArgumentException e) {
    System.err.println("Invalid dataset format: " + e.getMessage());
} catch (Exception e) {
    System.err.println("Unexpected error: " + e.getMessage());
}
```

### Data Validation

Validate vector data:

```java
public static void validateVectors(DatasetView<float[]> vectors) {
    int dimensions = vectors.getVectorDimensions();
    int count = vectors.getCount();
    
    System.out.printf("Validating %d vectors of %d dimensions...%n", count, dimensions);
    
    int zeroVectors = 0;
    int nanVectors = 0;
    int infVectors = 0;
    
    for (int i = 0; i < count; i++) {
        float[] vector = vectors.get(i);
        
        if (vector.length != dimensions) {
            throw new IllegalStateException(
                String.format("Vector %d has wrong dimensions: %d vs %d", 
                    i, vector.length, dimensions));
        }
        
        boolean isZero = true;
        boolean hasNaN = false;
        boolean hasInf = false;
        
        for (float value : vector) {
            if (value != 0.0f) isZero = false;
            if (Float.isNaN(value)) hasNaN = true;
            if (Float.isInfinite(value)) hasInf = true;
        }
        
        if (isZero) zeroVectors++;
        if (hasNaN) nanVectors++;
        if (hasInf) infVectors++;
    }
    
    System.out.printf("Validation complete: %d zero, %d NaN, %d infinite vectors%n", 
        zeroVectors, nanVectors, infVectors);
}
```

## Integration Examples

### Similarity Search Implementation

```java
public class VectorSearch {
    private final DatasetView<float[]> baseVectors;
    private final String distanceFunction;
    
    public VectorSearch(TestDataView testData) {
        this.baseVectors = testData.getBaseVectors();
        this.distanceFunction = testData.getDistance();
    }
    
    public List<SearchResult> search(float[] query, int k) {
        PriorityQueue<SearchResult> heap = new PriorityQueue<>(
            Comparator.comparing(SearchResult::getDistance).reversed());
        
        int count = baseVectors.getCount();
        
        // Prebuffer for better performance
        baseVectors.prebuffer(0, count).join();
        
        for (int i = 0; i < count; i++) {
            float[] candidate = baseVectors.get(i);
            double distance = calculateDistance(query, candidate);
            
            if (heap.size() < k) {
                heap.offer(new SearchResult(i, distance));
            } else if (distance < heap.peek().getDistance()) {
                heap.poll();
                heap.offer(new SearchResult(i, distance));
            }
        }
        
        List<SearchResult> results = new ArrayList<>(heap);
        results.sort(Comparator.comparing(SearchResult::getDistance));
        return results;
    }
    
    private double calculateDistance(float[] a, float[] b) {
        switch (distanceFunction) {
            case "euclidean": return euclideanDistance(a, b);
            case "cosine": return cosineDistance(a, b);
            case "inner_product": return innerProductDistance(a, b);
            default: throw new IllegalArgumentException("Unknown distance: " + distanceFunction);
        }
    }
}

class SearchResult {
    private final int index;
    private final double distance;
    
    public SearchResult(int index, double distance) {
        this.index = index;
        this.distance = distance;
    }
    
    public int getIndex() { return index; }
    public double getDistance() { return distance; }
}
```

### Evaluation Framework

```java
public class ANNEvaluator {
    private final TestDataView testData;
    
    public ANNEvaluator(TestDataView testData) {
        this.testData = testData;
    }
    
    public EvaluationResults evaluate(VectorSearch searchEngine, int k) {
        DatasetView<float[]> queries = testData.getQueryVectors();
        DatasetView<int[]> groundTruth = testData.getNeighborIndices();
        
        int queryCount = queries.getCount();
        double totalRecall = 0.0;
        
        for (int i = 0; i < queryCount; i++) {
            float[] query = queries.get(i);
            int[] expected = groundTruth.get(i);
            
            List<SearchResult> results = searchEngine.search(query, k);
            
            // Calculate recall@k
            Set<Integer> expectedSet = Arrays.stream(expected)
                .limit(k)
                .boxed()
                .collect(Collectors.toSet());
            
            Set<Integer> actualSet = results.stream()
                .mapToInt(SearchResult::getIndex)
                .boxed()
                .collect(Collectors.toSet());
            
            actualSet.retainAll(expectedSet);
            double recall = (double) actualSet.size() / Math.min(k, expected.length);
            totalRecall += recall;
        }
        
        double avgRecall = totalRecall / queryCount;
        return new EvaluationResults(avgRecall);
    }
}

class EvaluationResults {
    private final double recall;
    
    public EvaluationResults(double recall) {
        this.recall = recall;
    }
    
    public double getRecall() { return recall; }
}
```

## Best Practices

### 1. Resource Management

Always close resources:

```java
try (TestDataView testData = TestDataView.open(Paths.get("dataset.hdf5"))) {
    // Use testData...
} // Automatically closed
```

### 2. Async Programming

Use async methods for better performance:

```java
// Good: Non-blocking
CompletableFuture<List<float[]>> future = vectors.getRangeAsync(0, 1000);
future.thenAccept(this::processVectors);

// Avoid: Blocking main thread
List<float[]> vectors = vectors.getRange(0, 1000); // Blocks
```

### 3. Memory Management

Prebuffer strategically:

```java
// Prebuffer data you'll access soon
vectors.prebuffer(startIndex, batchSize).join();

// Process the prebuffered data
for (int i = startIndex; i < startIndex + batchSize; i++) {
    float[] vector = vectors.get(i); // Fast
    processVector(vector);
}
```

### 4. Error Handling

Handle errors gracefully:

```java
vectors.getAsync(index)
    .thenAccept(this::processVector)
    .exceptionally(error -> {
        logger.warn("Failed to load vector {}: {}", index, error.getMessage());
        return null;
    });
```

## Summary

The NBDataTools APIs provide:

- **Type-safe** access to vector datasets
- **Asynchronous** operations for performance
- **Streaming** support for large datasets
- **Metadata** access and manipulation
- **Extensible** architecture through SPIs

Key patterns:
- Use `TestDataView` for complete test datasets
- Use `DatasetView<T>` for individual vector collections
- Leverage async methods and prebuffering for performance
- Always handle errors and close resources properly

Next: Learn about [Advanced Topics](07-advanced-topics.md) including Merkle trees and performance optimization.