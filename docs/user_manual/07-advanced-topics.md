# Advanced Topics

This chapter covers advanced NBDataTools features including Merkle tree verification, performance optimization, and system architecture.

## Merkle Tree Data Integrity

### Overview

NBDataTools uses Merkle trees to ensure data integrity and enable efficient partial verification:

```
                 Root Hash
                /          \
         Hash(AB)            Hash(CD)
        /        \          /        \
   Hash(A)    Hash(B)   Hash(C)    Hash(D)
      |          |         |          |
   Chunk A    Chunk B   Chunk C    Chunk D
```

### Benefits

- **Integrity**: Detect any data corruption
- **Efficiency**: Verify chunks independently  
- **Resumability**: Resume downloads from valid chunks
- **Trust**: Cryptographic proof of correctness

### Creating Merkle Trees

Generate a Merkle reference for a file:

```bash
java -jar nbvectors.jar merkle create \
  --file large_dataset.hdf5 \
  --output large_dataset.mref \
  --chunk-size 1MB \
  --algorithm SHA-256
```

Options:
- `--chunk-size`: Size of data chunks (default: 1MB)
- `--algorithm`: Hash algorithm (SHA-256, SHA-512, Blake2b)
- `--parallel`: Number of threads for hashing

### Verification

Verify file integrity:

```bash
java -jar nbvectors.jar merkle verify \
  --file large_dataset.hdf5 \
  --reference large_dataset.mref
```

Output example:
```
Verifying large_dataset.hdf5...
Chunks verified: 512/512 (100%)
Status: VALID
Verification time: 2.3 seconds
```

### Partial Verification

Check specific byte ranges:

```bash
java -jar nbvectors.jar merkle verify \
  --file large_dataset.hdf5 \
  --reference large_dataset.mref \
  --range 1000000:2000000
```

### Merkle State Management

Track verification state:

```bash
# Show current verification state
java -jar nbvectors.jar merkle status \
  --state large_dataset.mrkl

# Resume partial verification
java -jar nbvectors.jar merkle verify \
  --file large_dataset.hdf5 \
  --reference large_dataset.mref \
  --state large_dataset.mrkl \
  --resume
```

## Performance Optimization

### Memory Management

#### Heap Size Configuration

For large datasets, configure JVM heap:

```bash
# 16GB heap for very large datasets
java -Xmx16g -jar nbvectors.jar analyze describe --file huge_dataset.hdf5

# Monitor memory usage
java -XX:+PrintGCDetails -Xmx8g -jar nbvectors.jar export_hdf5 --input large.fvec --output large.hdf5
```

#### Garbage Collection Tuning

For high-throughput processing:

```bash
# G1 garbage collector for low latency
java -XX:+UseG1GC -XX:MaxGCPauseMillis=100 -jar nbvectors.jar ...

# Parallel GC for high throughput
java -XX:+UseParallelGC -jar nbvectors.jar ...
```

### Concurrent Processing

#### Thread Pool Configuration

Configure concurrent operations:

```bash
# Set specific thread counts
java -Dnbdatatools.threads.io=8 \
     -Dnbdatatools.threads.compute=16 \
     -jar nbvectors.jar export_hdf5 --input huge.fvec --output huge.hdf5
```

#### Async Processing Example

```java
// Configure custom executor
ExecutorService executor = Executors.newFixedThreadPool(8);

// Process multiple datasets concurrently
List<CompletableFuture<Void>> futures = datasets.stream()
    .map(dataset -> CompletableFuture.runAsync(() -> {
        processDataset(dataset);
    }, executor))
    .collect(Collectors.toList());

// Wait for all to complete
CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
```

### I/O Optimization

#### Chunked Reading

Optimize chunk sizes for your use case:

```java
// Large chunks for sequential access
dataset.setChunkSize(10 * 1024 * 1024); // 10MB chunks

// Small chunks for random access  
dataset.setChunkSize(64 * 1024); // 64KB chunks
```

#### Network Optimization

For remote datasets:

```bash
# Configure connection pooling
java -Dnbdatatools.http.max_connections=20 \
     -Dnbdatatools.http.connection_timeout=30000 \
     -jar nbvectors.jar datasets download large_dataset:default
```

### Profiling and Monitoring

#### Built-in Metrics

Enable detailed metrics:

```bash
java -Dnbdatatools.metrics.enabled=true \
     -Dnbdatatools.metrics.output=metrics.json \
     -jar nbvectors.jar analyze describe --file dataset.hdf5
```

Example metrics output:
```json
{
  "io_operations": {
    "total_reads": 1000,
    "total_bytes_read": 134217728,
    "avg_read_time_ms": 12.5,
    "cache_hit_rate": 0.85
  },
  "merkle_verification": {
    "chunks_verified": 500,
    "verification_time_ms": 2300,
    "invalid_chunks": 0
  }
}
```

#### Custom Monitoring

Implement custom metrics:

```java
public class DatasetMonitor {
    private final MeterRegistry meterRegistry;
    private final Timer readTimer;
    private final Counter cacheHits;
    
    public DatasetMonitor(MeterRegistry registry) {
        this.meterRegistry = registry;
        this.readTimer = Timer.builder("dataset.read.duration").register(registry);
        this.cacheHits = Counter.builder("dataset.cache.hits").register(registry);
    }
    
    public <T> T monitorRead(Supplier<T> operation) {
        return readTimer.recordCallable(operation);
    }
    
    public void recordCacheHit() {
        cacheHits.increment();
    }
}
```

## Custom Transport Providers

### Implementing Transport Providers

Create custom transport for specialized protocols:

```java
public class S3TransportProvider implements ChunkedTransportProvider {
    
    @Override
    public ChunkedTransportClient getClient(URL url) {
        if ("s3".equals(url.getProtocol())) {
            return new S3TransportClient(url);
        }
        return null; // Not supported
    }
}

public class S3TransportClient implements ChunkedTransportClient {
    private final S3Client s3Client;
    private final String bucket;
    private final String key;
    
    public S3TransportClient(URL url) {
        this.s3Client = S3Client.create();
        // Parse S3 URL: s3://bucket/key
        this.bucket = url.getHost();
        this.key = url.getPath().substring(1);
    }
    
    @Override
    public CompletableFuture<FetchResult<ByteBuffer>> fetchRange(long start, int length) {
        return CompletableFuture.supplyAsync(() -> {
            GetObjectRequest request = GetObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .range(String.format("bytes=%d-%d", start, start + length - 1))
                .build();
                
            ResponseBytes<GetObjectResponse> response = s3Client.getObjectAsBytes(request);
            return new FetchResult<>(response.asByteBuffer(), response.response().contentLength());
        });
    }
    
    @Override
    public CompletableFuture<Long> getSize() {
        return CompletableFuture.supplyAsync(() -> {
            HeadObjectRequest request = HeadObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build();
                
            HeadObjectResponse response = s3Client.headObject(request);
            return response.contentLength();
        });
    }
    
    @Override
    public boolean supportsRangeRequests() {
        return true;
    }
    
    @Override
    public void close() {
        s3Client.close();
    }
}
```

### Registering Custom Providers

Create service registration:

```
# src/main/resources/META-INF/services/io.nosqlbench.nbdatatools.api.transport.ChunkedTransportProvider
com.example.S3TransportProvider
```

## Custom Vector File Formats

### Implementing Custom Readers

Create readers for proprietary formats:

```java
public class CustomVectorFileStream implements VectorFileStream<float[]> {
    private final Path filePath;
    private final FileChannel channel;
    private final int dimensions;
    private final int vectorCount;
    
    public CustomVectorFileStream(Path filePath) throws IOException {
        this.filePath = filePath;
        this.channel = FileChannel.open(filePath, StandardOpenOption.READ);
        
        // Read header
        ByteBuffer header = ByteBuffer.allocate(8);
        channel.read(header);
        header.flip();
        
        this.dimensions = header.getInt();
        this.vectorCount = header.getInt();
    }
    
    @Override
    public Iterator<float[]> iterator() {
        return new VectorIterator();
    }
    
    private class VectorIterator implements Iterator<float[]> {
        private int currentVector = 0;
        private final ByteBuffer buffer = ByteBuffer.allocate(dimensions * 4);
        
        @Override
        public boolean hasNext() {
            return currentVector < vectorCount;
        }
        
        @Override
        public float[] next() {
            try {
                buffer.clear();
                channel.read(buffer);
                buffer.flip();
                
                float[] vector = new float[dimensions];
                buffer.asFloatBuffer().get(vector);
                
                currentVector++;
                return vector;
                
            } catch (IOException e) {
                throw new RuntimeException("Failed to read vector", e);
            }
        }
    }
    
    @Override
    public String getName() {
        return filePath.getFileName().toString();
    }
    
    @Override
    public void close() throws IOException {
        channel.close();
    }
}
```

### Service Registration

Register your custom format:

```
# src/main/resources/META-INF/services/io.nosqlbench.nbdatatools.api.fileio.VectorFileStream
com.example.CustomVectorFileStream
```

## Advanced Configuration

### System Properties

Configure NBDataTools behavior:

```bash
# Cache configuration
java -Dnbdatatools.cache.size=1000000 \
     -Dnbdatatools.cache.ttl=3600 \
     -jar nbvectors.jar ...

# Network configuration
java -Dnbdatatools.http.timeout=60000 \
     -Dnbdatatools.http.retries=3 \
     -jar nbvectors.jar ...

# Logging configuration
java -Dnbdatatools.log.level=DEBUG \
     -Dnbdatatools.log.file=nbdatatools.log \
     -jar nbvectors.jar ...
```

### Configuration Files

Create configuration profiles:

```json
{
  "profiles": {
    "development": {
      "cache": {
        "size": 100000,
        "ttl_seconds": 300
      },
      "http": {
        "timeout_ms": 30000,
        "max_connections": 10
      }
    },
    "production": {
      "cache": {
        "size": 10000000,
        "ttl_seconds": 3600
      },
      "http": {
        "timeout_ms": 60000,
        "max_connections": 50
      }
    }
  }
}
```

Load configuration:

```bash
java -Dnbdatatools.config.file=config.json \
     -Dnbdatatools.config.profile=production \
     -jar nbvectors.jar ...
```

## Advanced Scheduling

### Custom Chunk Schedulers

Implement custom scheduling strategies:

```java
public class OptimizedChunkScheduler implements ChunkScheduler {
    private final int maxConcurrentDownloads;
    private final ExecutorService executorService;
    
    public OptimizedChunkScheduler(int maxConcurrent) {
        this.maxConcurrentDownloads = maxConcurrent;
        this.executorService = Executors.newFixedThreadPool(maxConcurrent);
    }
    
    @Override
    public void scheduleDownloads(long offset, int length, 
                                 MerkleShape shape, MerkleState state,
                                 SchedulingTarget target) {
        
        // Find optimal download strategy based on:
        // - Network conditions
        // - Cache state  
        // - Access patterns
        // - Data locality
        
        List<DownloadTask> tasks = optimizeDownloadPlan(offset, length, shape, state);
        
        // Execute tasks with controlled concurrency
        Semaphore semaphore = new Semaphore(maxConcurrentDownloads);
        
        for (DownloadTask task : tasks) {
            executorService.submit(() -> {
                try {
                    semaphore.acquire();
                    executeTask(task, target);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    semaphore.release();
                }
            });
        }
    }
    
    private List<DownloadTask> optimizeDownloadPlan(long offset, int length, 
                                                   MerkleShape shape, MerkleState state) {
        // Custom optimization logic
        // Consider factors like:
        // - Chunk locality
        // - Network bandwidth
        // - Storage device characteristics
        // - Historical access patterns
        
        return Collections.emptyList(); // Simplified
    }
}
```

## Testing and Validation

### Unit Testing with NBDataTools

Create test datasets:

```java
@Test
public void testVectorDatasetProcessing() {
    // Create test data
    List<float[]> testVectors = generateRandomVectors(1000, 128);
    Path testFile = createTemporaryHDF5(testVectors);
    
    try (TestDataView testData = TestDataView.open(testFile)) {
        DatasetView<float[]> vectors = testData.getBaseVectors();
        
        assertEquals(1000, vectors.getCount());
        assertEquals(128, vectors.getVectorDimensions());
        
        // Test async access
        CompletableFuture<float[]> future = vectors.getAsync(0);
        float[] vector = future.join();
        
        assertNotNull(vector);
        assertEquals(128, vector.length);
    }
}

private Path createTemporaryHDF5(List<float[]> vectors) {
    // Implementation to create temporary test file
    return null; // Simplified
}
```

### Performance Testing

Benchmark different configurations:

```java
@Test
public void benchmarkDatasetAccess() {
    Path largeDataset = Paths.get("large_test_dataset.hdf5");
    
    // Test different chunk sizes
    int[] chunkSizes = {64 * 1024, 1024 * 1024, 10 * 1024 * 1024};
    
    for (int chunkSize : chunkSizes) {
        long startTime = System.nanoTime();
        
        try (TestDataView testData = TestDataView.open(largeDataset)) {
            DatasetView<float[]> vectors = testData.getBaseVectors();
            vectors.setChunkSize(chunkSize);
            
            // Sequential access test
            for (int i = 0; i < 10000; i++) {
                vectors.get(i);
            }
        }
        
        long duration = System.nanoTime() - startTime;
        System.out.printf("Chunk size %d: %.2f ms%n", 
            chunkSize, duration / 1_000_000.0);
    }
}
```

### Integration Testing

Test complete workflows:

```java
@Test
public void testCompleteWorkflow() {
    // 1. Convert data
    convertVectorData("input.fvec", "output.hdf5");
    
    // 2. Verify conversion
    verifyDatasetStructure("output.hdf5");
    
    // 3. Test access patterns
    testSequentialAccess("output.hdf5");
    testRandomAccess("output.hdf5");
    
    // 4. Validate integrity
    createAndVerifyMerkleTree("output.hdf5");
}
```

## Debugging and Troubleshooting

### Debug Logging

Enable detailed debugging:

```bash
java -Dorg.slf4j.simpleLogger.defaultLogLevel=DEBUG \
     -Dorg.slf4j.simpleLogger.showDateTime=true \
     -jar nbvectors.jar analyze describe --file dataset.hdf5
```

### Memory Analysis

Profile memory usage:

```bash
# Generate heap dump on OutOfMemoryError
java -XX:+HeapDumpOnOutOfMemoryError \
     -XX:HeapDumpPath=/tmp/nbdatatools_heap.hprof \
     -jar nbvectors.jar ...

# Continuous memory monitoring
java -XX:+PrintGCDetails \
     -XX:+PrintGCTimeStamps \
     -Xloggc:gc.log \
     -jar nbvectors.jar ...
```

### Network Debugging

Debug network issues:

```bash
# Enable network tracing
java -Dcom.sun.net.httpserver.HttpServer.debug=true \
     -Djava.net.preferIPv4Stack=true \
     -jar nbvectors.jar datasets download large_dataset:default
```

### Custom Debug Tools

Create debugging utilities:

```java
public class DatasetDebugger {
    
    public static void analyzeDatasetStructure(Path datasetPath) {
        try (TestDataView testData = TestDataView.open(datasetPath)) {
            System.out.println("=== Dataset Structure ===");
            
            DatasetView<float[]> base = testData.getBaseVectors();
            System.out.printf("Base vectors: %d × %d%n", 
                base.getCount(), base.getVectorDimensions());
            
            if (testData.getQueryVectors() != null) {
                DatasetView<float[]> queries = testData.getQueryVectors();
                System.out.printf("Query vectors: %d × %d%n", 
                    queries.getCount(), queries.getVectorDimensions());
            }
            
            // Check for common issues
            validateDimensions(testData);
            checkForZeroVectors(testData);
            analyzeValueDistribution(testData);
            
        } catch (Exception e) {
            System.err.println("Error analyzing dataset: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private static void validateDimensions(TestDataView testData) {
        // Implementation for dimension validation
    }
    
    private static void checkForZeroVectors(TestDataView testData) {
        // Implementation for zero vector detection
    }
    
    private static void analyzeValueDistribution(TestDataView testData) {
        // Implementation for statistical analysis
    }
}
```

## Security Considerations

### Data Integrity

Always verify downloaded data:

```bash
# Verify checksums
java -jar nbvectors.jar datasets download \
  sensitive_dataset:default \
  --verify \
  --checksum-algorithm SHA-256

# Create and verify Merkle trees
java -jar nbvectors.jar merkle create --file sensitive_data.hdf5 --output sensitive_data.mref
java -jar nbvectors.jar merkle verify --file sensitive_data.hdf5 --reference sensitive_data.mref
```

### Access Control

Implement access controls:

```java
public class SecureDatasetAccess {
    private final Set<String> authorizedUsers;
    
    public SecureDatasetAccess(Set<String> authorizedUsers) {
        this.authorizedUsers = authorizedUsers;
    }
    
    public TestDataView openDataset(Path path, String userId) throws SecurityException {
        if (!authorizedUsers.contains(userId)) {
            throw new SecurityException("Unauthorized access attempt by: " + userId);
        }
        
        return TestDataView.open(path);
    }
}
```

### Network Security

Configure secure connections:

```bash
# Use HTTPS only
java -Dnbdatatools.http.ssl_only=true \
     -Dnbdatatools.http.verify_certificates=true \
     -jar nbvectors.jar datasets download dataset:default
```

## Summary

Advanced NBDataTools features include:

- **Merkle Tree Verification**: Ensure data integrity with cryptographic proofs
- **Performance Optimization**: Memory management, concurrency, and I/O tuning
- **Custom Extensions**: Transport providers, file formats, and schedulers
- **Advanced Configuration**: System properties, profiles, and monitoring
- **Testing and Debugging**: Comprehensive tools for validation and troubleshooting
- **Security**: Data integrity, access control, and secure networking

These advanced features enable NBDataTools to scale to enterprise requirements while maintaining flexibility for specialized use cases.

Next: Check the [Troubleshooting Guide](08-troubleshooting.md) for solutions to common issues.
