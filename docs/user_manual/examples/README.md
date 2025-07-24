# Examples

This directory contains practical examples for using NBDataTools.

## Command Line Examples

Ready-to-use command examples for common tasks:

### Data Conversion
```bash
# Convert float vectors to HDF5
java -jar nbvectors.jar export_hdf5 \
  --input vectors.fvec \
  --output vectors.hdf5 \
  --dataset-name "my_vectors" \
  --distance euclidean

# Convert complete test dataset
java -jar nbvectors.jar export_hdf5 \
  --input base_vectors.fvec \
  --queries query_vectors.fvec \
  --neighbors ground_truth.ivec \
  --distances ground_truth_distances.fvec \
  --output complete_dataset.hdf5 \
  --distance cosine
```

### Dataset Analysis
```bash
# Analyze dataset structure
java -jar nbvectors.jar analyze describe \
  --file dataset.hdf5 \
  --detailed

# Verify ground truth
java -jar nbvectors.jar analyze verify_knn \
  --file dataset.hdf5 \
  --sample-size 1000 \
  --k 100

# Count zero vectors
java -jar nbvectors.jar analyze count_zeros \
  --file vectors.fvec
```

### Dataset Management
```bash
# List available datasets
java -jar nbvectors.jar datasets list \
  --filter "dimensions=128" \
  --format table

# Download dataset
java -jar nbvectors.jar datasets download \
  --name "sift-128-euclidean" \
  --output ./datasets/ \
  --verify \
  --resume

# Create catalog
java -jar nbvectors.jar catalog_hdf5 \
  --directory ./datasets \
  --output catalog.json \
  --recursive
```

### Data Integrity
```bash
# Create Merkle tree
java -jar nbvectors.jar merkle create \
  --file large_dataset.hdf5 \
  --output large_dataset.mref \
  --chunk-size 1MB

# Verify integrity
java -jar nbvectors.jar merkle verify \
  --file large_dataset.hdf5 \
  --reference large_dataset.mref

# Check status
java -jar nbvectors.jar merkle status \
  --state large_dataset.mrkl
```

## API Examples

See [api-examples.md](api-examples.md) for programming examples.

## Configuration Examples

Sample configuration files and settings:

### Basic Configuration (`~/.config/vectordata/config.json`)
```json
{
  "cache": {
    "size": 1000000,
    "ttl_seconds": 3600
  },
  "http": {
    "timeout_ms": 30000,
    "max_connections": 10,
    "user_agent": "NBDataTools/1.0"
  },
  "catalogs": [
    "https://vectordata.org/catalog.json",
    "file:///local/catalog.json"
  ]
}
```

### Profile Configuration
```json
{
  "profiles": {
    "development": {
      "cache": {"size": 100000},
      "log_level": "DEBUG"
    },
    "production": {
      "cache": {"size": 10000000},
      "log_level": "INFO"
    }
  }
}
```

## Workflow Scripts

### Batch Conversion Script
```bash
#!/bin/bash
# convert_all.sh - Convert all .fvec files in a directory

INPUT_DIR="$1"
OUTPUT_DIR="$2"

if [ -z "$INPUT_DIR" ] || [ -z "$OUTPUT_DIR" ]; then
    echo "Usage: $0 <input_dir> <output_dir>"
    exit 1
fi

mkdir -p "$OUTPUT_DIR"

for file in "$INPUT_DIR"/*.fvec; do
    if [ -f "$file" ]; then
        base=$(basename "$file" .fvec)
        echo "Converting $file to $OUTPUT_DIR/${base}.hdf5"
        
        java -jar nbvectors.jar export_hdf5 \
          --input "$file" \
          --output "$OUTPUT_DIR/${base}.hdf5" \
          --dataset-name "$base" \
          --distance euclidean
    fi
done

echo "Conversion complete!"
```

### Dataset Validation Script
```bash
#!/bin/bash
# validate_dataset.sh - Comprehensive dataset validation

DATASET="$1"

if [ -z "$DATASET" ]; then
    echo "Usage: $0 <dataset.hdf5>"
    exit 1
fi

echo "=== Validating $DATASET ==="

echo "1. Checking file structure..."
java -jar nbvectors.jar show_hdf5 --file "$DATASET" --tree

echo "2. Analyzing dataset..."
java -jar nbvectors.jar analyze describe --file "$DATASET" --detailed

echo "3. Checking for zero vectors..."
java -jar nbvectors.jar analyze count_zeros --file "$DATASET"

echo "4. Verifying ground truth (if present)..."
if java -jar nbvectors.jar show_hdf5 --file "$DATASET" --path /neighbors >/dev/null 2>&1; then
    java -jar nbvectors.jar analyze verify_knn --file "$DATASET" --sample-size 100
else
    echo "   No ground truth found - skipping"
fi

echo "5. Creating integrity signature..."
java -jar nbvectors.jar merkle create \
  --file "$DATASET" \
  --output "${DATASET%.hdf5}.mref"

echo "=== Validation complete! ==="
```

## Docker Examples

### Dockerfile
```dockerfile
FROM openjdk:17-jre-slim

# Install dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Download NBDataTools
RUN curl -L -o /usr/local/bin/nbvectors.jar \
    https://github.com/nosqlbench/nbdatatools/releases/latest/download/nbvectors.jar \
    && chmod +x /usr/local/bin/nbvectors.jar

# Create working directory
WORKDIR /data

# Set entry point
ENTRYPOINT ["java", "-jar", "/usr/local/bin/nbvectors.jar"]
```

### Docker Compose
```yaml
version: '3.8'
services:
  nbdatatools:
    build: .
    volumes:
      - ./data:/data
      - ./config:/root/.config/vectordata
    environment:
      - JAVA_OPTS=-Xmx4g
    command: analyze describe --file /data/dataset.hdf5
```

## Integration Examples

### Python Integration
```python
#!/usr/bin/env python3
"""
NBDataTools Python Integration Example
Requires: h5py, numpy
"""

import h5py
import numpy as np
import subprocess
import json

def convert_to_hdf5(fvec_file, hdf5_file):
    """Convert fvec to HDF5 using NBDataTools"""
    cmd = [
        'java', '-jar', 'nbvectors.jar',
        'export_hdf5',
        '--input', fvec_file,
        '--output', hdf5_file,
        '--distance', 'euclidean'
    ]
    subprocess.run(cmd, check=True)

def load_dataset(hdf5_file):
    """Load dataset using h5py"""
    with h5py.File(hdf5_file, 'r') as f:
        base_vectors = f['/base/data'][:]
        distance_func = f.attrs['distance'].decode('utf-8')
        dimensions = f['/base'].attrs['dimensions']
        
        return {
            'vectors': base_vectors,
            'distance': distance_func,
            'dimensions': dimensions
        }

def get_dataset_info(hdf5_file):
    """Get dataset info using NBDataTools"""
    cmd = [
        'java', '-jar', 'nbvectors.jar',
        'analyze', 'describe',
        '--file', hdf5_file,
        '--format', 'json'
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    return json.loads(result.stdout)

# Example usage
if __name__ == '__main__':
    # Convert data
    convert_to_hdf5('vectors.fvec', 'dataset.hdf5')
    
    # Load with h5py
    data = load_dataset('dataset.hdf5')
    print(f"Loaded {len(data['vectors'])} vectors of {data['dimensions']}D")
    
    # Get detailed info
    info = get_dataset_info('dataset.hdf5')
    print(f"Dataset info: {info}")
```

### Spark Integration
```scala
// NBDataTools Spark Integration Example
import org.apache.spark.sql.SparkSession
import sys.process._

object NBDataToolsIntegration {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("NBDataTools Integration")
      .getOrCreate()
    
    // Convert HDF5 to Parquet using NBDataTools
    val convertCmd = Seq(
      "java", "-jar", "nbvectors.jar",
      "export_parquet",
      "--input", "dataset.hdf5",
      "--output", "dataset.parquet"
    )
    convertCmd.!
    
    // Load in Spark
    val vectors = spark.read.parquet("dataset.parquet")
    vectors.show()
    
    // Process data
    val processed = vectors.select("vector")
      .rdd
      .map(row => {
        val vector = row.getAs[Seq[Float]]("vector")
        // Your processing logic here
        vector.sum
      })
      .collect()
    
    println(s"Processed ${processed.length} vectors")
    spark.stop()
  }
}
```

## Performance Examples

### Memory Optimization
```bash
# Large dataset processing with memory optimization
java -Xmx16g \
     -XX:+UseG1GC \
     -XX:MaxGCPauseMillis=100 \
     -jar nbvectors.jar analyze describe \
     --file huge_dataset.hdf5 \
     --streaming
```

### Parallel Processing
```bash
# Parallel conversion with custom thread count
java -Dnbdatatools.threads.io=8 \
     -Dnbdatatools.threads.compute=16 \
     -jar nbvectors.jar export_hdf5 \
     --input large_vectors.fvec \
     --output large_vectors.hdf5 \
     --parallel
```

### Network Optimization
```bash
# Optimized download with multiple connections
java -Dnbdatatools.http.max_connections=8 \
     -Dnbdatatools.http.timeout=60000 \
     -jar nbvectors.jar datasets download \
     --name large_dataset \
     --threads 4 \
     --resume
```

## Testing Examples

### Unit Test Setup
```java
// JUnit test example
@Test
public void testDatasetConversion() {
    Path inputFile = createTestFvecFile(1000, 128);
    Path outputFile = Files.createTempFile("test", ".hdf5");
    
    // Convert using NBDataTools
    ProcessBuilder pb = new ProcessBuilder(
        "java", "-jar", "nbvectors.jar",
        "export_hdf5",
        "--input", inputFile.toString(),
        "--output", outputFile.toString()
    );
    
    Process process = pb.start();
    int exitCode = process.waitFor();
    assertEquals(0, exitCode);
    
    // Verify result
    try (TestDataView data = TestDataView.open(outputFile)) {
        assertEquals(1000, data.getBaseVectors().getCount());
        assertEquals(128, data.getBaseVectors().getVectorDimensions());
    }
}
```

### Performance Benchmark
```java
@Benchmark
public void benchmarkSequentialAccess(Blackhole bh) {
    for (int i = 0; i < VECTOR_COUNT; i++) {
        float[] vector = dataset.get(i);
        bh.consume(vector);
    }
}

@Benchmark  
public void benchmarkRandomAccess(Blackhole bh) {
    Random random = new Random(42);
    for (int i = 0; i < SAMPLE_SIZE; i++) {
        int index = random.nextInt(VECTOR_COUNT);
        float[] vector = dataset.get(index);
        bh.consume(vector);
    }
}
```

These examples demonstrate real-world usage patterns and can be adapted for specific use cases.