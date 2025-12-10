# Examples

This page provides updated NBDataTools examples using dataset directories (`dataset.yaml` + facet files).

## Data Conversion

```bash
# Convert float vectors to CSV
java -jar nbvectors.jar convert file \
  --input vectors.fvec \
  --output vectors.csv

# Convert byte vectors to Parquet
java -jar nbvectors.jar convert file \
  --input base.bvec \
  --output base.parquet
```

## Dataset Analysis

```bash
# Describe dataset metadata and facets
datasets/mteb-lite$ java -jar nbvectors.jar analyze describe .

# Verify ground truth
datasets/mteb-lite$ java -jar nbvectors.jar analyze verify_knn . --sample-size 500 --k 100
```

## Dataset Management

```bash
# List datasets from configured catalogs
java -jar nbvectors.jar datasets list --format table

# Download a dataset
java -jar nbvectors.jar datasets download \
  sift-128-euclidean:default \
  --output ./datasets/ \
  --verify \
  --resume

# Create catalog entries for local datasets
java -jar nbvectors.jar catalog --directories ./datasets --recursive --basename catalog
```

## Data Integrity

```bash
# Create Merkle tree for a facet
java -jar nbvectors.jar merkle create \
  --file datasets/mteb-lite/base.fvec \
  --output base.mref \
  --chunk-size 1MB

# Verify integrity later
java -jar nbvectors.jar merkle verify \
  --file datasets/mteb-lite/base.fvec \
  --reference base.mref
```

## Batch Conversion Script

```bash
#!/bin/bash
INPUT_DIR=$1
OUTPUT_DIR=$2
mkdir -p "$OUTPUT_DIR"
for file in "$INPUT_DIR"/*.fvec; do
  base=$(basename "$file" .fvec)
  java -jar nbvectors.jar convert file \
    --input "$file" \
    --output "$OUTPUT_DIR/${base}.csv"
done
```

## Dataset Validation Script

```bash
#!/bin/bash
DATASET=${1:-datasets/mteb-lite}

java -jar nbvectors.jar analyze describe "$DATASET" --detailed || exit 1
java -jar nbvectors.jar analyze verify_knn "$DATASET" --sample-size 100 || exit 1
java -jar nbvectors.jar datasets prebuffer "$DATASET" --profile default > /dev/null
java -jar nbvectors.jar merkle create \
  --file "$DATASET/base.fvec" \
  --output "$DATASET/base.mref"
```

## Python Example

```python
import numpy as np

def read_fvec(path):
    data = np.fromfile(path, dtype='<f4')
    dims = int.from_bytes(open(path, 'rb').read(4), 'little', signed=False)
    return data.reshape(-1, dims + 1)[:, 1:]

base = read_fvec('datasets/mteb-lite/base.fvec')
print(base.shape)
```

## Spark Example

```bash
java -jar nbvectors.jar convert file \
  --input datasets/mteb-lite/base.fvec \
  --output base.parquet
```

```scala
val spark = SparkSession.builder().appName("VectorAnalysis").getOrCreate()
val vectors = spark.read.parquet("base.parquet")
vectors.show()
```
