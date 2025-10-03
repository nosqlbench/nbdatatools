# Command Line Interface Reference

The NBDataTools CLI provides comprehensive tools for vector data management. All commands are accessed through the main `nbvectors.jar` executable.

## General Usage

```bash
java -jar nbvectors.jar <command> [options]
```

### Global Options

- `--help` - Show help for any command
- `--version` - Display version information
- `-v, --verbose` - Enable verbose output
- `--debug` - Enable debug logging

## Command Categories

### Analysis Commands

Commands for analyzing and verifying vector datasets.

#### analyze

Main command for dataset analysis with subcommands:

##### count_zeros

Count zero vectors in vector files.

```bash
java -jar nbvectors.jar analyze count_zeros --file <path>
```

**Options:**
- `--file, -f` - Path to vector file (required)
- `--format` - File format (auto-detected if not specified)

**Example:**
```bash
java -jar nbvectors.jar analyze count_zeros --file vectors.fvec
```

##### verify_knn

Verify k-nearest neighbor ground truth data.

```bash
java -jar nbvectors.jar analyze verify_knn --file <path>
```

**Options:**
- `--file, -f` - Path to HDF5 file (required)
- `--sample-size` - Number of queries to verify (default: 100)
- `--k` - Number of neighbors to check (default: 10)

**Example:**
```bash
java -jar nbvectors.jar analyze verify_knn \
  --file dataset.hdf5 \
  --sample-size 1000 \
  --k 100
```

##### describe

Describe dataset contents and structure.

```bash
java -jar nbvectors.jar analyze describe --file <path>
```

**Options:**
- `--file, -f` - Path to dataset file (required)
- `--detailed` - Show detailed statistics
- `--format` - Output format (text, json)

**Example:**
```bash
java -jar nbvectors.jar analyze describe \
  --file dataset.hdf5 \
  --detailed \
  --format json
```

### Data Conversion Commands

Commands for converting between different vector formats.

#### export_hdf5

Export vector files to HDF5 format.

```bash
java -jar nbvectors.jar export_hdf5 [options]
```

**Options:**
- `--input, -i` - Input file path (required)
- `--output, -o` - Output HDF5 file path (required)
- `--dataset-name` - Name for the dataset
- `--input-format` - Input format (fvec, ivec, bvec, parquet)
- `--dimensions` - Vector dimensions (auto-detected if possible)
- `--distance` - Distance function (euclidean, cosine, inner-product)

**Examples:**

Convert float vectors:
```bash
java -jar nbvectors.jar export_hdf5 \
  --input vectors.fvec \
  --output vectors.hdf5 \
  --dataset-name "my_vectors" \
  --distance euclidean
```

Convert with ground truth:
```bash
java -jar nbvectors.jar export_hdf5 \
  --input base.fvec \
  --queries queries.fvec \
  --neighbors neighbors.ivec \
  --distances distances.fvec \
  --output complete_dataset.hdf5
```

#### export_json

Export HDF5 datasets to JSON format.

```bash
java -jar nbvectors.jar export_json --input <hdf5> --output <json>
```

**Options:**
- `--input, -i` - Input HDF5 file (required)
- `--output, -o` - Output JSON file (required)
- `--pretty` - Pretty-print JSON
- `--include-vectors` - Include vector data (warning: large!)

**Example:**
```bash
java -jar nbvectors.jar export_json \
  --input dataset.hdf5 \
  --output summary.json \
  --pretty
```

#### build_hdf5

Build HDF5 files from JSON specifications.

```bash
java -jar nbvectors.jar build_hdf5 --spec <json> --output <hdf5>
```

**Options:**
- `--spec, -s` - JSON specification file (required)
- `--output, -o` - Output HDF5 file (required)
- `--validate` - Validate after building

### Dataset Management Commands

Commands for working with dataset catalogs and downloads.

#### datasets

Main command for dataset operations with subcommands:

##### list

List available datasets from catalogs.

```bash
java -jar nbvectors.jar datasets list [options]
```

**Options:**
- `--catalog` - Specific catalog to query
- `--filter` - Filter expression
- `--format` - Output format (table, json, csv)

**Example:**
```bash
java -jar nbvectors.jar datasets list \
  --filter "dimensions=128" \
  --format table
```

##### download

Download datasets from catalogs.

```bash
java -jar nbvectors.jar datasets download <dataset:profile>... --output <dir>
```

**Parameters:**
- `<dataset:profile>...` - One or more dataset/profile specs. Escape literal colons in dataset names with `\:` (for example `vector\:set:default`).

**Options:**
- `--output, -o` - Output directory (required)
- `--catalog` - Specific catalog
- `--verify` - Verify download integrity
- `--resume` - Resume interrupted downloads

**Example:**
```bash
java -jar nbvectors.jar datasets download \
  mnist:default \
  --output ./datasets/ \
  --verify
```

##### info

Show detailed information about a dataset.

```bash
java -jar nbvectors.jar datasets info --name <dataset>
```

**Example:**
```bash
java -jar nbvectors.jar datasets info --name "glove-100-angular"
```

### HDF5-Specific Commands

Commands for working with HDF5 files.

#### show_hdf5

Display HDF5 file structure and contents.

```bash
java -jar nbvectors.jar show_hdf5 --file <path> [options]
```

**Options:**
- `--file, -f` - HDF5 file path (required)
- `--tree` - Show tree structure
- `--attributes` - Show all attributes
- `--data` - Show data samples
- `--path` - Specific path within HDF5

**Example:**
```bash
java -jar nbvectors.jar show_hdf5 \
  --file dataset.hdf5 \
  --tree \
  --attributes
```

#### tag_hdf5

Read or write HDF5 attributes.

```bash
java -jar nbvectors.jar tag_hdf5 <subcommand> [options]
```

**Subcommands:**

##### read
```bash
java -jar nbvectors.jar tag_hdf5 read \
  --file dataset.hdf5 \
  --path /base \
  --attribute distance
```

##### write
```bash
java -jar nbvectors.jar tag_hdf5 write \
  --file dataset.hdf5 \
  --path / \
  --attribute license \
  --value "MIT"
```

##### list
```bash
java -jar nbvectors.jar tag_hdf5 list --file dataset.hdf5
```

#### catalog_hdf5

Create catalog entries for HDF5 files.

```bash
java -jar nbvectors.jar catalog_hdf5 --directory <dir> --output <catalog>
```

**Options:**
- `--directory, -d` - Directory to scan (required)
- `--output, -o` - Output catalog file
- `--recursive` - Scan recursively
- `--pattern` - File pattern to match

**Example:**
```bash
java -jar nbvectors.jar catalog_hdf5 \
  --directory ./datasets \
  --output catalog.json \
  --recursive
```

### Utility Commands

General utility commands.

#### fetch

Download files from URLs with progress and resumption.

```bash
java -jar nbvectors.jar fetch --url <url> --output <file>
```

**Options:**
- `--url, -u` - URL to download (required)
- `--output, -o` - Output file path (required)
- `--resume` - Resume partial downloads
- `--verify` - Verify with checksum
- `--threads` - Number of download threads

**Example:**
```bash
java -jar nbvectors.jar fetch \
  --url https://example.com/large-dataset.hdf5 \
  --output dataset.hdf5 \
  --resume \
  --threads 4
```

#### merkle

Merkle tree operations for data integrity.

```bash
java -jar nbvectors.jar merkle <subcommand> [options]
```

**Subcommands:**

##### create
Create merkle tree for a file:
```bash
java -jar nbvectors.jar merkle create \
  --file data.bin \
  --output data.mref
```

##### verify
Verify file against merkle tree:
```bash
java -jar nbvectors.jar merkle verify \
  --file data.bin \
  --reference data.mref
```

##### status
Show verification status:
```bash
java -jar nbvectors.jar merkle status \
  --state data.mrkl
```

#### generate

Generate test vector data.

```bash
java -jar nbvectors.jar generate [options]
```

**Options:**
- `--count, -n` - Number of vectors (required)
- `--dimensions, -d` - Vector dimensions (required)
- `--output, -o` - Output file (required)
- `--format` - Output format (fvec, hdf5)
- `--distribution` - Data distribution (uniform, gaussian)
- `--seed` - Random seed

**Example:**
```bash
java -jar nbvectors.jar generate \
  --count 10000 \
  --dimensions 128 \
  --output test_vectors.fvec \
  --distribution gaussian \
  --seed 42
```

### Advanced Commands

#### jjq

JQ-like JSON processor with streaming support.

```bash
java -jar nbvectors.jar jjq '<expression>' [file]
```

**Examples:**
```bash
# Extract specific fields
java -jar nbvectors.jar jjq '.datasets[].name' catalog.json

# Filter and transform
java -jar nbvectors.jar jjq '.datasets[] | select(.dimensions == 128)' catalog.json
```

#### huggingface

Hugging Face integration commands.

##### dl (download)
```bash
java -jar nbvectors.jar huggingface dl \
  --repo <repo-id> \
  --file <filename> \
  --output <path>
```

**Example:**
```bash
java -jar nbvectors.jar huggingface dl \
  --repo "organization/dataset-name" \
  --file "vectors.hdf5" \
  --output ./downloads/
```

## Common Workflows

### 1. Convert and Verify Workflow

```bash
# Convert vectors to HDF5
java -jar nbvectors.jar export_hdf5 \
  --input raw_vectors.fvec \
  --output dataset.hdf5

# Describe the result
java -jar nbvectors.jar analyze describe --file dataset.hdf5

# Verify if ground truth is included
java -jar nbvectors.jar analyze verify_knn --file dataset.hdf5
```

### 2. Dataset Discovery Workflow

```bash
# List available datasets
java -jar nbvectors.jar datasets list

# Get info about a specific dataset
java -jar nbvectors.jar datasets info --name "sift-128-euclidean"

# Download it
java -jar nbvectors.jar datasets download \
  sift-128-euclidean:default \
  --output ./data/
```

### 3. Data Integrity Workflow

```bash
# Create merkle tree
java -jar nbvectors.jar merkle create \
  --file large_dataset.hdf5 \
  --output large_dataset.mref

# Later, verify integrity
java -jar nbvectors.jar merkle verify \
  --file large_dataset.hdf5 \
  --reference large_dataset.mref
```

## Performance Tips

### Memory Management

For large datasets, increase heap size:
```bash
java -Xmx16g -jar nbvectors.jar analyze describe --file huge_dataset.hdf5
```

### Parallel Processing

Some commands support parallel execution:
```bash
java -jar nbvectors.jar export_hdf5 \
  --input vectors.fvec \
  --output vectors.hdf5 \
  --parallel 8
```

### Progress Monitoring

Use verbose mode for detailed progress:
```bash
java -jar nbvectors.jar export_hdf5 \
  --input large.fvec \
  --output large.hdf5 \
  --verbose
```

## Error Handling

### Common Error Messages

**"Unsupported format"**
- Solution: Specify format explicitly with `--format`

**"Out of memory"**
- Solution: Increase heap size with `-Xmx`

**"File not found"**
- Solution: Check path and permissions

**"Invalid dimensions"**
- Solution: Verify file format and specify dimensions

### Debug Mode

Enable debug output for troubleshooting:
```bash
java -jar nbvectors.jar --debug <command> ...
```

## Next Steps

- Explore [Data Formats](04-data-formats.md) for format details
- See [Working with Datasets](05-working-with-datasets.md) for practical examples
- Check the [API Guide](06-api-guide.md) for programmatic access
