# Getting Started

This guide will help you get NBDataTools up and running on your system.

## Prerequisites

### System Requirements

- **Disk Space**: Varies by dataset size
- **OS**: Linux, macOS, or Windows (Only Linux is a tested build)

#### For building from source:

- **Java**: Version 25 or higher. This project supports Java 11 clients but requires Java 25 to build some command line features which are separate.

#### For using the command line nbvectors tool:

- Java 25 is recommended, especially if you need to use SIMD to speed up vector computations.
- Java 11 is supported without the optimizations.

#### For using the vectordata access library:

- Java 11 or any newer version is supported.

## Installation

### Option 1: Download Pre-built JAR

1. Download the latest `nbvectors.jar` from the releases page
2. Place it in a convenient location (e.g., `~/bin/` or `/usr/local/bin/`)
3. Make it executable:
   ```bash
   chmod +x nbvectors.jar
   ```

### Option 2: Build from Source

1. Clone the repository:
   ```bash
   git clone https://github.com/nosqlbench/nbdatatools.git
   cd nbdatatools
   ```

2. Build with Maven:
   ```bash
   mvn clean package
   ```

3. The executable JAR will be in `nbvectors/target/nbvectors.jar`

### Setting Up an Alias (Optional)

For convenience, create an alias:

```bash
alias nbvectors='java -jar /path/to/nbvectors.jar'
```

Add this to your shell configuration file (`~/.bashrc`, `~/.zshrc`, etc.).

## First Steps

### 1. Verify Installation

Run the help command:

```bash
java -jar nbvectors.jar --help
```

You should see a list of available commands.

### 2. Check Available Commands

List all subcommands:

```bash
java -jar nbvectors.jar
```

Output will show commands like:
- `analyze` - Analyze test data files
- `export_hdf5` - Convert vector files to HDF5
- `datasets` - Browse available datasets
- And many more...

### 3. Get Command-Specific Help

For any command, add `--help`:

```bash
java -jar nbvectors.jar export_hdf5 --help
```

## Quick Start Examples

### Example 1: Convert Vector Files to HDF5

If you have `.fvec` files (common format for float vectors):

```bash
# Convert a single file
java -jar nbvectors.jar export_hdf5 \
  --input vectors.fvec \
  --output vectors.hdf5 \
  --dataset-name "my_vectors"
```

### Example 2: Analyze a Dataset

Check the contents of an HDF5 file:

```bash
java -jar nbvectors.jar analyze describe --file dataset.hdf5
```

### Example 3: Browse Available Datasets

List datasets from the catalog:

```bash
java -jar nbvectors.jar datasets list
```

### Example 4: Download a Dataset

Download a specific dataset:

```bash
java -jar nbvectors.jar datasets download \
  sift-128-euclidean:default \
  --output ./datasets/
```

## Configuration

### Default Configuration Location

NBDataTools stores configuration in:
```
~/.config/vectordata/
```

### Directory Structure

After first use, you'll see:
```
~/.config/vectordata/
├── catalogs/       # Dataset catalogs
├── cache/          # Downloaded data cache
└── config.json     # Main configuration
```

### Environment Variables

You can override defaults with environment variables:

- `VECTORDATA_CONFIG`: Configuration directory path
- `VECTORDATA_CACHE`: Cache directory path
- `VECTORDATA_CATALOGS`: Catalog directory path

## Common Workflows

### Converting Data for Testing

1. **Prepare your data** in a supported format (fvec, ivec, parquet)
2. **Convert to HDF5** using `export_hdf5`
3. **Verify the conversion** with `analyze describe`
4. **Use the data** in your tests

### Working with Existing Datasets

1. **Browse available** datasets with `datasets list`
2. **Download** the dataset you need
3. **Inspect** with `show_hdf5` or `analyze`
4. **Use** in your application or benchmarks

## Next Steps

Now that you have NBDataTools installed and working:

1. Learn about [Core Concepts](02-core-concepts.md) to understand the data model
2. Explore the [CLI Reference](03-cli-reference.md) for detailed command documentation
3. Check out [Working with Datasets](05-working-with-datasets.md) for practical workflows

## Troubleshooting Installation

### Java Version Issues

If you see an error about Java version:
```
Error: LinkageError occurred while loading main class
java.lang.UnsupportedClassVersionError: has been compiled by a more recent version
```

Solution: Update to Java 17 or higher.

### Memory Issues

For large datasets, increase heap size:
```bash
java -Xmx8g -jar nbvectors.jar analyze ...
```

### Permission Issues

If you can't execute the JAR:
```bash
chmod +x nbvectors.jar
```

Or run explicitly with Java:
```bash
java -jar nbvectors.jar
```

📝 **Note**: For production use, consider creating a wrapper script that sets appropriate JVM options for your workload.

Ready to dive deeper? Continue to [Core Concepts](02-core-concepts.md).
