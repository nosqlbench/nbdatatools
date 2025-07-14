# Non-TUI Mode for Merkle Create Command

## Overview
Added a new mode to the `merkle create` command that disables the TUI (Text User Interface) view and provides simple progress reporting at configurable intervals.

## New Command Line Options

### `--no-tui`
- **Description**: Disable TUI view and use simple progress reporting
- **Default**: false (TUI mode enabled)
- **Usage**: `merkle create --no-tui files...`

### `--progress-interval`
- **Description**: Progress reporting interval in seconds (default: 5)
- **Default**: 5 seconds
- **Usage**: `merkle create --no-tui --progress-interval 10 files...`

## Use Cases

### 1. Headless Environments
Perfect for servers without display capabilities:
```bash
merkle create --no-tui --progress-interval 30 /data/large-file.dat
```

### 2. CI/CD Pipelines
Ideal for automated builds and deployments:
```bash
merkle create --no-tui --progress-interval 60 /build/artifacts/*.dat
```

### 3. Background Processing
When you want to run merkle creation in the background:
```bash
nohup merkle create --no-tui --progress-interval 120 /data/*.dat > merkle.log 2>&1 &
```

### 4. Docker Containers
When running in containerized environments:
```bash
docker run -v /data:/data myimage merkle create --no-tui --progress-interval 15 /data/*.dat
```

## Output Examples

### Non-TUI Mode Output
```
2025-07-11 00:00:00 INFO  SimpleProgressReporter - Starting Merkle tree creation for: /data/large-file.dat
2025-07-11 00:00:00 INFO  SimpleProgressReporter - File size: 1073741824 bytes
2025-07-11 00:00:00 INFO  SimpleProgressReporter - Processing file: 1024 leaf chunks + 1023 internal nodes = 2047 total blocks
2025-07-11 00:00:00 INFO  SimpleProgressReporter - Estimated Merkle tree file size: 64.00 KB
2025-07-11 00:00:05 INFO  SimpleProgressReporter - Merkle tree progress - File: 15.2% (156/1024 chunks) | Phase: Processing leaf nodes | Rate: 31.2 chunks/sec | Session: 7.6% (156/2047 blocks)
2025-07-11 00:00:10 INFO  SimpleProgressReporter - Merkle tree progress - File: 28.7% (294/1024 chunks) | Phase: Processing leaf nodes | Rate: 29.4 chunks/sec | Session: 14.4% (294/2047 blocks)
2025-07-11 00:00:15 INFO  SimpleProgressReporter - Merkle tree progress - File: 43.1% (441/1024 chunks) | Phase: Processing leaf nodes | Rate: 29.4 chunks/sec | Session: 21.5% (441/2047 blocks)
...
2025-07-11 00:00:35 INFO  SimpleProgressReporter - Merkle tree progress - File: 100.0% (1024/1024 chunks) | Phase: Processing internal nodes | Rate: 29.3 chunks/sec | Session: 100.0% (2047/2047 blocks)
2025-07-11 00:00:36 INFO  SimpleProgressReporter - Merkle tree creation completed for: /data/large-file.dat
2025-07-11 00:00:36 INFO  SimpleProgressReporter - Processing time: 36.24s | Total chunks: 1024 | Rate: 28.3 chunks/sec | Throughput: 28.3 MB/s
```

### TUI Mode Output (Default)
```
Creating Merkle tree for: /data/large-file.dat
File size: 1073741824 bytes
Processing file: 1024 leaf chunks + 1023 internal nodes = 2047 total blocks

Progress: [████████████████████████████████████████████████████████████████] 100%
Phase: Processing internal nodes
Rate: 28.3 chunks/sec
Throughput: 28.3 MB/s
Session: 100.0% (2047/2047 blocks)

Merkle tree creation completed successfully
```

## Implementation Details

### Components Added

1. **SimpleProgressReporter** (`commands/src/main/java/io/nosqlbench/command/merkle/console/SimpleProgressReporter.java`)
   - Provides log-based progress reporting
   - Configurable reporting intervals
   - Session-wide progress tracking
   - Performance metrics logging

2. **Command Line Options** (in `CMD_merkle_create.java`)
   - `--no-tui`: Disables TUI mode
   - `--progress-interval`: Sets reporting interval

3. **Method Refactoring**
   - Split `createMerkleFile` into TUI and non-TUI versions
   - `createMerkleFileWithTuiDisplay`: Original TUI implementation
   - `createMerkleFileWithSimpleReporter`: New simple progress implementation

### Features

- **Configurable Intervals**: Set progress reporting frequency from 1 second to any desired interval
- **Session Progress**: Tracks progress across multiple files
- **Performance Metrics**: Reports throughput and processing rates
- **Backwards Compatible**: Existing code continues to work unchanged
- **Auto-Cleanup**: Proper resource management with try-with-resources

### Progress Information Reported

1. **File Information**
   - File size and path
   - Number of leaf chunks and internal nodes
   - Estimated Merkle tree file size

2. **Progress Updates**
   - File progress percentage and chunk counts
   - Current processing phase
   - Processing rate (chunks/second)
   - Session-wide progress (when processing multiple files)

3. **Final Summary**
   - Total processing time
   - Final throughput metrics
   - Performance statistics

## Testing

All existing tests continue to pass, ensuring backward compatibility. The new functionality can be tested with:

```bash
# Test basic non-TUI mode
merkle create --no-tui test-file.dat

# Test with custom interval
merkle create --no-tui --progress-interval 2 test-file.dat

# Test with multiple files
merkle create --no-tui --progress-interval 10 *.dat

# Test traditional TUI mode (default)
merkle create test-file.dat
```

## Benefits

1. **Suitable for Automation**: Perfect for scripts and CI/CD pipelines
2. **Resource Efficient**: Lower CPU usage compared to TUI mode
3. **Log-Friendly**: Output integrates well with logging systems
4. **Flexible**: Configurable progress intervals for different use cases
5. **Container-Ready**: Works well in Docker and other containerized environments