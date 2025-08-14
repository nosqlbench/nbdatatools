# Troubleshooting

This chapter provides solutions to common issues when using NBDataTools.

## Common Error Messages

### Installation and Setup Issues

#### "java.lang.UnsupportedClassVersionError"

**Error:**
```
Exception in thread "main" java.lang.UnsupportedClassVersionError: 
Main has been compiled by a more recent version of the Java Runtime
```

**Cause:** NBDataTools requires Java 17 or higher.

**Solution:**
1. Check your Java version:
   ```bash
   java --version
   ```
2. Install Java 17 or higher
3. Update your PATH to use the correct Java version

#### "Could not find or load main class"

**Error:**
```
Error: Could not find or load main class io.nosqlbench.Main
```

**Cause:** Usually indicates a corrupted JAR file or incorrect execution.

**Solutions:**
1. Re-download the JAR file
2. Verify file integrity:
   ```bash
   java -jar nbvectors.jar --version
   ```
3. Check file permissions:
   ```bash
   chmod +x nbvectors.jar
   ```

### Memory Issues

#### "OutOfMemoryError: Java heap space"

**Error:**
```
Exception in thread "main" java.lang.OutOfMemoryError: Java heap space
```

**Cause:** Insufficient memory allocated to the JVM.

**Solutions:**

1. **Increase heap size:**
   ```bash
   java -Xmx8g -jar nbvectors.jar analyze describe --file large_dataset.hdf5
   ```

2. **Use streaming mode for large files:**
   ```bash
   java -jar nbvectors.jar analyze describe --file large_dataset.hdf5 --streaming
   ```

3. **Process in smaller chunks:**
   ```bash
   java -jar nbvectors.jar export_hdf5 \
     --input huge_vectors.fvec \
     --output huge_vectors.hdf5 \
     --chunk-size 100000
   ```

4. **Monitor memory usage:**
   ```bash
   java -XX:+PrintGCDetails -Xmx4g -jar nbvectors.jar ...
   ```

#### "OutOfMemoryError: Direct buffer memory"

**Error:**
```
java.lang.OutOfMemoryError: Direct buffer memory
```

**Cause:** Insufficient off-heap memory for I/O operations.

**Solution:**
```bash
java -XX:MaxDirectMemorySize=2g -jar nbvectors.jar ...
```

### File Format Issues

#### "Unsupported format" or "Invalid file format"

**Error:**
```
Exception: Unsupported format for file vectors.dat
```

**Causes and Solutions:**

1. **Unknown file extension:**
   ```bash
   # Specify format explicitly
   java -jar nbvectors.jar export_hdf5 \
     --input vectors.dat \
     --input-format fvec \
     --output vectors.hdf5
   ```

2. **Corrupted file header:**
   ```bash
   # Check file with hex dump
   hexdump -C vectors.fvec | head -5
   
   # Verify file size matches expected dimensions
   ```

3. **Incorrect dimensions:**
   ```bash
   # Force specific dimensions
   java -jar nbvectors.jar export_hdf5 \
     --input vectors.fvec \
     --dimensions 128 \
     --output vectors.hdf5
   ```

#### "Dimension mismatch"

**Error:**
```
Exception: Vector dimension mismatch: expected 128, got 256
```

**Solutions:**

1. **Check source file dimensions:**
   ```bash
   java -jar nbvectors.jar analyze describe --file vectors.fvec
   ```

2. **Verify file format:**
   ```bash
   # For fvec files, first 4 bytes should be dimension count
   hexdump -C vectors.fvec | head -1
   ```

3. **Fix dimension specification:**
   ```bash
   java -jar nbvectors.jar export_hdf5 \
     --input vectors.fvec \
     --dimensions 256 \
     --output vectors.hdf5
   ```

### Network and Download Issues

#### "Connection timeout" or "Unable to download"

**Error:**
```
java.net.SocketTimeoutException: Connect timed out
```

**Solutions:**

1. **Increase timeout:**
   ```bash
   java -Dnbdatatools.http.timeout=60000 \
        -jar nbvectors.jar datasets download --name dataset
   ```

2. **Check network connectivity:**
   ```bash
   # Test basic connectivity
   curl -I https://example.com/dataset.hdf5
   ```

3. **Use resume capability:**
   ```bash
   java -jar nbvectors.jar datasets download \
     --name dataset \
     --resume \
     --output ./datasets/
   ```

4. **Try fewer concurrent connections:**
   ```bash
   java -Dnbdatatools.http.max_connections=2 \
        -jar nbvectors.jar datasets download --name dataset
   ```

#### "Certificate verification failed"

**Error:**
```
javax.net.ssl.SSLHandshakeException: 
sun.security.validator.ValidatorException: PKIX path building failed
```

**Solutions:**

1. **Update Java certificates:**
   ```bash
   # Update your Java installation
   ```

2. **Disable certificate verification (not recommended):**
   ```bash
   java -Dnbdatatools.http.verify_certificates=false \
        -jar nbvectors.jar datasets download --name dataset
   ```

3. **Use HTTP instead of HTTPS (if available):**
   ```bash
   java -jar nbvectors.jar datasets download \
     --name dataset \
     --url http://example.com/dataset.hdf5
   ```

### HDF5-Specific Issues

#### "HDF5 library error" or "Unable to open HDF5 file"

**Error:**
```
Exception: HDF5-DIAG: Error detected in HDF5 (1.10.7) thread 0
```

**Causes and Solutions:**

1. **Corrupted HDF5 file:**
   ```bash
   # Check file integrity
   java -jar nbvectors.jar show_hdf5 --file dataset.hdf5 --tree
   
   # Try to repair (if tools available)
   h5recover dataset.hdf5
   ```

2. **Incomplete download:**
   ```bash
   # Re-download with verification
   java -jar nbvectors.jar datasets download \
     --name dataset \
     --verify \
     --force
   ```

3. **Version compatibility:**
   ```bash
   # Try opening with different HDF5 version
   java -Dnbdatatools.hdf5.version=1.10 \
        -jar nbvectors.jar show_hdf5 --file dataset.hdf5
   ```

#### "Dataset not found in HDF5 file"

**Error:**
```
Exception: Dataset '/base/data' not found in HDF5 file
```

**Solutions:**

1. **Check file structure:**
   ```bash
   java -jar nbvectors.jar show_hdf5 --file dataset.hdf5 --tree
   ```

2. **Specify correct path:**
   ```bash
   java -jar nbvectors.jar analyze describe \
     --file dataset.hdf5 \
     --dataset-path "/vectors/data"
   ```

3. **List all datasets:**
   ```bash
   java -jar nbvectors.jar show_hdf5 --file dataset.hdf5 --list-datasets
   ```

## Performance Issues

### Slow Processing

#### Large File Processing is Slow

**Symptoms:** Operations take much longer than expected.

**Diagnostic steps:**

1. **Check available memory:**
   ```bash
   java -XX:+PrintGCDetails -jar nbvectors.jar analyze describe --file large.hdf5
   ```

2. **Monitor I/O usage:**
   ```bash
   # On Linux
   iostat -x 1
   
   # On macOS  
   iostat -w 1
   ```

**Solutions:**

1. **Increase memory allocation:**
   ```bash
   java -Xmx16g -jar nbvectors.jar analyze describe --file large.hdf5
   ```

2. **Use streaming mode:**
   ```bash
   java -jar nbvectors.jar analyze describe \
     --file large.hdf5 \
     --streaming \
     --chunk-size 1000000
   ```

3. **Enable parallel processing:**
   ```bash
   java -jar nbvectors.jar export_hdf5 \
     --input large.fvec \
     --output large.hdf5 \
     --parallel 8
   ```

4. **Optimize chunk size:**
   ```bash
   # For SSD storage
   java -Dnbdatatools.chunk.size=10485760 \
        -jar nbvectors.jar ...
   
   # For spinning disk
   java -Dnbdatatools.chunk.size=1048576 \
        -jar nbvectors.jar ...
   ```

#### Network Downloads are Slow

**Solutions:**

1. **Increase parallel connections:**
   ```bash
   java -jar nbvectors.jar datasets download \
     --name large_dataset \
     --threads 8
   ```

2. **Use regional mirrors (if available):**
   ```bash
   java -jar nbvectors.jar datasets download \
     --name dataset \
     --mirror us-west
   ```

3. **Resume partial downloads:**
   ```bash
   java -jar nbvectors.jar datasets download \
     --name dataset \
     --resume
   ```

### High Memory Usage

#### Memory Usage Continuously Increases

**Symptoms:** Memory usage grows over time, eventually causing OutOfMemoryError.

**Diagnostic:**

1. **Enable GC logging:**
   ```bash
   java -XX:+PrintGC -XX:+PrintGCDetails -Xloggc:gc.log \
        -jar nbvectors.jar ...
   ```

2. **Generate heap dump:**
   ```bash
   java -XX:+HeapDumpOnOutOfMemoryError \
        -XX:HeapDumpPath=/tmp/heap.hprof \
        -jar nbvectors.jar ...
   ```

**Solutions:**

1. **Use try-with-resources:**
   ```java
   // Good
   try (TestDataView data = TestDataView.open(path)) {
       // Use data
   } // Automatically closed
   
   // Bad - resource leak
   TestDataView data = TestDataView.open(path);
   // Missing close()
   ```

2. **Clear caches periodically:**
   ```bash
   java -Dnbdatatools.cache.clear_interval=3600 \
        -jar nbvectors.jar ...
   ```

3. **Reduce cache size:**
   ```bash
   java -Dnbdatatools.cache.size=100000 \
        -jar nbvectors.jar ...
   ```

## Data Quality Issues

### Ground Truth Verification Failures

#### "Ground truth verification failed"

**Error:**
```
Ground truth verification failed: Expected neighbor 123, but computed 456
```

**Causes and Solutions:**

1. **Distance function mismatch:**
   ```bash
   # Check dataset distance function
   java -jar nbvectors.jar show_hdf5 --file dataset.hdf5 --attributes
   
   # Verify with correct distance
   java -jar nbvectors.jar analyze verify_knn \
     --file dataset.hdf5 \
     --distance euclidean
   ```

2. **Precision issues:**
   ```bash
   # Use higher precision tolerance
   java -jar nbvectors.jar analyze verify_knn \
     --file dataset.hdf5 \
     --tolerance 1e-5
   ```

3. **Incomplete ground truth:**
   ```bash
   # Check ground truth coverage
   java -jar nbvectors.jar analyze describe --file dataset.hdf5 --detailed
   ```

### Data Corruption Detection

#### "Merkle verification failed"

**Error:**
```
Merkle verification failed: Chunk 42 hash mismatch
```

**Solutions:**

1. **Re-download affected chunks:**
   ```bash
   java -jar nbvectors.jar merkle verify \
     --file dataset.hdf5 \
     --reference dataset.mref \
     --repair
   ```

2. **Check disk integrity:**
   ```bash
   # On Linux
   fsck /dev/sda1
   
   # On macOS
   diskutil verifyDisk disk0
   ```

3. **Verify source integrity:**
   ```bash
   # Re-download from source
   java -jar nbvectors.jar datasets download \
     --name dataset \
     --force \
     --verify
   ```

## Configuration Issues

### Missing Configuration Files

#### "Configuration file not found"

**Error:**
```
Exception: Configuration file ~/.config/vectordata/config.json not found
```

**Solutions:**

1. **Create default configuration:**
   ```bash
   mkdir -p ~/.config/vectordata
   java -jar nbvectors.jar config init
   ```

2. **Specify custom config location:**
   ```bash
   java -Dnbdatatools.config.file=/path/to/config.json \
        -jar nbvectors.jar ...
   ```

3. **Use embedded defaults:**
   ```bash
   java -Dnbdatatools.config.use_defaults=true \
        -jar nbvectors.jar ...
   ```

### Catalog Issues

#### "Dataset not found in catalog"

**Error:**
```
Exception: Dataset 'unknown-dataset' not found in any catalog
```

**Solutions:**

1. **Update catalogs:**
   ```bash
   java -jar nbvectors.jar datasets refresh-catalogs
   ```

2. **List available datasets:**
   ```bash
   java -jar nbvectors.jar datasets list
   ```

3. **Add custom catalog:**
   ```bash
   java -jar nbvectors.jar datasets add-catalog \
     --url https://example.com/custom-catalog.json
   ```

4. **Use direct URL:**
   ```bash
   java -jar nbvectors.jar datasets download \
     --url https://example.com/dataset.hdf5 \
     --output dataset.hdf5
   ```

## Platform-Specific Issues

### Windows-Specific Issues

#### "Path too long" error

**Error:**
```
java.nio.file.InvalidPathException: Illegal char in path
```

**Solutions:**

1. **Use shorter paths:**
   ```cmd
   # Move to shorter directory
   cd C:\data
   java -jar nbvectors.jar ...
   ```

2. **Enable long paths (Windows 10+):**
   ```cmd
   # Run as administrator
   reg add HKLM\SYSTEM\CurrentControlSet\Control\FileSystem /v LongPathsEnabled /t REG_DWORD /d 1
   ```

3. **Use UNC paths:**
   ```cmd
   java -jar nbvectors.jar analyze describe --file "\\?\C:\very\long\path\dataset.hdf5"
   ```

#### File locking issues

**Error:**
```
java.nio.file.AccessDeniedException: The process cannot access the file because it is being used by another process
```

**Solutions:**

1. **Close other applications using the file**
2. **Use exclusive mode:**
   ```bash
   java -Dnbdatatools.file.exclusive=true -jar nbvectors.jar ...
   ```
3. **Restart the system if necessary**

### macOS-Specific Issues

#### "Permission denied" for downloaded JAR

**Error:**
```
-bash: ./nbvectors.jar: Permission denied
```

**Solutions:**

1. **Make executable:**
   ```bash
   chmod +x nbvectors.jar
   ```

2. **Remove quarantine attribute:**
   ```bash
   xattr -d com.apple.quarantine nbvectors.jar
   ```

3. **Run with java explicitly:**
   ```bash
   java -jar nbvectors.jar
   ```

### Linux-Specific Issues

#### "Native library loading failed"

**Error:**
```
java.lang.UnsatisfiedLinkError: no hdf5_java in java.library.path
```

**Solutions:**

1. **Install HDF5 libraries:**
   ```bash
   # Ubuntu/Debian
   sudo apt-get install libhdf5-java
   
   # CentOS/RHEL
   sudo yum install hdf5-java
   
   # Arch Linux
   sudo pacman -S hdf5-java
   ```

2. **Set library path:**
   ```bash
   export LD_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/jni:$LD_LIBRARY_PATH
   java -jar nbvectors.jar ...
   ```

3. **Use bundled libraries:**
   ```bash
   java -Dnbdatatools.native.bundled=true -jar nbvectors.jar ...
   ```

## Debugging Techniques

### Enable Debug Logging

**Comprehensive debugging:**

```bash
java -Dorg.slf4j.simpleLogger.defaultLogLevel=DEBUG \
     -Dorg.slf4j.simpleLogger.showDateTime=true \
     -Dorg.slf4j.simpleLogger.dateTimeFormat="yyyy-MM-dd HH:mm:ss" \
     -jar nbvectors.jar analyze describe --file dataset.hdf5
```

**Specific component debugging:**

```bash
# Network debugging
java -Dorg.slf4j.simpleLogger.log.io.nosqlbench.transport=DEBUG \
     -jar nbvectors.jar datasets download --name dataset

# HDF5 debugging  
java -Dorg.slf4j.simpleLogger.log.io.nosqlbench.hdf5=DEBUG \
     -jar nbvectors.jar show_hdf5 --file dataset.hdf5

# Merkle debugging
java -Dorg.slf4j.simpleLogger.log.io.nosqlbench.merkle=DEBUG \
     -jar nbvectors.jar merkle verify --file dataset.hdf5 --reference dataset.mref
```

### Performance Profiling

**JVM profiling:**

```bash
# Basic GC monitoring
java -XX:+PrintGC -XX:+PrintGCTimeStamps \
     -jar nbvectors.jar ...

# Detailed GC analysis
java -XX:+PrintGCDetails -XX:+PrintGCApplicationStoppedTime \
     -Xloggc:gc.log \
     -jar nbvectors.jar ...

# JIT compilation monitoring
java -XX:+PrintCompilation \
     -jar nbvectors.jar ...
```

**Memory profiling:**

```bash
# Generate heap histogram
jcmd <pid> GC.class_histogram

# Generate heap dump
jcmd <pid> GC.dump /tmp/heap.hprof
```

### Network Debugging

**HTTP traffic analysis:**

```bash
# Enable HTTP wire logging
java -Dorg.apache.commons.logging.Log=org.apache.commons.logging.impl.SimpleLog \
     -Dorg.apache.commons.logging.simplelog.showdatetime=true \
     -Dorg.apache.commons.logging.simplelog.log.org.apache.http.wire=DEBUG \
     -jar nbvectors.jar datasets download --name dataset
```

**Network connectivity testing:**

```bash
# Test basic connectivity
telnet example.com 80

# Test HTTPS connectivity
openssl s_client -connect example.com:443

# Test with curl
curl -v -I https://example.com/dataset.hdf5
```

## Getting Help

### Collecting Debug Information

When reporting issues, include:

1. **System information:**
   ```bash
   java --version
   uname -a  # Linux/macOS
   systeminfo  # Windows
   ```

2. **NBDataTools version:**
   ```bash
   java -jar nbvectors.jar --version
   ```

3. **Full command and error output:**
   ```bash
   java -jar nbvectors.jar <command> > output.log 2>&1
   ```

4. **Configuration details:**
   ```bash
   # Show effective configuration
   java -jar nbvectors.jar config show
   ```

### Log Analysis

**Common log patterns to look for:**

- `OutOfMemoryError`: Memory issues
- `IOException`: File access problems  
- `SocketTimeoutException`: Network issues
- `IllegalArgumentException`: Configuration problems
- `HDF5Exception`: HDF5 file issues

### Community Resources

- **GitHub Issues**: Report bugs and feature requests
- **Documentation**: Latest user manual and API docs
- **Examples**: Sample code and configurations
- **Wiki**: Community-contributed solutions

### Creating Minimal Reproduction Cases

When reporting issues:

1. **Use smallest possible dataset**
2. **Simplify command to minimal failing case**
3. **Include sample data if possible**
4. **Provide complete error output**
5. **Test with latest version**

## Summary

This troubleshooting guide covers:

- **Common errors** and their solutions
- **Performance issues** and optimization techniques
- **Data quality problems** and validation methods
- **Platform-specific** considerations
- **Debugging techniques** and tools
- **How to get help** effectively

Most issues can be resolved by:
- Checking Java version and heap size
- Verifying file formats and integrity
- Using appropriate command-line options
- Enabling debug logging when needed

For persistent issues, collect comprehensive debug information and consult the community resources.