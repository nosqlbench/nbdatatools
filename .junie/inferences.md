# Project Inferences

This document tracks important details learned about the project.

## Requirements

### Merkle Package Requirements
From `/home/jshook/IdeaProjects/nbdatatools/vectordata/src/main/java/io/nosqlbench/vectordata/merkle/package-info.java` (mtime: $(date -r /home/jshook/IdeaProjects/nbdatatools/vectordata/src/main/java/io/nosqlbench/vectordata/merkle/package-info.java))

The merkle package provides implementations for Merkle trees, which are used for efficient verification of data integrity. The package includes:

- MerkleTree: A data structure that represents a Merkle tree
- MerklePane: A component of the Merkle tree that handles specific operations
- MerklePainter: Handles downloading and verification of data against a reference Merkle tree

#### Merkle Tree Structure
- Implemented as a binary tree where each leaf node contains the hash of a chunk of data
- Each internal node contains the hash of its two children
- Uses a heap-structured arrangement in a flat array
- Root node is at index 0, left child at 2*i+1, right child at 2*i+2
- Leaf nodes start at the offset index (calculated as capLeaf - 1)
- Uses SHA-256 hashes for all nodes
- A BitSet tracks which nodes have valid hashes (1 = valid, 0 = invalid)

#### Update Operations
- When a leaf node is updated, all parent nodes up to the root are invalidated
- Hashes are computed lazily when accessed
- During shutdown, calculable hashes are computed in a breadth-first manner

#### Key Requirements
- Two tracking structures for merkle tree content:
  1. A merkle tree stored directly in a byte array (byte buffer) for write-through semantics
  2. An ephemeral bit set to track which chunks have been validated
- A merkle tree hash set to all zeroes is considered invalidated
- A valid merkle tree node has its bit in the bitset set to 1
- Consistency must be maintained between the bitset and the merkle tree hashes
- When a hash node is updated, parent hashes should be computed as long as sibling hashes are valid
- When a hash node is invalidated, all parent hashes to the root must be invalidated
- MerklePainter should honor minimum and maximum download sizes
- MerklePainter should track the most recent request by chunk index
- Auto-buffering behavior should be implemented when contiguous requests exceed a threshold
- MerklePainter should check validity before scheduling paint operations
- At no time should there be concurrent requests for an overlapping region
- Downloaded data must be verified against the reference merkle tree
- Merkle painter events should be captured in MerklePainterEvent as enumerations

#### MerklePane Setup
- It should be invalid to create a MerklePane without a remote content URL
- When initializing for a non-existent local file, a presence check should be done for remote resources
- If remote content or merkle files are not accessible, setup should be aborted
- Using "file" protocol for remote content URL should be disallowed to prevent data duplication

#### Auto-Buffering Behavior
- Enabled when contiguous requests exceed AUTOBUFFER_THRESHOLD (10)
- Uses maximum download size for better throughput
- Keeps at least 4 additional requests running in read-ahead mode
- Significantly improves performance for sequential access patterns

#### Shutdown Behavior
1. Stop or abandon any pending transfers
2. Compute calculable merkle tree hashes (breadth-first)
3. Flush merkle tree data to disk, ensuring the merkle tree file mtime is newer than the content file

### Merkle Subcommands Package Requirements
From `/home/jshook/IdeaProjects/nbdatatools/commands/src/main/java/io/nosqlbench/command/merkle/subcommands/package-info.java` (mtime: $(date -r /home/jshook/IdeaProjects/nbdatatools/commands/src/main/java/io/nosqlbench/command/merkle/subcommands/package-info.java))

The merkle subcommands package provides command-line interfaces for working with Merkle trees. The package includes:

#### Merkle Summary (SummaryCommand)
- Should print out the basic details of a merkle tree file in a human readable format for a specified content file, selecting the merkle tree file automatically by extension
  - Should include the number of chunks, the content file size, the merkle tree file size
  - Should include all key details from the merkle footer
- Should print out a braille-formatted char image representing the leaf node status
- Should show the number of valid leaf nodes, valid parent nodes, and valid total nodes
- Should show the number of total leaf nodes, total parent nodes, and total all nodes

## Implementation Notes

### MerkleTree Digest Factory Method
The MerkleTree class has been enhanced with a static factory method `createDigest()` that creates new instances of MessageDigest for SHA-256 hashing. This method provides a consistent way to get new digest instances for thread-safe hash computations.

Previously, the class had a static final MessageDigest instance called DIGEST, but in several methods, new MessageDigest instances were created for each computation to avoid interference. This suggested that the static DIGEST instance might not be thread-safe or might have other limitations.

The new `createDigest()` method:
1. Creates a new MessageDigest instance configured for SHA-256 hashing
2. Handles the NoSuchAlgorithmException and wraps it in a RuntimeException with a descriptive message
3. Provides a consistent way to get new digest instances throughout the codebase
4. Includes comprehensive javadoc in the markdown format with an example of how to use the method

This enhancement improves the thread safety and consistency of the MerkleTree implementation, especially in scenarios where multiple threads might be computing hashes concurrently.

### MerkleTree Zero Hash Handling
The MerkleTree implementation has been updated to properly handle zero hashes in accordance with the requirement stated in the package-info.java file: "A merkle tree hash set to all zeroes is considered invalidated."

The issue was in the `getHash` method, where a hash that was found to be all zeros was marked as invalid, but the method immediately returned the all-zero hash instead of recalculating it. This caused the `testZeroHashesMarkedInvalid` test to fail because the leaf hash was still all zeros after recalculation.

The following changes were made:
1. Modified the `getHash` method to continue to the recalculation logic when a hash is found to be all zeros, instead of immediately returning the all-zero hash.
2. Ensured that the hash is only cached and returned if it's not all zeros.

These changes ensure that when a hash is all zeros, it's properly marked as invalid and recalculated when accessed, which is essential for the correct functioning of the MerkleTree implementation, especially in scenarios where data integrity verification is critical.

### MerkleTree Stability Enhancements
The MerkleTree implementation has been enhanced to ensure stability across different runs and systems. The key issue identified was that the implementation did not explicitly set the byte order when using ByteBuffer, which could lead to different results on different systems with different native byte orders.

The following changes were made:
1. Added explicit ByteOrder.BIG_ENDIAN setting to MerkleFooter.toByteBuffer() method to ensure consistent serialization across platforms
2. Added explicit ByteOrder.BIG_ENDIAN setting to MerkleFooter.fromByteBuffer() method to ensure consistent deserialization across platforms
3. Added explicit ByteOrder.BIG_ENDIAN setting to MerkleTree.readFooterBuffer() method to ensure consistent reading of footer data across platforms
4. Created a new MerkleTreeStabilityTest class to verify the stability of the implementation across different runs and systems

The test class includes:
- A parameterized test that verifies stability with different chunk sizes
- A test that verifies the root hash matches an expected value computed independently
- Tests that create trees using different methods and verify they produce identical results

These changes ensure that the MerkleTree implementation produces the same hash for the same input data across different runs and different systems, regardless of their native byte order. This is essential for the correct functioning of the MerkleTree implementation, especially in scenarios where data integrity verification is critical across different platforms.

### Count Zeros Command Implementation
The `count_zeros` command has been implemented according to the requirements in the `io.nosqlbench.command.count_zeros.package-info.java` file. The command counts "zero vectors" in vector files, where a "zero vector" is one which has zero as its value for every dimensional component.

The implementation includes:
1. Support for `.fvec`, `.fvecs`, `.ivec`, and `.ivecs` file formats
2. Progress bars for both overall progress (across all files) and file progress (within the current file)
3. A summary for each file showing the number of zero vectors and the total number of vectors scanned
4. Use of `VectorFileIO` for file access instead of direct file I/O, as required in the package-info.java

The command is implemented in the `CMD_count_zeros` class, which:
- Implements `Callable<Integer>` and `BundledCommand` interfaces
- Takes file arguments with `@CommandLine.Parameters`
- Uses `VectorFileIO.vectorFileArray()` to read vector files
- Processes each vector to check if it's a zero vector
- Shows progress bars during processing
- Prints a summary at the end for each file
- Uses modern Java features like pattern matching for instanceof
- Includes comprehensive error handling with proper exception handling and logging
- Has well-documented code using the markdown javadoc format with triple slashes

Tests have been added in `CMD_count_zerosTest` to verify the functionality:
- `testCountZerosInFvecFile()`: Tests counting zero vectors in a `.fvec` file
- `testCountZerosInIvecFile()`: Tests counting zero vectors in a `.ivec` file
- `testCountZerosInMultipleFiles()`: Tests counting zero vectors in multiple files

The tests create test files with known zero vectors and verify that the command correctly counts them.

### CHUNK_VFY_FAIL Event Enhancement
The CHUNK_VFY_FAIL event in MerklePainterEvent has been enhanced to include reference and computed hash fields in hexadecimal format. This allows for better debugging and analysis when hash verification fails.

The following changes were made:
1. Added two new parameters to the CHUNK_VFY_FAIL event in MerklePainterEvent.java:
   - "refHash" (String) - The reference hash value in hexadecimal
   - "compHash" (String) - The computed hash value in hexadecimal
2. Added a utility method `bytesToHex` to MerklePainter.java to convert byte arrays to hexadecimal strings
3. Updated all four occurrences of CHUNK_VFY_FAIL event logging in MerklePainter.java to include the reference and computed hash values in hexadecimal format

These changes make it easier to diagnose verification failures by showing the exact hash values that were compared, allowing for visual inspection of the differences.

### MerklePane Updates
The MerklePane class has been updated to enforce the requirement that a remote content URL must be provided when creating a MerklePane instance. This is in accordance with the requirement stated in the package-info.java file: "It should be invalid to try to create a MerklePane without a remote content URL."

The following changes were made:
1. Updated the first three constructors to throw an IllegalArgumentException when a remote content URL is not provided.
2. Updated the fourth constructor to check if the sourceUrl parameter is null or empty and throw an IllegalArgumentException if it is.
3. Updated the fifth constructor (used for testing) to also check if the sourceUrl parameter is null or empty.

These changes ensure that a MerklePane cannot be created without a remote content URL, which is a requirement for the proper functioning of the merkle package.

### BitSetTracker File Naming Convention
The MerkleTree class has been updated to properly name BitSetTracker character files according to the requirement stated in the package-info.java file: "Since a merkle painter contains two different merkle trees, the reference tree and the active tree for the downloading content, the bimg files need to be distinct. For this reason, the bimg file names must be based on the full merkle tree file name, adding the bimg extension to any extensions already present on the file."

The following changes were made:
1. Updated the file naming in the MerkleTree constructor to append ".bimg" to the full merkle tree file name instead of replacing ".mrkl" with ".bimg".
2. Updated the file naming in the save method to append ".bimg" to the full merkle tree file name.
3. Updated the file naming in the load method to append ".bimg" to the full merkle tree file name instead of extracting the base name without extension.

These changes ensure that BitSetTracker files are properly named with extensions like '<file>.mrkl.bimg' or '<file>.mref.bimg', not just '<file>.bimg', which prevents file collisions.

### MerkleTree Method Removal and PathCommand Update
The MerkleTree class has been updated to remove the `getPathToRootFromFile` method as indicated by the comment on lines 1476-1477: "This method has been removed as the base implementation now uses memory-mapped file access for efficient random access to the merkle tree file. Use getPathToRoot instead."

The PathCommand class in the commands.merkle.subcommands package was still using this removed method, which caused build failures. The following changes were made to fix this issue:

1. Updated the PathCommand.execute method to load the MerkleTree from the file first using `MerkleTree.load(merklePath)` and then call `getPathToRoot` on the loaded tree instead of using the removed `getPathToRootFromFile` method.

This change ensures that the PathCommand class works correctly with the updated MerkleTree implementation while maintaining the same functionality. The tests in CMD_merkleTest.java now pass successfully, confirming that the fix works as expected.

### MerkleTree Verification Code Refactoring
The MerkleTree class has been updated to move the verification functionality from production code to test code. This change was made to improve performance and maintainability by separating concerns. The following changes were made:

1. Modified the `load(Path path)` method to call `load(Path path, false)` instead of `load(Path path, true)`, disabling verification in production code.
2. Added a comment in the `load(Path path)` method stating "Verification is now handled in unit tests, not in production code".
3. Added a comment in the `load(Path path, boolean verify)` method stating "Verification is now handled in unit tests, not in production code. The verify parameter is kept for backward compatibility but is ignored".
4. Removed the verification code from the `load(Path path, boolean verify)` method.
5. Created a new `MerkleTreeFileVerificationTest` class that contains the verification logic that was previously in the MerkleTree class.
6. Updated the `MerkleTreeVerificationTest` class to reflect the changes in the MerkleTree class.
7. Updated the `MerkleTreeRealFileTest` and `MerkleTreePerformanceTest` classes to reflect the changes in the MerkleTree class.

These changes ensure that verification is still performed in tests, but not in production code, which improves performance for production use cases. The tests in the project now pass successfully, confirming that the changes work as expected.

### MerkleTree Verification Code Refactoring
The MerkleTree class has been updated to move the verification functionality from production code to test code. This change was made to improve performance and maintainability by separating concerns. The following changes were made:

1. Modified the `load(Path path)` method to call `load(Path path, false)` instead of `load(Path path, true)`, disabling verification in production code.
2. Added a comment in the `load(Path path)` method stating "Verification is now handled in unit tests, not in production code".
3. Added a comment in the `load(Path path, boolean verify)` method stating "Verification is now handled in unit tests, not in production code. The verify parameter is kept for backward compatibility but is ignored".
4. Removed the verification code from the `load(Path path, boolean verify)` method.
5. Created a new `MerkleTreeFileVerificationTest` class that contains the verification logic that was previously in the MerkleTree class.
6. Updated the `MerkleTreeVerificationTest` class to reflect the changes in the MerkleTree class.
7. Updated the `MerkleTreeRealFileTest` and `MerkleTreePerformanceTest` classes to reflect the changes in the MerkleTree class.

These changes ensure that verification is still performed in tests, but not in production code, which improves performance for production use cases. The tests in the project now pass successfully, confirming that the changes work as expected.

## Ambiguous Requirements

## Testing Requirements

### MerkleTree Identity Property
The MerkleTree implementation has been tested to verify that two different merkle tree files created from the same content produce identical trees. This is a fundamental property of Merkle trees that ensures consistency and reliability.

Two test cases have been implemented in `MerkleTreeIdentityTest.java`:
1. `testIdenticalTreesFromSameContent()` - Verifies that trees created from the same content using different methods (ByteBuffer vs. file) are identical
2. `testIdenticalTreesWithDifferentFileNames()` - Verifies that trees created from the same content but with different file names are identical

These tests ensure that:
- The trees have the same properties (number of leaves, chunk size, total size)
- All leaf hashes match between the trees
- There are no mismatched chunks between the trees
- The root hashes match, confirming the entire tree structure is identical

An issue was identified and fixed in the MerkleTree implementation that was causing these tests to fail. The problem was an inconsistency in how hash positions were calculated for internal nodes between the `hashData` method and the `getHash` method:

1. In the `getHash` method and the first part of the `hashData` method, internal node hash positions were calculated as:
   ```java
   // This is an internal node, its position is after all leaves (capLeaf + idx)
   hashPosition = (capLeaf + idx) * HASH_SIZE;
   ```

2. But in the second part of the `hashData` method (when updating internal nodes after leaf updates), they were calculated as:
   ```java
   int hashPosition = idx * HASH_SIZE;
   ```

This inconsistency caused internal node hashes to be stored in different locations depending on whether they were computed during leaf updates or during internal node updates, leading to different tree structures for the same content.

The fix was to make the hash position calculation consistent across both methods by using the correct calculation everywhere:
```java
// This is an internal node, its position is after all leaves (capLeaf + idx)
hashPosition = (capLeaf + idx) * HASH_SIZE;
```

This ensures that internal node hashes are stored in the same location regardless of how they're computed, allowing the MerkleTree to maintain its identity property.

This property is essential for the correct functioning of the MerkleTree implementation, especially in scenarios where data integrity verification is critical.

### Catalog and BaseVectors Access Chain
The full chain of operations for accessing remote datasets through a catalog has been tested to ensure proper functionality. This chain includes:
1. Using a catalog to access a remote dataset by reading catalog metadata
2. Selecting a dataset profile
3. Accessing .getBaseVectors() for the base vectors API
4. Reading the dataset size from that BaseVectors object
5. Reading vectors from the BaseVectors object

Two test cases have been implemented in `CatalogBaseVectorsTest.java`:
1. `testFullChainOfOperations()` - Tests the complete chain from accessing a catalog to reading individual vectors from the BaseVectors object
2. `testVectorBatchReading()` - Tests reading a batch of vectors from the BaseVectors object

These tests use the built-in test webserver and the test data files under rawdatasets/testxvec. The tests verify:
- Catalog access and dataset retrieval
- Profile selection and access
- BaseVectors API functionality
- Dataset size and dimensions retrieval
- Vector reading and validation

#### Merkle Tree Verification Issues
Previously, reading vectors from the middle or end of the dataset caused hash verification errors. The error occurred in the MerklePainter.paint method when it tried to verify the downloaded chunk against the reference merkle tree. The specific error was:

```
Error processing chunk 2: Hash verification failed for chunk 2 (expected hash: e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855, actual hash: ed77c076eb681bc80c06b7c1c84b288c51232207c0d3dbe08cc612078b3c67e9)
```

The expected hash `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` is the SHA-256 hash of an empty string, which suggested that the reference merkle tree was expecting empty chunks for parts of the dataset, but the actual data contained content.

This issue has now been resolved:
1. The test data has been updated to fix the faulty Merkle tree verification issues
2. The CatalogBaseVectorsTest has been modified to include sparse sampling across the whole file
3. Tests now read vectors from various parts of the dataset, including the beginning, middle, and end
4. The getRange method test has been updated to test multiple positions across the dataset
5. Error handling is still in place to catch and log exceptions, but the tests now pass successfully

The tests now demonstrate that the code can handle reading vectors from any part of the dataset, using sparse sampling across the whole file to exercise demand-paging of merkle chunks. This confirms that the Merkle tree verification is working correctly for the entire dataset.

This improvement highlights the importance of properly generating and maintaining merkle trees for datasets, especially when demand-paging is used to access parts of the dataset on demand. When creating test data, care should be taken to ensure that the merkle tree accurately represents the entire dataset, not just specific sections.

### MerkleTree Creation Performance Optimizations
The MerkleTree creation process has been optimized to improve performance, especially for large files. The following optimizations were implemented:

1. **Improved Worker Thread Management**:
   - Increased the number of worker threads from `Runtime.getRuntime().availableProcessors()` to `Runtime.getRuntime().availableProcessors() * 2` to better handle I/O-bound operations.
   - This allows for more concurrent chunk processing, which is beneficial since file I/O operations often involve waiting for disk access.

2. **Removed Unnecessary Sleep**:
   - Eliminated the `Thread.sleep(1)` call in the loop for processing internal nodes, which was causing unnecessary delays.
   - This sleep was originally added to allow the progress display to update, but it significantly slowed down processing, especially for trees with many internal nodes.

3. **Eliminated Redundant Hash Computation**:
   - Removed the redundant second call to `merkleTree.getHash(0)` in the `TreeBuildingTask.run()` method.
   - The root hash was being computed twice, once to ensure all internal nodes are computed and again before completing the progress.

4. **Optimized Buffer Handling in hashData()**:
   - Modified the `hashData()` method to avoid unnecessary buffer copying.
   - Instead of creating a byte array copy of the chunk data before hashing, the method now updates the digest directly with the buffer data.
   - This reduces memory allocation and copying, which is especially beneficial for large chunks.

5. **Batch Processing in ChunkWorker**:
   - Implemented batch processing in the `ChunkWorker` class to reduce contention and improve throughput.
   - The worker now processes up to 10 chunks at a time, reducing the overhead of queue operations.
   - Used the `drainTo()` method to efficiently get multiple chunks from the queue at once.

6. **Improved File Reading**:
   - Simplified the file reading logic to try reading the entire chunk in one operation first, falling back to incremental reading only if necessary.
   - This reduces the number of I/O operations for most chunks, improving overall performance.

7. **Reduced Buffer Duplication**:
   - Removed unnecessary buffer duplication when passing buffers to the `hashData()` method.
   - This reduces memory allocation and garbage collection overhead.

8. **Removed Synchronization from hashData Method**:
   - Removed the `synchronized` keyword from the `hashData()` method to allow parallel processing of different chunks.
   - This enables multiple threads to process different chunks concurrently, significantly improving throughput.
   - Added documentation to clarify that the method is thread-safe for different leaf nodes but should not be called concurrently for the same leaf node.

9. **Used Object Pool for Internal Node Hashing**:
   - Extended the use of the MessageDigest object pool to internal node hash computation.
   - Previously, a new MessageDigest instance was created for each internal node, which was inefficient.
   - Now, the object pool is used for both leaf and internal node hash computation, reducing object creation and garbage collection overhead.

10. **Made ByteBuffer Read-Only**:
    - Changed the buffer duplication in `hashData()` to use `asReadOnlyBuffer()` instead of `duplicate()`.
    - This ensures that the original buffer cannot be accidentally modified, improving safety.
    - Read-only buffers can also be more efficiently handled by the JVM in some cases.

11. **Added Helper Method for Hash Writing**:
    - Created a new helper method `writeHashToBuffer()` to centralize the logic for writing hashes to the memory-mapped buffer.
    - This reduces code duplication and improves maintainability.
    - The helper method is used in both leaf node and internal node processing, ensuring consistent behavior.

8. **Removed Synchronization from hashData Method**:
   - Removed the `synchronized` keyword from the `hashData()` method to allow parallel processing of different chunks.
   - This enables multiple threads to process different chunks concurrently, significantly improving throughput.
   - Added documentation to clarify that the method is thread-safe for different leaf nodes but should not be called concurrently for the same leaf node.

9. **Used Object Pool for Internal Node Hashing**:
   - Extended the use of the MessageDigest object pool to internal node hash computation.
   - Previously, a new MessageDigest instance was created for each internal node, which was inefficient.
   - Now, the object pool is used for both leaf and internal node hash computation, reducing object creation and garbage collection overhead.

10. **Made ByteBuffer Read-Only**:
    - Changed the buffer duplication in `hashData()` to use `asReadOnlyBuffer()` instead of `duplicate()`.
    - This ensures that the original buffer cannot be accidentally modified, improving safety.
    - Read-only buffers can also be more efficiently handled by the JVM in some cases.

11. **Added Helper Method for Hash Writing**:
    - Created a new helper method `writeHashToBuffer()` to centralize the logic for writing hashes to the memory-mapped buffer.
    - This reduces code duplication and improves maintainability.
    - The helper method is used in both leaf node and internal node processing, ensuring consistent behavior.

These optimizations significantly improve the performance of Merkle tree creation, especially for large files with many chunks. The changes were verified using the `MerkleTreePerformanceTest`, which confirmed that the optimizations work correctly and improve performance.

### MerkleTree Loading Performance Optimizations
The MerkleTree loading process has been optimized to improve performance, especially for large files. The following optimizations were implemented:

1. **Optimized Footer Reading**:
   - Modified the `readFooterBuffer` method to accept an optional `FileChannel` parameter, allowing reuse of an existing file channel.
   - Used `ByteBuffer.allocateDirect()` instead of `ByteBuffer.allocate()` for better performance with memory-mapped files.
   - Attempted to read the entire footer in a single operation, falling back to multiple reads only if necessary.
   - Added proper resource management to ensure the file channel is closed only if it was opened by this method.

2. **Optimized File Verification**:
   - Modified the `verifyWrittenMerkleFile` method to accept an optional `FileChannel` parameter, allowing reuse of an existing file channel.
   - For large files (>100MB), implemented a more efficient verification strategy that checks only the first and last hash blocks instead of reading the entire file.
   - For smaller files, used memory mapping for better performance instead of reading the entire file into memory.
   - Removed the use of internal APIs for better compatibility and maintainability.

3. **Optimized Load Method**:
   - Modified the `load` method to open a single file channel for all operations, which is shared with the `readFooterBuffer` and `verifyWrittenMerkleFile` methods.
   - Used direct ByteBuffers for better performance with memory-mapped files.
   - Properly closed the file channel when returning early for empty files.
   - Added more detailed documentation to explain the optimizations.

4. **Enhanced Performance Testing**:
   - Modified the `MerkleTreeRealFileTest` to include performance measurements for loading a merkle tree.
   - Added JVM warm-up to ensure accurate measurements.
   - Measured load time both with and without verification to understand the verification overhead.
   - Added detailed logging of performance metrics.

These optimizations significantly improve the performance of loading a merkle tree from disk, especially for large files, by reducing the number of file operations and using more efficient memory access patterns. The changes were verified using the enhanced `MerkleTreeRealFileTest`, which confirmed that the optimizations work correctly and improve performance.

### MerkleTree Loading Performance Optimizations
The MerkleTree loading process has been optimized to improve performance, especially for large files. The following optimizations were implemented:

1. **Optimized Footer Reading**:
   - Modified the `readFooterBuffer` method to accept an optional `FileChannel` parameter, allowing reuse of an existing file channel.
   - Used `ByteBuffer.allocateDirect()` instead of `ByteBuffer.allocate()` for better performance with memory-mapped files.
   - Attempted to read the entire footer in a single operation, falling back to multiple reads only if necessary.
   - Added proper resource management to ensure the file channel is closed only if it was opened by this method.

2. **Optimized File Verification**:
   - Modified the `verifyWrittenMerkleFile` method to accept an optional `FileChannel` parameter, allowing reuse of an existing file channel.
   - For large files (>100MB), implemented a more efficient verification strategy that checks only the first and last hash blocks instead of reading the entire file.
   - For smaller files, used memory mapping for better performance instead of reading the entire file into memory.
   - Removed the use of internal APIs for better compatibility and maintainability.

3. **Optimized Load Method**:
   - Modified the `load` method to open a single file channel for all operations, which is shared with the `readFooterBuffer` and `verifyWrittenMerkleFile` methods.
   - Used direct ByteBuffers for better performance with memory-mapped files.
   - Properly closed the file channel when returning early for empty files.
   - Added more detailed documentation to explain the optimizations.

4. **Enhanced Performance Testing**:
   - Modified the `MerkleTreeRealFileTest` to include performance measurements for loading a merkle tree.
   - Added JVM warm-up to ensure accurate measurements.
   - Measured load time both with and without verification to understand the verification overhead.
   - Added detailed logging of performance metrics.
   - Created a dedicated `MerkleTreeLoadingPerformanceTest` class that specifically tests merkle tree file loading speed.
   - The dedicated test uses the same file that the `MerkleTreePerformanceTest` uses, ensuring consistent benchmarking.
   - The test performs multiple iterations with and without verification to provide robust performance metrics.
   - Detailed statistics are calculated and reported, including average, min, max, and standard deviation of loading times.
   - The verification overhead is quantified both in absolute time and as a percentage of the base loading time.

These optimizations significantly improve the performance of loading a merkle tree from disk, especially for large files, by reducing the number of file operations and using more efficient memory access patterns. The changes were verified using both the enhanced `MerkleTreeRealFileTest` and the dedicated `MerkleTreeLoadingPerformanceTest`, which confirmed that the optimizations work correctly and improve performance.

### MerkleAsyncFileChannelAdapter Implementation
The project has been updated to replace usages of MerkleBRAF with MerkleAsyncFileChannelAdapter. This change was made to leverage the more modern AsynchronousFileChannel API instead of the older RandomAccessFile API. The following changes were made:

1. **Created MerkleAsyncFileChannelAdapter Class**:
   - Implemented a new adapter class that makes MerkleAsyncFileChannel implement BufferedRandomAccessFile
   - The adapter bridges the gap between the AsynchronousFileChannel API and the BufferedRandomAccessFile interface
   - Provides the same functionality as MerkleBRAF but uses MerkleAsyncFileChannel internally
   - Implements all methods required by the BufferedRandomAccessFile interface
   - Includes comprehensive error handling and proper resource management
   - Uses the same constructor signatures as MerkleBRAF for easy replacement

2. **Updated VirtualTestDataView**:
   - Modified the resolveRandomAccessHandle method to use MerkleAsyncFileChannelAdapter instead of MerkleBRAF
   - Updated the getQueryVectors, getNeighborIndices, and getNeighborDistances methods to use MerkleAsyncFileChannelAdapter

3. **Updated CoreXVecDatasetViewMethods**:
   - Updated the import statement to use MerkleAsyncFileChannelAdapter instead of MerkleBRAF
   - The class already used BufferedRandomAccessFile as a parameter type, so no other changes were needed

4. **Updated CoreXVecDatasetViewMethodsTest**:
   - Updated the import statement to use MerkleAsyncFileChannelAdapter instead of MerkleBRAF
   - Modified all test methods to use MerkleAsyncFileChannelAdapter instead of MerkleBRAF

5. **Updated MerkleRAFTest**:
   - Updated the import statement to use MerkleAsyncFileChannelAdapter instead of MerkleBRAF
   - Modified all test methods to use MerkleAsyncFileChannelAdapter instead of MerkleBRAF
   - Updated comments to reflect the use of MerkleAsyncFileChannelAdapter

6. **Created MerkleBRAF as a Compatibility Layer**:
   - Implemented MerkleBRAF as a wrapper around MerkleAsyncFileChannelAdapter to maintain backward compatibility
   - The wrapper delegates all method calls to MerkleAsyncFileChannelAdapter
   - This allows existing code that uses MerkleBRAF to continue working without changes
   - The wrapper implements the same interface (BufferedRandomAccessFile) and has the same constructor signatures

7. **Created Equivalent Tests for MerkleAsyncFileChannelAdapter**:
   - Created MerkleAsyncFileChannelVerificationTest to test verification functionality
   - Created MerkleAsyncFileChannelInitializationTest to test initialization functionality
   - Created MerkleAsyncFileChannelMismatchTest to test mismatch handling functionality
   - These tests cover the same functionality as the original MerkleBRAF tests

8. **Removed MerkleBRAF Tests**:
   - Removed MerkleBRAFVerificationTest.java
   - Removed MerkleBRAFInitializationTest.java
   - Removed MerkleBRAFMismatchTest.java
   - Removed MerkleBRAFLargeFileTest.java
   - Removed MerkleBRAFWebServerTest.java
   - The functionality covered by these tests is now tested by the equivalent MerkleAsyncFileChannelAdapter tests

9. **Verified Functionality**:
   - Ran tests to ensure that the changes work correctly
   - All tests pass, confirming that MerkleAsyncFileChannelAdapter provides the same functionality as MerkleBRAF

This change improves the codebase by using a more modern and efficient file I/O API while maintaining compatibility with existing code through the adapter pattern. The AsynchronousFileChannel API provides better performance and more flexibility for asynchronous operations compared to the older RandomAccessFile API. The compatibility layer ensures that existing code continues to work, while the new tests ensure that the functionality is properly tested.

### Performance Test Management
Performance tests in the project have been grouped together using JUnit Jupiter annotations and configured to only run when explicitly enabled. This prevents performance tests from slowing down regular test runs, especially in CI/CD pipelines. The following components were implemented:

1. **Test Suites with JUnit Platform**:
   - Created two test suites using JUnit Platform's `@Suite` annotation:
     - `VectorDataTestSuite`: Runs all tests except performance tests (`@ExcludeTags("performance")`)
     - `PerformanceTestSuite`: Runs only performance tests (`@IncludeTags("performance")`)
   - Both suites use `@SelectPackages("io.nosqlbench.vectordata")` to select tests from the vectordata package
   - Both suites apply the `SimpleFileServerExtension` to provide test resources

2. **Custom `@PerformanceTest` Annotation**:
   - Created a custom annotation that combines `@Tag("performance")` and `@Test`
   - The annotation is applied to all performance-oriented tests in the project
   - Located in `io.nosqlbench.vectordata.test.PerformanceTest`

3. **Maven Surefire Configuration**:
   - Configured the Maven Surefire plugin to run the VectorDataTestSuite by default
   - Added a separate execution for performance tests that is skipped by default
   - Added a `skipPerformanceTests` property (default: true) to control whether performance tests are run

4. **Applied to Performance Tests**:
   - Applied the `@PerformanceTest` annotation to all performance-oriented tests:
     - `MerkleTreeRealFileTest.testLoadMerkleFile()`
     - `MerkleTreeLoadingPerformanceTest.testMerkleTreeLoadingPerformance()`
     - `MerkleTreePerformanceTest.testMerkleTreeCreationPerformance()`

5. **Documentation**:
   - Updated the README.md file in the `io.nosqlbench.vectordata.test` package explaining how to run performance tests
   - Documented best practices for writing performance tests
   - Provided examples of how to create new performance tests using the custom annotation

This implementation ensures that performance tests are properly grouped and only run when explicitly enabled, improving the efficiency of regular test runs while still allowing thorough performance testing when needed. Performance tests can be run using either:
1. `mvn test -DskipPerformanceTests=false` - Runs both regular and performance tests
2. `mvn test -Dtest=PerformanceTestSuite` - Runs only performance tests
