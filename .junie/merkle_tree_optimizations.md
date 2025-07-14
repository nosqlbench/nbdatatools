# MerkleTree Creation Performance Optimizations

The MerkleTree creation process has been optimized to improve performance, especially for large files. The following optimizations were implemented:

## 1. Improved Worker Thread Management
- Increased the number of worker threads from `Runtime.getRuntime().availableProcessors()` to `Runtime.getRuntime().availableProcessors() * 2` to better handle I/O-bound operations.
- This allows for more concurrent chunk processing, which is beneficial since file I/O operations often involve waiting for disk access.

## 2. Removed Unnecessary Sleep
- Eliminated the `Thread.sleep(1)` call in the loop for processing internal nodes, which was causing unnecessary delays.
- This sleep was originally added to allow the progress display to update, but it significantly slowed down processing, especially for trees with many internal nodes.

## 3. Eliminated Redundant Hash Computation
- Removed the redundant second call to `merkleTree.getHash(0)` in the `TreeBuildingTask.run()` method.
- The root hash was being computed twice, once to ensure all internal nodes are computed and again before completing the progress.

## 4. Optimized Buffer Handling in hashData()
- Modified the `hashData()` method to avoid unnecessary buffer copying.
- Instead of creating a byte array copy of the chunk data before hashing, the method now updates the digest directly with the buffer data.
- This reduces memory allocation and copying, which is especially beneficial for large chunks.

## 5. Batch Processing in ChunkWorker
- Implemented batch processing in the `ChunkWorker` class to reduce contention and improve throughput.
- The worker now processes up to 10 chunks at a time, reducing the overhead of queue operations.
- Used the `drainTo()` method to efficiently get multiple chunks from the queue at once.

## 6. Improved File Reading
- Simplified the file reading logic to try reading the entire chunk in one operation first, falling back to incremental reading only if necessary.
- This reduces the number of I/O operations for most chunks, improving overall performance.

## 7. Reduced Buffer Duplication
- Removed unnecessary buffer duplication when passing buffers to the `hashData()` method.
- This reduces memory allocation and garbage collection overhead.

These optimizations significantly improve the performance of Merkle tree creation, especially for large files with many chunks. The changes were verified using the `MerkleTreePerformanceTest`, which confirmed that the optimizations work correctly and improve performance.