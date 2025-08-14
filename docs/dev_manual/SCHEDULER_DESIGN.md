# Stateless Scheduler Design

## Overview

The `ChunkScheduler` used by `MAFileChannel` has been redesigned to be stateless by design, enabling safe runtime scheduler swapping without affecting ongoing operations.

## Key Changes

### 1. Stateless Interface
- **Before**: Schedulers maintained internal state (`ConcurrentMap<Integer, CompletableFuture<Void>> inFlightDownloads`)
- **After**: All state management moved to `MAFileChannel`, schedulers are pure functions

### 2. New Signature
```java
// Before
List<CompletableFuture<Void>> scheduleDownloads(MerkleShape shape, MerkleState state, 
                                                long offset, int length,
                                                BlockingQueue<NodeDownloadTask> taskQueue);

// After  
void scheduleDownloads(MerkleShape shape, MerkleState state, 
                      long offset, int length,
                      ConcurrentMap<Integer, CompletableFuture<Void>> inFlightMap,
                      BlockingQueue<NodeDownloadTask> taskQueue);
```

### 3. State Management Location
- **MAFileChannel** now manages:
  - `inFlightDownloads` map for duplicate prevention
  - Task queue for execution
  - Future tracking and cleanup

- **ChunkScheduler** implementations now only:
  - Analyze the requested byte range
  - Determine optimal nodes to download
  - Create download tasks
  - Add tasks directly to provided queue (no return value)

### 4. Runtime Scheduler Swapping
New methods in `MAFileChannel`:
```java
public void setChunkScheduler(ChunkScheduler newScheduler)
public ChunkScheduler getChunkScheduler()
public int getInFlightDownloadCount()
```

## Benefits

1. **Swappable**: Schedulers can be changed at runtime without affecting ongoing downloads
2. **Stateless**: Scheduler instances are pure functions with no internal state
3. **Thread-safe**: Multiple scheduler instances can be used safely
4. **Testable**: Easier to test scheduler logic in isolation

## Usage Example

```java
import io.nosqlbench.vectordata.merklev2.schedulers.*;

MAFileChannel channel = new MAFileChannel(cachePath, statePath, remoteSource);

// Switch to aggressive scheduler for bulk operations
channel.setChunkScheduler(new AggressiveChunkScheduler());

// Later switch to conservative scheduler for sparse access
channel.setChunkScheduler(new ConservativeChunkScheduler());

// Monitor in-flight downloads
int active = channel.getInFlightDownloadCount();
```

## Implementation Details

### State Deduplication
The `MAFileChannel` uses `ConcurrentMap.computeIfAbsent()` to ensure only one download per node:

```java
CompletableFuture<Void> future = inFlightMap.computeIfAbsent(nodeIndex, 
    index -> new CompletableFuture<>());
```

### Task Execution
1. Scheduler adds `NodeDownloadTask` objects directly to the provided queue
2. `MAFileChannel` processes tasks from execution queue
3. `TaskExecutor` processes tasks concurrently
4. Futures complete when downloads finish

### Scheduler Implementations (in `io.nosqlbench.vectordata.merklev2.schedulers`)
- **DefaultChunkScheduler**: Balanced approach, prefers internal nodes when possible
- **AggressiveChunkScheduler**: Downloads larger regions, optimized for high bandwidth
- **ConservativeChunkScheduler**: Downloads minimal chunks, optimized for limited bandwidth
- **AdaptiveChunkScheduler**: Adapts behavior based on performance metrics (simplified for compatibility)

This design ensures that schedulers remain stateless and can be safely swapped at runtime while maintaining all existing functionality.