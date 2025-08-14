# ChunkScheduler Architecture Migration Guide

This guide helps developers migrate existing code to the new node-centric ChunkScheduler architecture introduced to improve testability, traceability, and concurrent stream handling.

## Overview of Changes

The ChunkScheduler architecture has been redesigned with the following key improvements:

1. **Node-Centric Scheduling**: Transfer planning now uses merkle node indices as the primary identifier
2. **Scheduling Decisions**: All scheduling choices are now traceable through `SchedulingDecision` records
3. **Precise Future Blocking**: Callers only block on futures required for their specific reads
4. **Enhanced Testability**: New testing framework with scenario-based evaluation
5. **Chunk-Based Filtering**: Improved filtering logic prevents unnecessary blocking

## API Changes

### ChunkScheduler Interface

#### New Methods Added
```java
// New primary scheduling method with full traceability
default List<SchedulingDecision> analyzeSchedulingDecisions(
    long offset, int length, MerkleShape shape, MerkleState state)

// New node selection method for advanced use cases
default List<SchedulingDecision> selectOptimalNodes(
    List<Integer> requiredChunks, MerkleShape shape, MerkleState state)
```

#### Existing Methods
The existing `scheduleDownloads()` method remains unchanged for backward compatibility:
```java
void scheduleDownloads(long offset, int length, MerkleShape shape, 
                      MerkleState state, SchedulingTarget schedulingTarget);
```

### New Records and Enums

#### SchedulingDecision Record
```java
public record SchedulingDecision(
    int nodeIndex,              // Merkle node to download
    SchedulingReason reason,    // Why this node was selected
    int priority,               // Download priority
    long estimatedBytes,        // Expected download size
    List<Integer> requiredChunks,   // Chunks actually needed
    List<Integer> coveredChunks,    // Chunks covered by this node
    String explanation          // Human-readable explanation
) {
    // Provides efficiency calculation and debug methods
    public double getEfficiency() { /* ... */ }
    public String getDebugInfo() { /* ... */ }
}
```

#### SchedulingReason Enum
```java
public enum SchedulingReason {
    EXACT_MATCH,           // Node exactly matches required chunks
    EFFICIENT_COVERAGE,    // Node efficiently covers multiple chunks
    PREFETCH,             // Speculative download for performance
    MINIMAL_DOWNLOAD,     // Conservative minimal bandwidth usage
    FALLBACK,             // Default when no better option exists
    CONSOLIDATION,        // Combining multiple small requests
    CACHE_OPTIMIZATION,   // Leveraging existing cached data
    BANDWIDTH_OPTIMIZATION, // Maximizing network utilization
    LATENCY_OPTIMIZATION,  // Minimizing round trips
    SPECULATIVE_PREFETCH   // Advanced predictive downloading
}
```

## Migration Steps

### 1. Updating Custom Schedulers

#### Before (Old API)
```java
public class CustomScheduler implements ChunkScheduler {
    @Override
    public void scheduleDownloads(long offset, int length, MerkleShape shape, 
                                 MerkleState state, SchedulingTarget target) {
        // Direct implementation of scheduling logic
        // Limited visibility into decisions made
    }
}
```

#### After (New API)
```java
public class CustomScheduler implements ChunkScheduler {
    @Override
    public void scheduleDownloads(long offset, int length, MerkleShape shape, 
                                 MerkleState state, SchedulingTarget target) {
        // Delegate to new method for consistency
        List<SchedulingDecision> decisions = analyzeSchedulingDecisions(
            offset, length, shape, state);
        executeSchedulingDecisions(decisions, target);
    }
    
    @Override
    public List<SchedulingDecision> analyzeSchedulingDecisions(
            long offset, int length, MerkleShape shape, MerkleState state) {
        // Implement your scheduling logic here
        List<SchedulingDecision> decisions = new ArrayList<>();
        
        // Calculate required chunks
        int startChunk = shape.getChunkIndexForPosition(offset);
        int endChunk = shape.getChunkIndexForPosition(Math.min(
            offset + length - 1, shape.getTotalContentSize() - 1));
        
        List<Integer> requiredChunks = new ArrayList<>();
        for (int chunk = startChunk; chunk <= endChunk; chunk++) {
            requiredChunks.add(chunk);
        }
        
        // Use selectOptimalNodes for the actual selection logic
        return selectOptimalNodes(requiredChunks, shape, state);
    }
    
    @Override
    public List<SchedulingDecision> selectOptimalNodes(
            List<Integer> requiredChunks, MerkleShape shape, MerkleState state) {
        // Your custom node selection logic here
        List<SchedulingDecision> decisions = new ArrayList<>();
        int priority = 0;
        
        for (Integer chunkIndex : requiredChunks) {
            if (state.isValid(chunkIndex)) {
                continue; // Skip valid chunks
            }
            
            int leafNodeIndex = shape.chunkIndexToLeafNode(chunkIndex);
            MerkleShape.MerkleNodeRange byteRange = shape.getByteRangeForNode(leafNodeIndex);
            
            SchedulingDecision decision = new SchedulingDecision(
                leafNodeIndex,
                SchedulingReason.EXACT_MATCH,
                priority++,
                byteRange.getLength(),
                List.of(chunkIndex),
                List.of(chunkIndex),
                "Custom scheduler: exact chunk download"
            );
            
            decisions.add(decision);
        }
        
        return decisions;
    }
}
```

### 2. Testing with New Framework

#### Before (Manual Testing)
```java
@Test
public void testScheduler() {
    // Manual setup and verification
    ChunkScheduler scheduler = new CustomScheduler();
    // Limited ability to verify scheduling decisions
}
```

#### After (Structured Testing)
```java
@Test
public void testSchedulerWithFramework() {
    SchedulingTestFramework framework = new SchedulingTestFramework();
    
    TestScenario scenario = framework.createScenario()
        .withFileSize(1_000_000)
        .withChunkSize(64 * 1024)
        .withValidChunks(Set.of(0, 2, 4))
        .withReadRequest(100_000, 50_000);
    
    SchedulingTestResult result = framework.testScheduler(
        new CustomScheduler(), scenario);
    
    // Rich assertions available
    result.assertDecisionCount(3)
          .assertAllChunksCovered()
          .assertReasonUsed(SchedulingReason.EXACT_MATCH)
          .assertMinimumEfficiency(0.8);
}
```

### 3. MAFileChannel Integration

#### Before (Internal Implementation)
```java
// Internal scheduling was opaque
OptimizedChunkQueue.SchedulingResult result = chunkQueue.executeScheduling(/*...*/);
```

#### After (Traceable Decisions)
```java
// Scheduling decisions are now visible and traceable
List<SchedulingDecision> decisions = chunkScheduler.analyzeSchedulingDecisions(
    position, requestedLength, merkleShape, merkleState);

// Decisions can be logged, analyzed, or modified before execution
for (SchedulingDecision decision : decisions) {
    logger.debug("Scheduling decision: {}", decision.getDebugInfo());
}

// Execute decisions with full traceability
OptimizedChunkQueue.SchedulingResult result = chunkQueue.executeSchedulingWithTasks(/*...*/);
```

## Backward Compatibility

### Guaranteed Compatibility
- All existing `ChunkScheduler` implementations continue to work unchanged
- The `scheduleDownloads()` method is still the primary interface
- MAFileChannel automatically handles both old and new scheduler styles

### Default Implementations
- All new methods have sensible default implementations
- Default behavior delegates to existing `scheduleDownloads()` method
- No breaking changes to existing code

### Gradual Migration Path
1. **Phase 1**: Use existing schedulers with new MAFileChannel (no changes needed)
2. **Phase 2**: Add `analyzeSchedulingDecisions()` implementation for traceability
3. **Phase 3**: Implement `selectOptimalNodes()` for advanced features
4. **Phase 4**: Adopt new testing framework for comprehensive validation

## Performance Considerations

### Improved Performance
- **Precise Future Blocking**: Reduces unnecessary waiting time
- **Chunk-Based Filtering**: More efficient filtering logic
- **Better Concurrency**: Reduced conflicts between concurrent streams

### Migration Impact
- **Zero Performance Impact**: Existing code runs at same speed
- **Gradual Improvements**: Each migration step provides incremental benefits
- **Testing Overhead**: New testing framework adds comprehensive validation

## Common Pitfalls and Solutions

### 1. Chunk Index Bounds
**Problem**: Internal nodes may reference chunks beyond the valid range.
```java
// Problematic: assuming all leaf indices are valid chunks
int chunkIndex = (int) leafIndex; // May be out of bounds
```

**Solution**: Always validate chunk indices.
```java
// Safe: validate before using
int chunkIndex = (int) leafIndex;
if (chunkIndex < shape.getTotalChunks() && !state.isValid(chunkIndex)) {
    // Process valid chunk
}
```

### 2. Node Index Conversion
**Problem**: Confusion between node indices and chunk indices.
```java
// Incorrect: treating node index as chunk index
int chunkIndex = nodeIndex;
```

**Solution**: Use proper conversion methods.
```java
// Correct: convert using MerkleShape
int leafNodeIndex = shape.chunkIndexToLeafNode(chunkIndex);
MerkleShape.MerkleNodeRange leafRange = shape.getLeafRangeForNode(nodeIndex);
```

### 3. Decision Traceability
**Problem**: Losing visibility into scheduling decisions.
```java
// Limited visibility
scheduler.scheduleDownloads(offset, length, shape, state, target);
```

**Solution**: Use traceable decision API.
```java
// Full traceability
List<SchedulingDecision> decisions = scheduler.analyzeSchedulingDecisions(
    offset, length, shape, state);
    
// Log or analyze decisions before execution
for (SchedulingDecision decision : decisions) {
    if (decision.getEfficiency() < 0.5) {
        logger.warn("Low efficiency decision: {}", decision.getDebugInfo());
    }
}
```

## Examples

See the following files for complete implementation examples:
- `AggressiveChunkScheduler.java` - Aggressive prefetching strategy
- `ConservativeChunkScheduler.java` - Minimal bandwidth usage
- `DefaultChunkScheduler.java` - Balanced approach
- `AdaptiveChunkScheduler.java` - Dynamic strategy selection
- `SimpleChunkSelectionTest.java` - Testing framework usage

## Support

For questions or issues during migration:
1. Check the existing scheduler implementations for patterns
2. Use the `SchedulingTestFramework` to validate behavior
3. Review `SchedulingDecision` debug output for troubleshooting
4. Consult the comprehensive test suite for usage examples