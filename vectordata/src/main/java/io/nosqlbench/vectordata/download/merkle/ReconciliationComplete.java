package io.nosqlbench.vectordata.download.merkle;

/// Represents the completion status of a file reconciliation or download operation.
///
/// This record contains information about the final state of a completed reconciliation
/// operation, including total bytes processed, elapsed time, and success status.
///
public record ReconciliationComplete(
    /// Total number of bytes processed during the operation
    long totalBytesProcessed,
    /// Time elapsed in milliseconds
    long timeElapsedMs,
    /// Whether the operation completed successfully
    boolean successful,
    /// Error that occurred during the operation, or null if successful
    Throwable error
) {}