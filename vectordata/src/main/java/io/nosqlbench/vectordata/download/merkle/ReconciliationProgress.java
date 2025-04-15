package io.nosqlbench.vectordata.download.merkle;

/// Represents the progress of a file reconciliation or download operation.
///
/// This record contains information about the current progress of a reconciliation
/// operation, including bytes processed, completion percentage, and section counts.
///
public record ReconciliationProgress(
    /// Number of bytes processed so far
    long bytesProcessed,
    /// Total number of bytes to process
    long totalBytes,
    /// Number of sections that have been completed
    int completedSections,
    /// Total number of sections to process
    int totalSections,
    /// Progress as a percentage (0-100)
    double progressPercent
) {}