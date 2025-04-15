package io.nosqlbench.vectordata.download.merkle;

/*
 * Copyright (c) nosqlbench
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


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
