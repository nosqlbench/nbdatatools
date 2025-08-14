package io.nosqlbench.nbvectors.datasource.parquet.traversal;

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

/// Record type to store the result of a pre-scanning phase for Parquet files
/// Contains information about the total number of records in the file tree
/// @param totalRecords the total number of records in the file tree
public record ParquetScanResult(long totalRecords) {
    
    /// Create a new ParquetScanResult with the given total record count
    /// @param totalRecords the total number of records in the file tree
    public ParquetScanResult {
        if (totalRecords < 0) {
            throw new IllegalArgumentException("Total records cannot be negative: " + totalRecords);
        }
    }
    
    /// Get the total number of records as an int, checking for overflow
    /// @return the total number of records as an int
    /// @throws RuntimeException if the total records exceeds Integer.MAX_VALUE
    public int getTotalRecordsAsInt() {
        if (totalRecords > Integer.MAX_VALUE) {
            throw new RuntimeException("int overflow on long size: " + totalRecords);
        }
        return (int) totalRecords;
    }
}