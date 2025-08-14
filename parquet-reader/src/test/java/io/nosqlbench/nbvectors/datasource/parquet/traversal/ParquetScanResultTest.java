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

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/// Test for the ParquetScanResult class
public class ParquetScanResultTest {

    @Test
    public void testParquetScanResultCreation() {
        // Test valid creation
        ParquetScanResult result = new ParquetScanResult(100);
        assertEquals(100, result.totalRecords());
        assertEquals(100, result.getTotalRecordsAsInt());
        
        // Test with zero records
        ParquetScanResult zeroResult = new ParquetScanResult(0);
        assertEquals(0, zeroResult.totalRecords());
        assertEquals(0, zeroResult.getTotalRecordsAsInt());
    }
    
    @Test
    public void testParquetScanResultNegativeRecords() {
        // Test with negative records, should throw IllegalArgumentException
        assertThrows(IllegalArgumentException.class, () -> new ParquetScanResult(-1));
    }
    
    @Test
    public void testParquetScanResultIntOverflow() {
        // Test with a value larger than Integer.MAX_VALUE
        ParquetScanResult largeResult = new ParquetScanResult(Integer.MAX_VALUE + 1L);
        assertEquals(Integer.MAX_VALUE + 1L, largeResult.totalRecords());
        
        // Getting as int should throw RuntimeException due to overflow
        assertThrows(RuntimeException.class, largeResult::getTotalRecordsAsInt);
    }
}