package io.nosqlbench.command.convert;

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

/// Test to verify that ParquetVectorStreamer is available in the service loader
public class ParquetVectorStreamerTest {

//    /// Test that ParquetVectorStreamer is available in the service loader
//    @Test
//    public void testParquetVectorStreamerAvailable() {
//        // Load all BoundedVectorFileStream implementations using ServiceLoader
//        ServiceLoader<BoundedVectorFileStream> serviceLoader = ServiceLoader.load(BoundedVectorFileStream.class);
//
//        // Check if ParquetVectorStreamer is among the loaded implementations
//        boolean parquetVectorStreamerFound = StreamSupport.stream(serviceLoader.spliterator(), false)
//            .anyMatch(impl -> impl.getClass().equals(ParquetVectorStreamer.class));
//
//        // Assert that ParquetVectorStreamer was found
//        assertTrue(parquetVectorStreamerFound, "ParquetVectorStreamer should be available in the service loader");
//
//        // Print debug information
//        System.out.println("[DEBUG_LOG] Available BoundedVectorFileStream implementations:");
//        StreamSupport.stream(serviceLoader.spliterator(), false)
//            .forEach(impl -> System.out.println("[DEBUG_LOG] - " + impl.getClass().getName()));
//    }
}