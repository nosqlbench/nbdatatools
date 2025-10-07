/**
 * Transport layer APIs for data access with support for partial data callbacks.
 * 
 * <p>This package provides interfaces for implementing transport clients that can
 * fetch data from various sources (HTTP, file system, etc.) with support for
 * incremental data processing through the {@link PartialData} callback interface.
 * 
 * <h2>Key Components</h2>
 * 
 * <ul>
 *   <li>{@link ChunkedTransportClient} - Core interface for range-based data access</li>
 *   <li>{@link PartialData} - Callback interface for handling partial data with client-side tracking</li>
 *   <li>{@link ChunkedTransportProvider} - Service provider interface for transport implementations</li>
 * </ul>
 * 
 * <h2>Usage Example</h2>
 * 
 * <pre>{@code
 * // Create a callback with custom tracking object
 * PartialData<String> callback = (data, requestId) -> {
 *     System.out.println("Received " + data.remaining() + 
 *                        " bytes for request: " + requestId);
 *     // Process the data...
 * };
 * 
 * // Use with transport client (hypothetical method)
 * transportClient.fetchRangeWithCallback(url, 0, 1024, callback, "req-123");
 * }</pre>
 * 
 * @since 1.0
 */
package io.nosqlbench.nbdatatools.api.transport;

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

