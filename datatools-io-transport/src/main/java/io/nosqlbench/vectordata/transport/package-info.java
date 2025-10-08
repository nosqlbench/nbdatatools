/// Transport layer implementations for data fetching from various sources.
///
/// This package provides concrete implementations of transport providers for fetching data
/// from different sources including HTTP/HTTPS and file systems. These implementations support
/// byte-range requests and streaming for efficient data access.
///
/// ## Key Components
///
/// ### HTTP Transport
/// - {@link io.nosqlbench.vectordata.transport.HttpTransportProvider}: HTTP/HTTPS transport provider
/// - {@link io.nosqlbench.vectordata.transport.HttpByteRangeFetcher}: HTTP byte-range fetcher
/// - {@link io.nosqlbench.vectordata.transport.HttpStreamingFetchResult}: HTTP streaming result
///
/// ### File Transport
/// - {@link io.nosqlbench.vectordata.transport.FileTransportProvider}: File system transport provider
/// - {@link io.nosqlbench.vectordata.transport.FileByteRangeFetcher}: File byte-range fetcher
/// - {@link io.nosqlbench.vectordata.transport.FileStreamingFetchResult}: File streaming result
///
/// ### Utilities
/// - {@link io.nosqlbench.vectordata.transport.LimitedReadableByteChannel}: Limited byte channel reader
///
/// ## Usage Example
///
/// ```java
/// // HTTP transport
/// HttpTransportProvider httpProvider = new HttpTransportProvider();
/// ChunkedTransportClient client = httpProvider.getClient(url);
/// FetchResult result = client.fetch(0, 1024);
///
/// // File transport
/// FileTransportProvider fileProvider = new FileTransportProvider();
/// ChunkedTransportClient client = fileProvider.getClient(filePath);
/// FetchResult result = client.fetch(0, 1024);
/// ```
///
/// ## Service Provider
///
/// These implementations are registered as service providers and can be discovered
/// via {@link java.util.ServiceLoader} mechanism for the
/// {@link io.nosqlbench.nbdatatools.api.transport.ChunkedTransportProvider} interface.
package io.nosqlbench.vectordata.transport;

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
