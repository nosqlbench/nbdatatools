/// Dataset downloading and catalog management for vector test data.
///
/// This package provides functionality for downloading vector datasets from remote repositories,
/// managing download progress, and discovering available datasets through catalog files.
/// It supports chunked downloads with progress tracking and virtual dataset views.
///
/// ## Key Components
///
/// - {@link io.nosqlbench.vectordata.downloader.Catalog}: Catalog of available datasets
/// - {@link io.nosqlbench.vectordata.downloader.ResourceTransportService}: Download orchestration
/// - {@link io.nosqlbench.vectordata.downloader.ChunkedResourceTransportService}: Chunked download management
/// - {@link io.nosqlbench.vectordata.downloader.DownloadProgress}: Progress tracking
/// - {@link io.nosqlbench.vectordata.downloader.VirtualTestDataView}: Virtual access to remote datasets
/// - {@link io.nosqlbench.vectordata.downloader.DatasetEntry}: Dataset metadata
///
/// ## Usage Example
///
/// ```java
/// Catalog catalog = Catalog.forBuiltinCatalogs();
/// Optional<DatasetEntry> entry = catalog.findEntry("dataset-name");
/// ResourceTransportService service = new ChunkedResourceTransportService();
/// DownloadResult result = service.download(entry.get(), targetPath);
/// ```
package io.nosqlbench.vectordata.downloader;

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
