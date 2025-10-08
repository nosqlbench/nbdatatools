/// Concurrent utility classes for progress tracking and asynchronous operations.
///
/// This package provides interfaces and implementations for tracking progress of long-running operations,
/// including {@link io.nosqlbench.nbdatatools.api.concurrent.ProgressIndicator} for monitoring asynchronous tasks
/// and {@link io.nosqlbench.nbdatatools.api.concurrent.ProgressIndicatingFuture} for CompletableFutures with
/// built-in progress tracking capabilities.
///
/// ## Key Components
///
/// - {@link io.nosqlbench.nbdatatools.api.concurrent.ProgressIndicator}: Interface for tracking work progress
/// - {@link io.nosqlbench.nbdatatools.api.concurrent.ProgressIndicatingFuture}: CompletableFuture with progress tracking
///
/// ## Usage Example
///
/// ```java
/// ProgressIndicatingFuture<Data> future = loadDataAsync();
/// future.monitorProgress(500); // Monitor every 500ms
/// Data result = future.get();
/// ```
package io.nosqlbench.nbdatatools.api.concurrent;

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
