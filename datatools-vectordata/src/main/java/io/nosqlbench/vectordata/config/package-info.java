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

/// Configuration management for vectordata cache directory.
///
/// This package provides classes for managing vectordata settings, including:
/// - Loading and saving settings from ~/.config/vectordata/settings.yaml
/// - Resolving auto:* directives for cache directory selection
/// - Mount point detection for optimal storage selection
///
/// The main entry point is [VectorDataSettings], which handles all configuration
/// loading, validation, and persistence.
package io.nosqlbench.vectordata.config;
