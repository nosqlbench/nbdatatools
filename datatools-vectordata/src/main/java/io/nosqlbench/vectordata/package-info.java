/// Java implementation of the vectordata access module.
///
/// This module implements the vectordata dynamic access layer as defined by the
/// reference implementation in
/// [vectordata-rs](https://github.com/nosqlbench/vectordata-rs).
/// The normative specification lives in the vectordata-rs design docs
/// (`docs/design/`), particularly:
///
/// - **05-dataset-specification.md** — dataset.yaml schema, facet keys, profile
///   resolution, window syntax, sized expansion, and xvec/slab file formats.
/// - **09-vectordata-access-layer.md** — layered architecture (merkle core,
///   chunked transport, cache-backed channel, view layer), wire-format
///   compatibility, and facet manifest API.
/// - **13-data-access-layer.md** — location-transparent access, lazy on-demand
///   fetching, merkle-verified integrity, incremental resume, and dual-path
///   (channel vs mmap) optimization with automatic promotion.
///
/// ## Conformance
///
/// Test scenarios in this module mirror the integration tests from the Rust
/// `vectordata` crate (`vectordata/tests/`). Any behavioral divergence from
/// the Rust reference should be treated as a bug in this implementation unless
/// explicitly documented as a Java-specific extension.
///
/// ## Key entry points
///
/// - {@link io.nosqlbench.vectordata.discovery.TestDataGroup} — load a dataset
///   from a local directory or URL and select a profile.
/// - {@link io.nosqlbench.vectordata.discovery.vector.TestDataView} — access
///   typed facets (base vectors, query vectors, neighbor indices, etc.) and
///   discover available facets via {@code getFacetManifest()}.
/// - {@link io.nosqlbench.vectordata.merklev2.MAFileChannel} — merkle-verified
///   cache-backed file channel for transparent remote data access.
package io.nosqlbench.vectordata;

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

