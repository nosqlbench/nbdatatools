/// Picocli-based CLI commands for slabtastic file maintenance.
///
/// The entry point is {@link io.nosqlbench.slabtastic.cli.CMD_slab}, which provides
/// subcommands for:
/// - {@link io.nosqlbench.slabtastic.cli.CMD_slab_analyze analyze} — file stats, sampling statistics, and content detection
/// - {@link io.nosqlbench.slabtastic.cli.CMD_slab_check check} — structural validation
/// - {@link io.nosqlbench.slabtastic.cli.CMD_slab_get get} — record extraction by ordinal
/// - {@link io.nosqlbench.slabtastic.cli.CMD_slab_rewrite rewrite} — clean rewrite with fresh alignment and monotonic ordering
/// - {@link io.nosqlbench.slabtastic.cli.CMD_slab_append append} — append records from another slab file
/// - {@link io.nosqlbench.slabtastic.cli.CMD_slab_import import} — import records from non-slab files
/// - {@link io.nosqlbench.slabtastic.cli.CMD_slab_export export} — export records to files or stdout
/// - {@link io.nosqlbench.slabtastic.cli.CMD_slab_explain explain} — illustrate slab page layout with block diagrams
/// - {@link io.nosqlbench.slabtastic.cli.CMD_slab_namespaces namespaces} — list all namespaces in a file
///
/// All data-oriented commands accept `--namespace` / `-n` to operate on a specific
/// namespace instead of the default.
///
/// Supporting types:
/// - {@link io.nosqlbench.slabtastic.cli.SlabFileValidator} — reusable structural validation
/// - {@link io.nosqlbench.slabtastic.cli.OrdinalRange} — range record and picocli converter
package io.nosqlbench.slabtastic.cli;

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
