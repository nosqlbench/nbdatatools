/// # REQUIREMENTS
///
/// ## Create Merkle File (CreateCommand)
/// * if -u is provided, then only files which already have an associated mrkl file will have
/// that mrkl file updated. The -u implies the -f option.
/// * if -f is provided, then mrkl files which would be overwritten are. Otherwise, unless -u is
/// provided, merkle tree files are not overwritten.
/// * if -m is provided then a merkle tree file is created for files under the matching
/// extensions but only if they don't already have one.
///
/// ## Merkle Summary (SummaryCommand)
///
/// * Should print out the basic details of a merkle tree file in a human readable format for a
/// specified content file, selecting the merkle tree file automatically by extension.
///   * Should include the number of chunks, the The content file size, the merkle tree file size.
///   * Should include all key details from the merkle footer.
/// * Should print out a braille-formatted char image representing the leaf node status.
/// * Should show the number of valid leaf nodes and the number of valid parent nodes and the
/// valid number of total nodes.
/// * Should show the number of total leaf nodes, total parent nodes and total all nodes.
package io.nosqlbench.command.merkle.subcommands;

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

